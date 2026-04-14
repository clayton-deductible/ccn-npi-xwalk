# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — Ingest CMS POS + Hospital Enrollments
# MAGIC
# MAGIC Ingests two CMS source files and writes two Delta tables:
# MAGIC
# MAGIC | Source | Target Table | Key |
# MAGIC |--------|-------------|-----|
# MAGIC | CMS Provider of Services (POS) | `{catalog}.{schema}.cms_pos` | CCN |
# MAGIC | CMS Hospital Enrollments | `{catalog}.{schema}.hospital_enrollments` | CCN + NPI |
# MAGIC
# MAGIC **Run this notebook first**, then `02_ingest_nppes.py`, then `03_match_ccn_to_npi.py`.
# MAGIC
# MAGIC ## Finding current source URLs
# MAGIC
# MAGIC **CMS POS** (quarterly):
# MAGIC https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities
# MAGIC
# MAGIC **Hospital Enrollments** (updated periodically):
# MAGIC https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-enrollments

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text(
    "cms_pos_url",
    "https://data.cms.gov/sites/default/files/2026-01/c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv",
    "CMS POS CSV URL"
)
dbutils.widgets.text(
    "hospital_enrollments_url",
    "https://data.cms.gov/sites/default/files/2024-11/2bf5c555-46a1-4165-8ea7-4c38c2844cbc/Hospital_Enrollments_2024.11.01.csv",
    "Hospital Enrollments CSV URL"
)
dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.text("schema", "ccn_npi_xwalk", "Target Schema")

CMS_POS_URL = dbutils.widgets.get("cms_pos_url")
ENROLLMENTS_URL = dbutils.widgets.get("hospital_enrollments_url")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1 — CMS Provider of Services (POS)

# COMMAND ----------

import subprocess, os
from pyspark.sql.functions import col, trim, upper, regexp_replace, when, lit
from pyspark.sql.types import IntegerType

DBFS_POS = "/tmp/ccn_npi_xwalk/cms_pos.csv"
os.makedirs("/dbfs/tmp/ccn_npi_xwalk", exist_ok=True)

result = subprocess.run(["wget", "-q", "-O", f"/dbfs{DBFS_POS}", CMS_POS_URL], capture_output=True, text=True)
if result.returncode != 0:
    raise RuntimeError(f"POS download failed: {result.stderr}")
print(f"POS downloaded: {os.path.getsize(f'/dbfs{DBFS_POS}') / 1024 / 1024:.1f} MB")

# COMMAND ----------

FACILITY_TYPE_MAP = {
    "01": "Short Term Acute Care",
    "02": "Long Term Care",
    "04": "Psychiatric",
    "05": "Rehabilitation",
    "06": "Childrens",
    "11": "Critical Access Hospital",
    "12": "Rural Emergency Hospital",
}

OWNERSHIP_TYPE_MAP = {
    "01": "Church",
    "02": "Private Non-Profit",
    "03": "Other Non-Profit",
    "04": "Private For-Profit",
    "05": "Federal Government",
    "06": "State Government",
    "07": "Local Government",
    "08": "Hospital District",
    "09": "Physician Owned",
    "10": "Tribal",
}

pos_raw = spark.read.csv(f"dbfs:{DBFS_POS}", header=True, inferSchema=False)

hospitals = pos_raw.filter(
    (trim(col("PRVDR_CTGRY_CD")) == "01") | (trim(col("PRVDR_CTGRY_CD")) == "1")
).select(
    trim(col("PRVDR_NUM")).alias("ccn"),
    trim(col("FAC_NAME")).alias("facility_name"),
    trim(col("CITY_NAME")).alias("city"),
    trim(col("STATE_CD")).alias("state"),
    trim(col("ZIP_CD")).alias("zip_code_raw"),
    col("BED_CNT").cast(IntegerType()).alias("total_bed_count"),
    col("CRTFD_BED_CNT").cast(IntegerType()).alias("certified_bed_count"),
    trim(col("GNRL_FAC_TYPE_CD")).alias("facility_type_code"),
    trim(col("GNRL_CNTL_TYPE_CD")).alias("ownership_type_code"),
    col("PSYCH_UNIT_BED_CNT").cast(IntegerType()).alias("psych_unit_beds"),
    col("REHAB_UNIT_BED_CNT").cast(IntegerType()).alias("rehab_unit_beds"),
).withColumn(
    "zip_code", regexp_replace(col("zip_code_raw"), r"[^0-9]", "").substr(1, 5)
).drop("zip_code_raw")

facility_type_expr = col("facility_type_code")
for code, label in FACILITY_TYPE_MAP.items():
    facility_type_expr = when(col("facility_type_code") == code, lit(label)).otherwise(facility_type_expr)
hospitals = hospitals.withColumn("facility_type", facility_type_expr)

ownership_expr = col("ownership_type_code")
for code, label in OWNERSHIP_TYPE_MAP.items():
    ownership_expr = when(col("ownership_type_code") == code, lit(label)).otherwise(ownership_expr)
hospitals = hospitals.withColumn("ownership_type", ownership_expr)

hospitals = hospitals.withColumn(
    "bed_size_band",
    when(col("total_bed_count") < 100, lit("Small"))
    .when(col("total_bed_count") < 300, lit("Medium"))
    .when(col("total_bed_count") < 500, lit("Large"))
    .otherwise(lit("Major"))
).filter(col("total_bed_count").isNotNull())

hospitals.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.cms_pos")
print(f"Written: {CATALOG}.{SCHEMA}.cms_pos ({hospitals.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2 — CMS Hospital Enrollments
# MAGIC
# MAGIC This file is an authoritative CMS Medicare enrollment record containing
# MAGIC explicit CCN:NPI pairs for Medicare-participating hospitals. It covers
# MAGIC acute care hospitals, Critical Access Hospitals (CAH), and Rural Emergency
# MAGIC Hospitals (REH). Used as Strategy 0 in the matching algorithm — these
# MAGIC CCN:NPI pairs are taken as ground truth and do not require name matching.

# COMMAND ----------

DBFS_ENROLLMENTS = "/tmp/ccn_npi_xwalk/hospital_enrollments.csv"

result = subprocess.run(
    ["wget", "-q", "-O", f"/dbfs{DBFS_ENROLLMENTS}", ENROLLMENTS_URL],
    capture_output=True, text=True
)
if result.returncode != 0:
    raise RuntimeError(f"Hospital Enrollments download failed: {result.stderr}")
print(f"Enrollments downloaded: {os.path.getsize(f'/dbfs{DBFS_ENROLLMENTS}') / 1024 / 1024:.2f} MB")

# COMMAND ----------

from pyspark.sql.types import LongType

enrollments_raw = spark.read.csv(
    f"dbfs:{DBFS_ENROLLMENTS}",
    header=True,
    inferSchema=False,
    encoding="UTF-8",
)

# Select only the fields we need: CCN, NPI, provider type, organization name, state
enrollments = enrollments_raw.select(
    trim(col("CCN")).alias("ccn"),
    trim(col("NPI")).alias("npi_str"),
    trim(col("PROVIDER TYPE TEXT")).alias("provider_type"),
    trim(col("ORGANIZATION NAME")).alias("enrollment_org_name"),
    trim(col("STATE")).alias("enrollment_state"),
).filter(
    col("ccn").isNotNull() & (col("ccn") != "") &
    col("npi_str").isNotNull() & (col("npi_str") != "")
).withColumn(
    "npi", col("npi_str").cast(LongType())
).drop("npi_str")

enrollments.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.hospital_enrollments")
print(f"Written: {CATALOG}.{SCHEMA}.hospital_enrollments ({enrollments.count():,} rows)")
enrollments.groupBy("provider_type").count().show(truncate=False)
