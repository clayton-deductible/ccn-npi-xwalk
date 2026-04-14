# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 1 — Ingest CMS Provider of Services (POS) File
# MAGIC
# MAGIC **Source**: CMS.gov — Provider of Services File (Hospital & Non-Hospital Facilities)
# MAGIC https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities
# MAGIC
# MAGIC **What this does**:
# MAGIC 1. Downloads the CMS POS CSV directly from CMS.gov to DBFS
# MAGIC 2. Filters to hospitals only
# MAGIC 3. Writes a clean Delta table: `{catalog}.{schema}.cms_pos`
# MAGIC
# MAGIC **Run this notebook first**, then run `02_ingest_nppes.py`, then `03_match_ccn_to_npi.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Update the `CMS_POS_URL` widget with the latest quarterly file from CMS.gov.
# MAGIC Find the current URL at:
# MAGIC https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities

# COMMAND ----------

dbutils.widgets.text(
    "cms_pos_url",
    "https://data.cms.gov/sites/default/files/2026-01/c500f848-83b3-4f29-a677-562243a2f23b/Hospital_and_other.DATA.Q4_2025.csv",
    "CMS POS CSV URL"
)
dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.text("schema", "ccn_npi_xwalk", "Target Schema")

CMS_POS_URL = dbutils.widgets.get("cms_pos_url")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DBFS_PATH = "/tmp/ccn_npi_xwalk/cms_pos.csv"

print(f"CMS POS URL: {CMS_POS_URL}")
print(f"Target:      {CATALOG}.{SCHEMA}.cms_pos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download CMS POS File to DBFS

# COMMAND ----------

import subprocess
result = subprocess.run(
    ["wget", "-q", "-O", f"/dbfs{DBFS_PATH}", CMS_POS_URL],
    capture_output=True, text=True
)
if result.returncode != 0:
    raise RuntimeError(f"Download failed: {result.stderr}")

import os
size_mb = os.path.getsize(f"/dbfs{DBFS_PATH}") / 1024 / 1024
print(f"Downloaded: {size_mb:.1f} MB to {DBFS_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Filter to Hospitals

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, regexp_replace, when, lit
from pyspark.sql.types import IntegerType

pos_raw = spark.read.csv(f"dbfs:{DBFS_PATH}", header=True, inferSchema=False)
print(f"Raw columns: {len(pos_raw.columns)} | Raw rows: {pos_raw.count():,}")

# COMMAND ----------

# Facility type mappings (GNRL_FAC_TYPE_CD)
FACILITY_TYPE_MAP = {
    "01": "Short Term Acute Care",
    "02": "Long Term Care",
    "04": "Psychiatric",
    "05": "Rehabilitation",
    "06": "Childrens",
    "11": "Critical Access Hospital",
    "12": "Rural Emergency Hospital",
}

# Ownership type mappings (GNRL_CNTL_TYPE_CD)
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

# Filter to hospitals (PRVDR_CTGRY_CD = '01') and select relevant columns
hospitals = pos_raw.filter(
    (trim(col("PRVDR_CTGRY_CD")) == "01") |
    (trim(col("PRVDR_CTGRY_CD")) == "1")
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
)

# Normalize ZIP to 5 digits
hospitals = hospitals.withColumn(
    "zip_code",
    regexp_replace(col("zip_code_raw"), r"[^0-9]", "").substr(1, 5)
).drop("zip_code_raw")

# Decode facility type
facility_type_expr = col("facility_type_code")
for code, label in FACILITY_TYPE_MAP.items():
    facility_type_expr = when(col("facility_type_code") == code, lit(label)).otherwise(facility_type_expr)
hospitals = hospitals.withColumn("facility_type", facility_type_expr)

# Decode ownership type
ownership_expr = col("ownership_type_code")
for code, label in OWNERSHIP_TYPE_MAP.items():
    ownership_expr = when(col("ownership_type_code") == code, lit(label)).otherwise(ownership_expr)
hospitals = hospitals.withColumn("ownership_type", ownership_expr)

# Bed size band
hospitals = hospitals.withColumn(
    "bed_size_band",
    when(col("total_bed_count") < 100, lit("Small"))
    .when(col("total_bed_count") < 300, lit("Medium"))
    .when(col("total_bed_count") < 500, lit("Large"))
    .otherwise(lit("Major"))
)

# Drop rows with no bed count (non-hospital facilities that slipped through)
hospitals = hospitals.filter(col("total_bed_count").isNotNull())

print(f"Hospital records: {hospitals.count():,}")
hospitals.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta Table

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

hospitals.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.cms_pos")
print(f"Written: {CATALOG}.{SCHEMA}.cms_pos ({hospitals.count():,} rows)")
