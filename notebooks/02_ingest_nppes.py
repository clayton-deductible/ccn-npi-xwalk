# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 2 — Ingest NPPES Full Replacement File
# MAGIC
# MAGIC **Source**: CMS.gov — NPPES NPI Registry (Full Replacement Monthly File)
# MAGIC https://download.cms.gov/nppes/NPI_Files.html
# MAGIC
# MAGIC **What this does**:
# MAGIC 1. Downloads the NPPES Full Replacement ZIP from CMS.gov to DBFS (~8 GB unzipped)
# MAGIC 2. Filters to organization records (Entity Type Code = 2) with active NPIs
# MAGIC 3. Writes a clean Delta table: `{catalog}.{schema}.nppes_orgs`
# MAGIC
# MAGIC **Note on file size**: The NPPES full replacement file is large. Download and
# MAGIC processing takes 15–30 minutes depending on cluster size. A single-node cluster
# MAGIC with 32+ GB RAM is recommended (e.g., r5.2xlarge on AWS).
# MAGIC
# MAGIC **Find the current download URL**:
# MAGIC Visit https://download.cms.gov/nppes/NPI_Files.html and copy the link for
# MAGIC "NPPES Data Dissemination - Full Replacement Monthly File".
# MAGIC The URL changes each month.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text(
    "nppes_zip_url",
    "",
    "NPPES Full Replacement ZIP URL (from download.cms.gov/nppes/NPI_Files.html)"
)
dbutils.widgets.text("catalog", "main", "Target Catalog")
dbutils.widgets.text("schema", "ccn_npi_xwalk", "Target Schema")

NPPES_ZIP_URL = dbutils.widgets.get("nppes_zip_url")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

if not NPPES_ZIP_URL:
    raise ValueError(
        "nppes_zip_url widget is required. "
        "Find the current URL at https://download.cms.gov/nppes/NPI_Files.html"
    )

DBFS_ZIP = "/tmp/ccn_npi_xwalk/nppes.zip"
DBFS_EXTRACT_DIR = "/tmp/ccn_npi_xwalk/nppes_extracted"

print(f"NPPES URL: {NPPES_ZIP_URL}")
print(f"Target:    {CATALOG}.{SCHEMA}.nppes_orgs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download and Extract NPPES ZIP

# COMMAND ----------

import subprocess, os

# Download
print("Downloading NPPES ZIP (this may take several minutes)...")
result = subprocess.run(
    ["wget", "-q", "-O", f"/dbfs{DBFS_ZIP}", NPPES_ZIP_URL],
    capture_output=True, text=True
)
if result.returncode != 0:
    raise RuntimeError(f"Download failed: {result.stderr}")

size_mb = os.path.getsize(f"/dbfs{DBFS_ZIP}") / 1024 / 1024
print(f"Downloaded: {size_mb:.0f} MB")

# Extract
os.makedirs(f"/dbfs{DBFS_EXTRACT_DIR}", exist_ok=True)
result = subprocess.run(
    ["unzip", "-o", "-q", f"/dbfs{DBFS_ZIP}", "-d", f"/dbfs{DBFS_EXTRACT_DIR}"],
    capture_output=True, text=True
)
if result.returncode != 0:
    raise RuntimeError(f"Extraction failed: {result.stderr}")

# Find the main NPI data file (not the header/footer files)
extracted_files = [
    f for f in os.listdir(f"/dbfs{DBFS_EXTRACT_DIR}")
    if f.endswith(".csv") and "npidata" in f.lower() and "fileheader" not in f.lower()
]
if not extracted_files:
    raise RuntimeError(f"No NPI data CSV found in extracted files: {os.listdir(f'/dbfs{DBFS_EXTRACT_DIR}')}")

NPPES_CSV = f"{DBFS_EXTRACT_DIR}/{extracted_files[0]}"
print(f"Extracted: {extracted_files[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load NPPES — Organization Records Only
# MAGIC
# MAGIC We only need a subset of NPPES columns for the CCN matching:
# MAGIC - NPI
# MAGIC - Entity Type Code (filter to 2 = organization)
# MAGIC - Provider Organization Name
# MAGIC - State, City, Postal Code
# MAGIC - Primary taxonomy code
# MAGIC - NPI Deactivation Date (exclude deactivated NPIs)

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, when, lit

# NPPES column names as published by CMS
NPPES_COLS = {
    "NPI": "npi",
    "Entity Type Code": "entity_type_code",
    "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    "Provider Business Mailing Address State Name": "npi_state",
    "Provider Business Mailing Address City Name": "npi_city",
    "Provider Business Mailing Address Postal Code": "npi_postal_code",
    "Healthcare Provider Taxonomy Code_1": "taxonomy_code_1",
    "NPI Deactivation Date": "npi_deactivation_date",
}

nppes_raw = spark.read.csv(
    f"dbfs:{NPPES_CSV}",
    header=True,
    inferSchema=False,
)

# Select and rename only the columns we need
nppes_orgs = nppes_raw.select(
    *[col(f"`{src}`").alias(dst) for src, dst in NPPES_COLS.items()]
).filter(
    # Organizations only, active NPIs only
    (trim(col("entity_type_code")) == "2") &
    (col("npi_deactivation_date").isNull() | (trim(col("npi_deactivation_date")) == ""))
).withColumn(
    "npi_state", upper(trim(col("npi_state")))
).withColumn(
    "npi_city", upper(trim(col("npi_city")))
).withColumn(
    "npi_postal_code", trim(col("npi_postal_code"))
).withColumn(
    "provider_organization_name", trim(col("provider_organization_name"))
)

print(f"NPPES organization records (active): {nppes_orgs.count():,}")
nppes_orgs.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta Table

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

nppes_orgs.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.nppes_orgs")
print(f"Written: {CATALOG}.{SCHEMA}.nppes_orgs ({nppes_orgs.count():,} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup DBFS temp files

# COMMAND ----------

import shutil
shutil.rmtree(f"/dbfs{DBFS_EXTRACT_DIR}", ignore_errors=True)
os.remove(f"/dbfs{DBFS_ZIP}")
print("Temp files removed.")
