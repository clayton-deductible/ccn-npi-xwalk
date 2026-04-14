# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 — Match CCN to NPI
# MAGIC
# MAGIC **What this does**:
# MAGIC Joins the CMS POS hospital records (keyed by CCN) to NPPES organization records
# MAGIC (keyed by NPI) using a 4-strategy cascade matching algorithm.
# MAGIC
# MAGIC **Prerequisites**: Run `01_ingest_cms_pos.py` and `02_ingest_nppes.py` first.
# MAGIC
# MAGIC ## Matching Strategy (in priority order)
# MAGIC
# MAGIC | Strategy | Method | Confidence |
# MAGIC |----------|--------|------------|
# MAGIC | 1 | Normalized name + state + city (exact) | High |
# MAGIC | 2 | Normalized name + state only | Medium-High |
# MAGIC | 3 | 3-word name prefix + state + city | Medium |
# MAGIC | 4 | Zip code + hospital taxonomy (28xxx) + Levenshtein ≤ 7 | Medium-Low |
# MAGIC | — | Unmatched (no NPI found) | — |
# MAGIC
# MAGIC **Name normalization** expands common hospital abbreviations before matching:
# MAGIC HOSP → HOSPITAL, MED CTR → MEDICAL CENTER, CTR → CENTER, etc.
# MAGIC This is applied identically to both POS and NPPES names.
# MAGIC
# MAGIC **Output**: `{catalog}.{schema}.ccn_npi_crosswalk` Delta table + optional CSV export

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "ccn_npi_xwalk", "Schema")
dbutils.widgets.text("export_csv_path", "", "Export CSV to DBFS path (optional, e.g. /tmp/ccn_npi_crosswalk.csv)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
EXPORT_CSV_PATH = dbutils.widgets.get("export_csv_path").strip()

print(f"Source: {CATALOG}.{SCHEMA}.cms_pos + {CATALOG}.{SCHEMA}.nppes_orgs")
print(f"Output: {CATALOG}.{SCHEMA}.ccn_npi_crosswalk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Tables

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, regexp_replace, when, lit
from pyspark.sql.types import LongType

hospitals = spark.table(f"{CATALOG}.{SCHEMA}.cms_pos")
nppes_raw = spark.table(f"{CATALOG}.{SCHEMA}.nppes_orgs")

print(f"POS hospitals:  {hospitals.count():,}")
print(f"NPPES org rows: {nppes_raw.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Name Normalization
# MAGIC
# MAGIC Applied to both POS facility names and NPPES organization names before matching.
# MAGIC Abbreviation list is intentionally conservative — single-letter and ambiguous
# MAGIC patterns (N/S/E/W, CO, ST) are excluded to avoid false positives.

# COMMAND ----------

ABBREVIATION_MAP = [
    (r"\bMED CTR\b",  "MEDICAL CENTER"),
    (r"\bMED CNTR\b", "MEDICAL CENTER"),
    (r"\bHOSP\b",     "HOSPITAL"),
    (r"\bHOSPS\b",    "HOSPITALS"),
    (r"\bGENL\b",     "GENERAL"),
    (r"\bREGL\b",     "REGIONAL"),
    (r"\bNATL\b",     "NATIONAL"),
    (r"\bCTR\b",      "CENTER"),
    (r"\bCNTR\b",     "CENTER"),
    (r"\bUNIV\b",     "UNIVERSITY"),
    (r"\bCOMM\b",     "COMMUNITY"),
    (r"\bMEM\b",      "MEMORIAL"),
    (r"\bREHAB\b",    "REHABILITATION"),
    (r"\bPSYCH\b",    "PSYCHIATRIC"),
    (r"\bHLTH\b",     "HEALTH"),
    (r"\bSVCS\b",     "SERVICES"),
    (r"\bSVC\b",      "SERVICE"),
]

def normalize_name(name_col):
    """Normalize a hospital name column for matching."""
    result = upper(trim(regexp_replace(name_col, r"\s+", " ")))
    # Strip legal suffixes that differ between POS and NPPES registrations
    result = regexp_replace(result, r",?\s*(INC\.?|LLC|LP|LLP|CORP\.?|LTD\.?)$", "")
    # Expand abbreviations
    for pattern, replacement in ABBREVIATION_MAP:
        result = regexp_replace(result, pattern, replacement)
    # Re-collapse double spaces introduced by expansions
    result = trim(regexp_replace(result, r"\s+", " "))
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare NPPES for Matching

# COMMAND ----------

nppes = nppes_raw.select(
    col("npi"),
    normalize_name(col("provider_organization_name")).alias("nppes_name_norm"),
    upper(trim(col("npi_state"))).alias("nppes_state"),
    upper(trim(col("npi_city"))).alias("nppes_city"),
    col("npi_postal_code").substr(1, 5).alias("nppes_zip"),
    col("taxonomy_code_1"),
)

hospitals_norm = hospitals.withColumn(
    "pos_name_norm", normalize_name(col("facility_name"))
).withColumn(
    "pos_state_norm", upper(trim(col("state")))
).withColumn(
    "pos_city_norm", upper(trim(col("city")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 1: Exact Match — Normalized Name + State + City

# COMMAND ----------

match_exact = hospitals_norm.join(
    nppes,
    (hospitals_norm.pos_name_norm == nppes.nppes_name_norm) &
    (hospitals_norm.pos_state_norm == nppes.nppes_state) &
    (hospitals_norm.pos_city_norm == nppes.nppes_city),
    "inner"
).withColumn("match_method", lit("exact_name_state_city"))

matched_ccns_exact = match_exact.select("ccn").distinct()
print(f"Strategy 1 (exact): {matched_ccns_exact.count():,} CCNs matched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 2: Normalized Name + State Only
# MAGIC
# MAGIC Catches city name variations (e.g., "Saint Louis" vs "St. Louis").

# COMMAND ----------

unmatched_1 = hospitals_norm.join(matched_ccns_exact, "ccn", "left_anti")

match_state = unmatched_1.join(
    nppes,
    (unmatched_1.pos_name_norm == nppes.nppes_name_norm) &
    (unmatched_1.pos_state_norm == nppes.nppes_state),
    "inner"
).withColumn("match_method", lit("name_state"))

matched_ccns_state = match_state.select("ccn").distinct()
print(f"Strategy 2 (name+state): {matched_ccns_state.count():,} CCNs matched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 3: 3-Word Name Prefix + State + City
# MAGIC
# MAGIC Catches name suffix variations, e.g.:
# MAGIC   POS:   "NORTON HOSPITAL PAVILION A"
# MAGIC   NPPES: "NORTON HOSPITAL"

# COMMAND ----------

from pyspark.sql.functions import split, size, concat_ws, slice as spark_slice

unmatched_2 = unmatched_1.join(matched_ccns_state, "ccn", "left_anti")

unmatched_2 = unmatched_2.withColumn(
    "pos_name_words", split(col("pos_name_norm"), r"\s+")
).withColumn(
    "pos_prefix",
    when(size(col("pos_name_words")) >= 3,
         concat_ws(" ", spark_slice(col("pos_name_words"), 1, 3)))
)

nppes_prefix = nppes.withColumn(
    "nppes_name_words", split(col("nppes_name_norm"), r"\s+")
).withColumn(
    "nppes_prefix",
    when(size(col("nppes_name_words")) >= 3,
         concat_ws(" ", spark_slice(col("nppes_name_words"), 1, 3)))
)

match_prefix = unmatched_2.filter(col("pos_prefix").isNotNull()).join(
    nppes_prefix.filter(col("nppes_prefix").isNotNull()),
    (unmatched_2.pos_prefix == nppes_prefix.nppes_prefix) &
    (unmatched_2.pos_state_norm == nppes_prefix.nppes_state) &
    (unmatched_2.pos_city_norm == nppes_prefix.nppes_city),
    "inner"
).drop("pos_name_words", "pos_prefix", "nppes_name_words", "nppes_prefix"
).withColumn("match_method", lit("prefix_name_state_city"))

matched_ccns_prefix = match_prefix.select("ccn").distinct()
print(f"Strategy 3 (prefix): {matched_ccns_prefix.count():,} CCNs matched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 4: Zip Code + Hospital Taxonomy + Levenshtein Distance
# MAGIC
# MAGIC For hospitals where names differ significantly between POS and NPPES
# MAGIC (e.g., rebrands, DBA names). Matches on:
# MAGIC   - Same 5-digit zip code
# MAGIC   - Same state
# MAGIC   - NPPES taxonomy code starts with "28" (hospital taxonomy codes)
# MAGIC   - Levenshtein edit distance between normalized names ≤ 7
# MAGIC
# MAGIC When multiple NPPES candidates match a CCN, the closest name wins.
# MAGIC Threshold of 7 was chosen empirically — tight enough to avoid false matches
# MAGIC at the zip level, loose enough to catch common rebrand patterns.

# COMMAND ----------

from pyspark.sql.functions import levenshtein, row_number
from pyspark.sql.window import Window

unmatched_3 = unmatched_2.join(matched_ccns_prefix, "ccn", "left_anti").drop(
    "pos_name_words", "pos_prefix"
)

nppes_hospital_tax = nppes.filter(
    col("taxonomy_code_1").like("28%")
).select(
    col("npi"),
    col("nppes_name_norm"),
    col("nppes_state"),
    col("nppes_zip"),
)

match_zip_tax = unmatched_3.join(
    nppes_hospital_tax,
    (unmatched_3.zip_code == nppes_hospital_tax.nppes_zip) &
    (unmatched_3.pos_state_norm == nppes_hospital_tax.nppes_state),
    "inner"
).withColumn(
    "lev_dist", levenshtein(col("pos_name_norm"), col("nppes_name_norm"))
).filter(
    col("lev_dist") <= 7
)

w_lev = Window.partitionBy("ccn").orderBy(col("lev_dist"))
match_zip_tax = match_zip_tax.withColumn("_rn", row_number().over(w_lev)).filter(
    col("_rn") == 1
).drop("_rn", "lev_dist", "nppes_zip"
).withColumn("match_method", lit("zip_taxonomy_levenshtein"))

matched_ccns_zip = match_zip_tax.select("ccn").distinct()
print(f"Strategy 4 (zip+taxonomy+levenshtein): {matched_ccns_zip.count():,} CCNs matched")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Unmatched

# COMMAND ----------

unmatched_final = unmatched_3.join(matched_ccns_zip, "ccn", "left_anti").withColumn(
    "npi", lit(None).cast("long")
).withColumn(
    "match_method", lit("unmatched")
)
print(f"Unmatched: {unmatched_final.count():,} CCNs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine, Deduplicate, and Write

# COMMAND ----------

final_columns = [
    "npi", "ccn", "facility_name", "city", "state", "zip_code",
    "total_bed_count", "certified_bed_count",
    "facility_type_code", "facility_type",
    "ownership_type_code", "ownership_type",
    "psych_unit_beds", "rehab_unit_beds",
    "bed_size_band", "match_method",
]

drop_cols = ["pos_name_norm", "pos_state_norm", "pos_city_norm",
             "nppes_name_norm", "nppes_state", "nppes_city",
             "nppes_zip", "taxonomy_code_1"]

combined = (
    match_exact.drop(*[c for c in drop_cols if c in match_exact.columns]).select(*final_columns)
    .unionByName(match_state.drop(*[c for c in drop_cols if c in match_state.columns]).select(*final_columns))
    .unionByName(match_prefix.drop(*[c for c in drop_cols if c in match_prefix.columns]).select(*final_columns))
    .unionByName(match_zip_tax.drop(*[c for c in drop_cols if c in match_zip_tax.columns]).select(*final_columns))
    .unionByName(unmatched_final.drop(*[c for c in drop_cols if c in unmatched_final.columns]).select(*final_columns))
)

# Deduplicate: if a CCN matched via multiple strategies, keep highest confidence
match_priority = (
    when(col("match_method") == "exact_name_state_city", 1)
    .when(col("match_method") == "name_state", 2)
    .when(col("match_method") == "prefix_name_state_city", 3)
    .when(col("match_method") == "zip_taxonomy_levenshtein", 4)
    .otherwise(5)
)

w = Window.partitionBy("ccn").orderBy(match_priority)
deduped = combined.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")

# Cast NPI to bigint
deduped = deduped.withColumn("npi", col("npi").cast(LongType()))

# Write
deduped.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk")

# Summary
total = spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk").count()
matched = spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk").filter(col("npi").isNotNull()).count()
print(f"\nOutput: {CATALOG}.{SCHEMA}.ccn_npi_crosswalk")
print(f"  Total CCNs:   {total:,}")
print(f"  NPI matched:  {matched:,} ({matched * 100 // total}%)")
print(f"  Unmatched:    {total - matched:,} ({(total - matched) * 100 // total}%)")
print()
spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk").groupBy("match_method").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optional: Export to CSV

# COMMAND ----------

if EXPORT_CSV_PATH:
    (
        spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk")
        .coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .csv(f"dbfs:{EXPORT_CSV_PATH}_parts")
    )
    # Rename the single part file to the desired path
    import subprocess
    parts = dbutils.fs.ls(f"{EXPORT_CSV_PATH}_parts")
    csv_part = [p.path for p in parts if p.name.startswith("part-")][0]
    dbutils.fs.cp(csv_part, EXPORT_CSV_PATH)
    dbutils.fs.rm(f"{EXPORT_CSV_PATH}_parts", recurse=True)
    print(f"CSV exported to dbfs:{EXPORT_CSV_PATH}")
