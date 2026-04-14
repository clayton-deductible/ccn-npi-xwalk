# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook 3 — Match CCN to NPI
# MAGIC
# MAGIC **Prerequisites**: Run `01_ingest_cms_pos.py` and `02_ingest_nppes.py` first.
# MAGIC
# MAGIC ## Matching Strategy (in priority order)
# MAGIC
# MAGIC | Strategy | Method | Source | Confidence |
# MAGIC |----------|--------|--------|------------|
# MAGIC | 0 | Direct enrollment lookup | CMS Hospital Enrollments | Authoritative |
# MAGIC | 1 | Normalized name + state + city | NPPES | High |
# MAGIC | 2 | Normalized name + state only | NPPES | Medium-High |
# MAGIC | 3 | 3-word name prefix + state + city | NPPES | Medium |
# MAGIC | 4 | Zip + hospital taxonomy (28xxx) + Levenshtein ≤ 7 | NPPES | Medium-Low |
# MAGIC | — | Unmatched | — | — |
# MAGIC
# MAGIC ## Cardinality rules
# MAGIC
# MAGIC - **CCN is the parent**: one CCN can map to multiple NPIs (one row per NPI)
# MAGIC - **NPI is distinct**: each NPI appears at most once in the output, assigned
# MAGIC   to the highest-confidence strategy that matched it
# MAGIC - Strategies 1–4 only run against CCNs with no match from Strategy 0

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

print(f"Source tables: {CATALOG}.{SCHEMA}.cms_pos + nppes_orgs + hospital_enrollments")
print(f"Output table:  {CATALOG}.{SCHEMA}.ccn_npi_crosswalk")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Source Tables

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, regexp_replace, when, lit, row_number
from pyspark.sql.types import LongType
from pyspark.sql.window import Window

hospitals   = spark.table(f"{CATALOG}.{SCHEMA}.cms_pos")
nppes_raw   = spark.table(f"{CATALOG}.{SCHEMA}.nppes_orgs")
enrollments = spark.table(f"{CATALOG}.{SCHEMA}.hospital_enrollments")

print(f"POS hospitals:       {hospitals.count():,}")
print(f"NPPES org rows:      {nppes_raw.count():,}")
print(f"Enrollment records:  {enrollments.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Name Normalization
# MAGIC
# MAGIC Applied identically to POS and NPPES names before any comparison.
# MAGIC Abbreviation list is conservative — single-letter and ambiguous patterns
# MAGIC excluded to avoid false positives.

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
    result = upper(trim(regexp_replace(name_col, r"\s+", " ")))
    result = regexp_replace(result, r",?\s*(INC\.?|LLC|LP|LLP|CORP\.?|LTD\.?)$", "")
    for pattern, replacement in ABBREVIATION_MAP:
        result = regexp_replace(result, pattern, replacement)
    result = trim(regexp_replace(result, r"\s+", " "))
    return result

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 0 — CMS Hospital Enrollments (Authoritative)
# MAGIC
# MAGIC Direct CCN:NPI pairs from Medicare enrollment records.
# MAGIC No name matching required — these are taken as ground truth.

# COMMAND ----------

# Each enrollment row is already a (CCN, NPI) pair.
# Keep the facility metadata from POS by joining on CCN.
match_enrollments = enrollments.join(
    hospitals, "ccn", "inner"
).select(
    col("npi"),
    col("ccn"),
    col("facility_name"),
    col("city"),
    col("state"),
    col("zip_code"),
    col("total_bed_count"),
    col("certified_bed_count"),
    col("facility_type_code"),
    col("facility_type"),
    col("ownership_type_code"),
    col("ownership_type"),
    col("psych_unit_beds"),
    col("rehab_unit_beds"),
    col("bed_size_band"),
    lit("hospital_enrollments").alias("match_method"),
)

enrolled_ccns = match_enrollments.select("ccn").distinct()
enrolled_npis = match_enrollments.select("npi").distinct()

print(f"Strategy 0 (enrollments): {match_enrollments.count():,} CCN:NPI pairs "
      f"across {enrolled_ccns.count():,} CCNs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare NPPES for Name Matching
# MAGIC
# MAGIC Strategies 1–4 only run against CCNs not covered by Strategy 0.
# MAGIC NPPES is also filtered to exclude NPIs already claimed by Strategy 0.

# COMMAND ----------

nppes = nppes_raw.select(
    col("npi"),
    normalize_name(col("provider_organization_name")).alias("nppes_name_norm"),
    upper(trim(col("npi_state"))).alias("nppes_state"),
    upper(trim(col("npi_city"))).alias("nppes_city"),
    col("npi_postal_code").substr(1, 5).alias("nppes_zip"),
    col("taxonomy_code_1"),
).join(enrolled_npis, "npi", "left_anti")  # exclude already-claimed NPIs

# Hospitals not covered by Strategy 0
unmatched_0 = hospitals.join(enrolled_ccns, "ccn", "left_anti")

hospitals_norm = unmatched_0.withColumn(
    "pos_name_norm", normalize_name(col("facility_name"))
).withColumn(
    "pos_state_norm", upper(trim(col("state")))
).withColumn(
    "pos_city_norm", upper(trim(col("city")))
)

print(f"CCNs entering NPPES matching: {hospitals_norm.count():,}")
print(f"NPPES orgs available:         {nppes.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 1 — Exact Name + State + City

# COMMAND ----------

match_exact = hospitals_norm.join(
    nppes,
    (hospitals_norm.pos_name_norm == nppes.nppes_name_norm) &
    (hospitals_norm.pos_state_norm == nppes.nppes_state) &
    (hospitals_norm.pos_city_norm == nppes.nppes_city),
    "inner"
).withColumn("match_method", lit("exact_name_state_city"))

matched_ccns_exact = match_exact.select("ccn").distinct()
print(f"Strategy 1 (exact):  {match_exact.count():,} pairs across {matched_ccns_exact.count():,} CCNs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 2 — Normalized Name + State Only

# COMMAND ----------

unmatched_1 = hospitals_norm.join(matched_ccns_exact, "ccn", "left_anti")

match_state = unmatched_1.join(
    nppes,
    (unmatched_1.pos_name_norm == nppes.nppes_name_norm) &
    (unmatched_1.pos_state_norm == nppes.nppes_state),
    "inner"
).withColumn("match_method", lit("name_state"))

matched_ccns_state = match_state.select("ccn").distinct()
print(f"Strategy 2 (name+state): {match_state.count():,} pairs across {matched_ccns_state.count():,} CCNs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 3 — 3-Word Name Prefix + State + City

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
print(f"Strategy 3 (prefix): {match_prefix.count():,} pairs across {matched_ccns_prefix.count():,} CCNs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Strategy 4 — Zip + Hospital Taxonomy + Levenshtein Distance
# MAGIC
# MAGIC Threshold of ≤ 7 edit distance was calibrated empirically against known
# MAGIC hospital rebrands. See docs/methodology.md for details.

# COMMAND ----------

from pyspark.sql.functions import levenshtein

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
).drop("lev_dist", "nppes_zip"
).withColumn("match_method", lit("zip_taxonomy_levenshtein"))

matched_ccns_zip = match_zip_tax.select("ccn").distinct()
print(f"Strategy 4 (zip+tax+lev): {match_zip_tax.count():,} pairs across {matched_ccns_zip.count():,} CCNs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unmatched CCNs

# COMMAND ----------

unmatched_final = unmatched_3.join(matched_ccns_zip, "ccn", "left_anti").withColumn(
    "npi", lit(None).cast(LongType())
).withColumn(
    "match_method", lit("unmatched")
)
print(f"Unmatched: {unmatched_final.count():,} CCNs")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine All Strategies

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

def clean_select(df):
    return df.drop(*[c for c in drop_cols if c in df.columns]).select(*final_columns)

combined = (
    clean_select(match_enrollments)
    .unionByName(clean_select(match_exact))
    .unionByName(clean_select(match_state))
    .unionByName(clean_select(match_prefix))
    .unionByName(clean_select(match_zip_tax))
    .unionByName(clean_select(unmatched_final))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Deduplicate on NPI
# MAGIC
# MAGIC **Cardinality rules:**
# MAGIC - CCN can appear multiple times (one row per matched NPI)
# MAGIC - NPI must be globally distinct — if an NPI matched via multiple strategies,
# MAGIC   keep only the highest-confidence match
# MAGIC - Unmatched rows (npi = null) are always kept, one per CCN

# COMMAND ----------

match_priority = (
    when(col("match_method") == "hospital_enrollments",     1)
    .when(col("match_method") == "exact_name_state_city",   2)
    .when(col("match_method") == "name_state",              3)
    .when(col("match_method") == "prefix_name_state_city",  4)
    .when(col("match_method") == "zip_taxonomy_levenshtein",5)
    .otherwise(6)  # unmatched
)

# For matched rows: deduplicate on NPI, keeping highest-confidence strategy
matched_rows = combined.filter(col("npi").isNotNull())
w_npi = Window.partitionBy("npi").orderBy(match_priority)
deduped_matched = (
    matched_rows
    .withColumn("_rn", row_number().over(w_npi))
    .filter(col("_rn") == 1)
    .drop("_rn")
)

# For unmatched rows: one row per CCN (CCNs with zero NPIs found)
matched_ccns_all = deduped_matched.select("ccn").distinct()
unmatched_rows = combined.filter(col("npi").isNull()).join(
    matched_ccns_all, "ccn", "left_anti"
)

output = deduped_matched.unionByName(unmatched_rows)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Output Table

# COMMAND ----------

output.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk")

total   = spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk").count()
matched = spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk").filter(col("npi").isNotNull()).count()
unique_ccns = spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk").select("ccn").distinct().count()

print(f"\nOutput: {CATALOG}.{SCHEMA}.ccn_npi_crosswalk")
print(f"  Total rows:      {total:,}")
print(f"  Matched rows:    {matched:,}")
print(f"  Unmatched CCNs:  {total - matched:,}")
print(f"  Unique CCNs:     {unique_ccns:,}")
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
    parts = dbutils.fs.ls(f"{EXPORT_CSV_PATH}_parts")
    csv_part = [p.path for p in parts if p.name.startswith("part-")][0]
    dbutils.fs.cp(csv_part, EXPORT_CSV_PATH)
    dbutils.fs.rm(f"{EXPORT_CSV_PATH}_parts", recurse=True)
    print(f"CSV exported to dbfs:{EXPORT_CSV_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Against Reference Set
# MAGIC
# MAGIC Measures precision and recall against a known-good CCN:NPI reference file.
# MAGIC Set `validation_csv_path` to run this section. The reference file must have
# MAGIC columns `CCN` and `NPI`.

# COMMAND ----------

dbutils.widgets.text("validation_csv_path", "", "Validation CSV path (DBFS, optional)")
VALIDATION_PATH = dbutils.widgets.get("validation_csv_path").strip()

if VALIDATION_PATH:
    ref = spark.read.csv(f"dbfs:{VALIDATION_PATH}", header=True, inferSchema=False).select(
        trim(col("CCN")).alias("ccn"),
        trim(col("NPI")).alias("npi_str"),
    ).filter(
        col("ccn").isNotNull() & (col("ccn") != "") &
        col("npi_str").isNotNull() & (col("npi_str") != "") &
        (col("ccn") != "N/A")
    ).withColumn("npi", col("npi_str").cast(LongType())).drop("npi_str")

    our = spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk").filter(col("npi").isNotNull()).select("ccn", "npi")

    ref_count  = ref.count()
    our_count  = our.count()
    tp         = our.join(ref, ["ccn", "npi"], "inner").count()
    precision  = tp / our_count * 100 if our_count else 0
    recall     = tp / ref_count  * 100 if ref_count  else 0

    print(f"Validation results:")
    print(f"  Reference pairs:  {ref_count:,}")
    print(f"  Our pairs:        {our_count:,}")
    print(f"  True positives:   {tp:,}")
    print(f"  Precision:        {precision:.1f}%  (our matches that are in reference)")
    print(f"  Recall:           {recall:.1f}%  (reference pairs we found)")
    print()

    # Breakdown by strategy
    print("Precision by strategy:")
    our_with_method = spark.table(f"{CATALOG}.{SCHEMA}.ccn_npi_crosswalk").filter(col("npi").isNotNull())
    breakdown = our_with_method.join(ref.withColumn("in_ref", lit(True)), ["ccn", "npi"], "left").fillna(False, ["in_ref"])
    breakdown.groupBy("match_method").agg(
        {"npi": "count", "in_ref": "sum"}
    ).withColumnRenamed("count(npi)", "our_count").withColumnRenamed("sum(in_ref)", "in_ref_count").orderBy("our_count", ascending=False).show()
else:
    print("No validation_csv_path set — skipping validation.")
    print("To validate, upload your reference CSV to DBFS and set the widget.")
