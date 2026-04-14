# ccn-npi-xwalk

**An open-source CCN to NPI crosswalk for CMS hospital facilities.**

The [CMS Provider of Services (POS) file](https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities) is the authoritative source for hospital bed counts, facility type, and ownership data. It is keyed by **CCN** (CMS Certification Number). Most healthcare datasets — including Transparency in Coverage negotiated rate files — are keyed by **NPI**.

No maintained, public CCN→NPI crosswalk exists. This project builds one from CMS public data and publishes it quarterly.

---

## Just give me the CSV

**Option A: Direct download** (no install required)

Go to [Releases](https://github.com/clayton-deductible/ccn-npi-xwalk/releases/latest) and download `ccn_npi_crosswalk.csv`.

**Option B: pip install**

```bash
pip install ccn-npi-xwalk
ccn-npi-xwalk get
```

This downloads the latest release CSV to `ccn_npi_crosswalk.csv` in your current directory.

```bash
# Save to a specific path
ccn-npi-xwalk get --output /path/to/crosswalk.csv

# Check release metadata without downloading
ccn-npi-xwalk info
```

---

## CSV Schema

| Column | Description |
|---|---|
| `npi` | NPI from NPPES (blank if unmatched) |
| `ccn` | CMS Certification Number |
| `facility_name` | Hospital name (from CMS POS) |
| `city` | City |
| `state` | 2-character state code |
| `zip_code` | 5-digit ZIP |
| `total_bed_count` | All beds including non-participating |
| `certified_bed_count` | Medicare/Medicaid certified beds |
| `facility_type_code` | CMS code (01, 02, 04, 05, 06, 11, 12) |
| `facility_type` | Short Term Acute Care, Critical Access Hospital, etc. |
| `ownership_type_code` | CMS code |
| `ownership_type` | Private Non-Profit, Government, etc. |
| `psych_unit_beds` | Psychiatric unit beds |
| `rehab_unit_beds` | Rehabilitation unit beds |
| `bed_size_band` | Small (<100), Medium (100–299), Large (300–499), Major (500+) |
| `match_method` | How the NPI was found (see below) |

### `match_method` values

| Value | Meaning | Confidence |
|---|---|---|
| `exact_name_state_city` | Name + state + city matched exactly | High |
| `name_state` | Name + state matched (city varied) | Medium-High |
| `prefix_name_state_city` | First 3 words + state + city matched | Medium |
| `zip_taxonomy_levenshtein` | Zip + hospital taxonomy + edit distance ≤ 7 | Medium-Low |
| `unmatched` | No NPI found — `npi` column is blank | — |

---

## Current match rate

Based on Q4 2025 POS + most recent NPPES:

| Strategy | Count | % |
|---|---|---|
| exact_name_state_city | 3,967 | 29.4% |
| prefix_name_state_city | 1,221 | 9.0% |
| zip_taxonomy_levenshtein | 623 | 4.6% |
| name_state | 265 | 2.0% |
| **Total matched** | **6,076** | **45%** |
| unmatched | 7,432 | 55% |

---

## Run the algorithm yourself (Databricks)

The three notebooks in `notebooks/` reproduce the full pipeline from raw CMS source files. They require a Databricks workspace.

**Prerequisites:**
- Databricks workspace with Unity Catalog
- A cluster with enough memory for NPPES (~32 GB RAM recommended)

**Step 1: Ingest CMS POS**

Import `notebooks/01_ingest_cms_pos.py` into your Databricks workspace and run it. Set the `cms_pos_url` widget to the [current quarterly file](https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities).

**Step 2: Ingest NPPES**

Import `notebooks/02_ingest_nppes.py` and run it. Set the `nppes_zip_url` widget to the current [NPPES Full Replacement Monthly File](https://download.cms.gov/nppes/NPI_Files.html). This step takes 15–30 minutes.

**Step 3: Run the matching algorithm**

Import `notebooks/03_match_ccn_to_npi.py` and run it. Output is written to `{catalog}.{schema}.ccn_npi_crosswalk`. Optionally set `export_csv_path` to export a CSV to DBFS.

**Default catalog/schema**: `main.ccn_npi_xwalk`. Override with the `catalog` and `schema` widgets.

---

## Algorithm

See [docs/methodology.md](docs/methodology.md) for full documentation of the matching strategies, name normalization logic, known limitations, and match rate breakdown.

---

## Update schedule

This crosswalk is updated quarterly when CMS publishes a new POS file (January, April, July, October). Each release includes the POS quarter and NPPES month in the release notes.

---

## Contributing

Issues and pull requests are welcome for:
- Improving match rate on the unmatched 55%
- Adding new matching strategies
- Reporting false positives in the Levenshtein matches

This repository is maintained by [DeductibleData Co.](https://deductibledata.com).

---

## License

MIT. Data is sourced from CMS.gov public files and is not subject to copyright.
