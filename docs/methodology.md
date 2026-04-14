# Matching Methodology

## The Problem

The CMS Provider of Services (POS) file — the authoritative source for hospital facility metadata (bed counts, ownership type, facility type) — is keyed by **CCN** (CMS Certification Number). Virtually all other healthcare datasets, including Transparency in Coverage negotiated rate files, are keyed by **NPI** (National Provider Identifier).

No maintained, public CCN→NPI crosswalk exists. The NBER crosswalk ended in 2017. Commercial vendors (Definitive Healthcare, Strata, others) sell one, but it is paywalled.

This project builds the crosswalk from first principles using two public CMS datasets.

---

## Data Sources

| Source | Key | Update Cadence | URL |
|--------|-----|----------------|-----|
| CMS Provider of Services File | CCN | Quarterly | [data.cms.gov](https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/provider-of-services-file-hospital-non-hospital-facilities) |
| NPPES Full Replacement File | NPI | Monthly | [download.cms.gov/nppes](https://download.cms.gov/nppes/NPI_Files.html) |

---

## Cardinality

- **CCN is the parent**: one CCN can map to multiple NPIs. Each row in the output represents one CCN:NPI pair.
- **NPI is distinct**: each NPI appears at most once in the output, assigned to the highest-confidence strategy that matched it.
- **Unmatched rows**: CCNs with zero NPIs found appear once with `npi` blank.

## Algorithm

Matching proceeds through five strategies. Strategy 0 is authoritative and requires no name matching. Strategies 1–4 cascade — each runs only against CCNs not covered by Strategy 0. Once a CCN enters the cascade and matches, it exits; lower strategies only see CCNs still unmatched.

### Strategy 0: CMS Hospital Enrollments (Authoritative)
Direct CCN:NPI pairs from CMS Medicare enrollment records. Covers acute care hospitals, Critical Access Hospitals (CAH), and Rural Emergency Hospitals (REH) that participate in Medicare. No name matching — these pairs are taken as ground truth.

**Source**: CMS Hospital Enrollments file (data.cms.gov)
**Coverage**: ~9,300 Medicare-enrolled hospitals
**Confidence**: Authoritative.

### Strategy 1: Exact Name + State + City
Both names are normalized identically (see below), then matched on the combination of normalized name, 2-character state code, and city name.

**Confidence**: High. Both sides must agree on name, state, and city.

### Strategy 2: Normalized Name + State Only
Same name normalization, but city is dropped from the join condition. Handles city name inconsistencies between datasets (e.g., "St. Louis" vs "Saint Louis").

**Confidence**: Medium-High.

### Strategy 3: 3-Word Name Prefix + State + City
Extracts the first 3 words of the normalized name and matches on prefix + state + city. Catches hospitals registered with name suffixes or additional descriptors in one dataset but not the other.

Example: `NORTON HOSPITAL PAVILION A` (POS) matches `NORTON HOSPITAL` (NPPES) via prefix `NORTON HOSPITAL`.

Minimum prefix length is 3 words to avoid spurious matches on short common names.

**Confidence**: Medium.

### Strategy 4: Zip Code + Hospital Taxonomy + Levenshtein Distance
For hospitals where names differ substantially — rebrands, DBA names, legal name changes — the algorithm:
1. Filters NPPES to records with taxonomy code starting with `28` (hospital taxonomy codes per NUCC)
2. Joins on matching 5-digit zip code and state
3. Computes Levenshtein edit distance between normalized names
4. Accepts matches with edit distance ≤ 7
5. When multiple NPPES candidates match a CCN, takes the closest name

The threshold of 7 was calibrated empirically against known hospital rebrands. It is tight enough to avoid false positives at the zip level while recovering common rebrand patterns (e.g., a 6-character brand name swap).

**Confidence**: Medium-Low. These matches should be validated for high-stakes use cases.

---

## Name Normalization

Applied identically to both POS and NPPES names before any comparison:

1. Uppercase and trim whitespace
2. Collapse multiple spaces to single space
3. Strip legal suffixes: `INC`, `LLC`, `LP`, `LLP`, `CORP`, `LTD`
4. Expand abbreviations:

| Abbreviation | Expansion |
|---|---|
| MED CTR, MED CNTR | MEDICAL CENTER |
| HOSP | HOSPITAL |
| HOSPS | HOSPITALS |
| CTR, CNTR | CENTER |
| GENL | GENERAL |
| REGL | REGIONAL |
| NATL | NATIONAL |
| UNIV | UNIVERSITY |
| COMM | COMMUNITY |
| MEM | MEMORIAL |
| REHAB | REHABILITATION |
| PSYCH | PSYCHIATRIC |
| HLTH | HEALTH |
| SVCS | SERVICES |
| SVC | SERVICE |

Single-letter directional abbreviations (N, S, E, W) and ambiguous patterns (CO, ST) are intentionally excluded — they cause false positives and provide minimal match lift.

5. Re-collapse spaces after expansion

---

## Match Rate (Q4 2025 POS + April 2026 NPPES)

| Strategy | CCNs Matched | % of Total |
|---|---|---|
| exact_name_state_city | 3,967 | 29.4% |
| prefix_name_state_city | 1,221 | 9.0% |
| zip_taxonomy_levenshtein | 623 | 4.6% |
| name_state | 265 | 2.0% |
| **Total matched** | **6,076** | **45.0%** |
| unmatched | 7,432 | 55.0% |
| **Total CCNs** | **13,508** | |

The unmatched 55% includes VA hospitals, Indian Health Service facilities, and facilities where the legal name in NPPES diverges significantly from the operating name in POS. Contributions to improve match rate are welcome — see the [contributing guide](../README.md#contributing).

---

## Known Limitations

- **VA and federal facilities**: Many lack standard taxonomy codes and use non-matching name formats
- **Multi-campus systems**: A single CCN may correspond to multiple NPIs; the algorithm keeps the highest-confidence match
- **Name changes**: If a hospital rebranded after the NPPES record was last updated, no strategy will catch it
- **Levenshtein threshold**: The threshold of 7 is empirical. Edge cases exist in both directions

---

## Output Schema

| Column | Type | Description |
|---|---|---|
| `npi` | bigint | NPI from NPPES (null if unmatched) |
| `ccn` | string | CMS Certification Number |
| `facility_name` | string | Hospital name from POS |
| `city` | string | City |
| `state` | string | 2-character state code |
| `zip_code` | string | 5-digit ZIP |
| `total_bed_count` | int | All beds including non-participating |
| `certified_bed_count` | int | Medicare/Medicaid certified beds |
| `facility_type_code` | string | CMS code (01=Short Term, 11=CAH, etc.) |
| `facility_type` | string | Human-readable facility type |
| `ownership_type_code` | string | CMS code |
| `ownership_type` | string | Human-readable ownership type |
| `psych_unit_beds` | int | Psychiatric unit bed count |
| `rehab_unit_beds` | int | Rehabilitation unit bed count |
| `bed_size_band` | string | Small (<100), Medium (100-299), Large (300-499), Major (500+) |
| `match_method` | string | Strategy that produced the match |
