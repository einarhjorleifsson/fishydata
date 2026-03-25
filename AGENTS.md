# fishydata — Agent Memory

## Project

A data pipeline project that converts raw source data (ZIP archives, CSVs, etc.)
into analysis-ready hive-partitioned Parquet datasets. Multiple data sources are
processed by dedicated `data-raw/` scripts. A `targets`-based pipeline may be
introduced later to orchestrate the full sweep.

## About "running" the project

The project is either run on:

* Desctop MacBook
* Linux server

Directories "data" and "data-raw" are gitignored, primarily because they contain
large amount of data-files.

On the Linux server the
directories are actually pointers to a different location, while on the MacBook
the directories are in place. A synch maintainance of these two is a bit of a hassle for
now, done manually with some risks. One of the problem is that the MacBook does not
understand the path stated in the pointer.

Posit Assistant: Could you suggest a way to make the above life a little easier?

## General note / instructions

### In scripts only use a single dash line for sectioning

like:

```
# Header -----------------------------------------------------------------------
## Subheader -------------------------------------------------------------------
### Subsubheader ---------------------------------------------------------------
```

not like:

```
# ------------------------------------------------------------------------------
# Header
# ------------------------------------------------------------------------------
```

### Stick with dplyr code as much as possible

* parquet files are the base storage format in the data-directory
* Those files are either imported fully into R or a connection is made to a temporary duckdb-database
* In cases of a connection one most often would use duckdbfs::open_dataset
* Because sometime one works with imported tables sometimes a duckdb-connection the dplyr syntax is prefered
* In case that a code process is not available in duckdb, suggest SQL-function equivalence.

### Addtional notes on code comments

* Write only comments to R-code that are non-obvious
* At the head of each commented script write clearly input file and output file
* If you note dependencies of other scripts, add note (this could later be used if we take up the target-package code flow)

## Overview — `scripts/`

Scripts are numbered to reflect a loose execution order. Raw source data lives
in `data-raw/`; cleaned/processed outputs go to `data/`.

### 00-series — Reference / auxiliary datasets (run once or rarely)

| Script | Source | Output | Notes |
|--------|--------|--------|-------|
| `00_DATASET_aux_shapes.R` | LMI/GIS data | `data/auxillary/shoreline.gpkg` | Iceland shoreline polygons |
| `00_DATASET_aux_various-lookups.R` | Hard-coded / flat files | lookup tables in `data/` | Miscellaneous reference lookups |
| `00_DATASET_eflalo-dictionary.R` | EFLALO standard definitions | dictionary mapping | Maps Icelandic fields to EFLALO standard |
| `00_DATASET_gear.R` | Hard-coded gear table | `data/gear/gear_mapping.parquet` | Icelandic gear codes → ICES standards (OTB, LLS, …) |
| `00_DATASET_global-fishing-watch_version3.R` | GFW v3 data | processed GFW parquet | Converts Global Fishing Watch v3 |
| `00_DATASET_optiger.R` | Optiger system | Optiger reference file | Optiger vessel tracking reference |
| `00_DATASET_ports.R` | STK trail harbour data + Kvóti DB | `data/ports/ports.gpkg` | Fishing ports with spatial geometries |
| `00_DATASET_vessel-scrape_FRO.R` | Web-scraped Faroese registry | Faroese vessel reference | Faroe Islands vessel registrations |
| `00_DATASET_vessels_iceland.R` | Multiple Icelandic registries + MMSI data | `data/vessels/vessels_iceland.parquet` | Comprehensive Icelandic vessel registry |
| `00_DATASET_vessels_iceland-mmsi.R` | MMSI source data | MMSI lookup | Icelandic vessel MMSI assignments |
| `00_DATASET_mmsi_icelandic.R` | Historical parquet + fjarskiptastofa XLSX | `data-raw/mmsi/mmsi_iceland_YYYY-MM-DD.parquet` | Merges fjarskiptastofa updates into historical MMSI archive |
| `00_DATASET_ais_astd-zip_sniff-schema.R` | ASTD ZIPs | `data-raw/ais/astd_canonical_schema.rds` | Schema discovery for ASTD monthly ZIPs |
| `00_DATASET_ais_astd_zip-to-parquet.R` | ASTD ZIPs + canonical schema | `data/ais/astd/` hive parquet | Converts ASTD ZIPs to hive-partitioned Parquet via DuckDB |
| `00_DATASET_ais_stk_oracle-to-parquet.R` | Oracle `stk.trail` (via `mar`) | `data-raw/ais/stk/year=YYYY/` | Raw STK trail extraction; feeds `05_DATASET_ais_stk-to-parquet.R` |

### 01-series — Vessel registry & ID matching (periodic updates)

| Script | Source | Output | Notes |
|--------|--------|--------|-------|
| `01_DATASET_vessels_combine-registry.R` | Iceland + Faroe + Norway registries | `data-raw/vessels/vessel-registry.parquet` | Unified multi-country vessel registry |
| `01_DATASET_vessels_stk-vid-match.R` | STK trail/mobile + vessel registry + MMSI | `data-raw/stk-mmsi-vessel-match/*.parquet` | Maps STK mobile IDs → vessel IDs + MMSIs |
| `01_DATASET_mobileid_classification.R` | STK DB + vessel registry | mobile ID classifications | Assigns STK mobile IDs to vessel identities |

### 05-series — AIS raw → Parquet (long-running, run headless)

| Script | Source | Output | Notes |
|--------|--------|--------|-------|
| `05_DATASET_ais_astd-classB-to-parquet.R` | ASTD Class B AIS ZIPs | partitioned parquet | Class B variant of the ASTD pipeline |
| `05_DATASET_ais_norway-to-parquet.R` | Norwegian AIS data | partitioned parquet | Norwegian AIS conversion |
| `05_DATASET_ais_stk-to-parquet.R` | STK database + GIS layers | `/u3/geo/fishydata/data/ais/stk-raw/*.parquet` (by year) | STK AIS with spatial enrichment (depth, EEZ, ICES, ports) |
| `05_DATASET_fangstdata_norway-to-parquet.R` | Norwegian catch data (fangstdata) | partitioned parquet | Norwegian catch records conversion |
| `05_DATASET_logbooks_norway-to-parquet.R` | Norwegian logbook data | partitioned parquet | Norwegian logbook conversion |

### 11-12-series — Icelandic landings & logbooks (regular updates)

| Script | Source | Output | Notes |
|--------|--------|--------|-------|
| `11_DATASET_landings.R` | Oracle DBs (LODS, AGF) | `data/landings/{agf,lods}_{stations,catch}.parquet` | Landing records; input to logbook coupling |
| `11_DATASET_landings.sh` | — | runs `11_DATASET_landings.R` | Shell wrapper for scheduling |
| `12_DATASET_logbooks.R` | Oracle DBs (AFLI, ADB) + landings | `data/logbooks/parquet/{station,catch}.parquet` | Old + new schema merge; 1973–present |
| `12_DATASET_logbooks_dump.R` | Logbook databases | RDS dumps | Legacy export to RDS |
| `12_DATASET_logbooks.sh` | — | runs `12_DATASET_logbooks.R` | Shell wrapper for scheduling |

### 41-43-series — Spatial enrichment & trail processing

| Script | Source | Output | Notes |
|--------|--------|--------|-------|
| `41_DATASET_astd-isleez.R` | ASTD AIS parquet + GIS layers | spatially enriched parquet | Adds depth, EEZ, port context to ASTD data |
| `43_DATASET_ais_trail.R` | STK raw parquet + logbooks + spatial | enhanced AIS trail dataset | Trajectory analysis + logbook coupling |

### 80-91-series — Demos & datacalls (ad hoc)

| Script | Purpose |
|--------|---------|
| `80_DATASET_SMB_demo.R` | Demo/sample dataset for SMB use case |
| `91_datacall_bækats_2025-06-12.R` | Targeted export for Bækats datacall |
| `91_datacall_buseful_2025-05-05.R` | Business-focused export for stakeholder |
| `update_web.R` | Pushes current data outputs to web-accessible location |

---


## Conventions

### Script naming


| Prefix | Purpose |
|--------|---------|
| `SNIFF_<dataset>.R` | Schema discovery — reads a sample from each source file, produces canonical schema RDS |
| `DATASET_<dataset>_<description>.R` | Builds the Parquet dataset from raw source files |

### File layout

```
data-raw/
  ais/
    astd/                     # raw ZIP files
    astd_schema_sniff.rds     # raw per-month schema (output of SNIFF)
    astd_canonical_schema.rds # one row per column  (output of SNIFF)

scripts/
  log/                        # nohup logs
  00_DATASET_ais_astd-zip_sniff-schema.R
  00_DATASET_ais_astd_zip-to-parquet.R

data/
  ais/
    astd/                     # hive-partitioned Parquet output
      year=YYYY/
        month=MM/
          astd_YYYYMM.parquet
```

### Running headless

```bash
nohup R < scripts/00_DATASET_ais_astd_zip-to-parquet.R --vanilla \
  > scripts/log/astd_$(date +%F).log 2>&1 &
```

---

## Dataset: ASTD — AIS hive-partitioned Parquet

### Source files

- `data-raw/ais/astd/ASTD_area_level1_YYYYMM[_manual].zip`
- 157 monthly ZIP files, 201301–202601
- Each ZIP contains one CSV with the same stem name as the ZIP
- Semicolon-delimited (`delim = ";"`)
- A few files are corrupt / missing CSV inside: 202308\_manual, 202309\_manual, 202507

### Scripts (run in order)

1. `scripts/00_DATASET_ais_astd-zip_sniff-schema.R`
2. `scripts/00_DATASET_ais_astd_zip-to-parquet.R`

---

### Script 1: `SNIFF_astd_schema.R`

**Purpose:** Discover the column names and types for every monthly ZIP without
extracting files to disk. Produces two RDS artefacts consumed by the pipeline.

**Key configuration:**
- `n_max = 1e4` — number of rows read from each CSV for type inference

**How it works:**

`sniff_zip()` is the per-file workhorse:
1. Reads the ZIP directory listing (`unzip(..., list = TRUE)`) to find the CSV
   entry name — no decompression yet.
2. Streams the first `n_max` rows directly from the compressed stream via
   `readr::read_delim(unz(zip, csv_entry), ...)` — never writes to disk.
3. Returns a tibble with one row per column: `yyyymm`, `col_idx`, `column_name`,
   `column_type` (the first class of each column as inferred by readr).

`map2()` applies `sniff_zip()` across all ZIPs and row-binds the results into
`schema_df` (the raw per-month schema).

**Canonical schema construction:**

`schema_df` is aggregated to one row per `column_name`, with:

| Field | Description |
|-------|-------------|
| `n_months` | How many months the column appears in |
| `first_month` / `last_month` | YYYYMM of first and last appearance |
| `types_seen` | All distinct R types observed (collapsed to a string) |
| `type_conflict` | TRUE if 2+ distinct types remain after stripping `logical` |
| `canonical_type` | Resolved type via `resolve_type()` |

**Type resolution (`resolve_type()`):**
- `logical` from readr = all-NA sample artefact; stripped when other types
  are present.
- Numeric branch: `logical` < `integer` < `numeric` — winner is highest rank.
- Datetime branch: `Date` < `POSIXct` — winner is highest rank.
- Mixed branches: fall back to `character`.

**Known type overrides** applied after sniff (hardcoded `mutate`):
- `date_time_utc` → `POSIXct` (readr can't parse the mdy-period format, so those
  months land as `character`; the pipeline handles formatting explicitly)
- `mmsi`, `imonumber` → `integer` (readr infers `numeric` for large IDs)

**Outputs:**
- `astd_schema_sniff.rds` — `schema_df`: long tibble, one row per
  (yyyymm × column). Used by pipeline to know which columns existed each month.
- `astd_canonical_schema.rds` — `canonical`: one row per column, with
  `canonical_type` as resolved above.

**Diagnostics printed at the end:**
- Any type conflicts (resolved via hierarchy)
- Months with non-standard column sets (deviating from the dominant set)

---

### Script 2: `DATASET_astd_zip-to-parquet.R`

**Purpose:** Convert every ZIP to a hive-partitioned Parquet file via DuckDB.
No source data passes through R memory — only the unzip step touches R.

**Key configuration (top of script):**

```r
zip_dir     <- "data-raw/ais/astd"
parquet_dir <- "data/ais/astd"
csv_delim   <- ";"

# Months using MM/DD/YYYY HH:MM:SS (not ISO) — requires explicit timestampformat
mdy_period <- c("202311","202312","202401","202402","202403","202404")

r_to_ddb <- c(
  numeric   = "DOUBLE",
  integer   = "INTEGER",
  character = "VARCHAR",
  POSIXct   = "TIMESTAMP"
)
```

**Pre-computation:**

`cols_by_month` is a named list (`yyyymm → character vector`) built from
`schema_df` via `summarise(..., cols = list(column_name), .by = yyyymm) |>
tibble::deframe()`. This is looked up per-month in the loop to decide CAST vs NULL.

**`on.exit` / function-wrapper pattern — important:**

The DuckDB connection and the entire processing loop live inside `run_pipeline()`.
This is intentional: `on.exit(dbDisconnect(con, shutdown = TRUE))` fires when
the *function* returns, not when the top-level `source()` call exits. In
RStudio's evaluation context, top-level `on.exit` fires prematurely (before the
loop runs), disconnecting `con` and causing "Invalid connection" errors on every
file. The wrapper fixes this.

**Per-file processing loop (inside `run_pipeline()`):**

1. **Unzip** to a `tempfile()` directory — the only step that uses R memory.
   - `tryCatch` wraps `unzip()` for corrupt ZIPs.
   - If no CSV is found inside, warns and skips.
   - If multiple CSVs, warns and uses the first.

2. **Resolve present columns** from `cols_by_month[[ym]]`.
   - If `ym` not found (new file added after SNIFF last ran): borrow column set
     from the nearest prior month and warn. Re-run SNIFF if new columns may exist.
   - If no prior month exists at all: assume full canonical column set.

3. **Skip-if-exists:** if the output parquet already exists, skip and record
   `ok = NA` in results. Supports incremental updates.

4. **Build SQL** using two helpers:
   - `csv_options(ym)` — returns the `timestampformat` snippet for mdy-period
     months, or `""` otherwise.
   - `build_select_clause(present_cols)` — iterates `canonical` to build a
     comma-separated list of `CAST(col AS TYPE) AS col` for present columns and
     `NULL::TYPE AS col` for absent columns. `date_time_utc` is always
     `CAST(date_time_utc AS TIMESTAMP)` and appended last.

5. **Execute** via `dbExecute(con, sql)` — DuckDB runs:
   ```sql
   COPY (
     SELECT <explicit cast list>
     FROM read_csv('<csv_path>',
                   delim       = ';',
                   header      = true,
                   auto_detect = true
                   [, timestampformat = '...'])
   ) TO '<out_file>'
   (FORMAT PARQUET, COMPRESSION ZSTD)
   ```

6. **Clean up** temp dir with `unlink(tmp, recursive = TRUE)` after each file.

7. **Record** result: `data.frame(yyyymm, ok, seconds)`.

**Summary at end:** prints written / skipped / failed counts and lists any failed
months by YYYYMM.

---

### Timestamp column: `date_time_utc`

| Period | Format | Handling |
|--------|--------|---------|
| 201301–202310 | ISO `YYYY-MM-DD HH:MM:SS` | DuckDB auto-detects |
| 202311–202404 | `MM/DD/YYYY HH:MM:SS` | `timestampformat = '%m/%d/%Y %H:%M:%S'` passed to `read_csv` |
| 202405+ | ISO with T-separator / timezone | DuckDB auto-detects |

**Key pitfall:** DuckDB `auto_detect` samples the first 20 480 rows and can
infer `DD/MM/YYYY` from ambiguous early dates (e.g., January), then fail mid-file
when it encounters a row where the "day" field exceeds 12. The fix is the
hardcoded `mdy_period` vector and the `csv_options()` helper.

---

### Schema evolution — 4 distinct periods

| Period | Key changes |
|--------|-------------|
| 201301–201912 | `pm` present; no `flagcode` |
| 202001–202212 | `pm` dropped; `flagcode` added; `blackcarbon` emission group still present |
| 202301–202503 | `blackcarbon` group dropped; vessel dimension columns added |
| 202504+ | `arcticshiptype` added |

---

### Canonical schema

- Produced by `00_DATASET_ais_astd-zip_sniff-schema.R`; saved to `data-raw/ais/astd_canonical_schema.rds`
- **38 columns total**
- Key type overrides applied post-sniff: `mmsi` → INTEGER, `imonumber` → INTEGER,
  `date_time_utc` → POSIXct
- `logical` typed columns in the sniff = all-NA sample artefact; stripped before
  type resolution
- Type hierarchy for conflicts: `logical` < `integer` < `numeric` (numeric
  branch); `Date` < `POSIXct` (datetime branch); mixed → `character`

---

### Reading the dataset back

```r
library(duckdbfs)
library(dplyr)

ds <- duckdbfs::open_dataset("data/ais/astd") |>
  mutate(year = as.integer(year), month = as.integer(month))
```

`year` and `month` are **not stored in the parquet files**; they are encoded in
the hive directory paths (`year=YYYY/month=MM`). `open_dataset` returns them as
VARCHAR — cast to integer explicitly.

Quick sanity check:
```r
ds |> count(year, month) |> arrange(year, month) |> print(n = Inf)
```

---

---


## Dataset sections (detailed)

---

## Dataset: STK Oracle → raw Parquet

**Script:** `scripts/00_DATASET_ais_stk_oracle-to-parquet.R`
**Depends on:** none (direct Oracle pull)
**Feeds into:** `05_DATASET_ais_stk-to-parquet.R` (spatial enrichment)

### Source

Oracle table `stk.trail`, accessed via `mar::connect_mar()` and `mar::tbl_mar()`.
Coverage starts 2007; upper bound is the current calendar year.

### Unit conversions applied at query time

| Column | Oracle unit | Output unit |
|--------|-------------|-------------|
| `poslon` / `poslat` | radians | degrees (`* 180/pi`) |
| `heading` | radians | degrees (`* 180/pi`) |
| `speed` | m/s | knots (`* 3600/1852`) |

### Column mapping

| Output name | Oracle source |
|-------------|--------------|
| `.id` | `trailid` |
| `mid` | `mobileid` |
| `time` | `posdate` |
| `hid` | `harborid` |
| `io` | `in_out_of_harbor` |

### Per-year loop

- Iterates year boundaries `T1[i]` → `T2[i]` (exclusive end).
- **Skip logic:** checks for `data_0.parquet` (default duckdbfs filename) in the
  target partition. Existing years are skipped — except the current year, which
  is always re-fetched to pick up records added since the last run.
- **Partition key:** `recdate` (DB receipt timestamp), not `posdate`. Filtering
  on receipt time ensures each row falls in exactly one annual partition, avoiding
  duplicates if position timestamps straddle year boundaries.

### Output

```
data-raw/ais/stk/
  year=YYYY/
    data_0.parquet
```

Hive-partitioned by `year` (derived from `recdate`). Written via
`duckdbfs::write_dataset(..., partitioning = "year")`.

### Reading back

```r
library(duckdbfs)
library(dplyr)

ds <- duckdbfs::open_dataset("data-raw/ais/stk") |>
  mutate(year = as.integer(year))
```

---

---

## Dataset: Icelandic MMSI archive

**Script:** `scripts/00_DATASET_mmsi_icelandic.R`
**Depends on:** none
**Feeds into:** vessel registry matching, AIS trail joins

### Purpose

Maintains a dated, cumulative archive of Icelandic MMSI numbers linking `mmsi`
to vessel ID (`sknr`) with time-validity windows (`mmsi_t1`, `mmsi_t2`). The
archive is designed for temporal joins, e.g.:

```r
left_join(astd, mmsi, by = join_by(mmsi, between(time, mmsi_t1, mmsi_t2)))
```

### Source files

| File | Role |
|------|------|
| `data-raw/mmsi/mmsi_iceland_YYYY-MM-DD.parquet` | Historical archive (input) |
| `data-raw/mmsi/mmsi-iceland_fjarskiptastofa_YYYY-MM-DD.xlsx` | New fjarskiptastofa export (update) |

### Output

`data-raw/mmsi/mmsi_iceland_YYYY-MM-DD.parquet` — dated file named with today's
date. **Latest file as of 2026-03-25:** `mmsi_iceland_2026-03-25.parquet`
(4,077 rows).

### Schema

| Column | Type | Description |
|--------|------|-------------|
| `mmsi` | chr | MMSI number |
| `mmsi_cat` | chr | Category: `vessel`, `Child vessel`, `Navigational aid`, `coast station`, `testing`, etc. |
| `sknr` | chr | Ship registration number |
| `cs` | chr | Call sign |
| `nafn` | chr | Vessel name |
| `note` | chr | Free-text note from fjarskiptastofa |
| `note_date` | date | Date parsed from note text |
| `note_fate` | chr | Parsed fate: `Niðurrif`, `Seldur`, `Afskráður`, `Úr rekstri` |
| `source` | dbl | Source version number (incremented each fjarskiptastofa update) |
| `mmsi_t1` | date | Start of validity window |
| `mmsi_t2` | date | End of validity window |
| `zombie_no` | dbl | 1 for first assignment; incremented when mmsi migrates to a new sknr |

**Constants:**
- `MMSI_T1 = 2007-06-01` (start of STK)
- `MMSI_T2 = 2028-12-24` (nominal far-future end)

### Merge logic (four cases)

Each time a new fjarskiptastofa XLSX is processed, rows are classified:

| Case | Condition | Action |
|------|-----------|--------|
| A | `mmsi + sknr` exists in both | Keep historical record unchanged |
| B | `mmsi` not in historical at all | Append; `zombie_no = 1`, `mmsi_t1 = MMSI_T1`, `mmsi_t2 = MMSI_T2` |
| C | `mmsi` in both but different `sknr` | Close old record (`mmsi_t2 = TODAY - 1`); append new (`mmsi_t1 = TODAY`, `zombie_no = old + 1`) |
| D | `mmsi` only in historical | Kept automatically as base of `bind_rows` |

**Column alignment:** extra xlsx columns (`ja_nei`, `standard_c`, `selcall`,
`aukanumer`, `eigandi`, `stadur`) are dropped before merging; output schema
matches the historical file.

**Closing date for Case C:** uses `note_date` from the update row if available,
otherwise falls back to `TODAY - 1`.

### Reading the archive

```r
mmsi <- nanoparquet::read_parquet("data-raw/mmsi/mmsi_iceland_2026-03-25.parquet")
```

---

## Possible future: `targets` pipeline

A `_targets.R` file could orchestrate the full sweep:
- One target per SNIFF script
- One target per DATASET script, depending on its SNIFF target
- Incremental re-runs when new ZIP files appear
