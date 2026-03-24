# ASTD ZIP → Hive-partitioned Parquet
#
# Input:  data-raw/ais/astd/ASTD_area_level1_YYYYMM[_manual].zip
# Output: data/ais/astd/year=YYYY/month=MM/astd_YYYYMM.parquet
#
# Run headless:
#   nohup R < data-raw/ais/DATASET_astd_zip-to-parquet.R --vanilla \
#     > data-raw/ais/log/astd_$(date +%F).log 2>&1 &

library(DBI)
library(duckdb)
library(dplyr)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

zip_dir     <- "data-raw/ais/astd"
parquet_dir <- "data/ais/astd"

# CSV delimiter used in ASTD files
csv_delim <- ";"

# Files in this period use MM/DD/YYYY HH:MM:SS instead of ISO. DuckDB
# auto_detect guesses DD/MM/YYYY from ambiguous early rows then fails on
# months > 12 later in the file. Pass timestampformat explicitly.
mdy_period <- c("202311", "202312", "202401", "202402", "202403", "202404")

# Canonical schema produced by SNIFF_astd_schema.R
schema_sniff_file <- "data-raw/ais/astd_schema_sniff.rds"
canonical_file    <- "data-raw/ais/astd_canonical_schema.rds"

if (!file.exists(canonical_file))
  stop("Canonical schema not found — run scripts/00_DATASET_ais_astd-zip_sniff-schema.R first.\n  Expected: ",
       canonical_file)

canonical  <- readRDS(canonical_file)   # one row per column: column_name, canonical_type, ...
schema_df  <- readRDS(schema_sniff_file) # per-month column presence

# R class → DuckDB type
r_to_ddb <- c(
  numeric   = "DOUBLE",
  integer   = "INTEGER",
  character = "VARCHAR",
  POSIXct   = "TIMESTAMP"
)

# Pre-compute which columns were present in each sniffed month
# (used to decide CAST vs NULL for each column in the SELECT)
cols_by_month <- schema_df |>
  summarise(cols = list(column_name), .by = yyyymm) |>
  tibble::deframe()  # named list: yyyymm → character vector of column names

# ------------------------------------------------------------------------------
# Discover + sort ZIP files
# ------------------------------------------------------------------------------

zip_files <- list.files(zip_dir, pattern = "\\.zip$", full.names = TRUE)

if (length(zip_files) == 0) stop("No ZIP files found in: ", zip_dir)

# Extract YYYYMM — always the first 6-digit run after the last underscore/digit block
yyyymm <- regmatches(basename(zip_files), regexpr("\\d{6}", basename(zip_files)))

ord       <- order(yyyymm)
zip_files <- zip_files[ord]
yyyymm    <- yyyymm[ord]

message("Found ", length(zip_files), " ZIP files (",
        yyyymm[1], " – ", yyyymm[length(yyyymm)], ")")

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

csv_options <- function(ym) {
  if (ym %in% mdy_period)
    ",\n         timestampformat = '%m/%d/%Y %H:%M:%S'"
  else
    ""
}

build_select_clause <- function(present_cols) {
  ts_line <- "CAST(date_time_utc AS TIMESTAMP) AS date_time_utc"
  other_lines <- canonical |>
    filter(column_name != "date_time_utc") |>
    mutate(
      ddb_type = r_to_ddb[canonical_type],
      sql_expr = if_else(
        column_name %in% present_cols,
        sprintf("CAST(%s AS %s) AS %s", column_name, ddb_type, column_name),
        sprintf("NULL::%s AS %s",        ddb_type,    column_name)
      )
    ) |>
    pull(sql_expr)
  paste(c(other_lines, ts_line), collapse = ",\n         ")
}

# ------------------------------------------------------------------------------
# Process each ZIP — wrapped in a function so on.exit fires at function exit,
# not at top-level source() exit (which is unpredictable in RStudio).
# ------------------------------------------------------------------------------

run_pipeline <- function(zip_files, yyyymm, parquet_dir) {

  con <- dbConnect(duckdb())
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

  dir.create(parquet_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create("data-raw/ais/log", recursive = TRUE, showWarnings = FALSE)

  results <- vector("list", length(zip_files))

  for (i in seq_along(zip_files)) {

  zip <- zip_files[i]
  ym  <- yyyymm[i]
  yr  <- as.integer(substr(ym, 1, 4))
  mo  <- as.integer(substr(ym, 5, 6))

  message(sprintf(
    "[%d/%d] %s  (%s)",
    i, length(zip_files), basename(zip), format(Sys.time(), "%H:%M:%S")
  ))

  # -- Unzip -------------------------------------------------------------------
  tmp <- tempfile(pattern = "astd_")
  dir.create(tmp)

  csv_files <- tryCatch(
    unzip(zip, exdir = tmp),
    error = function(e) {
      warning("Cannot unzip ", basename(zip), ": ", conditionMessage(e))
      character(0)
    }
  )

  csv_files <- csv_files[grepl("\\.csv$", csv_files, ignore.case = TRUE)]

  if (length(csv_files) == 0) {
    warning("No CSV found in ", basename(zip), " — skipping")
    unlink(tmp, recursive = TRUE)
    next
  }
  if (length(csv_files) > 1) {
    warning(length(csv_files), " CSVs in ", basename(zip),
            " — using: ", basename(csv_files[1]))
    csv_files <- csv_files[1]
  }

  csv_path <- csv_files[1]

  # -- Resolve which columns are present in this month -------------------------
  present_cols <- cols_by_month[[ym]]
  if (is.null(present_cols)) {
    # Month not in sniff — likely a new file added after SNIFF_astd_schema.R was
    # last run. Re-run the sniff if the new file may introduce new columns.
    prior <- sort(names(cols_by_month)[names(cols_by_month) < ym])
    if (length(prior) > 0) {
      fallback_ym  <- tail(prior, 1)
      present_cols <- cols_by_month[[fallback_ym]]
      warning(ym, " not in schema sniff — borrowing column set from ",
              fallback_ym, ". Re-run SNIFF_astd_schema.R if new columns may exist.")
    } else {
      present_cols <- canonical$column_name
      warning(ym, " not in schema sniff — assuming full canonical column set.")
    }
  }

  # -- Build output path -------------------------------------------------------
  out_dir  <- file.path(parquet_dir,
                        sprintf("year=%d", yr),
                        sprintf("month=%02d", mo))
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  out_file <- file.path(out_dir, sprintf("astd_%s.parquet", ym))

  # -- Skip if parquet already exists ------------------------------------------
  if (file.exists(out_file)) {
    message("  -> already exists, skipping")
    results[[i]] <- data.frame(yyyymm = ym, ok = NA, seconds = 0)
    unlink(tmp, recursive = TRUE)
    next
  }

  # -- COPY CSV → Parquet via DuckDB (no R memory) ----------------------------
  # year and month are encoded in the hive directory path (year=YYYY/month=MM)
  # and are NOT written into the file to avoid type conflicts on read-back.
  sql <- sprintf(
    "COPY (
       SELECT
         %s
       FROM read_csv(
         '%s',
         delim       = '%s',
         header      = true,
         auto_detect = true%s
       )
     ) TO '%s'
     (FORMAT PARQUET, COMPRESSION ZSTD)",
    build_select_clause(present_cols),
    csv_path,
    csv_delim,
    csv_options(ym),
    out_file
  )

  t0 <- proc.time()[["elapsed"]]

  ok <- tryCatch({
    dbExecute(con, sql)
    TRUE
  }, error = function(e) {
    warning("FAILED on ", basename(zip), ": ", conditionMessage(e))
    FALSE
  })

  elapsed <- round(proc.time()[["elapsed"]] - t0, 1)
  results[[i]] <- data.frame(yyyymm = ym, ok = ok, seconds = elapsed)

  message(sprintf("  -> %s  (%.1fs)", if (ok) "OK" else "FAILED", elapsed))

    # -- Clean up temp dir for this iteration ----------------------------------
    unlink(tmp, recursive = TRUE)
  }

  # ----------------------------------------------------------------------------
  # Summary
  # ----------------------------------------------------------------------------

  summary_df <- do.call(rbind, Filter(Negate(is.null), results))

  n_ok      <- sum(summary_df$ok %in% TRUE)
  n_skipped <- sum(is.na(summary_df$ok))
  n_fail    <- sum(summary_df$ok %in% FALSE)

  message("\n--- Summary ---")
  message(sprintf("  Written:  %d", n_ok))
  message(sprintf("  Skipped:  %d  (parquet already existed)", n_skipped))
  message(sprintf("  Failed:   %d", n_fail))
  if (n_fail > 0) {
    message("  Failed months:")
    message(paste0("    ", summary_df$yyyymm[summary_df$ok %in% FALSE], collapse = "\n"))
  }
  message(sprintf("  Total time: %.0fs", sum(summary_df$seconds, na.rm = TRUE)))
  message("  Output: ", parquet_dir)

  invisible(summary_df)

}  # end run_pipeline()

# ------------------------------------------------------------------------------
# Run
# ------------------------------------------------------------------------------

results <- run_pipeline(zip_files, yyyymm, parquet_dir)

# ------------------------------------------------------------------------------
# Read back (verify)
# ------------------------------------------------------------------------------
# year and month come from hive partition paths as VARCHAR; cast to integer:
#
# ds <- duckdbfs::open_dataset(parquet_dir) |>
#   mutate(year = as.integer(year), month = as.integer(month))
# ds |> count(year, month) |> arrange(year, month) |> print(n = Inf)
