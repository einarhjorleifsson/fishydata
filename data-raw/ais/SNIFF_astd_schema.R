# ASTD schema sniff pass
#
# Streams the first n_max rows of each ZIP/CSV (no disk extraction) via
# readr + unz(), collects column names and R types, checks consistency
# across months, and resolves conflicts using a type hierarchy.
#
# Output: data-raw/ais/astd_schema_sniff.rds   (raw per-month schema)
#         data-raw/ais/astd_canonical_schema.rds (one row per column)

library(tidyverse)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

zip_dir   <- "data-raw/ais/astd"
out_raw   <- "data-raw/ais/astd_schema_sniff.rds"
out_canon <- "data-raw/ais/astd_canonical_schema.rds"
csv_delim <- ";"
n_max     <- 1e4

# ------------------------------------------------------------------------------
# Type hierarchy for resolving cross-month conflicts
#
# Within numeric branch:  logical < integer < numeric
# Within datetime branch: Date < POSIXct
# Across branches:        anything mixed → character (safe fallback)
# ------------------------------------------------------------------------------

resolve_type <- function(types) {
  types <- unique(types[!is.na(types)])

  # readr types all-NA column samples as 'logical' — not a true observation.
  # Drop it when other types are present.
  if (length(types) > 1L) types <- types[types != "logical"]

  if (length(types) == 1L) return(types)

  num_rank <- c(logical = 1L, integer = 2L, numeric = 3L, double = 3L)
  dt_rank  <- c(Date = 1L, POSIXct = 2L, POSIXt = 2L)

  if (all(types %in% names(num_rank)))
    return(names(which.max(num_rank[types])))

  if (all(types %in% names(dt_rank)))
    return(names(which.max(dt_rank[types])))

  "character"
}

# ------------------------------------------------------------------------------
# Helper: sniff one ZIP without extracting to disk
# ------------------------------------------------------------------------------

sniff_zip <- function(zip, yyyymm, delim, n_max) {

  # Read only the ZIP directory (no decompression yet)
  entries   <- unzip(zip, list = TRUE)$Name
  csv_entry <- str_subset(entries, regex("\\.csv$", ignore_case = TRUE))[1]

  if (is.na(csv_entry)) {
    warning("No CSV entry in ", basename(zip), " — skipping")
    return(NULL)
  }

  # Stream first n_max rows directly from the ZIP
  df <- tryCatch(
    read_delim(unz(zip, csv_entry),
               delim          = delim,
               n_max          = n_max,
               show_col_types = FALSE,
               name_repair    = "minimal"),
    error = function(e) {
      warning(basename(zip), ": ", conditionMessage(e))
      NULL
    }
  )

  if (is.null(df) || ncol(df) == 0) return(NULL)

  tibble(
    yyyymm      = yyyymm,
    col_idx     = seq_along(df),
    column_name = names(df),
    column_type = map_chr(df, \(x) class(x)[1])
  )
}

# ------------------------------------------------------------------------------
# Discover + sort ZIP files
# ------------------------------------------------------------------------------

zip_files <- list.files(zip_dir, pattern = "\\.zip$", full.names = TRUE)
yyyymm    <- str_extract(basename(zip_files), "\\d{6}")
ord       <- order(yyyymm)
zip_files <- zip_files[ord]
yyyymm    <- yyyymm[ord]

message("Sniffing ", length(zip_files), " files (",
        yyyymm[1], " – ", yyyymm[length(yyyymm)], ")")

# ------------------------------------------------------------------------------
# Sniff all ZIPs
# ------------------------------------------------------------------------------

schema_df <- map2(zip_files, yyyymm, sniff_zip,
                  delim = csv_delim, n_max = n_max,
                  .progress = TRUE) |>
  list_rbind()

message("Saving raw schema to ", out_raw)
saveRDS(schema_df, out_raw)

# ------------------------------------------------------------------------------
# Canonical schema: one row per column
#
# For each column:
#   - n_months:       how many months it appears in
#   - first / last:   first and last month seen
#   - types_seen:     all distinct R types observed (for audit)
#   - type_conflict:  TRUE if more than one type was observed
#   - canonical_type: resolved type via hierarchy (or "character" if mixed)
# ------------------------------------------------------------------------------

canonical <-
  schema_df |>
  summarise(
    n_months      = n_distinct(yyyymm),
    first_month   = min(yyyymm),
    last_month    = max(yyyymm),
    types_seen    = list(unique(column_type)),
    .by           = column_name
  ) |>
  mutate(
    # True conflict: 2+ distinct types remain after stripping 'logical'
    # (logical on its own = all-NA sample, not a real type observation)
    type_conflict  = map_lgl(types_seen, \(x) {
                       x <- x[x != "logical"]
                       length(unique(x)) > 1
                     }),
    canonical_type = map_chr(types_seen, resolve_type),
    types_seen     = map_chr(types_seen, \(x) paste(sort(x), collapse = " | "))
  ) |>
  arrange(desc(n_months), column_name)

# Known type overrides:
#  date_time_utc — readr can't parse MM/DD/YYYY months so they land as
#                  'character'; the pipeline handles the format explicitly.
#  mmsi/imonumber — readr infers numeric for large integers; these are IDs.
canonical <- canonical |>
  mutate(canonical_type = case_when(
    column_name == "date_time_utc"              ~ "POSIXct",
    column_name %in% c("mmsi", "imonumber")     ~ "integer",
    .default = canonical_type
  ))

message("Saving canonical schema to ", out_canon)
saveRDS(canonical, out_canon)

# ------------------------------------------------------------------------------
# Print
# ------------------------------------------------------------------------------

n_total   <- n_distinct(schema_df$yyyymm)
n_conflict <- sum(canonical$type_conflict)

message(sprintf(
  "\n--- Canonical schema across %d months ---\n", n_total
))
print(canonical, n = Inf)

if (n_conflict > 0) {
  message(sprintf(
    "\n%d column(s) with type conflicts (resolved via hierarchy):", n_conflict
  ))
  canonical |>
    filter(type_conflict) |>
    select(column_name, types_seen, canonical_type) |>
    print(n = Inf)
} else {
  message("\nNo type conflicts — all columns are consistent across months.")
}

# Flag months with non-standard column sets
col_sets <-
  schema_df |>
  summarise(col_set = paste(sort(column_name), collapse = "|"), .by = yyyymm) |>
  add_count(col_set, name = "n_months_with_set")

dominant_set <- col_sets |> slice_max(n_months_with_set, n = 1) |> pull(col_set)

deviants <- col_sets |> filter(col_set != dominant_set)

if (nrow(deviants) > 0) {
  message("\nMonths with non-standard column sets:")
  print(arrange(deviants, yyyymm), n = Inf)
} else {
  message("\nAll months share the same column set.")
}
