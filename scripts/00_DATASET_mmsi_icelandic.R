# Input:  data-raw/mmsi/mmsi_iceland_YYYY-MM-DD.parquet  (latest dated file)
#         data-raw/mmsi/mmsi-iceland_fjarskiptastofa_YYYY-MM-DD.xlsx  (newest update)
# Output: data-raw/mmsi/mmsi_iceland_YYYY-MM-DD.parquet  (today's date)
#
# Run whenever a new fjarskiptastofa XLSX is received.
# Merge logic — four cases:
#   A: mmsi+sknr exists in both           → keep historical record unchanged
#   B: mmsi not in historical at all       → append; zombie_no=1, default t1/t2
#   C: mmsi in both but sknr differs       → close old (t2 = note_date or TODAY-1),
#                                             append new (zombie_no+1, t1 = TODAY)
#   D: mmsi only in historical             → kept automatically

library(tidyverse)
library(nanoparquet)

# Configuration ----------------------------------------------------------------
# Update these two paths when a new xlsx arrives:
hist_path   <- "data-raw/mmsi/mmsi_iceland_2025-MM-DD.parquet"
update_path <- "data-raw/mmsi/mmsi-iceland_fjarskiptastofa_2026-03-09.xlsx"

TODAY   <- Sys.Date()
MMSI_T1 <- ymd("2007-06-01")   # Start of STK
MMSI_T2 <- ymd("2028-12-24")   # Einar's retirement date

cat("Historical file:", hist_path, "\n")
cat("Update file:    ", update_path, "\n")

# 1. Load historical archive ---------------------------------------------------
mmsi_hist <- read_parquet(hist_path)
max_source <- max(mmsi_hist$source, na.rm = TRUE)

# 2. Load and parse fjarskiptastofa xlsx ---------------------------------------
update <-
  update_path |>
  readxl::read_excel() |>
  janitor::clean_names() |>
  select(mmsi = mmsi_nr, sknr, cs = kallm,
         nafn = skip,
         note = athugasemdir,
         everything()) |>
  mutate(
    note_date = str_extract(note, "\\d+\\D+\\d+\\D+\\d+"),
    note_date = dmy(note_date),
    .after = note) |>
  mutate(note_fate =
           case_when(
             str_detect(tolower(note), "brota") ~ "Niðurrif",
             str_detect(tolower(note), "niður") ~ "Niðurrif",
             str_detect(tolower(note), "seld")  ~ "Seldur",
             str_detect(tolower(note), "selt")  ~ "Seldur",
             str_detect(tolower(note), "afsk")  ~ "Afskráður",
             str_detect(tolower(note), "afsráð")  ~ "Afskráður",
             str_detect(tolower(note), "afrskráð") ~ "Afskráður",
             str_starts(tolower(note), "afmáð")   ~ "Afskráður",
             str_starts(tolower(note), "afskráð")  ~ "Afskráður",
             str_starts(tolower(note), "teki")     ~ "Úr rekstri",
             .default = NA),
         .after = note_date) |>
  mutate(mmsi_cat = ramb::rb_mmsi_category(mmsi), .after = mmsi) |>
  mutate(mmsi_cat =
           case_when(
             mmsi == "251991540" ~ "SAR aircraft",
             note == "V/prófana á fjarskiptabúnaði" ~ "testing",
             nafn %in% c("Radíómiðun hf", "Brimrún hf",
                         "Stefja ehf Ánanaustum 15") ~ "testing",
             .default = mmsi_cat)) |>
  mutate(source = max_source + 1, .after = nafn)

# Drop xlsx-only columns not in the historical schema
update_slim <- update |>
  select(mmsi, mmsi_cat, sknr, cs, nafn, note, note_date, note_fate, source)

# 3. Pre-merge diagnostics -----------------------------------------------------
cat("\nExisting mmsi migrations (zombie_no > 1) in historical:\n")
mmsi_hist |>
  filter(zombie_no > 1) |>
  arrange(mmsi, zombie_no) |>
  select(mmsi, sknr, nafn, mmsi_t1, mmsi_t2, zombie_no) |>
  print(n = Inf)

cat("\nCase C candidates (mmsi in both, sknr differs):\n")
update_slim |>
  filter(mmsi %in% mmsi_hist$mmsi) |>
  anti_join(mmsi_hist |> select(mmsi, sknr), by = c("mmsi", "sknr")) |>
  select(mmsi, mmsi_cat, sknr, nafn, note_date) |>
  print(n = Inf)

# 4. Merge ---------------------------------------------------------------------

# Case C: close old records where sknr has changed
case_c_mmsi <- update_slim |>
  filter(mmsi %in% mmsi_hist$mmsi) |>
  anti_join(mmsi_hist |> select(mmsi, sknr), by = c("mmsi", "sknr")) |>
  pull(mmsi)

mmsi_closed <- mmsi_hist |>
  mutate(mmsi_t2 = if_else(mmsi %in% case_c_mmsi, TODAY - 1, mmsi_t2))

case_c_new <- update_slim |>
  filter(mmsi %in% case_c_mmsi) |>
  left_join(
    mmsi_hist |>
      filter(mmsi %in% case_c_mmsi) |>
      group_by(mmsi) |>
      summarise(zombie_no = max(zombie_no) + 1, .groups = "drop"),
    by = "mmsi"
  ) |>
  mutate(
    # Use note_date from update as t1 if available, otherwise TODAY
    mmsi_t1 = coalesce(note_date, TODAY),
    mmsi_t2 = MMSI_T2
  )

# Case B: entirely new mmsi
case_b_new <- update_slim |>
  filter(!mmsi %in% mmsi_hist$mmsi) |>
  mutate(mmsi_t1 = MMSI_T1, mmsi_t2 = MMSI_T2, zombie_no = 1)

mmsi_updated <- bind_rows(mmsi_closed, case_c_new, case_b_new) |>
  arrange(mmsi, zombie_no)

# 5. Write output --------------------------------------------------------------
out_path <- paste0("data-raw/mmsi/mmsi_iceland_", TODAY, ".parquet")
write_parquet(mmsi_updated, out_path)

mmsi_updated |> write_parquet("data/mmsi/mmsi_iceland.parquet")

cat("\nDone.\n")
cat("  Historical rows:", nrow(mmsi_hist), "\n")
cat("  New (Case B):   ", nrow(case_b_new), "\n")
cat("  Migrations (C): ", length(case_c_mmsi), "\n")
cat("  Output rows:    ", nrow(mmsi_updated), "\n")
cat("  Written to:     ", out_path, "\n")
