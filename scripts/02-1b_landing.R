# Objective --------------------------------------------------------------------
# Consolidate landings databases
#
# Input:  Landings: Oracle database
# Output: data/landings/agf_catch.rds
#         data/landings/agf_stations.rds
#         data/landings/lods_catch.rds
#         data/landings/lods_stations.rds
#
# Downstream usage: 02-2_logbooks-landings-coupling.R
# 
# Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < scripts/02-1b_landing.R --vanilla > lgs/02-1b_landing_2024-02-12.log &
library(tictoc)

tic()

lubridate::now()


# NOTE: If using years further back: need to double check that visir and station_id
#       are not the same.
YEARS <- 2024:2001

library(tidyverse)
library(omar)
con <- connect_mar()


# AGF landings -----------------------------------------------------------------
## Get data --------------------------------------------------------------------
LN_raw <- 
  omar::ln_agf(con) |> 
  filter(wt > 0,            # 2023-12-28 - lots of zero records, why?
         year(date) %in% YEARS) |> 
  rename(# .lid = .id,  # this is NOT a landing id
    hid_ln = hid,
    gid_ln = gid,
    datel = date) |> 
  collect(n = Inf) |> 
  filter(vid %in% c(2, 5:3699, 5000:9998)) |> 
  mutate(datel = as_date(datel),
         vid = as.integer(vid),
         gid_ln = as.integer(gid_ln),
         hid_ln = as.integer(hid_ln)) |> 
  arrange(vid, datel, .id, gid_ln, sid) |>
  group_by(vid, datel) |> 
  mutate(.lid = min(.id)) |> 
  ungroup() |> 
  # 2023-12-28: summarise the lot
  group_by(.lid, vid, datel, hid_ln, gid_ln, sid) |> 
  summarise(wt = sum(wt),
            .groups = "drop")

### Checks
checks <- 
  LN_raw |> 
  group_by(vid, datel) |> 
  summarise(n_harbours = n_distinct(hid_ln),
            n_gears = n_distinct(gid_ln),
            n_lid = n_distinct(.lid),
            .groups = "drop")
checks |> 
  count(n_harbours) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different harbours")
checks |> 
  count(n_gears) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different gear")
checks |> 
  count(n_lid) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different landings id (expect zero)")

## Station table ---------------------------------------------------------------
#  Here we will loose dual harbours and dual gears
LN <-  
  LN_raw |> 
  arrange(.lid, desc(wt)) |> # the most prevalent catch controls what
  #                            harbour and gear is kept
  select(vid, datel, gid_ln, hid_ln, .lid) |> 
  # here only keep one gid and hid record within a landings date
  distinct(vid, datel, .keep_all = TRUE)
### Checks, again
checks <- 
  LN |> 
  group_by(vid, datel) |> 
  summarise(n_harbours = n_distinct(hid_ln),
            n_gears = n_distinct(gid_ln),
            n_lid = n_distinct(.lid),
            .groups = "drop")
checks |> 
  count(n_harbours) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different harbours")
checks |> 
  count(n_gears) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different gear")
checks |> 
  count(n_lid) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different landings id (expect zero)")
LN_raw |> 
  filter(!.lid %in% LN$.lid) |> 
  knitr::kable(caption = "Number of landings id lost (expect zero)")
## Catch table -----------------------------------------------------------------
CA <- 
  LN_raw |> 
  group_by(.lid, sid) |> 
  summarise(wt = sum(wt),
            .groups = "drop")
## Save stuff ------------------------------------------------------------------
LN |> 
  arrange(vid, datel) |> 
  write_rds("data/landings/agf_stations.rds")
CA |> 
  write_rds("data/landings/agf_catch.rds")


# LODS landings ----------------------------------------------------------------
## Get data --------------------------------------------------------------------
LN_raw2 <- 
  omar::tbl_mar(con, "kvoti.lods_oslaegt") |> 
  rename(vid = skip_nr,
         hid = hofn,
         .id = komunr,
         date = l_dags,
         sid = fteg,
         wt = magn_oslaegt,
         gid = veidarf) |> 
  filter(wt > 0,
         year(date) %in% YEARS) |> 
  rename(hid_ln = hid,
         gid_ln = gid,
         datel = date) |> 
  collect(n = Inf) |> 
  filter(vid %in% c(2, 5:3699, 5000:9998)) |> 
  mutate(datel = as_date(datel),
         vid = as.integer(vid),
         gid_ln = as.integer(gid_ln),
         hid_ln = as.integer(hid_ln)) |> 
  arrange(vid, datel, .id, gid_ln, sid) |>
  group_by(vid, datel) |> 
  mutate(.lid = min(.id)) |> 
  ungroup() |> 
  # 2023-12-28: summarise the lot
  group_by(.lid, vid, datel, hid_ln, gid_ln, sid) |> 
  summarise(wt = sum(wt),
            .groups = "drop")

### Checks
checks <- 
  LN_raw2 |> 
  group_by(vid, datel) |> 
  summarise(n_harbours = n_distinct(hid_ln),
            n_gears = n_distinct(gid_ln),
            n_lid = n_distinct(.lid),
            .groups = "drop")
checks |> 
  count(n_harbours) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different harbours")
checks |> 
  count(n_gears) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different gear")
checks |> 
  count(n_lid) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different landings id (expect zero)")

## Station table ---------------------------------------------------------------
#  Here we will loose dual harbours and dual gears
LN2 <-  
  LN_raw2 |> 
  arrange(.lid, desc(wt)) |> # the most prevalent catch controls what
  #                            harbour and gear is kept
  select(vid, datel, gid_ln, hid_ln, .lid) |> 
  # here only keep one gid and hid record within a landings date
  distinct(vid, datel, .keep_all = TRUE)
### Checks, again
checks <- 
  LN2 |> 
  group_by(vid, datel) |> 
  summarise(n_harbours = n_distinct(hid_ln),
            n_gears = n_distinct(gid_ln),
            n_lid = n_distinct(.lid),
            .groups = "drop")
checks |> 
  count(n_harbours) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different harbours")
checks |> 
  count(n_gears) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different gear")
checks |> 
  count(n_lid) |> 
  mutate(p = round(n / sum(n), 3)) |> 
  knitr::kable(caption = "Same landings date, different landings id (expect zero)")
LN_raw2 |> 
  filter(!.lid %in% LN2$.lid) |> 
  knitr::kable(caption = "Number of landings id lost (expect zero)")
## Catch table -----------------------------------------------------------------
CA2 <- 
  LN_raw2 |> 
  group_by(.lid, sid) |> 
  summarise(wt = sum(wt),
            .groups = "drop")
## Save stuff ------------------------------------------------------------------
LN2 |> 
  arrange(vid, datel) |> 
  write_rds("data/landings/lods_stations.rds")
CA2 |> 
  write_rds("data/landings/lods_catch.rds")

# AGF-LODS crosschecks ---------------------------------------------------------
# Note that the record numbers from AGF and LODS are different
nrow(LN)
nrow(LN2)
check <- 
  LN |> 
  select(vid, datel, .lid_agf = .lid) |> 
  full_join(LN2 |> 
              select(vid, datel, .lid_lods = .lid)) |> 
  mutate(what = case_when(!is.na(.lid_agf) & !is.na(.lid_lods) ~ "both",
                          !is.na(.lid_agf) &  is.na(.lid_lods) ~ "lods",
                           is.na(.lid_agf) & !is.na(.lid_lods) ~ "agf",
                          .default = NA)) 
check |> count(what)
check |> 
  mutate(year = year(datel)) |> 
  count(year, what) |> 
  spread(what, n) |> 
  knitr::kable(caption = "Difference in vid-datel records in AGF vs LODS")
# So mostly in 2022 where 13% of records in AGF but not in LODS, needs checking

# Catch records are substantially different, likely the units in for the pelagics

sum(CA$wt)
sum(CA2$wt)
CA |> 
  group_by(sid) |> 
  summarise(wt_agf = sum(wt) / 1e6) |> 
  full_join(CA2 |> 
              group_by(sid) |> 
              summarise(wt_lods = sum(wt) / 1e6)) |> 
  mutate(diff = wt_agf - wt_lods,
         p = wt_lods / wt_agf) |> 
  knitr::kable(caption = "Difference in weight reported")

# agf gear ---------------------------------------------------------------------
tbl_mar(con, "agf.aflagrunnur_v") |> 
  select(starts_with("veidarfaeri")) |> 
  distinct() |> 
  rename(gid = 1,
         gid_id = 2,
         heiti = 3) |> 
  collect() |> 
  arrange(gid) |> 
  write_rds("data/landings/agf_gear.rds")


# Info -------------------------------------------------------------------------
toc()

print(devtools::session_info())
