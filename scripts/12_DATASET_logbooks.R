# Objective --------------------------------------------------------------------
# Merge the old (schema afli) and the new logbook files (schema adb)
#
# Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < scripts/52_DATASET_logbooks.R --vanilla > scripts/log/52_DATASET_logbooks_2024-11-22.log &

## 2024-12-16
# * attempt to add grasleppa
## 2024-05-30
# * incorporated 02-2_logbooks-landings-coupling.R into this script
## 2024-05-29
# * Changed output dump, no longer saving rds
## 2024-03-08 changes
# * added gear 10 and 12 to mobile - approach like as is done for demersal seine
## 2024-02-12 changes
# * Go back to 2001
# * Set id for older logbooks to negative values
# * create both parquet and rds files, store in new subdirectories in data/logbooks
# * use the new vessel database

# Input:  Oracle database
#         data/landings/lods_stations.parquet
#         data/landings/agf_stations.parquet
# Output: data/logbooks/parquet/station.parquet
#         data/lgobooks/parquet/catch.parquet
# Downstream usage: R/02-2_logbooks-gear-correction.R

## Brief summary ---------------------------------------------------------------
# The main output file is just a flat file containing station information as 
#   well as basic effort information. The latter are for some historical reasons
#   stored in tables for different "gear types".
# In the merge, new database takes precedents over the old database for logbook
#  data for the same vessel on the same logbook date entry.
# In addition, the catch by species is also dumped
#
# Processing data loss are related to orphan effort files

# NOTE: If using years further back than 2009: need to double check that visir and station_id
#       are not the same.

library(tictoc)
tic()

lubridate::now()

# 1991 generates an error
YEARS <- c(2025:1973)

library(arrow)
library(data.table)
library(tidyverse)
library(lubridate)
library(omar)
#### Function
match_nearest_date <- function(lb, ln) {
  
  lb.dt <-
    lb %>%
    select(vid, datel) %>%
    distinct() %>%
    setDT()
  
  ln.dt <-
    ln %>%
    select(vid, datel) %>%
    distinct() %>%
    mutate(dummy = datel) %>%
    setDT()
  
  res <-
    lb.dt[, date.ln := ln.dt[lb.dt, dummy, on = c("vid", "datel"), roll = "nearest"]] %>%
    as_tibble()
  
  lb %>%
    left_join(res,
              by = c("vid", "datel")) %>%
    left_join(ln %>% select(vid, date.ln = datel, gid_ln, .lid),
              by = c("vid", "date.ln"))
  
}

con <- connect_mar()

if(FALSE) {
  GEARS <- 
    omar::gid_orri_plus(con) |> 
    collect(n = Inf) |> 
    rename(gclass = gid2,
           m4 = dcf4,
           m5 = dcf5b)
  GEARS_trim <-
    GEARS |> 
    select(gid, veidarfaeri, gclass, m4, m5)
}
# Only Icelandic vessels
q_vessels_icelandic <- 
  omar::tbl_mar(con, "vessel.vessel_v") |> 
  select(vid = registration_no) |> 
  filter(!vid %in% c(0, 1, 3:5, 9999)) %>% 
  filter(!between(vid, 3700, 4999))

# Read in landings data
LODS <- 
  open_dataset("~/stasi/fishydata/data/landings/lods_stations.parquet") |> 
  collect() |> 
  rename(datel = date,
         gid_ln = gid,
         hid_ln = hid)
AGF <-  
  open_dataset("~/stasi/fishydata/data/landings/agf_stations.parquet") |> 
  collect() |> 
  rename(datel = date,
         gid_ln = gid,
         hid_ln = hid)

# 1 Old logbooks ---------------------------------------------------------------
if(FALSE) {
  ## Mobile gear ----------------------------------------------------------------
  MOBILE_old <-
    omar::lb_mobile(con, trim = FALSE) |>
    filter(year %in% YEARS,
           # only towing gear
           gid %in% c(5, 6, 7, 8, 9, 10, 12, 14, 15, 38, 40)) |> 
    # limit to Icelandic vesssels
    inner_join(q_vessels_icelandic %>% select(vid),
               by = join_by(vid)) |> 
    select(visir, vid, gid, date, t1, t2, lon, lat, lon2, lat2, z1, z2, datel, effort, effort_unit,
           sweeps, plow_width) |> 
    collect(n = Inf) |> 
    mutate(table = "mobile",
           date = as_date(date),
           datel = as_date(datel))
  
  ## Static gear -----------------------------------------------------------------
  STATIC_old <-
    omar::lb_static(con, trim = TRUE) |> 
    filter(year %in% YEARS,
           gid %in% c(1, 2, 3)) |> 
    # limit to Icelandic vesssels
    inner_join(q_vessels_icelandic %>% select(vid),
               by = join_by(vid)) |> 
    select(visir, vid, gid, date, t0, t1, t2, lon, lat, lon2, lat2, z1, z2, datel, effort, effort_unit) |> 
    collect(n = Inf) |> 
    mutate(table = "static",
           date = as_date(date),
           datel = as_date(datel))
  
  ## Traps -----------------------------------------------------------------------
  TRAP_old <- 
    omar::lb_trap(con, trim = TRUE) |> 
    filter(year %in% YEARS,
           # only towing gear
           gid %in% c(18, 39))  |> 
    # limit to Icelandic vesssels
    inner_join(q_vessels_icelandic %>% select(vid),
               by = join_by(vid)) |> 
    select(visir, vid, gid, date, lon, lat, lon2, lat2, datel, effort, effort_unit) |> 
    collect(n = Inf) |> 
    mutate(table = "trap",
           date = as_date(date),
           datel = as_date(datel))
  
  ## Pelagic seine ---------------------------------------------------------------
  SEINE_old <-
    omar::lb_seine(con, trim = TRUE) |> 
    filter(year %in% YEARS,
           # only towing gear
           gid %in% c(10, 12))  |> 
    # limit to Icelandic vesssels
    inner_join(q_vessels_icelandic %>% select(vid),
               by = join_by(vid)) |> 
    select(visir, vid, gid, date, t1, lon, lat, datel, effort, effort_unit) |> 
    collect(n = Inf) |> 
    mutate(table = "seine",
           date = as_date(date),
           datel = as_date(datel))
  
  ## Lumpfish --------------------------------------------------------------------
  # 1980 -> 2016
  # JK: "from 2013-2016 coverage was low with logbooks available for, in some years, less than half the fleet."
  GRASL <-
    tbl_mar(con,'afli.grasl_sokn') |> 
    dplyr::left_join(tbl_mar(con,'afli.grasl_stofn'),
                     by = c("skipnr", "vear"))  |> 
    dplyr::mutate(sr = round(reitur / 10, 0),
                  uppruni_grasl = 'grasl_sokn') |> 
    dplyr::select(-vear) |> 
    collect (n = Inf) |> 
    mutate(vedags = as_date(vedags),
           roe = case_when(teg_verkunar == "1" ~ kg_hrogn,
                           teg_verkunar == "2" ~ kg_hrogn/0.936,
                           teg_verkunar == "3" ~ kg_hrogn/0.772,
                           teg_verkunar == "4" ~ kg_hrogn/0.813,
                           TRUE ~ NA)) %>% 
    #calculate whole fish weight from roe, if roe weight is missing, whole fish from number caught
    mutate(bio = case_when(!is.na(roe) ~ roe * 3.28,
                           TRUE ~ fj_grasl * 3.02)) |> 
    filter(year(vedags) >= 2001) |> 
    mutate(lon = geo::sr2d(sr)$lon,
           lat = geo::sr2d(sr)$lat) |> 
    arrange(vedags)
  GRASL <- 
    GRASL |> 
    mutate(dregin = ifelse(is.na(dregin), median(dregin, na.rm = TRUE), dregin),
           naetur = ifelse(is.na(naetur), median(naetur, na.rm = TRUE), naetur)) |> 
    mutate(visir = 1:n(),  # check
           gid = 25,
           effort = dregin * naetur,
           effort_unit = "netnights",
           table = "grasl",
           datel = vedags) |>    # check
    select(visir,
           vid = skipnr,
           gid,
           date = vedags,
           lon,
           lat,
           z1 = dypi, # in fathoms?
           datel,  # really need this
           effort,
           effort_unit,
           table,
           catch = bio)
  GRASL     
  
  # Need to split grasleppa into stofn and catch - issue is that visir is not
  #  a true thing, may create problems downstream
  
  
  ## Combine the logbooks --------------------------------------------------------
  LGS_old <-
    bind_rows(MOBILE_old,
              STATIC_old,
              SEINE_old,
              TRAP_old) |> 
    mutate(base = "old") |> 
    rename(.sid = visir)
  LGS_old <- 
    LGS_old |> 
    select(.sid, vid, gid, date, t1, t2, lon, lat, lon2, lat2, z1, z2,
           datel, effort, effort_unit, sweeps, plow_width, base, t0)
  
  ## The catch -------------------------------------------------------------------
  CATCH_old <-
    lb_catch(con) |> 
    collect(n = Inf) |> 
    rename(.sid = visir) |> 
    filter(.sid %in% LGS_old$.sid)
  
  ## Checks ----------------------------------------------------------------------
  ### Data loss ------------------------------------------------------------------
  n0 <- 
    lb_base(con) |> 
    filter(year %in% YEARS) |> 
    count() |> 
    collect() |> 
    pull(n)
  n1 <- nrow(LGS_old)
  print(paste0("Original settings: ", n0, " Settings retained: ", n1))
  print(paste0("Records lossed: ", n0-n1, " Proportion retained: ", n1/n0))
  
  ### Missingness ----------------------------------------------------------------
  LGS_old |> 
    mutate(no_effort = is.na(effort)) |> 
    count(gid, no_effort) |> 
    spread(no_effort, n) |> 
    knitr::kable()
  
  LGS_old   |> write_parquet("~/stasi/fishydata/data-raw/logbooks/LGS_old.parquet")
  CATCH_old |> write_parquet("~/stasi/fishydata/data-raw/logbooks/CATCH_old.parquet")
} else {
  LGS_old <-   read_parquet("~/stasi/fishydata/data-raw/logbooks/LGS_old.parquet")
  CATCH_old <- read_parquet("~/stasi/fishydata/data-raw/logbooks/CATCH_old.parquet")
}
# 2 New logbooks ---------------------------------------------------------------
# The new logbooks are in principle a total mess that need to be fixed upstream
#  Following is thus just an interrim hack. The function call to the new data
#  are a little different since it is still in development.

## Functions -------------------------------------------------------------------
# should possible move functions to the omar-package
lb_trip_new <- function(con) {
  tbl_mar(con, "adb.trip_v") |> 
    select(trip_id, 
           vid = vessel_no,
           T1 = departure,
           hid1 = departure_port_no,
           T2 = landing,
           hid2 = landing_port_no,
           source)
}
lb_station_new0 <- function(con) {
  tbl_mar(con, "adb.station_v") |> 
    select(trip_id,
           station_id,
           gid = gear_no,
           t1 = fishing_start,
           t2 = fishing_end,
           lon = longitude,
           lat = latitude,
           lon2 = longitude_end,
           lat2 = latitude_end,
           z1 = depth,
           z2 = depth_end,
           tow_start,
           everything())
}
lb_base_new <- function(con) {
  lb_trip_new(con) |> 
    inner_join(lb_station_new0(con) |> 
                 select(trip_id:tow_start),
               by = "trip_id") |> 
    select(vid, gid, t1:tow_start, everything()) |> 
    mutate(whack = case_when(between(lon, 10, 30) & between(lat, 62.5, 67.6) ~ "mirror",
                             between(lon, -3, 3) & gid != 7 ~ "ghost",
                             .default = "ok"),
           lon = ifelse(whack == "mirror",
                        -lon,
                        lon),
           lon2 = ifelse(whack == "mirror",
                         -lon2,
                         lon2))
}
lb_catch_new <- function(con) {
  tbl_mar(con, "adb.catch") |> 
    mutate(catch = case_when(condition == "GUTT" ~ quantity / 0.8,
                             condition == "UNGU" ~ quantity,
                             .default = NA)) |> 
    select(station_id = fishing_station_id,
           sid = species_no, 
           catch, 
           weight, 
           quantity, 
           condition, 
           catch_type = source_type)
}


## Only records not in old logbooks --------------------------------------------
BASE_new <- 
  lb_base_new(con) |> 
  filter(year(t1) >= 2021) |> 
  filter(year(t1) %in% YEARS) |> 
  collect(n = Inf) |> 
  select(vid:z2, trip_id, datel = T2, source:whack) |> 
  mutate(date = as_date(t1),
         datel = as_date(datel),
         base = "new")
BASE_new_n0 <- nrow(BASE_new)
# only data where the date fishing and vessels are not already in the old
#  logbooks. This reduces the number of records from ~212 thousand to
#  ~88 thousand
BASE_new <-
  BASE_new |> 
  left_join(LGS_old |> 
              select(vid, date) |> 
              distinct() |> 
              mutate(in.old = "yes"),
            #multiple = "all",
            by = join_by(vid, date)) |> 
  mutate(in.old = replace_na(in.old, "no"))
BASE_new |> 
  count(source, in.old) |> 
  spread(in.old, n) |> 
  knitr::kable(caption = "Number of records in new database that are also in the old database.")
BASE_new <- 
  BASE_new |> 
  filter(in.old == "no") |> 
  select(-in.old)

## Checks ----------------------------------------------------------------------
### Should one remove whacks?? - not if using positions from ais ---------------
#   mirror: record where lon is positive but should be negative
#   ghost: records around the meridian
BASE_new |> 
  count(source, whack) |> 
  spread(whack, n)
### Any abberrant trend in the number of sets by month? ------------------------
if(FALSE) {
  bind_rows(
    LGS_old |>   select(gid, date, base),
    BASE_new  |> select(gid, date, base)) |> 
    left_join(GEARS_trim |> mutate(gclass = paste(str_pad(gid, 2, pad = "0"), veidarfaeri)) |> select(gid, gclass)) |> 
    mutate(date = floor_date(date, "month")) |> 
    count(date, gclass) |> 
    filter(year(date) %in% 2018:2022) |> 
    ggplot(aes(date, n)) +
    geom_point(size = 0.5) +
    facet_wrap(~ gclass, scales = "free_y")
}


## Mobile gear -----------------------------------------------------------------
MOBILE_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.trawl_and_seine_net_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(effort = case_when(gid %in% c(6, 7) ~ as.numeric(difftime(t2, t1, units = "hours")),
                            gid %in% 5 ~ 1,
                            .default = NA),
         effort_unit = case_when(gid %in% c(6, 7) ~ "hours towed",
                                 gid %in% 5  ~  "setting",
                                 .default = NA)) |> 
  rename(sweeps = bridle_length) |> 
  select(station_id, sweeps, effort, effort_unit)
## Static gear -----------------------------------------------------------------
STATIC_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.line_and_net_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(dt = as.numeric(difftime(t2, t1, unit = "hours")),
         effort = case_when(gid == 3 ~ dt,
                            gid %in% c(2, 11, 25, 29, 91, 92) ~ dt/24 * nets,
                            gid %in% 1 ~ hooks,
                            .default = NA),
         effort_unit = case_when(gid == 3 ~ "hookhours",
                                 gid %in% c(2, 11, 25, 29, 91, 92) ~ "netnights",
                                 gid %in% 1 ~ "hooks",
                                 .default = NA)) |> 
  select(station_id, effort, effort_unit)
## Dredge gear -----------------------------------------------------------------
DREDGE_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.dredge_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(effort = as.numeric(difftime(t2, t1, units = "hours")),
         effort_unit = "hours towed",
         plow_width = 2) |> 
  select(station_id, plow_width, effort, effort_unit)
## Trap gear -------------------------------------------------------------------
TRAP_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.trap_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(dt = as.numeric(difftime(t2, t1, units = "hours")),
         effort = dt * number_of_traps,
         effort_unit = "trap hours") |> 
  select(station_id, effort, effort_unit)
## Seine gear ------------------------------------------------------------------
SEINE_new <- 
  BASE_new |> 
  inner_join(
    tbl_mar(con, "adb.surrounding_net_v") |> collect(n = Inf),
    by = join_by(station_id)
  ) |> 
  mutate(effort = 1,
         effort_unit = "setting") |> 
  select(station_id, effort, effort_unit)

BASE_new_aux <- 
  bind_rows(MOBILE_new,
            STATIC_new,
            DREDGE_new,
            TRAP_new,
            SEINE_new)

## Check -----------------------------------------------------------------------
### Orphan effort files --------------------------------------------------------
n1 <- nrow(BASE_new)
n2 <- nrow(BASE_new_aux)
print(paste0("Records in base: ", n1, " Records in auxillary: ", n2))
BASE_new |> 
  mutate(orphan = ifelse(station_id %in% BASE_new_aux$station_id, "no", "yes")) |> 
  count(source, orphan) |> 
  spread(orphan, n) |> 
  knitr::kable(caption = "Source of effort orphan files")
BASE_new |> 
  mutate(orphan = ifelse(station_id %in% BASE_new_aux$station_id, "no", "yes")) |> 
  filter(orphan == "yes") |> 
  count(source, gid) |> 
  spread(gid, n) |> 
  knitr::kable(caption = "Gear list of effort orphan files")

## Combine the (new) logbooks --------------------------------------------------
LGS_new <- 
  BASE_new |> 
  left_join(BASE_new_aux,
            by = join_by(station_id)) |> 
  select(.sid = station_id, vid, gid, date, t1, t2, lon, lat, lon2, lat2, z1, z2,
         datel, effort, effort_unit, sweeps, plow_width, base)

## Catch -----------------------------------------------------------------------
CATCH_new <- 
  lb_catch_new(con) |> 
  collect(n = Inf) |> 
  filter(station_id %in% BASE_new$station_id) |> 
  select(.sid = station_id, sid, catch)

# Do we have to worry about id (visir)
LGS_old |> 
  select(.sid, base) |> 
  mutate(.sid.in.new = ifelse(.sid %in% LGS_new$.sid,
                              "screeeeeeam",
                              "ok")) |> 
  count(.sid.in.new)
LGS_new |> 
  select(.sid, base) |> 
  mutate(.sid.in.old = ifelse(.sid %in% LGS_old$.sid,
                              "screeeeeeam",
                              "ok")) |> 
  count(.sid.in.old)
LGS_old |> 
  select(.sid, base) |> 
  mutate(.sid.in.new = ifelse(.sid %in% LGS_new$.sid,
                              "screeeeeeam",
                              "ok")) |> 
  filter(.sid.in.new == "screeeeeeam")

# 3. Merge the old and the new logbooks ----------------------------------------
LGS <- 
  bind_rows(LGS_old |> rename(lb_base = base),
            LGS_new |> rename(lb_base = base))
CATCH <- 
  bind_rows(CATCH_old |> mutate(lb_base = "old"),
            CATCH_new |> mutate(lb_base = "new"))

# 4. Add "target" species to each setting --------------------------------------
#  Used downstream when attempting to correct gear
catch_target <- 
  CATCH |> 
  group_by(.sid, lb_base) |> 
  mutate(total = sum(catch, na.rm = TRUE),
         p = ifelse(total > 0, catch / total, NA),
         .groups = "drop") |> 
  # highest catch, lowest species number
  #   ... so in case of equal proportion, species 1 rules over 2 over ...
  arrange(.sid, lb_base, desc(p), sid) |> 
  group_by(.sid, lb_base) |> 
  slice(1) |> 
  ungroup() |> 
  select(.sid, lb_base, sid_target = sid, p_target = p, catch_total = total)

print(paste0("Number of logbook records with no catch: ", 
             nrow(LGS) - nrow(catch_target),
             " (",
             100 * (1 - round(nrow(catch_target) / nrow(LGS), 3)),
             "%)"))
LGS <- 
  LGS |> 
  left_join(catch_target,
            by = join_by(.sid, lb_base)) |> 
  mutate(sid_target = replace_na(sid_target, 0),
         p_target = replace_na(p_target, 0),
         catch_total = replace_na(catch_total, 0))

# 5. Add landings id and gear --------------------------------------------------
# Match landings id and landings gear to logbooks
# The matching is done by date not time. Landings date have hence been consolidated
#  by date, the landings id is the lowest landings id value within a date


## Landings data - agf ---------------------------------------------------------
# The AGF data starts 2007-09-01 so:
#  split the data by that date and join only the latter period with AGF
n_before_nearest_match <- nrow(LGS)
LGS1 <- LGS |> filter(datel < ymd("2007-09-01"))
LGS2 <- LGS |> filter(datel >= ymd("2007-09-01"))

LGS2 <-
  LGS2 |> 
  match_nearest_date(AGF) |> 
  rename(date_ln = date.ln)
# add the harbour id
LGS2 <- 
  LGS2 |> 
  left_join(AGF |> select(.lid, hid_ln))


LGS <- 
  bind_rows(LGS1, LGS2)
n_after_nearest_match <- nrow(LGS)
print(c(n_before_nearest_match, n_after_nearest_match))

LGS <- 
  LGS |> 
  rename(date_agf = date_ln,
         gid_agf = gid_ln,
         hid_agf = hid_ln,
         .lid_agf = .lid)
### Checks ---------------------------------------------------------------------
LGS |> 
  mutate(has.lid = !is.na(.lid_agf)) |> 
  count(has.lid) |> 
  mutate(p = n / sum(n)) |> 
  knitr::kable(caption = "Missing AGF landings id")
LGS |> 
  filter(is.na(.lid_agf)) |> 
  mutate(year = year(date)) |> 
  count(year) |> 
  knitr::kable(caption = "Missing AGF landings id by year (high numbers prior to 2008 expected)")

LGS |> 
  filter(year(date) >= 2008) |> 
  mutate(dt = as.integer(difftime(datel, date_agf, units = "days")),
         dt = ifelse(dt <= -5, -5, dt),
         dt = ifelse(dt >=  5,  5, dt)) |> 
  count(dt) |> 
  mutate(p = n / sum(n) * 100,
         pc = cumsum(p),
         p = round(p, 2),
         pc = round(pc, 2)) |> 
  knitr::kable(caption = "Difference in matched logbook and landings dates")

## Landings data - kvoti-lods --------------------------------------------------
n_before_nearest_match <- nrow(LGS)
LGS <-
  LGS |> 
  match_nearest_date(LODS) |> 
  rename(date_ln = date.ln)
n_after_nearest_match <- nrow(LGS)
print(c(n_before_nearest_match, n_after_nearest_match))
LGS <- 
  LGS |> 
  rename(date_lods = date_ln,
         gid_lods = gid_ln,
         .lid_lods = .lid)
### Checks ---------------------------------------------------------------------
LGS |> 
  mutate(has.lid = !is.na(.lid_lods)) |> 
  count(has.lid) |> 
  mutate(p = n / sum(n)) |> 
  knitr::kable(caption = "Missing landings id")
LGS |> 
  mutate(dt = as.integer(difftime(datel, date_lods, units = "days")),
         dt = ifelse(dt <= -5, -5, dt),
         dt = ifelse(dt >=  5,  5, dt)) |> 
  count(dt) |> 
  mutate(p = n / sum(n) * 100,
         pc = cumsum(p),
         p = round(p, 2),
         pc = round(pc, 2)) |> 
  knitr::kable(caption = "Difference in matched logbook and landings dates")

v <- 
  read_parquet("~/stasi/fishydata/data/vessels/vessels_iceland.parquet") |> 
  select(vid, mmsi, mmsi_t1, mmsi_t2) |> 
  filter(!is.na(mmsi)) |> 
  mutate(mmsi = as.integer(mmsi))
LGS <-
  LGS |> 
  left_join(v,
            by = join_by(vid, between(date, mmsi_t1, mmsi_t2)))

# 6A. Corrections --------------------------------------------------------------
# Known issues:
#     Ísafjarðardjúp - botnvarpa -> rækjuvarpa
LGS <- 
  LGS |> 
  mutate(gid_agf = case_when(gid_agf == 2 & sid_target == 22 ~ 22,    # þorskfisknet -> grálúðunet
                             gid_agf == 6 & sid_target == 40 ~ 7,     # botnvarpa -> humarvarpa
                             gid_agf == 6 & sid_target == 41 ~ 8,     # botnvarpa -> rækjuvarpa
                             gid_agf == 6 & sid_target == 11 ~ 9,     # botnvarpa -> flotvarpa
                             gid_agf == 9 & sid_target %in% c(1:9, 21:29) ~ 6,  # flotvarpa -> botnvarpa
                             .default = gid_agf))
# Cap sweeps -------------------------------------------------------------------
LGS <- 
  LGS |> 
  mutate(sweeps = case_when(gid_agf %in% c(1:5, 10:14, 16:23) ~ NA,
                            gid_agf == 6 & sweeps > 250 ~ 250,
                            gid_agf == 7 & sweeps > 100 ~ 100,
                            gid_agf == 8 & sweeps > 100 ~ 100,
                            gid_agf == 9 & sweeps > 250 ~ 250,
                            gid_agf == 15 & sweeps > 10 ~ 10,
                            .default = sweeps))
# What to do with missing sweeps
LGS |> 
  filter(gid_agf %in% c(6:9, 15)) |> 
  mutate(has.sweeps = !is.na(sweeps)) |> 
  count(gid_agf, has.sweeps) |> 
  group_by(gid_agf) |> 
  mutate(p = round(n / sum(n) * 100, 2)) |> 
  knitr::kable(caption = "Missing sweeps")
LGS |> 
  filter(gid_agf %in% c(6:9, 15)) |> 
  filter(between(year(date), 2009, 2024)) |> 
  group_by(gid_agf) |> 
  summarise(median = median(sweeps, na.rm = TRUE)) |> 
  ungroup()
LGS <- 
  LGS |> 
  mutate(sweeps = case_when(gid_agf == 6 & is.na(sweeps) ~ 100,
                            gid_agf == 7 & is.na(sweeps) ~  45,
                            gid_agf == 8 & is.na(sweeps) ~  28,
                            gid_agf == 9 & is.na(sweeps) ~  80,
                            gid_agf == 15 & is.na(sweeps) ~  1,
                            .default = sweeps)) |> 
  mutate(sweeps = case_when(gid_agf == 6 & sweeps < 40 ~ 40,
                            gid_agf == 7 & sweeps < 25 ~ 25,
                            gid_agf == 8 & sweeps < 10 ~ 10,
                            gid_agf == 9 & sweeps < 50 ~ 50,
                            gid_agf == 15 & sweeps < 1 ~ 1,
                            .default = sweeps))

# 6. Save the stuff ------------------------------------------------------------

LGS   |> arrow::write_parquet("~/stasi/fishydata/data/logbooks/stations.parquet")
CATCH |> arrow::write_parquet("~/stasi/fishydata/data/logbooks/catch.parquet")
LGS   |> arrow::write_parquet("/u3/haf//stasi/fishydata/data/logbooks/stations.parquet")
CATCH |> arrow::write_parquet("/u3/haf//stasi/fishydata/data/logbooks/catch.parquet")

# 7. Issues --------------------------------------------------------------------
# We should cap the effort "a priori", check this:

lgs <- 
  read_parquet("~/stasi/fishydata/data/logbooks/stations.parquet")
lgs |> 
  filter(between(year(date), 2009, 2024)) |> 
  mutate(dt = difftime(t2, t1, units = "mins"),
         dt = as.numeric(dt)) |>  
  filter(dt > 0) |> 
  group_by(gid_agf) |> 
  mutate(dt = ifelse(dt > quantile(dt, 0.99), quantile(dt, 0.99), dt)) |> 
  ggplot(aes(dt / 60)) +
  geom_histogram() +
  facet_wrap(~ gid_agf, scales = "free")


# 8. For ais -------------------------------------------------------------------
lb <- 
  read_parquet("~/stasi/fishydata/data/logbooks/stations.parquet") |> 
  filter(year(date) >= 2008,
         vid > 5) |> 
  filter(!vid %in% 3700:4999)
ca <- read_parquet("~/stasi/fishydata/data/logbooks/catch.parquet")


## 8.1 median efort if missing and cap effort ----------------------------------
median.effort <- 
  lb %>% 
  group_by(gid) %>% 
  summarise(median = median(effort, na.rm = TRUE)) %>% 
  drop_na()
lb <- 
  lb %>% 
  left_join(median.effort, by = "gid") %>% 
  mutate(effort = ifelse(!is.na(effort), effort, median)) %>% 
  select(-median) %>% 
  # cap effort hours
  # TODO: DO THIS UPSTREAM
  mutate(effort = case_when(effort > 50000 & gid == 1 ~ 50000,
                            effort < 0 & gid == 2 ~ 50,
                            effort > 1000 & gid == 2 ~ 1000,
                            effort < 0 & gid == 3 ~ 10,
                            effort > 100 & gid == 3 ~ 100,
                            effort > 12 & gid ==  6 ~ 12,
                            effort > 24 & gid ==  7 ~ 24,
                            effort > 10 & gid == 9 ~ 10,
                            effort > 25 & gid == 11 ~ 25,
                            effort > 15 & gid == 14 ~ 15,
                            effort > 15 & gid == 15 ~ 15,
                            effort > 20000 & gid == 18 ~ 20000,
                            effort < 0 & gid == 25 ~ 500,
                            effort > 500 & gid == 25 ~ 500,
                            effort > 400 & gid == 29 ~ 400,
                            effort > 10 & gid == 38 ~ 10,
                            effort > 10 & gid == 40 ~ 10,
                            effort > 5 & gid == 52 ~ 5,
                            effort > 2 & gid == 53 ~ 2,
                            effort > 50 & gid == 91 ~ 50,
                            effort > 2000 & gid == 92 ~ 2000,
                            TRUE ~ effort))

## 8.2 Collapse records with missing t1 or t2 to daily records -----------------
# first an overview by gear
lb |> 
  mutate(flag = ifelse(!is.na(t1) & !is.na(t2), "has_t1_t2", "rest")) |> 
  count(gid, flag) |> 
  spread(flag, n) |> 
  mutate(total = has_t1_t2 + rest,
         p = has_t1_t2 / total) |> 
  knitr::kable()

lb <- 
  lb |> 
  select(.sid, lb_base, vid, gid, date, t1, t2, datel, effort, effort_unit,
         sweeps, plow_width, catch_total,
         agf_date = date_agf,
         agf_gid = gid_agf,
         agf_lid = .lid_agf,
         lods_date = date_lods,
         lods_gid = gid_lods,
         lods_lid = .lid_lods)

lb_t1_t2 <- 
  lb |> 
  filter(!is.na(t1) & !is.na(t2)) |> 
  mutate(n.sids = 1,
         .after = lb_base)
lb_rest <- 
  lb |> 
  filter(!.sid %in% lb_t1_t2$.sid)

# now for the catches
ca_t1_t2 <-
  ca |> 
  filter(.sid %in% lb_t1_t2$.sid)
ca_rest <- 
  lb_rest |> 
  select(.sid, lb_base, vid, date) |> 
  left_join(ca |> 
              filter(!.sid %in% lb_t1_t2$.sid),
            relationship = "many-to-many") |> 
  group_by(vid, date, lb_base, sid) %>%
  summarise(.sid = min(.sid),
            catch = sum(catch, na.rm = TRUE),
            .groups = "drop") |> 
  select(.sid, lb_base, sid, catch)
ca <- 
  bind_rows(ca_t1_t2, ca_rest)


lb_rest <- 
  lb_rest |> 
  group_by(lb_base, vid, gid, date, datel, effort_unit,
           agf_date, agf_gid, agf_lid, lods_date, lods_gid, lods_lid) |> 
  # get here all essential variables that are needed downstream
  summarise(.sid = min(.sid),
            n.sids = n(),
            effort = sum(effort, na.rm = TRUE),
            catch_total = sum(catch_total, na.rm = TRUE),
            sweeps = mean(sweeps, na.rm = TRUE),
            plow_width = mean(plow_width, na.rm = TRUE),
            .groups = "drop") %>%
  mutate(t1 = ymd_hms(paste0(year(date), "-", month(date), "-", day(date),
                             " 00:00:00")),
         t2 = ymd_hms(paste0(year(date), "-", month(date), "-", day(date),
                             " 23:59:00")))
lb <-
  bind_rows(lb_t1_t2,
            lb_rest) |> 
  arrange(vid, t1)

# Save -------------------------------------------------------------------------

lb |> write_parquet("~/stasi/fishydata/data/logbooks/station-for-ais.parquet")
ca |> write_parquet("~/stasi/fishydata/data/logbooks/catch-for-ais.parquet")


# 8. Info ----------------------------------------------------------------------
toc()

print(devtools::session_info())

