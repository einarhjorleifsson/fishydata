# nohup R < scripts/43_DATASET_ais_trail.R --vanilla > scripts/log/43_DATASET_ais_trail_2025-07-18.log &

# TODO:
#  * Move all spatial joins upstream
#  * Suggest at the same time to merge the STK and ASTD data at the astd_isleez-step
#  * Reason: This stuff should only been done once in a while from scratch
#            leaving more time to fiddle with potential downstream coding
#            problems without the time consuming spatial join stuff
#  * Here we could also generate the trip id stuff, label whacky distance and speed points
#  * Would be nice to extend the points on land analysis beyound Iceland - but then what about rivers

# checkout: https://stackoverflow.com/questions/63821533/find-the-nearest-polygon-for-a-given-point
# pts <- st_join(pts, p, join = st_nearest_feature)
library(tictoc)
tic.clearlog()
tic(msg = "seeding")
time_logging <- list()
counter <- 0

lubridate::now() |> print()

YEARS <- 2025:2007

library(conflicted)
library(traipse)
library(ramb)
library(duckdbfs)
library(tidyverse)

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
conflicts_prefer(dplyr::lead)

# auxillary data ---------------------------------------------------------------


# Logbooks ---------------------------------------------------------------------
lb <- 
  nanoparquet::read_parquet("data/logbooks/station-for-ais.parquet") |> 
  filter(year(date) %in% YEARS) |> 
  filter(!is.na(agf_gid)) |> 
  left_join(nanoparquet::read_parquet("data/logbooks/catch-for-ais.parquet") |> 
              group_by(.sid, lb_base) |> 
              summarise(catch = sum(catch, na.rm = TRUE),
                        .groups = "drop"),
            by = join_by(.sid, lb_base)) |> 
  mutate(catch.info = case_when(is.na(catch) ~ "no",
                               catch == 0 ~ "zero",
                               !is.na(catch) ~ "yes",
                               .default = "something else")) |> 
  filter(!vid %in% c(8008, 9908))
vessels <- 
  nanoparquet::read_parquet("data/vessels/vessels_iceland.parquet") |> 
  filter(!vid %in% 3700:4999) |> 
  select(vid, loa, kw) |> 
  filter(vid %in% unique(lb$vid))

# data -------------------------------------------------------------------------
stk_vid <- 
  open_dataset("data/vessels/stk_vessel_match.parquet") |> 
  select(mid, d1, d2, vid, mmsi) |> 
  filter(vid > 0) |> 
  filter(!vid %in% 3700:4999)
STK <- 
  open_dataset("data/ais/stk-raw") |> 
  filter(between(lon, -35, 30),
         between(lat, 50, 79)) |>
  select(mid, time, lon, lat, speed, heading, hid, io,
         .pid:year) |> 
  inner_join(stk_vid,
             by = join_by(mid, between(time, d1, d2)))  |> 
  select(-c(d1, d2))
ASTD <- 
  open_dataset("data/ais/astd_isleez") |> 
  filter(between(lon, -35, 30),
         between(lat, 50, 79)) |> 
  select(vid, mmsi, time, lon, lat, speed, 
         .pid = pid,
         .iceland:year) |> 
  inner_join(stk_vid |> select(vid, mmsi),
             by = join_by(vid, mmsi))
# Table contains agf_gid and metier5
gears <-
  nanoparquet::read_parquet("data/gear/gear_mapping.parquet") |> 
  select(agf_gid = gid_agf, met5, met6, s1, s2)

toc(log = TRUE, quiet = FALSE)

# processing - year loop -------------------------------------------------------

for(y in YEARS) {
  
  print(paste0(y, "  ----------------------------------------"))
  tic(msg = "Import STK and ASTD and bind")
  stk <-
    STK |> 
    filter(year == y) |> 
    collect()
  astd <- 
    ASTD |> 
    filter(year == y) |> 
    collect() |> 
    distinct(vid, time, .keep_all = TRUE)
  trail <- 
    bind_rows(astd |> mutate(source = "astd", mmsi = as.character(mmsi)),
              stk |> mutate(source = "stk")) |> 
    arrange(vid, time, source) |> 
    mutate(.rid = 1:n(), .before = vid)
  toc(log = TRUE, quiet = FALSE)
  rm(stk)
  rm(astd)

  # drop points on land (and where wrong stk hid?)
  tic(msg = "Remove points on (Ice)land")
  trail <- 
    trail |> 
    mutate(where = case_when(.iceland == TRUE &  is.na(.pid) ~ "on_land",
                             .iceland == TRUE & !is.na(.pid) ~ "in_harbour",
                             is.na(.iceland) &  !is.na(.pid) ~ "in_harbour",
                             is.na(.iceland) &   is.na(.pid) ~ "at_sea",
                             .default = "bug")) |> 
    filter(where %in% c("in_harbour", "at_sea")) |> 
    select(-.iceland) 
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "Create unique trip id per vessel per year, negative in if in harbour")
  trail <- 
    trail |> 
    # hail mary
    #filter(is.na(io)) |> 
    group_by(vid) |> 
    mutate(.cid = ramb::rb_trip(!is.na(.pid))) |> 
    # may want to do this after filtering whacky
    mutate(pid_dep = .pid,
           pid_arr = .pid) |> 
    fill(pid_dep, .direction = "downup") |> 
    fill(pid_arr, .direction = "updown") |> 
    ungroup()
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "Distinct vessel-time")
  # drop duplicate time - here order is critical (first record is kept)
  trail <- 
    trail |> 
    # order matters for the distinct below
    arrange(vid, time, source, .pid, io) |> 
    distinct(vid, time, .keep_all = TRUE)
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "Filter out 'distant' points")
  # remove some whackies - the poor man's version
  trail <- 
    trail |> 
    group_by(vid, .cid) |>
    mutate(dd = traipse::track_distance(lon, lat)) |> 
    fill(dd) |> 
    ungroup() |> 
    filter(dd <= 1852 * 20)
  toc(log = TRUE, quiet = FALSE)

  tic(msg = "Derive and label whacky speed")
  trail <- 
    trail |> 
    group_by(vid, .cid) |> 
    mutate(whack = case_when(.cid > 0 ~ ramb::rb_whacky_speed(lon, lat, time),
                             .default = FALSE)) |> 
    ungroup()
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "Calculate step time and distance")
  trail <- 
    trail |> 
    # SHOULD ONE GROUP BY whack?
    group_by(vid, .cid, whack) |> 
    mutate(dt = track_time(time),
           dd = track_distance(lon, lat)) |> 
    fill(dt, .direction = "up") |> 
    fill(dd, .direction = "up") |> 
    mutate(ct = cumsum(dt),
           cd = cumsum(dd)) |> 
    ungroup() |> 
    mutate(year = year(time),
           month = month(time)) |> 
    rename(hid_stk = hid, io_stk = io)
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "Join with logbooks")
  trail <- 
    trail |> 
    left_join(lb |> 
                filter(year(date) == y) |> 
                select(vid, .sid, lb_base, date, t1, t2, gid, datel, agf_date, agf_gid, agf_lid,
                       catch_total = catch, catch.info),
              by = join_by(vid, between(time, t1, t2)))
  # flag a trip if it is fishing
  trail <- 
    trail |> 
    mutate(gid_trip = case_when(!is.na(.sid) ~ agf_gid,
                                .default = NA)) |> 
    group_by(vid, .cid) |> 
    fill(gid_trip, .direction = "downup") |> 
    ungroup()
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "Join with vessel")
  trail <- 
    trail |> 
    left_join(vessels,
              by = join_by(vid))
  
  tic(msg = "Join with gear")
  trail <- 
    trail |> 
    left_join(gears |> rename(gid_trip = agf_gid),
              by = join_by(gid_trip))
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "Export to parquet")
  trail |> 
    arrow::write_dataset(path = "~/stasi/fishydata/data/ais/trail",
                         format = "parquet",
                         existing_data_behavior = "overwrite",
                         partitioning = c("year"))
  toc(log = TRUE, quiet = FALSE)
  
  counter <- counter + 1
  time_logging[[counter]] <-
    tic.log(format = FALSE) |> 
    map_df(as_tibble) |> 
    mutate(year = y)
  tic.clearlog()
}

time_logging |> write_rds("time_logging.rds")
lubridate::now() |> print()
devtools::session_info()
