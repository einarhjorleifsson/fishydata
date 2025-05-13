# nohup R < scripts/43_DATASET_ais_trail.R --vanilla > scripts/log/43_DATASET_ais_trail_2025-05-12.log &

# checkout: https://stackoverflow.com/questions/63821533/find-the-nearest-polygon-for-a-given-point
# pts <- st_join(pts, p, join = st_nearest_feature)
library(tictoc)
tic()
lubridate::now() |> print()

YEARS <- 2025:2007

library(conflicted)
library(traipse)
library(sf)
library(ramb)
library(arrow)
library(tidyverse)

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
conflicts_prefer(dplyr::lead)

# auxillary data ---------------------------------------------------------------
sf::sf_use_s2(use_s2 = FALSE)  # because eusm has invalids, thus st_join 
#                              #  creates error
island <- read_sf("data/auxillary/shoreline.gpkg")
ports <-  
  read_sf("~/stasi/fishydata/data/ports/ports.gpkg") |> 
  select(pid)
# DO LATER
if(FALSE) {
  eusm <- 
    read_sf("data/auxillary/eusm.gpkg") |> 
    select(.eusm = MSFD_BBHT)
  gebco <- 
    read_sf("data/auxillary/ICES_GEBCO.gpkg")
  ia <- 
    read_sf("data/auxillary/ICESareas.gpkg")
}

# Logbooks ---------------------------------------------------------------------
lb <- 
  read_parquet("data/logbooks/station-for-ais.parquet") |> 
  filter(year(date) %in% YEARS) |> 
  filter(!is.na(agf_gid))

# data -------------------------------------------------------------------------
stk_vid <- 
  open_dataset("data/vessels/stk_vessel_match.parquet") |> 
  to_duckdb() |> 
  select(mid, d1, d2, vid, mmsi) |> 
  filter(vid > 0) |> 
  filter(!vid %in% 3700:4999)
STK <- 
  open_dataset("data/ais/stk-raw") |> 
  to_duckdb() |> 
  filter(between(lon, -35, 30),
         between(lat, 50, 79)) |> 
  select(mid, time, lon, lat, speed, heading, hid, io, year) |> 
  inner_join(stk_vid,
             by = join_by(mid, between(time, d1, d2)))  |> 
  select(-c(d1, d2))
ASTD <- 
  open_dataset("data/ais/astd_isleez") |> 
  to_duckdb() |> 
  filter(between(lon, -35, 30),
         between(lat, 50, 79)) |> 
  select(vid, mmsi, time, lon, lat, speed, year) |> 
  inner_join(stk_vid |> select(vid, mmsi),
             by = join_by(vid, mmsi)) 

gears <-
  read_parquet("data/gear/gear_mapping.parquet") |> 
  select(agf_gid = gid_agf, met5, s1, s2)
# processing - year loop -------------------------------------------------------

for(y in YEARS) {
  print(y)
  stk <-
    STK |> 
    filter(year == y) |> 
   #filter(vid == 2359) |> 
    # when testing
    #filter(month(time) == 6) |> 
    collect() #|> 
  # Replace stk harbour with standardized harbour name 
  #left_join(harbours.standards,
  #          by = join_by(hid)) |> 
  #select(-c(hid)) |> 
  #rename(hid = pid)
  astd <- 
    ASTD |> 
    filter(year == y) |> 
    #filter(vid == 2359) |>
    collect() |> 
    # when testing
    # filter(month(time) == 6) |> 
    distinct(vid, time, .keep_all = TRUE)
  
  trail <- 
    bind_rows(astd |> mutate(source = "astd", mmsi = as.character(mmsi)),
              stk |> mutate(source = "stk")) |> 
    arrange(vid, time, source) |> 
    mutate(.rid = 1:n(), .before = vid) |> 
    st_as_sf(coords = c("lon", "lat"),
             crs = 4326,
             remove = FALSE) |> 
    st_join(ports) |> 
    st_join(island)
  # drop points on land (and where wrong stk hid?)
  trail2 <- 
    trail |> 
    mutate(where = case_when(on_land == TRUE &  is.na(pid) ~ "on_land",
                             on_land == TRUE & !is.na(pid) ~ "in_harbour",
                             is.na(on_land) &  !is.na(pid) ~ "in_harbour",
                             is.na(on_land) &   is.na(pid) ~ "at_sea",
                             .default = "bug")) |> 
    filter(where %in% c("in_harbour", "at_sea")) |> 
    select(-on_land) #|> 
  # drop records where stk hid is different than pid
  #filter(!(!is.na(hid) & !is.na(pid) & hid != pid))
  
  trail3 <- 
    trail2 |> 
    # hail mary
    #filter(is.na(io)) |> 
    group_by(vid) |> 
    mutate(.cid = ramb::rb_trip(!is.na(pid))) |> 
    # may want to do this after filtering whacky
    mutate(pid_dep = pid,
           pid_arr = pid) |> 
    fill(pid_dep, .direction = "downup") |> 
    fill(pid_arr, .direction = "updown") |> 
    ungroup()
  
  
  # drop duplicate time - here order is critical (first record is kept)
  trail4 <- 
    trail3 |> 
    # order matters for the distinct below
    arrange(vid, time, source, pid, io) |> 
    # have to drop geometry, because it is sticky
    st_drop_geometry() |> 
    distinct(vid, time, .keep_all = TRUE)
  
  # # include first and last point in harbour as part of trip
  # group_by(vid) |> 
  # mutate(.cid = case_when(.cid < 0 & lead(.cid) > 0 ~ lead(.cid),
  #                         .cid < 0 & lag(.cid)  > 0 ~ lag(.cid),
  #                         .default = .cid)) |> 
  # ungroup()
  
  # remove some whackies - the poor man's version
  trail5 <- 
    trail4 |> 
    group_by(vid, .cid) |>
    mutate(dd = traipse::track_distance(lon, lat)) |> 
    fill(dd) |> 
    ungroup() |> 
    filter(dd <= 1852 * 20) |> 
    select(.rid, vid, mmsi, .cid, source, time, lon, lat, speed, heading, pid, pid_dep, pid_arr, hid, io)
  
  if(FALSE) {
    trail5 |> 
      filter(.cid > 0) |> 
      group_by(vid) |> 
      ramb::rb_mapdeck(no_lines = FALSE, col = "source", radius = 100)
  }
  
  
  # can i generate trips
  if(FALSE) {trips <-
    trail5 |> 
    filter(.cid > 0) |> 
    group_by(vid, .cid) |> 
    mutate(dd = traipse::track_distance(lon, lat)) |> 
    fill(dd, .direction = "up") |> 
    summarise(T1 = min(time),
              hid1 = hid_dep[1],
              T2 = max(time),
              hid2 = hid_arr[1],
              pings = n(),
              dist = sum(dd) / 1852,
              .groups = "drop")
  trips |> filter(is.na(hid1) | is.na(hid2)) |> count(.cid)
  }
  
  trail6 <- 
    trail5 |> 
    group_by(vid, .cid) |> 
    mutate(whack = case_when(.cid > 0 ~ ramb::rb_whacky_speed(lon, lat, time),
                             .default = NA)) |> 
    ungroup()
  
  trail7 <- 
    trail6 |> 
    group_by(vid, .cid) |> 
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
  
  trail8 <- 
    trail7 |> 
    left_join(lb |> 
                filter(year(date) == y) |> 
                select(vid, .sid, lb_base, date, t1, t2, gid, datel, agf_date, agf_gid, agf_lid),
              by = join_by(vid, between(time, t1, t2)))
  # flag a trip if it is fishing
  trail8 <- 
    trail8 |> 
    mutate(gid_trip = case_when(!is.na(.sid) ~ agf_gid,
                                    .default = NA)) |> 
    group_by(vid, .cid) |> 
    fill(gid_trip, .direction = "downup") |> 
    ungroup()
    
  trail8 <- 
    trail8 |> 
    left_join(gears |> rename(gid_trip = agf_gid),
              by = join_by(gid_trip))
  
  trail8 |> 
    arrow::write_dataset(path = "~/stasi/fishydata/data/ais/trail",
                         format = "parquet",
                         existing_data_behavior = "overwrite",
                         partitioning = c("year"))
  
}

toc()
lubridate::now() |> print()
devtools::session_info()


