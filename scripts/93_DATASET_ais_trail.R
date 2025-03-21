# nohup R < scripts/93_DATASET_ais_trail.R --vanilla > scripts/log/93_DATASET_ais_trail_2025-03-20.log &

# checkout: https://stackoverflow.com/questions/63821533/find-the-nearest-polygon-for-a-given-point
# pts <- st_join(pts, p, join = st_nearest_feature)
YEARS <- 2024:2007

library(conflicted)
library(traipse)
library(sf)
library(arrow)
library(tidyverse)

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
conflicts_prefer(dplyr::lead)

# auxillary data ---------------------------------------------------------------
island <- read_sf("~/stasi/gis/AIS_TRAIL/data-raw/island.gpkg")
harbour <- 
  read_sf("~/stasi/gis/AIS_TRAIL/data-raw/harbours-hidstd.gpkg") |> 
  select(hid_std, harbour)
harbours.standards <- 
  read_csv("~/stasi/fishydata/data-raw/harbours/stk_harbours.csv") |> 
  select(hid, hid_std)
# data -------------------------------------------------------------------------
stk_vid <- 
  open_dataset("~/stasi/fishydata/data/vessels/stk_vessel_match.parquet") |> 
  to_duckdb() |> 
  select(mid, d1, d2, vid, mmsi) |> 
  filter(vid > 0) |> 
  filter(!vid %in% 3700:4999)
STK <- 
  open_dataset("~/stasi/fishydata/data/ais/stk-raw") |> 
  to_duckdb() |> 
  filter(between(lon, -35, 30),
         between(lat, 50, 79)) |> 
  select(mid, time, lon, lat, speed, heading, hid, io, year) |> 
  inner_join(stk_vid,
             by = join_by(mid, between(time, d1, d2)))  |> 
  select(-c(d1, d2))
ASTD <- 
  open_dataset("~/stasi/fishydata/data/ais/astd_isleez") |> 
  to_duckdb() |> 
  filter(between(lon, -35, 30),
         between(lat, 50, 79)) |> 
  select(vid, mmsi, time, lon, lat, speed, year) |> 
  inner_join(stk_vid |> select(vid, mmsi),
             by = join_by(vid, mmsi)) 


# processing - year loop -------------------------------------------------------

for(y in YEARS) {
  stk <-
    STK |> 
    filter(year == y) |> 
    # when testing
    #filter(month(time) == 6) |> 
    collect() |> 
    # Replace stk harbour with standardized harbour name 
    left_join(harbours.standards,
              by = join_by(hid)) |> 
    select(-c(hid)) |> 
    rename(hid = hid_std)
  astd <- 
    ASTD |> 
    filter(year == y) |> 
    collect() |> 
    # when testing
    filter(month(time) == 6) |> 
    distinct(vid, time, .keep_all = TRUE)
  
  trail <- 
    bind_rows(astd |> mutate(source = "astd", mmsi = as.character(mmsi)),
              stk |> mutate(source = "stk")) |> 
    arrange(vid, time, source) |> 
    mutate(.rid = 1:n(), .before = vid) |> 
    st_as_sf(coords = c("lon", "lat"),
             crs = 4326,
             remove = FALSE) |> 
    st_join(harbour) |> 
    st_join(island)
  # drop points on land and where wrong stk hid
  trail2 <- 
    trail |> 
    mutate(where = case_when(on_land == TRUE &  is.na(hid_std) ~ "on_land",
                             on_land == TRUE & !is.na(hid_std) ~ "in_harbour",
                             is.na(on_land) &  !is.na(hid_std) ~ "in_harbour",
                             is.na(on_land) &   is.na(hid_std) ~ "at_sea",
                             .default = "bug")) |> 
    filter(where %in% c("in_harbour", "at_sea")) |> 
    select(-on_land) #|> 
    # drop records where stk hid is different than hid_std
    #filter(!(!is.na(hid) & !is.na(hid_std) & hid != hid_std))
  
  trail3 <- 
    trail2 |> 
    # hail mary
    #filter(is.na(io)) |> 
    group_by(vid) |> 
    mutate(.cid = ramb::rb_trip(!is.na(hid_std))) |> 
    # may want to do this after filtering whacky
    mutate(hid_dep = hid_std,
           hid_arr = hid_std) |> 
    fill(hid_dep, .direction = "downup") |> 
    fill(hid_arr, .direction = "updown") |> 
    ungroup()
  
  
  # drop duplicate time - here order is critical (first record is kept)
  trail4 <- 
    trail3 |> 
    # order matters for the distinct below
    arrange(vid, time, source, hid_std, io) |> 
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
    select(.rid, vid, mmsi, .cid, source, time, lon, lat, speed, heading, hid_std, harbour, hid_dep, hid_arr)
  
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
  
  trail5 |> 
    group_by(vid, .cid) |> 
    mutate(dt = track_time(time),
           dd = track_distance(lon, lat)) |> 
    fill(dt, .direction = "up") |> 
    fill(dd, .direction = "up") |> 
    ungroup() |> 
    mutate(year = year(time),
           month = month(time)) |> 
    arrow::write_dataset(path = "~/stasi/fishydata/data/ais/trail",
                         format = "parquet",
                         existing_data_behavior = "overwrite",
                         partitioning = c("year"))
  
}