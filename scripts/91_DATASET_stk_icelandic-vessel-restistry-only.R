# TODO:
#  * include astd data via mmsi match
#    * question what to do with speed (and heading)
#  * test for whacky points - this could be done per mobileid
#  * standardize/correct harbour code
#  * points in
#     * harbour
#     * eez
#     * habitat
#     * ..
#  * create trips
#  * link to logbooks
#  * link to landings id
#  ...
library(conflicted)
library(sf)
library(arrow)
library(tidyverse)

conflicts_prefer(dplyr::filter)

# auxillary data ---------------------------------------------------------------
island <- read_sf("~/stasi/gis/AIS_TRAIL/data-raw/island.gpkg")
harbour <- 
  read_sf("~/stasi/gis/AIS_TRAIL/data-raw/harbours-hidstd.gpkg") |> 
  select(hid_std)
# TODO: Create the csv file from scratch within the project
# read_csv("~/stasi/gis/AIS_TRAIL/data-raw/stk_harbours.csv") |> 
#   write_csv("data-raw/harbours/stk_harbours.csv")
harbours.standards <- 
  read_csv("data-raw/harbours/stk_harbours.csv") |> 
  select(hid, hid_std)

# data -------------------------------------------------------------------------
# TODO: Check the d2 below - may be less than last observation date
#       Need to use variable "no" if want to expand d2 
stk_vid <- 
  open_dataset("data/vessels/stk_vessel_match.parquet") |> to_duckdb() |> 
  select(mid, d1, d2, vid, mmsi) |> 
  filter(vid > 0)
q <- 
  open_dataset("data/ais/stk-raw") |> 
  to_duckdb() |> 
  select(mid, time, lon, lat, speed, heading, hid, io, year, month) |> 
  select(mid:io, year, month) |> 
  inner_join(stk_vid,
             by = join_by(mid, between(time, d1, d2)))  |> 
  select(-c(d1, d2)) |> 
  filter(between(lon, -35, 30),
         between(lat, 50, 79))

for(y in 2024:2007) {
  YEAR <- y
  print(YEAR)
  d <- 
    q |> 
    filter(year == YEAR) |> 
    collect() |> 
    left_join(harbours.standards,
              by = join_by(hid)) |> 
    select(-c(hid)) |> 
    rename(hid = hid_std) |> 
    st_as_sf(coords = c("lon", "lat"),
             crs = 4326,
             remove = FALSE) |> 
    st_join(harbour) |> 
    st_join(island)
  
  
  d |> 
    # order maters here when using distinct below
    arrange(vid, time, hid, io) |> 
    distinct(vid, time, .keep_all = TRUE) |> 
    mutate(date = as_date(time)) |> 
    arrow::write_dataset("data/ais/stk-vid", format = "parquet",
                         partitioning = c("year"))
}
