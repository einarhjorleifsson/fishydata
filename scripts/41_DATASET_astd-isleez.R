# nohup R < scripts/91_DATASET_astd-isleez.R --vanilla > scripts/log/91_DATASET_astd-isleez_2025-03-17.log &

library(tictoc)
tic()

#year_now <- lubridate::today() |> lubridate::year()
YEARS <- c(2024:2013)

library(arrow)
library(tidyverse)
library(sf)

harbours <- read_sf("data/auxillary/harbours.gpkg")
mmsi <- 
  open_dataset("data/vessels/mmsi_iceland_archieves.parquet") |> 
  filter(mmsi_cat == "vessel") |> 
  select(vid = sknr,
         mmsi,
         t1 = mmsi_t1,
         t2 = mmsi_t2) |> 
  mutate(mmsi = as.integer(mmsi)) |> 
  collect()
astd <- 
  open_dataset("data/ais/astd") |> 
  filter(eez == "ISL" | flag == "ISL")


for(y in 1:length(YEARS)) {
  YEAR <- YEARS[y]
  print(YEAR)
  
  trail <- 
    astd |> 
    filter(year == YEAR) |> 
    dplyr::select(.rid, mmsi, imo = imonumber, time, vessel,
           astd_cat, dmeters = dist_nextpoint, dminutes = sec_nextpoint,
           speed,
           lon, lat, imo_valid, flag, caff, pca, eez) |> 
    collect() |> 
    left_join(mmsi,
              by = join_by(mmsi, between(time, t1, t2))) |> 
    select(-c(t1, t2)) |> 
    mutate(dminutes = dminutes / 60)
  # Get rid of mmsi with few pings
  trail |> 
    count(mmsi) |> 
    arrange(n) |> 
    filter(n > 20) |> 
    pull(mmsi) ->
    MMSI
  trail <-
    trail |> 
    # only mmsi with more than 20 pings
    filter(mmsi %in% MMSI) |> 
    arrange(mmsi, time) |> 
    # Points in harbour
    st_as_sf(coords = c("lon", "lat"),
           crs = 4326,
           remove = FALSE) |> 
    st_join(harbours |> select(hid = hid_std)) |> 
    st_drop_geometry() |> 
    # cruise id (aka tripid), negative values: in harbour
    arrange(mmsi, time) |> 
    group_by(mmsi) %>%
    # NOTE: This generates trip ID within each year because of the loop
    mutate(.cid = ramb::rb_trip(!is.na(hid))) |> 
    ungroup() |> 
    # include first and last point in harbour as part of trip
    group_by(mmsi) |> 
    mutate(.cid = case_when(.cid < 0 & lead(.cid) > 0 ~ lead(.cid),
                            .cid < 0 & lag(.cid)  > 0 ~ lag(.cid),
                            .default = .cid)) |> 
    ungroup()
  
  trail |> 
    mutate(vid = as.integer(vid)) |> 
    mutate(year = year(time)) |>
    arrow::write_dataset("data/ais/astd_isleez", 
                         format = "parquet",
                         existing_data_behavior = "overwrite",
                         partitioning = c("year"))

}

toc()

devtools::session_info()

