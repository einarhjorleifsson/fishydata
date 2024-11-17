# nohup R < scripts/91_DATASET_astd-isleez.R --vanilla > scripts/log/91_DATASET_astd-isleez_2024-11-16.log &

library(arrow)
library(tidyverse)
library(sf)

lgs <- 
  read_parquet("data/logbooks/stations.parquet") |> 
  filter(year(date) >= 2013) |>  
  group_by(vid, date) |> 
  summarise(gid_ln_agf = min(gid_ln_agf),
            gid = min(gid),
            .groups = "drop")

harbours <- read_sf("/u3/haf/stasi/geoserver/misc/harbours.gpkg")
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
  open_dataset("data/astd") |> 
  filter(eez == "ISL" | flag == "ISL")

YEARS <- 2013:2024
for(y in 1:length(YEARS)) {
  YEAR <- YEARS[y]
  print(YEAR)
  
  trail <- 
    astd |> 
    filter(year == YEAR) |> 
    select(.rid, mmsi, imo = imonumber, time, vessel,
           astd_cat, dd = dist_nextpoint, dt = sec_nextpoint,
           speed,
           lon, lat, x, y, imo_valid, flag, caff, pca, eez) |> 
    collect() |> 
    left_join(mmsi,
              by = join_by(mmsi, between(time, t1, t2))) |> 
    select(-c(t1, t2))
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
    mutate(.cid = ramb::rb_trip(!is.na(hid))) |> 
    ungroup() |> 
    # include first and last point in harbour as part of trip
    group_by(mmsi) |> 
    mutate(.cid = case_when(.cid < 0 & lead(.cid) > 0 ~ lead(.cid),
                            .cid < 0 & lag(.cid)  > 0 ~ lag(.cid),
                            .default = .cid)) |> 
    ungroup() |> 
    filter(.cid > 0)
  
  trail |> 
    mutate(vid = as.integer(vid)) |> 
    mutate(year = year(time)) |> 
    mutate(date = as_date(time)) |> 
    left_join(lgs) |> 
    arrow::write_dataset("data/astd_isleez", format = "parquet",
                         partitioning = c("year"))

}
