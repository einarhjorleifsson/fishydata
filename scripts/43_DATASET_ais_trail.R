# nohup R < scripts/43_DATASET_ais_trail.R --vanilla > scripts/log/43_DATASET_ais_trail_2025-07-14.log &

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
library(sf)
library(ramb)
library(duckdbfs)
library(tidyverse)

conflicts_prefer(dplyr::filter)
conflicts_prefer(dplyr::lag)
conflicts_prefer(dplyr::lead)

# auxillary data ---------------------------------------------------------------
sf::sf_use_s2(use_s2 = FALSE)  # so invalid geometry does not crash, and
# seems to be faster
island <- 
  read_sf("data/auxillary/shoreline.gpkg") |> 
  rename(.iceland = on_land)
ports <-  
  read_sf("~/stasi/fishydata/data/ports/ports.gpkg") |> 
  select(.pid = pid)
eusm <- 
  read_sf("data/auxillary/eusm.gpkg") |> 
  select(.msfd_bbht = MSFD_BBHT)
gebco <- 
  read_sf("data/auxillary/ICES_GEBCO.gpkg") |>
  select(.depth = depth)
ices <- 
  read_sf("data/auxillary/ICESareas.gpkg") |> 
  select(.ices = Area_Full)
# data includes land
fao <- 
  read_sf("/u3/haf/gisland/data/area/fao.gpkg") |> 
  select(.fao = fao)


# Logbooks ---------------------------------------------------------------------
lb <- 
  nanoparquet::read_parquet("data/logbooks/station-for-ais.parquet") |> 
  filter(year(date) %in% YEARS) |> 
  filter(!is.na(agf_gid))

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
  select(mid, time, lon, lat, speed, heading, hid, io, year) |> 
  inner_join(stk_vid,
             by = join_by(mid, between(time, d1, d2)))  |> 
  select(-c(d1, d2))
ASTD <- 
  open_dataset("data/ais/astd_isleez") |> 
  filter(between(lon, -35, 30),
         between(lat, 50, 79)) |> 
  select(vid, mmsi, time, lon, lat, speed, year) |> 
  inner_join(stk_vid |> select(vid, mmsi),
             by = join_by(vid, mmsi))
# Table contains agf_gid and metier5
gears <-
  nanoparquet::read_parquet("data/gear/gear_mapping.parquet") |> 
  select(agf_gid = gid_agf, met5, s1, s2)

toc(log = TRUE, quiet = FALSE)

# processing - year loop -------------------------------------------------------

for(y in YEARS) {
  
  print(paste0(y, "  ----------------------------------------"))
  tic(msg = "step_01")
  stk <-
    STK |> 
    filter(year == y) |> 
    collect()
  astd <- 
    ASTD |> 
    filter(year == y) |> 
    collect() |> 
    distinct(vid, time, .keep_all = TRUE)
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_01_bind_rows")
  trail <- 
    bind_rows(astd |> mutate(source = "astd", mmsi = as.character(mmsi)),
              stk |> mutate(source = "stk")) |> 
    arrange(vid, time, source) |> 
    mutate(.rid = 1:n(), .before = vid)
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_01_st_as_sf")
  trail <- 
    trail |> 
    st_as_sf(coords = c("lon", "lat"),
             crs = 4326,
             remove = FALSE)
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_01_join_ports")
  trail <- 
    suppressMessages({
      trail |> 
        st_join(ports)
    })
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_01_join_island")
  trail <- 
    suppressMessages({
      trail |> 
        st_join(island)
    })
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_01_join_eusm")
  trail <-
    suppressMessages({
      trail |> 
        st_join(eusm)
    })
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_01_join_depth")
  trail <-
    suppressMessages({
      trail |> 
        st_join(gebco)
    })
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_01_join_ices")
  trail <-
    suppressMessages({
      trail |> 
        st_join(ices)
    })
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_01_join_fao")
  trail <-
    suppressMessages({
      trail |> 
        st_join(fao)
    })
  toc(log = TRUE, quiet = FALSE)

  tic(msg = "step_01_st_tranform_5325")
  xy_5325 <-
    trail |> 
    select(geometry) |> 
    st_transform(crs = 5325) |> 
    st_coordinates() |> 
    as_tibble() |> 
    rename(x5325 = X, y5325 = Y)
  trail <- 
    bind_cols(trail |> st_drop_geometry(), 
              xy_5325)
  toc(log = TRUE, quiet = FALSE)
  
  # End of spatial acrobatics - could possibly stop here, save things as
  #  parquet and run the rest from within duckdb
  #   Except possibly whacky speed checks
  
  # drop points on land (and where wrong stk hid?)
  tic(msg = "step_02")
  trail2 <- 
    trail |> 
    mutate(where = case_when(.iceland == TRUE &  is.na(.pid) ~ "on_land",
                             .iceland == TRUE & !is.na(.pid) ~ "in_harbour",
                             is.na(.iceland) &  !is.na(.pid) ~ "in_harbour",
                             is.na(.iceland) &   is.na(.pid) ~ "at_sea",
                             .default = "bug")) |> 
    filter(where %in% c("in_harbour", "at_sea")) |> 
    select(-.iceland) 
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_03")
  trail3 <- 
    trail2 |> 
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
  
  tic(msg = "step_04")
  # drop duplicate time - here order is critical (first record is kept)
  trail4 <- 
    trail3 |> 
    # order matters for the distinct below
    arrange(vid, time, source, .pid, io) |> 
    distinct(vid, time, .keep_all = TRUE)
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_05")
  # remove some whackies - the poor man's version
  trail5 <- 
    trail4 |> 
    group_by(vid, .cid) |>
    mutate(dd = traipse::track_distance(lon, lat)) |> 
    fill(dd) |> 
    ungroup() |> 
    filter(dd <= 1852 * 20) |> 
    select(.rid, vid, mmsi, .cid, source, time, lon, lat,  x5325, y5325,
           speed, heading, .pid, pid_dep, pid_arr, hid, io, .depth, .msfd_bbht, .ices, .fao)
  toc(log = TRUE, quiet = FALSE)
  
  if(FALSE) {
    trail5 |> 
      filter(.cid > 0) |> 
      group_by(vid) |> 
      ramb::rb_mapdeck(no_lines = FALSE, col = "source", radius = 100)
  }
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
  
  tic(msg = "step_06_whacky-speed")
  trail6 <- 
    trail5 |> 
    group_by(vid, .cid) |> 
    mutate(whack = case_when(.cid > 0 ~ ramb::rb_whacky_speed(lon, lat, time),
                             .default = FALSE)) |> 
    ungroup()
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_07")
  trail7 <- 
    trail6 |> 
    # SHOULD ONE GROUP BY whack?????????????????????????????????????????????????
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
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_08_logbook-join")
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
  toc(log = TRUE, quiet = FALSE)
  
  tic(msg = "step_09_gear-join")
  trail8 <- 
    trail8 |> 
    left_join(gears |> rename(gid_trip = agf_gid),
              by = join_by(gid_trip))
  toc(log = TRUE, quiet = FALSE)
  trail8 |> 
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


