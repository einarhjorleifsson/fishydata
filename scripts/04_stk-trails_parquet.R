# Tasks ------------------------------------------------------------------------
# 1.

# ------------------------------------------------------------------------------
# run this in terminal as:
#  nohup R < scripts/04_stk-trails_parquet.R --vanilla > lgs/04_stk-trails_parquet_2024-05-22F.log &
#

# 2024-05-23 Add ASTD trails


# 2024-05-01 Added habitat and depth variable

#
# Input:
#  Landings data (agf.aflagrunnur)
#  STK (stk.trail)
#  Harbour data (see below)
# Output:
#  A trail of each vessel: stasi/fishydata/data/ais
#  Contains the following additional variables:
#   trip id (.cid): A sequential number identifying trips of each vessel
#    Negative numbers: Boat in harbour
#  hid_dep & hid_arr: Standardized (3 character) harbour identification of
#    trip harbour departure and arrival.
#    NA's: ...
#  .rid: 
#  trip.n: sequential observations number within a trip (redundant info?)
#  v: point classifier: A vmask algorithm applied to find wacky points.
#     end_location: pings at end of trip (hence wacky points not detected via
#      the argos::vmask function)
#     not: pings that are ok
#     removed: whacky points
#     removed on land: points on Iceland
#     removed time duplicate: self explanatory
#     short: A trip with 5 pings or fewer, argos::vmask algorithm does not
#      work on those
# Next step:
#  Add logbook id (visir) for each ping - have already a first go at it here
#  Add: convert data to meters (crs = 3857?) then to bin the data within arrow could
#   do something like this (here bin to 100 meters, get center point
#     d |> mutate(x = x %/% 100 * 100 + 100/2)
#  Add landings id - could do this differently, by why
#
# Inspiration for file storage: https://blog.djnavarro.net
#
# for a rainy day: 
#  https://github.com/taniamendo/identify_fishing_workflow

test <- FALSE
lubridate::now()

library(arrow)
library(data.table)
library(sf)
sf::sf_use_s2(FALSE)
#library(mapdeck)
#source("~/R/Pakkar2/ramb/TOPSECRET.R")
library(tidyverse)
#options(ggplot2.continuous.colour = "viridis")
#options(ggplot2.continuous.fill = "viridis")
library(ramb)
library(omar)
library(argosfilter)
library(arrow)
con <- connect_mar()
# Auxilary data ----------------------------------------------------------------
island <- 
  read_sf("data-aux/shoreline.gpkg") |> 
  st_transform(crs = 3057) |> 
  st_buffer(dist = -100) |> 
  st_transform(crs = 4326)
harbour <- read_sf("data-aux/harbours.gpkg")
harbours.standards <- 
  readxl::read_excel("data-aux/harbours.xlsx") |> 
  select(hid, hid_std)
eusm <- read_sf("data-aux/EUSeaMap_2023.gpkg")
eusm <- 
  eusm |> 
  select(habitat = MSFD_BBHT)
# read_rds("~/prj2/garbage/ICES-VMS-and-Logbook-Data-Call/ICES-VMS-and-Logbook-Data-Call/ICES_GEBCO.rds") |> 
#   write_sf("data-aux/ices_gebco.gpkg")
gebco_z <- read_sf("data-aux/ices_gebco.gpkg")
LB <- read_rds("data/logbooks/rds/station-for-ais.rds")
# 2024-05-22 Add mmsi
mmsi_isl <-
  bind_rows(omar::mmsi_icelandic_registry(con) |> collect(),
            omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20220405") |> collect(),
            omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20201215") |> collect(),
            omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20190627") |> collect()) |>
  select(mmsi, vid) |>
  distinct() |>
  mutate(mmsi = as.integer(mmsi))
# Houston, we have a problem
mmsi_isl |> 
  group_by(mmsi) |> 
  mutate(n = n()) |> 
  ungroup() |> 
  filter(n > 1) |> 
  arrange(mmsi)

astd <- 
  open_dataset("data/astd") |> 
  filter(flag == "ISL") |> 
  select(mmsi,
         time = date_time_utc,
         lon = longitude,
         lat = latitude,
         speed_d = speed) |> 
  left_join(mmsi_isl |> 
              arrow_table())


YEARS <- 2007:2024

Y1 <- min(YEARS)
Y2 <- max(YEARS)
D1 <- paste0(min(YEARS), "-01-01")
D2 <- paste0(max(YEARS), "-12-31")

# Make the connection for each fishing vessel trail ----------------------------

# limit analysis by vessels listed in the landings database, excluding foreigners
vessels <- 
  omar::ln_agf(con) |> 
  filter(between(date, to_date(D1, "YYYY-MM-DD"), to_date(D2, "YYYY-MM-DD"))) |> 
  filter(!between(vid, 3700, 4999)) |>
  filter(vid != 9999) |> # apparantly used for "sjostöng"
  filter(vid > 0) |> 
  group_by(vid) |> 
  summarise(wt = sum(wt, na.rm = TRUE) / 1e3,
            .groups = "drop") |> 
  filter(wt > 0) |> 
  left_join(omar:::stk_midvid(con) |> 
              select(mid, vid, t1, t2, pings),
            by = "vid")
vessels |> 
  collect(n = Inf) |> 
  filter(is.na(mid)) |> 
  knitr::kable(caption = "Vessels with no mid-match")

### trail raw ------------------------------------------------------------------
trail <-
  vessels |> 
  filter(!is.na(mid)) |> 
  select(vid, mid, t1, t2) |> 
  mutate(t1 = to_date(t1, "YYYY:MM:DD"),
         t2 = to_date(t2, "YYYY:MM:DD")) |> 
  left_join(omar::stk_trail(con),
            by = "mid") |> 
  # ensures correct vid-mid match, because of many-to-many
  filter(time >= t1 & time <= t2)

# Extract trail by vessel ------------------------------------------------------
VID <- 
  vessels |> 
  filter(!is.na(mid)) |> 
  collect(n = Inf) |> 
  pull(vid) |> 
  sort() |> 
  unique()
# Temporary
VID <- VID[686:1787]
for(v in 1:length(VID)) {
  VIDv <- VID[v]
  print(VIDv)
  
  astdv <- 
    astd |> 
    filter(vid == VIDv) |> 
    collect() |> 
    select(vid, time, lon, lat, speed = speed_d) |> 
    arrange(time) |> 
    mutate(ref = "astd")
  trailv <- 
    trail |> 
    filter(vid == VIDv) |> 
    select(-c(t1, t2)) |> 
    collect(n = Inf) |> 
    mutate(ref = "lhg")
  
  if(nrow(astdv) > 100) {
    trailv <- 
      bind_rows(trailv,
                astdv) |> 
      distinct(time, .keep_all = TRUE) |> 
      arrange(time)
  }

  trailv <- 
    trailv |> 
    arrange(time) |> 
    filter(between(lon, -35, 30),
           between(lat, 50, 79)) %>%
    left_join(harbours.standards,
              by = "hid") |> 
    select(-hid) |> 
    rename(hid = hid_std) |> 
    st_as_sf(coords = c("lon", "lat"),
             crs = 4326,
             remove = FALSE) |> 
    st_join(harbour) |> 
    st_join(island) |> 
    st_drop_geometry() |> 
    # 2023-05-12: 
    #             if point in harbour, then not on land
    # mutate(on_land = replace_na(on_land, FALSE)) |> 
    mutate(on_land = case_when(!is.na(hid_std)  & on_land == TRUE ~ FALSE,
                               is.na(hid_std)   & on_land == TRUE ~ TRUE,
                               .default = FALSE)) |> 
    # The order matters when it comes to the distinct below
    arrange(vid, time, hid, io) |> 
    mutate(vid = as.integer(vid),
           .rid = 1:n(),
           vms = "yes")
  
  if(nrow(trailv) > 5) {
    
    if(test) {
      trailv |> mutate(in.harbour = !is.na(hid_std)) |> count(in.harbour, on_land)
    }
    
    tmp <- 
      trailv |> 
      filter(!on_land) |> 
      distinct(time, .keep_all = TRUE)
    removed <- 
      trailv |> 
      filter(!.rid %in% tmp$.rid)
    if(test) {
      removed |> count(on_land)
      # points truly on land
      removed |> 
        filter(on_land) |> 
        st_as_sf(coords = c("lon", "lat"),
                 crs = 4326) |> 
        ramb::rb_mapdeck()
      # time duplicates
      removed |> 
        filter(!on_land) |> 
        mutate(hid_std_found = !is.na(hid_std)) |> 
        count(io, hid_std) |> 
        spread(io, n)
      removed |> 
        filter(!on_land) |> 
        st_as_sf(coords = c("lon", "lat"),
                 crs = 4326) |> 
        ramb::rb_mapdeck()
    }
    
    ### define trips -------------------------------------------------------------
    trailv <- 
      tmp |> 
      arrange(time) |> 
      # cruise id (aka tripid), negative values: in harbour
      mutate(.cid = ramb::rb_trip(!is.na(hid_std))) |>
      mutate(hid_dep = hid_std,
             hid_arr = hid_std) |> 
      group_by(vid) |> 
      fill(hid_dep, .direction = "downup") |> 
      fill(hid_arr, .direction = "updown") |> 
      ungroup() |> 
      # should not be needed
      filter(between(year(time), Y1, Y2)) |> 
      select(vid, time, .cid, lon, lat, speed, hid_dep, hid_arr, .rid, vms, ref) |> 
      # filter(.cid > 0) |> 
      arrange(time) |> 
      distinct(time, .keep_all = TRUE) |> 
      group_by(vid, .cid) |> 
      mutate(v = ifelse(n() > 5 & .cid > 0,
                        try(vmask(lat, lon, time, vmax = rb_kn2ms(30)), TRUE),
                        "short")) |> 
      ungroup() |>
      mutate(v = as.character(v))
    
    if(test) {
      trailv |> 
        filter(v == "short", .cid > 0) |> 
        st_as_sf(coords = c("lon", "lat"),
                 crs = 4326) |> 
        group_by(.cid) |> 
        filter(n() > 1) |> 
        rb_mapdeck(no_lines = FALSE)
      trailv |> 
        filter(v == "short", .cid > 0) |> 
        count(.cid) |> 
        count(n)
    }
    
    trailv_store <- 
      trailv |> 
      filter(.cid < 0 | v %in% c("removed", "short"))
    trailv_trips <- 
      trailv |> 
      filter(.cid > 0 & v %in% c("end_location", "not")) |> 
      mutate(time = round_date(time, "minutes"))
    check <- nrow(trailv) == nrow(trailv_store) + nrow(trailv_trips)
    if(!check) print("rows not the same")
    
    # 2023-08-31: Added, problem with vid = 3010
    if(nrow(trailv_trips) > 20) {
      
      # Expand the time grid of each trip ------------------------------------------
      res <- list()
      vessel_cids <- unique(trailv_trips$.cid)
      # loop through each trip -----------------------------------------------------
      for(t in 1:length(vessel_cids)) {
        CID <- vessel_cids[t]
        # print(CID)
        tmp01 <- 
          trailv_trips |> 
          filter(.cid == CID)
        res[[t]] <- 
          tibble(time = seq(min(tmp01$time),
                            max(tmp01$time),
                            by = "30 sec")) |> 
          left_join(tmp01,
                    by = join_by(time)) |> 
          fill(vid, .cid, hid_dep, hid_arr) |> 
          mutate(vms = replace_na(vms, "no"),
                 y = 1:n()) |> 
          mutate(lon = approx(y, lon, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
                 lat = approx(y, lat, y, method = "linear", rule = 1, f = 0, ties = mean)$y,
                 speed = approx(y, speed, y, method = "linear", rule = 1, f = 0, ties = mean)$y) %>%
          select(-y)
      }
      
      # Here merge the logbooks ... ???
      LB_vessel <-
        LB %>%
        filter(vid == VIDv) %>% 
        select(vid, .sid, gid, t1, t2) %>%
        pivot_longer(cols = c(t1, t2),
                     names_to = "startend",
                     values_to = "time") %>%
        arrange(vid, time) %>%
        mutate(time = round_date(time, "minutes"),
               vid = as.integer(vid),
               gid = as.integer(gid),
               source = "logbooks")
      
      # allocation of visir not working
      #  need to get the gear right - want them in the processed data
      res2 <- 
        res |> 
        bind_rows() |> 
        bind_rows(LB_vessel) %>%
        arrange(time) %>%
        mutate(x = if_else(startend == "t1", 1, 0, 0)) %>%
        mutate(x = case_when(startend == "t1" ~ 1,
                             startend == "t2" ~ -1,
                             TRUE ~ 0)) %>%
        mutate(x = cumsum(x)) %>%
        fill(.sid, gid) |> 
        mutate(.sid = ifelse(x == 1 | startend == "t2", .sid, NA_integer_),
               gid = ifelse(x == 1 | startend == "t2", gid, NA_integer_)) |> 
        filter(is.na(source)) |> 
        select(-source)
      if(test) { 
        res2 |> 
          glimpse()
        res2 |> 
          mutate(has.visir = !is.na(.sid),
                 has.gid = !is.na(gid)) |> 
          count(has.visir, has.gid)
      }
      
      # assign the gear to the whole trip
      res2 <- 
        res2 |> 
        mutate(.gid_trip = gid) |> 
        group_by(.cid) |> 
        fill(.gid_trip, .direction = "downup") |> 
        ungroup()
      
      # include crs 3857
      xy <- 
        res2 |> 
        select(lon, lat) |> 
        st_as_sf(coords = c("lon", "lat"),
                 crs = 4326) |> 
        st_transform(crs = 3857) |> 
        st_coordinates() |> 
        as_tibble()
      
      res2$x <- xy$X
      res2$y <- xy$Y
      
      # 2024-01-24 NOTE: Could calculate the step distance here
      res2 <- 
        res2 |> 
        group_by(.cid) |> 
        mutate(dist = traipse::track_distance(lon, lat),
               dist = replace_na(dist, 0)) |> 
        ungroup() |> 
        st_as_sf(coords = c("lon", "lat"),
                 crs = 4326,
                 remove = FALSE) |> 
        st_join(eusm, join = st_intersects) |> 
        st_join(gebco_z, join = st_intersects) |> 
        st_drop_geometry()
      
      # and now for the save ...
      #  here only save positive trips with more than 5 pings
      
      pth <- paste0("data/ais/", str_pad(VIDv, width = 4, pad = "0"), ".parquet")
      res2 |> write_parquet(pth)
      
    }
    
  } # end of if statement: if(nrow(trailv) > 5)
  
}


lubridate::now()

devtools::session_info()


