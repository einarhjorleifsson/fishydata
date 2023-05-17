# ------------------------------------------------------------------------------
# run this in terminal as:
#  nohup R < rscripts/01_trails.R --vanilla > lgs/01_trails_YYYY_MM-DD.log &

lubridate::now()

# library(data.table)
library(sf)
library(tidyverse)
library(ramb)
library(omar)
library(argosfilter)

# function 
#  use because want to retain more info
stk_trail0 <- function(con) {
  q <- 
    tbl_mar(con, "stk.trail") %>% 
    dplyr::mutate(lon = poslon * 
                    180/pi, lat = poslat * 180/pi, heading = heading * 180/pi, 
                  speed = speed * 3600/1852)
  q %>% 
    dplyr::select(.id = trailid,
                  mid = mobileid, 
                  time = posdate, 
                  lon, 
                  lat, 
                  speed, 
                  heading, 
                  hid = harborid, 
                  io = in_out_of_harbor, 
                  rectime = recdate)
}


island <- 
  read_sf("gpkg/island.gpkg") |> 
  st_transform(crs = 3057) |> 
  st_buffer(dist = -100) |> 
  st_transform(crs = 4326)

harbours <- 
  read_sf("gpkg/harbours.gpkg") |> 
  select(hid_std_geo = hid_std)
harbours.standards <- 
  readxl::read_excel("data-raw/harbours.xlsx") |> 
  select(hid, hid_std_tbl = hid_std)

con <- connect_mar()

YEARS <- 2007:2023
D1 <- paste0(min(YEARS), "-01-01")
D2 <- paste0(max(YEARS), "-12-31")

# Make the connection for each fishing vessel trail ----------------------------
vessels <- 
  omar::ln_agf(con) |> 
  filter(between(date, to_date(D1, "YYYY-MM-DD"), to_date(D2, "YYYY-MM-DD"))) |> 
  filter(!between(vid, 3700, 4999)) |>
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

trail <-
  vessels |> 
  filter(!is.na(mid)) |> 
  select(vid, mid, t1, t2) |> 
  mutate(t1 = to_date(t1, "YYYY:MM:DD"),
         t2 = to_date(t2, "YYYY:MM:DD")) |> 
  left_join(stk_trail0(con),
            by = "mid") |> 
  filter(time >= t1 & time <= t2)

# Extract trail by vessel ------------------------------------------------------
VID <- 
  vessels |> 
  filter(!is.na(mid)) |> 
  collect(n = Inf) |> 
  pull(vid) |> 
  sort() |> 
  unique()

v_counter <- list()
for(v in 1:length(VID)) {
  VIDv <- VID[v]
  print(VIDv)
  trailv <- 
    trail |> 
    filter(vid == VIDv) |> 
    select(-c(t1, t2)) |> 
    collect(n = Inf) |> 
    arrange(time) |> 
    filter(between(lon, -35, 30),
           between(lat, 50, 79)) %>%
    left_join(harbours.standards,
              by = "hid") |> 
    st_as_sf(coords = c("lon", "lat"),
             crs = 4326,
             remove = FALSE) |> 
    st_join(harbours) |> 
    st_join(island) |> 
    st_drop_geometry() |> 
    # 2023-05-12: 
    #             if point in harbour, then not on land
    # mutate(on_land = replace_na(on_land, FALSE)) |> 
    mutate(on_land = case_when(!is.na(hid_std_geo)  & on_land == TRUE ~ FALSE,
                               is.na(hid_std_geo)   & on_land == TRUE ~ TRUE,
                               .default = FALSE)) |> 
    # The order matters
    arrange(vid, time, hid_std_tbl, io) |> 
    mutate(.rid = 1:n())
  tmp <- 
    trailv |> 
    filter(!on_land) |> 
    distinct(time, .keep_all = TRUE)
  removed <- 
    trailv |> 
    filter(!.rid %in% tmp$.rid)
  trailv <- 
    tmp |> 
    # cruise id (aka tripid), negative values: in harbour
    mutate(.cid = ramb::rb_trip(!is.na(hid_std_geo))) |>
    group_by(vid, .cid) |> 
    mutate(trip.n = 1:n()) |> 
    ungroup() |> 
    mutate(hid_dep = hid_std_geo,
           hid_arr = hid_std_geo) |> 
    group_by(vid) |> 
    fill(hid_dep, .direction = "down") |> 
    fill(hid_arr, .direction = "up") |> 
    ungroup() |> 
    filter(between(year(time), 2007, 2023)) |> 
    # check for whackies irrespective of trips
    group_by(vid) |> 
    mutate(v0 = vmask(lat, lon, time, vmax = rb_kn2ms(30))) |> 
    # whackies by trip
    group_by(vid, .cid) |> 
    mutate(pings = n()) |> 
    mutate(v = ifelse(pings > 5 & .cid > 0,
                      # Note: FIRST arguement is lat
                      vmask(lat, lon, time, vmax = rb_kn2ms(30)),
                      "short")) |> 
    ungroup() |> 
    # to ensure downstream binding
    mutate(v0 = as.character(v0),
           v = as.character(v)) |> 
    select(-pings)
  
  trailv <-
    bind_rows(trailv,
              removed |> 
                mutate(v0 = case_when(on_land == TRUE ~ "removed on land",
                                     .default = "removed time duplicate"),
                       v = v0)) |> 
    arrange(.rid)
  
  
  # split the data by year, because downstream we want to collate the data by
  #  year for all vessels
  YEARS <- year(min(trailv$time)):year(max(trailv$time))
  for(y in 1:length(YEARS)) {
    pth <- paste0("data/trails/trails_y", YEARS[y], "_v", str_pad(VIDv, width = 4, pad = "0"), ".rds") 
    tmp <- trailv |> filter(year(time) == YEARS[y]) 
    if(nrow(tmp) > 0) tmp |> write_rds(pth)
  }
}


lubridate::now()

devtools::session_info()


