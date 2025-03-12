dictionary <- 
  tibble::tribble(~ices, ~mfri, ~ices, ~std,
                  "VE_REF",   "vessel_no", "vid",
                  "VE_FLT",   NA, NA,
                  "VE_COU",   NA, NA,
                  "VE_LEN",   NA, "loa",
                  "VE_KW",    NA, "kw",
                  "VE_TON",   NA, "gt",
                  "FT_REF",   "trip_id", ".tid",
                  "FT_DCOU",  NA, NA,
                  "FT_DHAR",  "departure_port_no", "hid1",
                  "FT_DDAT",  NA, NA,
                  "FT_DTIME", NA, NA,
                  "FT_DDATIM", "departure", "T1",
                  "FT_LCOU",  NA, NA,
                  "FT_LHAR",  "landing_port_no", "hid2",
                  "FT_LDAT",  NA, NA,
                  "FT_LTIME", NA, NA,
                  "FT_LDATIM", "arrival", "T2",
                  "LE_ID",   "station_id", ".sid",
                  "LE_CDAT", NA, NA,
                  "LE_STIME", NA, NA,
                  NA, "fishing_start", "t1",
                  "LE_ETIME",
                  NA, "fishing_end", "t2",
                  "LE_SLAT", "latitude", "lat1",
                  "LE_SLON", "longitude", "lon1",
                  "LE_ELAT", "latitude_end", "lat2",
                  "LE_ELON", "longitude_end", "lon2",
                  "LE_GEAR", "gear_no", "gid",
                  "LE_MSZ",  NA, "mesh",
                  "LE_RECT", NA, "ir",
                  "LE_DIV",  NA, NA,
                  "LE_MET_level6", NA, NA,
                  "LE_UNIT", NA, NA,
                  "LE_EFF", NA, NA)


if(FALSE) {
  library(tidyverse)
  library(omar)
  con <- connect_mar()
  tbl_mar(con, "adb.trip_v") |> 
    filter(year(departure) == 2024) |> 
    select(trip_id, 
           vid = vessel_no,
           T1 = departure,
           hid1 = departure_port_no,
           T2 = landing,
           hid2 = landing_port_no) |> 
    # only trips were some logbook recorded
    left_join(tbl_mar(con, "adb.station_v") |> 
                select(trip_id,
                       station_id,
                       gid = gear_no,
                       t1 = fishing_start,
                       t2 = fishing_end,
                       lon1 = longitude,
                       lat1 = latitude,
                       lon2 = longitude_end,
                       lat2 = latitude_end,
                       z1 = depth,
                       z2 = depth_end),
              by = join_by(trip_id)) |> 
    left_join(tbl_mar(con, "adb.catch_v") |> 
                group_by(station_id) |> 
                summarise(catch = sum(quantity, na.rm = TRUE),
                          .groups = "drop")) |> 
    collect(n = Inf) ->
    trips_and_tows
}



