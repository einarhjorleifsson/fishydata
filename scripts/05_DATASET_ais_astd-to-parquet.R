# Converting astd csv files to parquet
# 
# INPUT:  /u3/geo/pame/ASTD_area_level1_YYYYMM.csv
# OUTPUT: /home/haf/einarhj/stasi/fishydata/data/ais/astd/*.parquet
#
# ISSUES -----------------------------------------------------------------------
# Match of vessel id 2654 suspect

# nohup run --------------------------------------------------------------------
# run this in terminal as:
#  nohup R < scripts/05_DATASET_ais_astd-to-parquet.R --vanilla > scripts/log/05_DATASET_ais_astd-to-parquet_2025-05-12.log &
#
# Wish list
#  NOTE: variables that were added later not appearing when open_dataset
#  mid: only accept 3 digits (now have sometimes things like "0")
#  Make sure/add schema prior to saving arrow file - note different variables
#   in use at different times
#  Think about using some polar coordinates
#  Think about uber H3
#
#
# Shapes source
#  CAFF: https://geoportal.arctic-sdi.org
#  https://map.arcticportal.org/cesium-dev/

library(arrow)
library(tidyverse)
library(sf)
library(tictoc)

# Spatial files ----------------------------------------------------------------
# These data will be used to classify the location of each point
#  Ideally these shapes should be put on the hafro geoserver

## CAFF area -------------------------------------------------------------------
fil <- "~/stasi/gis/gisland_data/data-raw/misc/CAFF_Boundary_4326.zip"
files <- unzip(fil, list = TRUE)
unzip(zipfile = fil, exdir = tempdir())
caff <-
  read_sf(paste0(tempdir(), "/", "CAFF_Boundary_Polygon_4326.shp")) |>
  mutate(caff = TRUE) |>
  select(caff)
## Highs seas and EEZ ----------------------------------------------------------
hs <-
  read_sf("~/stasi/gis/eez/World_High_Seas_v1_20200826_gpkg/High_Seas_v1.gpkg") |>
  st_cast("POLYGON") |>
  mutate(id = 1:n()) |>
  filter(id %in% c(8, 9, 10, 14)) |>
  mutate(eez = case_when(id == 8 ~ "Smugan",
                         id == 9 ~ "Sildarsmugan",
                         id == 10 ~ "Arctic",
                         id == 14 ~ "Atlantic",
                         .default = NA)) |>
  select(eez) |>
  rename(geometry = geom)
eez <-
  read_sf("~/stasi/gis/eez/EEZ_land_union_v3_202003/EEZ_land_union_v3_202003/EEZ_Land_v3_202030.shp") |>
  janitor::clean_names() |>
  filter(pol_type == "Union EEZ and country") |>
  filter(iso_ter1 %in% c("NOR", "FIN", "SWE", "DNK", "USA", "CAN", "GRL", "FRO", "ISL", "RUS",
                         "SJM", "NOR", "SJM", "GBR", "EST", "LVA", "LTU", "GER", "IRL") |
           territory1 %in% c("Alaska")) |>
  mutate(iso_ter1 = ifelse(is.na(iso_ter1) & territory1 == "Alaska",
                           "Alaska",
                           iso_ter1)) |>
  select(eez = iso_ter1) |>
  bind_rows(hs)
rm(hs)
## FAO areas -------------------------------------------------------------------
fao <-
  read_sf("~/stasi/gis/fao/fao.gpkg") |>
  rename(fao = name)
## Polar regions ---------------------------------------------------------------
pr <-
  tribble(~lat, ~lon,
          900000, -1800000,
          600000, -1800000,
          600000, -0563710,
          580000, -0420000,
          643700, -0352700,
          670309, -0263340,
          704956, -0085961,
          733160,  0190108,
          683829,  0432308,
          600000,  0432308,
          600000,  1800000,
          900000,  1800000) |>
  mutate(lon = geo::geoconvert.1(lon),
         lat = geo::geoconvert.1(lat)) |>
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) |>
  summarise(do_union = FALSE) |>
  st_cast("LINESTRING") |>
  st_cast("POLYGON") |>
  mutate(pca = TRUE)
#
# # crs: 3995   arctic
# # http://basemap.arctic-sdi.org/mapcache/wmts/?request=GetCapabilities&service=wmts
# # https://geoportal.arctic-sdi.org/

# Auxillary data ---------------------------------------------------------------
print(lubridate::now())

con <- omar::connect_mar()
mid_flag <-
  read_parquet("data/auxillary/maritime_identification_digits.parquet") |> 
  select(mid = MID, flag) |> 
  filter(!(mid == 306 & flag == "NLD")) |> 
  mutate(mid = as.integer(mid))
# for icelandic vessel get the unique vessel registry id (skipaskrarnumer)
# mmsi_isl <-
#   bind_rows(omar::mmsi_icelandic_registry(con) |> collect(),
#             omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20220405") |> collect(),
#             omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20201215") |> collect(),
#             omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20190627") |> collect()) |>
#   select(mmsi, vid) |>
#   distinct() |>
#   mutate(mmsi = as.integer(mmsi))
# # Houston, we have a problem
# mmsi_isl |>
#   group_by(mmsi) |>
#   mutate(n = n()) |>
#   ungroup() |>
#   filter(n > 1) |>
#   arrange(mmsi)


dir <-
  fs::dir_info("/u3/geo/pame") |>
  select(path, size:modification_time, change_time)
dir |>
  knitr::kable()

fil <- dir$path
base <- fil |> str_sub(31, 36)

# files with date-format MM/DD/YYYY
base_date <- c("202311",
               "202312",
               "202401",
               "202402",
               "202403",
               "202404")

#fil <- fil[144:length(fil)]
#base <- base[144:length(base)]
fil <- fil[145:147]

for(i in 1:length(fil)) {
  tic()
  print(fil[i])
  d <-
    arrow::read_delim_arrow(fil[i], delim = ";") |>  #, read_options = list(block_size = 2048576L)) |>
    mutate(.rowid = 1:n())

  if(base[i] %in% base_date) {
    d <-
      d |>
      mutate(date_time_utc = mdy_hms(date_time_utc))
  }

  nrow1 <- nrow(d)
  d <-
    d |>
    mutate(.rid = 1:n(), .before = mmsi) |>
    arrange(mmsi, date_time_utc) |>
    group_by(mmsi, date_time_utc) |>
    mutate(mmsi_time_distinct = n()) |>
    ungroup()
  nrow2 <- nrow(d)
  print(paste0("Distinct: Original - ", nrow1, " Left - ", nrow2, " Dropped - ", nrow1 - nrow2))

  # get xy-coordinates
  d <-
    d |>
    st_as_sf(coords = c("longitude", "latitude"),
             crs = 4326,
             remove = FALSE)

  # points in polygons
  nrow1 <- nrow(d)
  d <-
    d |>
    # is there a more efficient way to do this
    st_join(caff) |>
    st_join(eez) |>
    st_join(fao) |>
    st_join(pr)
  nrow2 <- nrow(d)
  if(nrow2 > nrow1) print(paste0("Rows: ", nrow1, " vs ", nrow2))

  d <-
    d |>
    st_transform(crs = 3857)
  xy <-
    st_coordinates(d) |>
    as_tibble() |>
    janitor::clean_names()
  d$x3857 <- xy$x
  d$y3857 <- xy$y
  d <-
    d |>
    st_drop_geometry()

  d <-
    d |>
    mutate(year = year(date_time_utc),
           month = month(date_time_utc),
           month = as.integer(month),
           imo_valid = case_when(omar::vessel_valid_imo(as.character(imonumber)) & imonumber > 0 & nchar(imonumber) == 7 ~ TRUE,
                           .default = FALSE),
           mid = case_when(nchar(mmsi) == 9 ~ as.integer(str_sub(mmsi, 1, 3)),
                           .default = 0L)) |>
    left_join(mid_flag,
              by = join_by(mid)) |>
    arrange(mmsi, date_time_utc) |> 
    group_by(mmsi) |>
    mutate(speed = ramb::rb_speed(longitude, latitude, date_time_utc)) |>
    ungroup()
  print(paste0(fil[i], " rows: ", nrow(d), " columns: ", ncol(d)))
  d |>
    rename(time = date_time_utc,
           vessel = vesselname,
           lon = longitude,
           lat = latitude) |>
    mutate(time = force_tz(time, "UTC"),
           imonumber = case_when(imonumber %in% c(0, -9999) ~ NA_integer_,
                                 .default = imonumber),
           datem = ymd(paste0(year, "-", month, "-01"), tz = "UTC"),
           caff = replace_na(caff, FALSE),
           pca = replace_na(pca, FALSE)) |>
    arrange(mmsi, time) |> 
    arrow::write_dataset("data/ais/astd", format = "parquet",
                         partitioning = c("year", "month"))
  toc()
}



devtools::session_info()


