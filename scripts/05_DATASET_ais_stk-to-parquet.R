# nohup R < scripts/05_DATASET_ais_stk-to-parquet.R --vanilla > scripts/log/05_DATASET_ais_stk-to-parquet_2025-07-16.log &

library(tidyverse)
library(mar)
library(sf)
library(terra)
con <- connect_mar()
# so invalid geometry does not crash, and seems to be faster:
sf::sf_use_s2(use_s2 = FALSE)  


gebco_ices <- 
  read_sf("data/auxillary/ICES_GEBCO.gpkg") |>
  select(.depth = depth)
gebco_raster <- 
  terra::rast("/u3/haf/gisland/data/gebco_2024_n89.0_s34.0_w-83.0_e69.0.tif")

if(FALSE) {
  test <- 
    tibble(lon = c(-20, -10, -10),
           lat = c(60, 69, -30)) |> 
    st_as_sf(coords = c("lon", "lat"),
             crs = 4326,
             remove = FALSE)
  z <- terra::extract(gebco_raster, test) |> pull(2)
}


island <- 
  read_sf("data/auxillary/shoreline.gpkg") |> 
  rename(.iceland = on_land)
ports <-  
  read_sf("~/stasi/fishydata/data/ports/ports.gpkg") |> 
  select(.pid = pid)
eusm_ices <- 
  read_sf("data/auxillary/eusm.gpkg") |> 
  select(.msfd_bbht = MSFD_BBHT)
ices <- 
  read_sf("data/auxillary/ICESareas.gpkg") |> 
  select(.ices = Area_Full)
# data includes land
fao <- 
  read_sf("/u3/haf/gisland/data/area/fao.gpkg") |> 
  select(.fao = fao)
## CAFF area -------------------------------------------------------------------
fil <- "~/stasi/gis/gisland_data/data-raw/misc/CAFF_Boundary_4326.zip"
files <- unzip(fil, list = TRUE)
unzip(zipfile = fil, exdir = tempdir())
caff <-
  read_sf(paste0(tempdir(), "/", "CAFF_Boundary_Polygon_4326.shp")) |>
  mutate(.caff = TRUE) |>
  select(.caff)
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
  select(.eez = eez) |>
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
  select(.eez = iso_ter1) |>
  bind_rows(hs)
rm(hs)
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
  mutate(.pca = TRUE)
#eusm <- 
#  read_sf("/u3/haf/gisland/data/area/euseamap_2023.gpkg") |> 
#  select(.euseamap2023_id = )

q <- 
  stk_trail(con) |> 
  left_join(tbl(con, dbplyr::in_schema("STK", "MOBILE")) |>
              select(mid = MOBILEID, loid = LOCALID, glid = GLOBALID)) |> 
  mutate(year = year(time),
         month = month(time)) |> 
  filter(time >= to_date("2007-06-01", "YYYY:MM:DD"),
         between(lon, -179.9, 179.9),
         between(lat, -89.9, 89.9))

#for(y in 2007:2025) {
for(y in 2025:2007) {
  YEAR <- y
  print(YEAR)
  trail <- 
    q |> 
    filter(year == YEAR) |> 
    collect() |> 
    select(mid, loid, glid, everything())
  trail <- 
    trail |> 
    st_as_sf(coords = c("lon", "lat"),
             crs = 4326,
             remove = FALSE) |> 
    #suppressMessages({
    st_join(ports) |> 
    st_join(island) |> 
    st_join(eusm_ices) |> 
    st_join(gebco_ices) |> 
    st_join(ices) |> 
    st_join(fao) |> 
    st_join(caff) |> 
    st_join(eez) |> 
    st_join(pr)
  #})
  
  z <- terra::extract(gebco_raster, trail) |> pull(2)
  trail <- 
    trail |> 
    mutate(.gebco_z = z)
  
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
  trail |> 
    mutate(month = as.integer(month)) |> 
    arrange(mid, time) |> 
    arrow::write_dataset("data/ais/stk-raw", format = "parquet",
                         existing_data_behavior = "overwrite",
                         partitioning = c("year"))
}

