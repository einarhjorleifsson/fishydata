library(osmdata)
library(sf)
library(dplyr)

bb <- c(-25.0, 34.0, 45.0, 72.0)       # europa

osm_fetch <-
  opq(bbox = bb, timeout = 120) |>
  add_osm_feature(key = "harbour") |>
  osmdata_sf()
bind_rows(osm_fetch$osm_points,
          osm_fetch$osm_polygons,
          osm_fetch$osm_multipolygons,
          osm_fetch$osm_lines,
          osm_fetch$osm_multilines) |>
  as_tibble() |>
  filter(!is.na(harbour)) |>
  select(osm_id, name, harbour, geometry, everything()) |>
  write_sf("data/ports/ports_osm.gpkg")
