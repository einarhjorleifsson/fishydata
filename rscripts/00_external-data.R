library(sf)
library(tidyverse)
library(gisland)

# Icelandic shoreline ----------------------------------------------------------
gisland::read_strandlinur() |> 
  mutate(area = st_area(geom)) |> 
  filter(area == max(area)) |> 
  mutate(on_land = TRUE) |> 
  select(on_land) |> 
  write_sf("gpkg/island.gpkg")
