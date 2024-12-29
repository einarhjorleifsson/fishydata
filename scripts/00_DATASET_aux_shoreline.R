# AUXILLARY DATA ---------------------------------------------------------------
library(sf)
library(tidyverse)
library(arrow)
library(sfarrow)

# Iceland - shoreline -----------------------------------------------------------
# safer way than gisland::read_strandlinur
read_shoreline <- function(mainland = TRUE) {
  s <- 
    "IS_50V:strandlina_flakar" %>% 
    gisland::read_lmi()
  tmp <- tempdir()
  write_sf(s, paste0(tmp, "/in.gpkg"))
  cmd <-
    paste0("ogr2ogr ",
           tmp,
           "/out.gpkg ",
           tmp,
           "/in.gpkg",
           " -explodecollections -nlt CONVERT_TO_LINEAR")
  system(cmd)
  s <- 
    read_sf(paste0(tmp, "/out.gpkg")) |> 
    dplyr::filter(!sf::st_is_empty(geom)) |>  
    #sf::st_collection_extract(type = "POLYGON")
    sf::st_make_valid() |> 
    dplyr::mutate(geom = lwgeom::lwgeom_make_valid(geom)) |> 
    mutate(area = st_area(geom),
           on_land = TRUE) |>
    select(area, on_land)
  if(mainland) {
    s <- s |> filter(area == max(area)) |> select(on_land)
  }
  return(s)
}

shoreline <- 
  read_shoreline() 
shoreline |> st_write("data/auxillary/shoreline.gpkg")
shoreline |> st_write("/net/hafgola.hafro.is/var/ftp/pub/data/fishydata/shoreline.gpkg")

# read_sf("ftp://ftp.hafro.is/pub/data/fishydata/shoreline.gpkg")
