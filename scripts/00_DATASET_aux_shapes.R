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

# ICES areas - from vmstools ---------------------------------------------------
library(vmstools)
data(ICESareas)
ICESareas <- 
  ICESareas |> 
  sf::st_make_valid()  |> 
  sf::st_transform(4326) |> 
  sf::st_zm()
ICESareas |> st_write("data/auxillary/ICESareas.gpkg")

# EUSM and gebco - from ices vms datacall --------------------------------------

file_id <- "1FRjZ8ByTlRZOJGDpMCjQDWMlrsqjqPfb"
zip_path <- paste0("data/auxillary/", "hab_and_bathy_layers.zip")

# No authentication needed for files with "Anyone with the link" permission
googledrive::drive_deauth()

# Download file
googledrive::drive_download(googledrive::as_id(file_id), path = zip_path, overwrite = TRUE)

if(file.exists(zip_path) && file.size(zip_path) > 1000000) {
  cat("Successfully downloaded file of size: ", file.size(zip_path)/1024/1024, " MB\n")
  # Extract the zip archive
  unzip_result <- try(unzip(zip_path, exdir = "data/auxillary", overwrite = TRUE, junkpaths = TRUE))
  if(inherits(unzip_result, "try-error")) {
    cat("Failed to extract zip file. It may be corrupted or require a different extraction method.\n")
  }
} else {
  cat("Failed to download the complete file from Google Drive.\n")
}

gebco <- read_rds("data/auxillary/ICES_GEBCO.rds")
gebco |> st_write("data/auxillary/ICES_GEBCO.gpkg")
system("rm data/auxillary/ICES_GEBCO.rds")

eusm <- read_rds("data/auxillary/eusm.rds")
eusm |> st_write("data/auxillary/eusm.gpkg")
system("rm data/auxillary/eusm.rds")

