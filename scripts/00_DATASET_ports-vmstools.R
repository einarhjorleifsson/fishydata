# VMSTOOLS ---------------------------------------------------------------------
# Download the VMStools .tar.gz file from GitHub
# url <- "https://github.com/nielshintzen/vmstools/releases/download/0.77/vmstools_0.77.tar.gz"
# download.file(url, destfile = "vmstools_0.77.tar.gz", mode = "wb")
# # Install the library from the downloaded .tar.gz file
# install.packages("vmstools_0.77.tar.gz", repos = NULL, type = "source")
library(vmstools)
library(tidyverse)
library(sf)
data("harbours")
harbours |>
  as_tibble() |>
  mutate(harbour = iconv(harbours$harbour, from = "latin1", to = "UTF-8")) |>
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) |>
  write_sf("data/ports/ports_vmstools.gpkg")
