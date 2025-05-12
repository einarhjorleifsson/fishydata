# All data from:
# https://globalfishingwatch.org/data-download/datasets/public-fishing-effort
# Downloaded 2025-05-09

library(arrow)
library(tidyverse)

read_zip <- function(zipfile) {
  
  filenames <- unzip(zipfile, list = TRUE)
  zipdir <- tempfile()
  # Create the dir using that name
  dir.create(zipdir)
  # Unzip the file into the dir
  unzip(zipfile, exdir = zipdir)
  
  d <- read_csv(paste0(zipdir, "/", filenames$Name))
  return(d)
}

# fleet monthly 10th degree resolution -----------------------------------------
zips <- dir("data-raw/gfw/version3", 
            pattern = "fleet-monthly-csvs-10", 
            full.names = TRUE)
res <- map(zips, read_zip)
res |> 
  bind_rows() |> 
  rename(lon = cell_ll_lon,
         lat = cell_ll_lat) |> 
  mutate(year = as.integer(year),
         month = as.integer(month),
         mmsi_present = as.integer(mmsi_present)) |> 
  write_parquet("data/gfw/fleet-monthly-10-v3.parquet")

# mmsi daily 10th degree resolution --------------------------------------------
zips <- dir("data-raw/gfw/version3", 
            pattern = "mmsi-daily-csvs-10-", 
            full.names = TRUE)
res <- map(zips, read_zip)
res |> 
  bind_rows() |> 
  rename(lon = cell_ll_lon,
         lat = cell_ll_lat) |> 
  mutate(mmsi = as.integer(mmsi),
         year = as.integer(year(date))) |> 
  write_dataset("data/gfw/mmsi-daily-10-v3",
                existing_data_behavio
                format = "parquet",r = "overwrite",
                partitioning = c("year"))

# mmsi daily 100th degree resolution -------------------------------------------
# this takes a while ...
zips <- dir("data-raw/gfw/version3/", 
            pattern = "fleet-daily-csvs-100", 
            full.names = TRUE)
for(i in 1:length(zips)) {
  read_zip(zips[i]) |> 
    rename(lon = cell_ll_lon,
           lat = cell_ll_lat) |> 
    mutate(year = as.integer(year(date))) |> 
    write_dataset("data/gfw/fleet-daily-csvs-100-v3",
                  format = "parquet",
                  existing_data_behavior = "overwrite",
                  partitioning = c("year"))
}
