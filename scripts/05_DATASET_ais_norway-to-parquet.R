# ...
#
# TODO:
#  * provide another projection (or check geoarrow)
#  * find a way to add the mmsi - this may not be achievable
#  * check if year in each zip is incorrect - seems to be the case for 2024

library(arrow)
library(tidyverse)

# The format of the data is not the same for each year
read_vms1 <- function(fil) {
  d <-
    read_delim(fil, delim = ";") |>
    janitor::clean_names() |>
    mutate(tidspunkt_utc = dmy_hms(tidspunkt_utc)) |>
    select(uid = registreringsmerke,
           vessel = fartoynavn,
           cs = radiokallesignal,
           time = tidspunkt_utc,
           lon = lengdegrad,
           lat = breddegrad,
           speed = fart,
           heading = kurs,
           everything())
  return(d)
}

read_vms2 <- function(fil) {
  read_csv2(fil) %>%
    janitor::clean_names() %>%
    separate(tidspunkt_utc, into = c("date", "time"), sep = " ") %>%
    mutate(time = str_replace_all(time, "\\.", "\\:"),
           time = str_replace_all(time, ",", "."),
           time = paste(date, time),
           time = dmy_hms(time)) |>
    select(-date) |>
    select(uid = registreringsmerke,
           vessel = fartoynavn,
           cs = radiokallesignal,
           time,
           lon = lengdegrad,
           lat = breddegrad,
           speed = fart,
           heading = kurs,
           everything())
}

fil <- dir("data-raw/norway/ais", full.names = TRUE, pattern = "*.zip")
base <- basename(fil)
tdir <- tempdir()
for(i in 1:length(fil)) {
  unzip(fil[i], exdir = tdir)
}
files <-
  tibble(fil = dir(tdir, full.names = TRUE)) |>
  mutate(year = str_replace_all(basename(fil), "\\D", ""),
         year = as.integer(year),
         out = paste0("data/ais/norway")) |>
  arrange(year)
files |> print()

for(i in 1:nrow(files)) {
  print(files[i,])
  yr <- files$year[i]
  if(i != 12) {
    d <- read_vms1(files$fil[i])
  } else {
    d <- read_vms2(files$fil[i])
  }
  d |> glimpse()
  d$time |> year() |> unique() |> print()
  range(d$lon, na.rm = TRUE) |> print()
  range(d$lat, na.rm = TRUE) |> print()
  d <- 
    d |>
    filter(!is.na(lat), !is.na(lon)) |>
    filter(between(lon, -180, 180),
           between(lat, -90, 90)) |>
    mutate(year = year(time))
  d |> count(year) |> print()
  d |>
    group_by(year) |>
    arrow::write_dataset(files$out[i], partitioning = "year", existing_data_behavior = "overwrite")
}
