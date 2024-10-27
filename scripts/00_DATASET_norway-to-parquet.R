# ...
#
# TODO:
#  * provide another projection (or check geoarrow)
#  * find a way to add the mmsi
#

library(arrow)
library(tidyverse)

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

fil <- dir("data-raw/norway", full.names = TRUE, pattern = "*.zip")
base <- basename(fil)
tdir <- tempdir()
for(i in 1:length(fil)) {
  unzip(fil[i], exdir = tdir)
}
files <-
  tibble(fil = dir(tdir, full.names = TRUE)) |>
  mutate(year = str_replace_all(basename(fil), "\\D", ""),
         year = as.integer(year),
         out = paste0("data/norway")) |>
  arrange(year)
files |> print()

for(i in 1:nrow(files)) {
  print(files$fil[i])
  if(i %in% c(1:11, 13)) {
    d <- read_vms1(files$fil[i])
  } else {
    d <- read_vms2(files$fil[i])
  }
  d |> glimpse()
  d$time |> year() |> unique() |> print()
  range(d$lon, na.rm = TRUE) |> print()
  range(d$lat, na.rm = TRUE) |> print()
  d |>
    filter(!is.na(lat), !is.na(lon)) |>
    filter(between(lon, -180, 180),
           between(lat, -90, 90)) |>
    mutate(year = year(time)) |>
    group_by(year) |>
    arrow::write_dataset(files$out[i])
}
