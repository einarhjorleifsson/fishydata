library(rvest)
library(countrycode)
library(tidyverse)
library(here)

## MMSI country code -----------------------------------------------------------
url <- "https://en.wikipedia.org/wiki/Maritime_identification_digits"
maritime_identification_digits <-
  url %>%
  read_html() %>%
  #html_nodes(xpath='//*[@id="mw-content-text"]/table[1]') %>%
  html_table() |>
  bind_rows() |>
  janitor::clean_names() |>
  rename(MID = codes)
maritime_identification_digits <-
  maritime_identification_digits |>
  mutate(MID = str_remove(MID, "\\(218 from former German Democratic Republic\\)")) |>
  separate(MID, into = paste0("c", 1:20), extra = "drop", sep = ";") |>
  gather(dummy, MID, -country) |>
  select(-dummy) |>
  mutate(MID = str_trim(MID)) |>
  drop_na() |>
  mutate(country =
           case_when(
             country == "Alaska (State of)" ~ "United States",
             country == "Ascension Island" ~ "Great Britain",
             country == "Azores (Portuguese isles of)" ~ "Portugal",
             country == "Bonaire, Sint Eustatius and Saba - Netherlands (Kingdom of the)" ~ "Netherlands",
             country == "Crozet Archipelago" ~ "France",
             country == "Curaçao - Netherlands (Kingdom of the)" ~ "Netherlands",
             country == "Sint Maarten (Dutch part)" ~ "Netherlands",
             country == "Guiana (French Department of)" ~ "French Guiana",
             country == "Kerguelen Islands" ~ "France",
             country == "Madeira (Portuguese isles of)" ~ "Portugal",
             country == "Rwandese Republic" ~ "Rwanda",
             country == "Saint Paul and Amsterdam Islands" ~ "France",
             TRUE ~ country),
         flag = countrycode(country, "country.name", "iso3c")) |>
  # here we drop some expat dutch
  distinct()

maritime_identification_digits |>
  group_by(MID) |>
  mutate(n = n()) |>
  filter(n > 1) |>
  knitr::kable(caption = "Expect nothing")

maritime_identification_digits |>
  nanoparquet::write_parquet("data/lookups/maritime_identification_digits.parquet")

## Call signs and country ------------------------------------------------------
mar::tbl_mar(mar::connect_mar(), "ops$einarhj.vessel_cs_itu_prefix") |>
  collect() |>
  nanoparquet::write_parquet("data/lookups/callsign_prefix.parquet")
