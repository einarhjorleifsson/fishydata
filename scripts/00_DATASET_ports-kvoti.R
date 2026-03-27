library(sf)
library(tidyverse)
library(nanoparquet)
library(mar)
con <- connect_mar()

ports_kvoti <-
  tbl_mar(con, "kvoti.stadur") |>
  collect() |>
  select(stad_nr, port = heiti) |>
  arrange(stad_nr) |>
  filter(stad_nr > 0) |>
  mutate(port = str_trim(port),
         port = case_when(port == "Vík í Mýrdal" ~ "Vík",
                          port == "Miðsandur, Hvalfirði" ~ "Miðsandur",
                          port == "Borgarfjörður Eystri" ~ "Borgarfjörður",
                          port == "Núpsskóli, Dýrafirði" ~ "Alviðruvör",
                          port == "Núpskatla" ~ "Núpskatla",
                          stad_nr == 102 ~ "Litli Árskógssandur",
                          #pid == 103 ~ "Árskógsströnd",
                          #port == "XXX Arnarstapi ekki nota" ~ NA,
                          port == "Svalbarðsströnd" ~ "Svalbarðseyri",
                          #port == "Reyðarfjörður" ~ "Búðareyri",
                          port == "Skútustaðahreppur" ~ "Mývatn",
                          port == "Flatey á Breiðafirði" ~ "Flatey",
                          .default = port)) |>
  filter(!stad_nr %in% c(150, 153, 155:212, 300:999)) |>
  add_row(stad_nr = 140, port = "Grundartangi")
ports_kvoti |> write_parquet("data/ports/ports_kvoti.parquet")