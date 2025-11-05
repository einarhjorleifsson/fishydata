# https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-elektronisk-rapportering-ers
# see data-raw/norway/logbooks

library(duckdb)
library(duckdbfs)
library(tidyverse)

fil <- dir("data-raw/norway/logbooks", full.names = TRUE, pattern = "*.zip")
base <- basename(fil)
tdir <- tempdir()
for(i in 1:length(fil)) {
  unzip(fil[i], exdir = tdir)
}
dir(tdir, full.names = TRUE)
files <-
  tibble(fil = dir(tdir, full.names = TRUE)) |>
  mutate(year = str_replace_all(basename(fil), "\\D", ""),
         year = as.integer(year)) |>
  arrange(year) |> 
  drop_na()
files |> print() |> knitr::kable()

# fangstmelding ----------------------------------------------------------------
files2 <- 
  files |> 
  filter(str_detect(fil, "fangstmelding")) |> 
  mutate(out = "data/logbooks/norway/fangstmelding")
files2
res <- list()
for(i in 1:nrow(files2)) {
  print(files2$year[i])
  res[[i]] <- 
    read_csv2(files$fil[i], show_col_types = FALSE, guess_max = 3e6) |> 
    janitor::clean_names()
}
names(res) <- files2$year
d <- 
  res |> 
  bind_rows(.id = "year") |> 
  mutate(year = as.integer(year),
         meldingstidspunkt = dmy_hms(meldingstidspunkt),
         meldingsdato = dmy(meldingsdato),
         ankomsttidspunkt = dmy_hms(ankomsttidspunkt),
         ankomstdato = dmy(ankomstdato),
         avgangstidspunkt = dmy_hms(avgangstidspunkt),
         avgangsdato = dmy(avgangsdato),
         fisketidspunkt = dmy_hms(fisketidspunkt),
         fiskedato = dmy(fiskedato),
         starttidspunkt = dmy_hms(starttidspunkt),
         startdato = dmy(startdato),
         stopptidspunkt = dmy_hms(stopptidspunkt),
         stoppdato = dmy(stoppdato))


d |>  duckdbfs::write_dataset("data/logbooks/fangstmelding.parquet")

if(FALSE) {
d |> 
  select(lon = startposisjon_lengde,
         lat = startposisjon_bredde) |> 
  drop_na() |> 
  ramb::rb_mapdeck()
}