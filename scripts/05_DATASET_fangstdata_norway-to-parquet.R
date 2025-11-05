# https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-fangstdata-seddel-koblet-med-fartoydata
# see data-raw/norway/fangstdata

library(duckdb)
library(duckdbfs)
library(tidyverse)

fil <- dir("data-raw/norway/fangstdata", full.names = TRUE, pattern = "*.zip")
base <- basename(fil)
tdir <- tempdir()
for(i in 1:length(fil)) {
  unzip(fil[i], exdir = tdir)
}
files <-
  tibble(fil = dir(tdir, full.names = TRUE)) |>
  filter(str_ends(fil, ".csv")) |> 
  mutate(year = str_replace_all(basename(fil), "\\D", ""),
         year = as.integer(year),
         out = paste0("data/landings/norway")) |>
  arrange(year) |> 
  drop_na()
files |> print() |> knitr::kable()
res <- list()
# the ifs below may not be needed given now using guess_max
for(i in 1:nrow(files)) {
  print(files$year[i])
  res[[i]] <- 
    read_csv2(files$fil[i], show_col_types = FALSE, guess_max = 3e6) |> 
    janitor::clean_names() |> 
   mutate(fartoytype_kode = as.character(fartoytype_kode),
          fartoy_id = as.character(fartoy_id),
          hovedomrade_fao_kode = as.character(hovedomrade_fao_kode),
          neste_mottaksstasjon = as.character(neste_mottaksstasjon),
          forrige_mottakstasjon = as.character(forrige_mottakstasjon))
  if(files$year[i] >= 2017) {
    res[[i]] <-
      res[[i]] |>
      mutate(
        oppdateringstidspunkt =
          paste0(str_sub(oppdateringstidspunkt, 1, 2),
                 ".",
                 str_sub(oppdateringstidspunkt, 3, 4),
                 ".",
                 str_sub(oppdateringstidspunkt, 5),
                 " 00:00:00"))
  }
}

names(res) <- files$year
d <- 
  res |> 
  bind_rows(.id = "year") |> 
  mutate(year = as.integer(year)) |> 
  mutate(dokument_salgsdato = dmy(dokument_salgsdato),
         dokument_versjonstidspunkt = dmy(dokument_versjonstidspunkt))
d |>
  duckdbfs::write_dataset("data/landings/fangstdata.parquet")

