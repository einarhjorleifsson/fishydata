library(rvest)
library(countrycode)
library(tidyverse)
library(here)
library(omar)
con <- connect_mar()



## Call signs and country ------------------------------------------------------
tbl_mar(con, "ops$einarhj.vessel_cs_itu_prefix") |>
  collect() |>
  nanoparquet::write_parquet("data/auxillary/callsign_prefix.parquet")


# agf auxillary ----------------------------------------------------------------
## agf gear --------------------------------------------------------------------
tbl_mar(con, "agf.aflagrunnur_v") |>
  select(starts_with("veidarfaeri")) |>
  distinct() |>
  rename(gid = 1,
         gid_id = 2,
         veidarfaeri = 3) |>
  collect() |>
  arrange(gid) |>
  nanoparquet::write_parquet("data/auxillary/agf_gear.parquet")

## agf species -----------------------------------------------------------------
tbl_mar(con, "agf.aflagrunnur_v") |>
  select(starts_with("fisktegund")) |>
  distinct() |>
  rename(sid = 1,
         sid_id = 2,
         tegund = 3) |>
  collect() |>
  arrange(sid) |>
  nanoparquet::write_parquet("data/auxillary/agf_species.parquet")

