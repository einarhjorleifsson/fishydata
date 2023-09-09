# Auxillary data ---------------------------------------------------------------
library(tidyverse)
library(omar)
con <- connect_mar()

lubridate::now()
library(sf)
library(tidyverse)
library(gisland)

gisland::read_strandlinur() |> 
  mutate(area = st_area(geom)) |> 
  filter(area == max(area)) |> 
  mutate(on_land = TRUE) |> 
  select(on_land) |> 
  st_transform(crs = 3057) |> 
  st_buffer(dist = -100) |> 
  st_transform(crs = 4326) |> 
  st_write("data-raw/island.gpkg")

sf::read_sf("~/stasi/gis/harbours/gpkg/harbours-hidstd_2023-05-02.gpkg") |> 
  st_write("data-raw/harbours-hidstd.gpkg")
readxl::read_excel("~/stasi/gis/harbours/data-raw/stk_harbours.xlsx") |> 
  write_csv("data-raw/stk_harbours.csv")

devtools::session_info()





# Gear code mess ---------------------------------------------------------------
# Ideally this should be fixed upstream e.g. in {omar}
## Orri gear code ---------------------------------------------------------------
gid_orri <- 
  tbl_mar(con, "ops$einarhj.gid_orri_plus") %>% 
  rename(gid = veidarfaeri, gclass = gid2) |> 
  collect(n = Inf) |> 
  arrange(gid)
## Landings (agf) gear code -----------------------------------------------------
gid_agf <- 
  tbl_mar(con, "ask.veidarfaeri") |>
  collect(n = Inf) |>
  select(gid_ln = veidarfaeri, veidarfaeri = heiti) |>
  arrange(gid_ln)
## Landings (kvoti) gear code --------------------------------------------------
# Same as in orri
## Mapping agf-gear to orri-gear
gid_orri |> 
  mutate(gid_ln_agf = case_when(gid == 91 ~  1,
                                gid ==  2 ~  2,
                                gid == 25 ~  3,
                                gid == 29 ~  4,
                                gid ==  2 ~  5, # duplicate of above - Reknet
                                gid ==  6 ~  6,
                                gid ==  9 ~  7,
                                gid == 14 ~  8,
                                gid == 21 ~  9,
                                gid == 12 ~ 10,
                                gid ==  1 ~ 12,
                                gid ==  1 ~ 13, # duplicates of above: Landbeitt lína
                                gid ==  3 ~ 14,
                                gid == 15 ~ 15,
                                gid == 18 ~ 16,
                                gid == 20 ~ 17,  # Note: 18 eldiskví
                                gid == 45 ~ 19,
                                gid == 42 ~ 20,
                                gid ==  1 ~ 21, # duplicates of above: Línutrekt
                                gid == 92 ~ 22,
                                gid == 41 ~ 23,  # Note 24: Sláttuprammi
                                gid == 15 ~ 25,
                                .default = -9)) |> 
  left_join(gid_agf |> rename(gid_ln_agf = gid_ln, veidarfaeri_agf = veidarfaeri)) |> 
  write_rds("data/aux/gear_codes.rds")

# Vessel data ------------------------------------------------------------------
omar::vessels_vessels(con) |>
  collect(n = Inf) |>
  arrange(vid) |>
  write_rds("data/aux/vessels.rds")

# Vesselid-mobileid match ------------------------------------------------------
# https://github.com/einarhjorleifsson/omar/blob/main/data-raw/00_SETUP_mobileid-vid-match.R
tbl_mar(con, "ops$einarhj.mobile_vid") |> 
  collect(n = Inf) |> 
  write_rds("data/aux/vesselid-mobileid-match.rds")

