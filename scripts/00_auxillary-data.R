# Auxillary data ---------------------------------------------------------------
library(tidyverse)
library(omar)
con <- connect_mar()

lubridate::now()
library(sf)
library(tidyverse)
library(gisland)

# Iceland - shoreline -----------------------------------------------------------
# safer way than gisland::read_strandlinur
read_shorline <- function(mainland = TRUE) {
  s <- 
    "IS_50V:strandlina_flakar" %>% 
    gisland::read_lmi()
  tmp <- tempdir()
  write_sf(s, paste0(tmp, "/in.gpkg"))
  cmd <-
    paste0("ogr2ogr ",
           tmp,
           "/out.gpkg ",
           tmp,
           "/in.gpkg",
           " -explodecollections -nlt CONVERT_TO_LINEAR")
  system(cmd)
  s <- 
    read_sf(paste0(tmp, "/out.gpkg")) |> 
    dplyr::filter(!sf::st_is_empty(geom)) |>  
    #sf::st_collection_extract(type = "POLYGON")
    sf::st_make_valid() |> 
    dplyr::mutate(geom = lwgeom::lwgeom_make_valid(geom)) |> 
    mutate(area = st_area(geom),
           on_land = TRUE) |>
    select(area, on_land)
  if(mainland) {
    s <- s |> filter(area == max(area)) |> select(on_land)
  }
  return(s)
}

read_shorline() |> 
  st_write("data-aux/shoreline.gpkg")

# code to be used in main scripts
# st_transform(crs = 3057) |> 
# st_buffer(dist = -100) |> 
# st_transform(crs = 4326)

# Standardized harbours --------------------------------------------------------




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

