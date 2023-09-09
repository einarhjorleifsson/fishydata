# AUXILLARY DATA ---------------------------------------------------------------
library(sf)
library(tidyverse)
library(omar)
con <- connect_mar()

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
# Inputs:
#  stk.trail
#  data-raw/stk_harbours.xlsx
# Output: gpkg/harbours.gpkg
#  
# Processing:
#  1. Extract all points from stk were variable io is defined as "I"
#  2. Add standardized harbour-id and name
#     some minor corrections also done
#  3. Create shape
#   3.1. A simple convex hull - because that does not work do:
#   3.3. Calculate the median lon-lat point for each point assigned to harbour
#   3.3. Create convex hull conditional on point within 10km from median
#         Sufficent in this specific case, could probably be less
#         Add a 100 buffer on the convex hull
#   4. Output: data-aux/harbours.gpkg


# Get standardized harbour character acronym
harbours.lookup <- 
  readxl::read_excel("data-aux/harbours.xlsx") |> 
  select(hid, hid_std, harbour) |> 
  filter(!is.na(hid_std))

## 1. Extract all points from stk were variable io is defined as "I" ------------
#  In the oracle stk.trail tail table the orginal variable names are:
#    io: "in_out_of_harbor"
#    hid: "harborid"
hb <-
  stk_trail(con) %>%
  filter(!is.na(hid)) %>%
  collect(n = Inf) %>%
  filter(io == "I") %>%
  select(hid, lon, lat) |>
  distinct()
##  2. Add standardized harbour-id and name
hb <- 
  hb |> 
  # Thorlaksofn vs Thorshofn
  mutate(hid = case_when(hid == "THH" & lat < 65 ~ "THH",
                         hid == "THH" & lat > 65 ~ "THO",
                         hid == "THO" & lat < 65 ~ "THH",
                         hid == "THO" & lat > 65 ~ "THO",
                         .default = hid)) |> 
  inner_join(harbours.lookup |> select(hid, hid_std, harbour),
             multiple = "all") |> 
  # Split Borgarfjordur eystri - (only for creating the harbour shapes)
  mutate(hid_std = ifelse(hid_std == "BGJ" & lon < -13.78, "BGJ0", hid_std))

##  3. Create shape -------------------------------------------------------------
hb.pt <-
  hb |> 
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326)
### 3.1. A simple convex hull ---------------------------------------------------
hb.po <- 
  hb.pt |> 
  select(hid_std) |> 
  group_by(hid_std) |> 
  summarise(do_union = FALSE) |> 
  st_convex_hull()
# mapview::mapview(hb.po)
# because of things above:
# generate a convex hull around the io == "I" points
#  because there are some cases were the hid is totally wrong, if done directly
#  the polygon will include the wrong coordinats. hence this Kriskuvikurleid

### 3.3. Calculate the median lon-lat point for each point assigned to harbour ---
hb.hid_std.median <- 
  hb |> 
  group_by(hid_std) |>
  summarise(lon = median(lon),
            lat = median(lat),
            .groups = "drop") |> 
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) |> 
  st_transform(crs = 3857) |> 
  st_buffer(dist = 10000) |> 
  st_transform(crs = 4326) |> 
  mutate(within10K = TRUE)
# hb.hid_std.median |> mapview::mapview()

### 3.3. Create convex hull conditional on point within 10km from median --------
# Loop through each harbour (hid_std)
HID_STD <- hb.hid_std.median$hid_std |> unique()
res <- list()
for(p in 1:length(HID_STD)) {
  print(p)
  res[[p]] <-
    hb.pt |> 
    filter(hid_std %in% HID_STD[p]) |> 
    select(-hid_std) |> 
    st_join(hb.hid_std.median |> 
              filter(hid_std == HID_STD[p])) |> 
    filter(within10K) |> 
    select(-within10K)
}
hb.pt <- 
  res |> 
  bind_rows()
hb.po <-
  hb.pt |> 
  group_by(hid_std, harbour) |> 
  summarise(do_union = FALSE,
            .groups = "drop") |> 
  st_convex_hull() |> 
  st_transform(crs = 3857) |> 
  st_buffer(dist = 100) |> 
  st_transform(crs = 4326)
# tmp <- hb.po |> select(hid_std)
# mapview::mapview(tmp)

## 4. Output: harbours/gpkg/harbours.gpkg ---------------------------------------
hb.po |> 
  mutate(hid_std = ifelse(hid_std == "BGJ0", "BGJ", hid_std)) |> 
  write_sf("data-aux/harbours.gpkg")

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
  write_rds("data-aux/gear_codes.rds")

# Vessel data ------------------------------------------------------------------
omar::vessels_vessels(con) |>
  collect(n = Inf) |>
  arrange(vid) |>
  write_rds("data-aux/vessels.rds")

# Vesselid-mobileid match ------------------------------------------------------
## NOT RUN ---------------------------------------------------------------------
# https://github.com/einarhjorleifsson/omar/blob/main/data-raw/00_SETUP_mobileid-vid-match.R
# tbl_mar(con, "ops$einarhj.mobile_vid") |> 
#   collect(n = Inf) |> 
#   write_rds("data/aux/vesselid-mobileid-match.rds")

