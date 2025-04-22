# Harbour identification numbers


# Polygons based on stk --------------------------------------------------------
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
#         Sufficient in this specific case, could probably be less
#         Add a 100 buffer on the convex hull
#   4. Output: data-aux/harbours.gpkg




# Harbour polygons from stk data -----------------------------------------------
## 1. Extract all points from stk were variable io is defined as "I" -----------
#  In the oracle stk.trail tail table the original variable names are:
#    io: "in_out_of_harbor"
#    hid: "harborid"


library(sf)
library(tidyverse)
library(arrow)


hb_raw <-
  open_dataset("data/ais/stk-raw") |> 
  filter(!is.na(hid)) %>%
  collect(n = Inf) |> 
  filter(io == "I") %>%
  select(stk = hid, lon, lat) |>
  mutate(lon = round(lon, 4),
         lat = round(lat, 4)) |> 
  distinct()
# 
hb <- 
  hb_raw |> 
  filter(!stk %in% c("ISL", "DNG",  "BER", "REYF", "RYH", "LSGUFA", "GYH", "IYH",
                     "AYH", "GRYYH", "R", "MJO", "MJOIF")) |> 
  filter(!(stk == "THH" & lat > 65)) |> 
  filter(!(stk == "THO" & lat < 65)) |> 
  filter(!(stk == "SEY" & lon < -20)) |> 
  filter(!(stk == "FLA" & lon < 65.963447)) |> 
  filter(!(stk == "RES" & lat < 65)) |> 
  filter(!(stk == "KRS" & lat < 64)) |> 
  filter(!(stk == "HUS" & lon > -16)) |> 
  filter(!(stk == "FA" & lat < 64.872063)) |> 
  filter(!(stk == "DRA" & lat > 65.866120)) |> 
  filter(!(stk == "HVF" & lon > -21.416204)) |> 
  filter(!(stk == "KRS" & lat < 65.699448)) |> 
  filter(!(stk == "FA" & lat < 64.916849)) |> 
  mutate(stk = case_when(stk == "H" ~ "HNR",
                         stk == "SNG" ~ "SAN",
                         stk == "G" ~ "GRD",
                         stk %in% c("KF", "KEF") ~ "KEV",
                         stk == "NJ" ~ "NJA",
                         stk == "VO" ~ "VOG",
                         stk == "HF" ~ "HAF",
                         stk == "GB" ~ "GRB",
                         stk == "KV" ~ "KOV",
                         stk == "HVF" ~ "HVR", 
                         stk %in% c("AK", "AKR2") ~ "AKR",
                         stk == "BG" ~ "BOR",
                         stk == "GF" ~ "GRF",
                         stk == "OFS" ~ "OLV",
                         stk %in% c("STM", "STY2") ~ "STY",
                         stk == "HVAL" ~ "HVL",
                         stk == "PURK" ~ "PUR",
                         stk == "REYK" ~ "RHA",
                         stk == "STAD" ~ "STU",
                         stk == "PT" ~ "PAT",
                         stk == "TF" ~ "TAL",
                         stk == "BD" ~ "BIL",
                         stk == "THI" ~ "TEY",
                         stk %in% c("SUD2", "SUG") ~ "SUD",
                         stk == "IS" ~ "ISA",
                         stk == "SK" ~ "SUV",
                         stk == "NFJ" ~ "NOU",
                         stk == "GJ" ~ "GJO",
                         stk == "DRN" ~ "DRA",
                         stk == "HK" ~ "HOL",
                         stk == "HVT" ~ "HVM",
                         stk == "BL" ~ "BLO",
                         stk == "SR" ~ "SAU",
                         stk == "HFS" ~ "HOF",
                         stk == "SG" ~ "SIG",
                         stk == "OF" ~ "OLF",
                         stk == "DL" ~ "DAL",
                         stk == "AR" ~ "ASS",
                         stk == "HG" ~ "HAU",
                         stk == "HJE" ~ "HJA",
                         stk == "A" ~ "AKU",
                         stk == "SVB" ~ "SVA",
                         stk == "GK" ~ "GRE",
                         stk == "HRY" ~ "HRI",
                         stk == "HU" ~ "HUS",
                         stk == "KP" ~ "KOP",
                         stk == "RH" ~ "RAU",
                         stk %in% c("BAK2", "BKF") ~ "BAK",
                         stk == "VP" ~ "VPN",
                         stk == "BF" ~ "BGJ",
                         stk == "SF" ~ "SEY",
                         stk == "NK" ~ "NES",
                         stk == "RF" ~ "RFJ",
                         stk == "FA" ~ "FAS",
                         stk == "STF" ~ "STD",
                         stk == "BRK" ~ "BRE",
                         stk == "DP" ~ "DJU",
                         stk == "VM" ~ "VES",
                         stk == "EB" ~ "EYB",
                         .default = stk))

# hb |> ramb::rb_mapdeck(radius = 10, col = "stk", tooltip = "stk")

# Split some harbours - older harbour retains name, newer harbour gets 2 added
hb_split <- 
  hb |> 
  mutate(stk = case_when(stk == "KEV" & between(lat, 64.002491, 64.007134) ~ NA,
                         stk == "KEV" & lat > 64.007134 ~ "KEV2",
                         stk == "KOV" & lon >= -21.932025 ~ "KOV2",
                         stk == "AKR" & lon < -22.091226 ~ "AKR2",
                         stk == "STY" & lon <  -22.732919 ~ "STK2",
                         stk == "SUD" & lon <  -23.537276 ~ "SUD2",     # Not really a harbour?
                         stk == "DRA" & lon <   -21.471696  ~ "DRA2",
                         stk == "AKU" & lat > 65.697073 ~ "KRS",        # relabelling
                         stk == "BAK" & lat > 66.029713 ~ NA,           # really not a harbour
                         stk == "BGJ" & lon > -13.787222 ~ "BGJ2",
                         stk == "DJU" & between(lon, -14.290093, -14.284439) ~ NA,
                         stk == "DJU" & lon < -14.290093 ~ "DJU2",
                         stk == "REY" & lat >= 64.159826 & lon > -21.859520 ~ "VID",
                         stk == "REY" & lon < -21.929564 ~ stk,
                         stk == "REY" & lon >  -21.881557 & lat >  64.136520 ~ "REY2",
                         stk == "REY" ~ NA,
                         .default = stk)) |> 
  filter(!is.na(stk)) |> 
  mutate(pid = case_when(str_starts(stk, "FO-") ~ stk,
                         .default = paste0("IS-", stk)))
                         


ports <-
  hb_split |> 
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) |> 
  select(pid) |> 
  group_by(pid) |> 
  summarise(do_union = FALSE) |> 
  st_convex_hull()
mapview::mapview(ports)

ports |>  st_write("data/auxillary/ports.gpkg")
