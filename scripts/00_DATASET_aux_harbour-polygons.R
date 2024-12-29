# AUXILLARY DATA ---------------------------------------------------------------
library(sf)
library(tidyverse)
library(omar)
library(arrow)
library(sfarrow)
con <- connect_mar()



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
#         Sufficient in this specific case, could probably be less
#         Add a 100 buffer on the convex hull
#   4. Output: data-aux/harbours.gpkg


# Get standardized harbour character acronym
harbours.lookup <- 
  readxl::read_excel("data-raw/harbours/harbours.xlsx") |> 
  select(hid, hid_std, harbour) |> 
  filter(!is.na(hid_std))

## 1. Extract all points from stk were variable io is defined as "I" ------------
#  In the oracle stk.trail tail table the original variable names are:
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

## 4. Output -------------------------------------------------------------------
hb.po |> 
  mutate(hid_std = ifelse(hid_std == "BGJ0", "BGJ", hid_std)) |> 
  write_sf("data/auxillary/harbours.gpkg")

