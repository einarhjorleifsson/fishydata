library(omar)
library(tidyverse)
library(mapdeck)
library(sf)
source("~/R/Pakkar2/ramb/TOPSECRET.R")
set_token(key)
con <- connect_mar()

# 2023-05-11: Harbour buffer increased from 100 to 150 meters

# Split Borgarfjordur eystri into two harbours if possible

# Main objective is to get rid of labelled hid points that are whacky, i.e.
#  the hid-id is not in "synch" with the actual harbour position
# Merge harbour points with a lookup table containing hid_std
# Create a 10K buffer around the median value of each hid_std
# 

# Standardized harbour character acronym
harbours.lookup <- 
  readxl::read_excel("~/stasi/gis/harbours/data-raw/stk_harbours.xlsx") |> 
  select(hid:harbour, hid_channel) |> 
  filter(!is.na(hid_std))

# only points were io is "I" (defined as when entering harbour) ---------------
hb <-
  stk_trail(con) %>%
  filter(!is.na(hid)) %>%
  collect(n = Inf) %>%
  filter(io == "I") %>%
  select(hid, lon, lat) |>
  distinct() |> 
  # Thorlaksofn vs Thorshofn
  mutate(hid = case_when(hid == "THH" & lat < 65 ~ "THH",
                         hid == "THH" & lat > 65 ~ "THO",
                         hid == "THO" & lat < 65 ~ "THH",
                         hid == "THO" & lat > 65 ~ "THO",
                         .default = hid)) |> 
  inner_join(harbours.lookup |> select(hid, hid_std, harbour)) |> 
  # Split Borgarfjordur eystri - (only for creating the harbour shapes)
  mutate(hid_std = ifelse(hid_std == "BGJ" & lon < -13.78, "BGJ0", hid_std))

hb |> 
  filter(hid %in% c("THH", "THO")) |> 
  ggplot(aes(lon, lat)) +
  geom_point() +
  facet_wrap(~ hid)

hb |> 
  filter(hid %in% c("THH", "THO")) |> 
  ggplot(aes(lon, lat)) +
  geom_point() +
  facet_wrap(~ hid_std)



hb.pt <-
  hb |> 
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326)
# hb.pt |> write_sf("gpkg/harbour-ioI_points.gpkg")

# convex hull ------------------------------------------------------------------
hb.po <- 
  hb.pt |> 
  select(hid_std) |> 
  group_by(hid_std) |> 
  summarise(do_union = FALSE) |> 
  st_convex_hull()
mapview::mapview(hb.po)
# because of things above:
# generate a convex hull around the io == "I" points
#  because there are some cases were the hid is totally wrong, if done directly
#  the polygon will include the wrong coordinats. hence this Kriskuvikurleid
# Find the median lon/lat for each hid_std
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
hb.hid_std.median |> mapview::mapview()

# Loop through each harbour (hid_std)
HID_STD <-hb.hid_std.median$hid_std |> unique()
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
hb.pt.tidy <- 
  res |> 
  bind_rows() 
hb.po.tidy <-
  hb.pt.tidy |> 
  group_by(hid_std, harbour) |> 
  summarise(do_union = FALSE,
            .groups = "drop") |> 
  st_convex_hull() |> 
  st_transform(crs = 3857) |> 
  st_buffer(dist = 150) |> 
  st_transform(crs = 4326)
tmp <- hb.po.tidy |> select(hid_std)
mapview::mapview(tmp)
hb.po.tidy |> 
  mutate(hid_std = ifelse(hid_std == "BGJ0", "BGJ", hid_std)) |> 
  write_sf("~/stasi/gis/harbours/gpkg/harbours-hidstd_2023-05-11.gpkg")
                       