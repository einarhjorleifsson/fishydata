# TODO:
#  Check why catch is NA in a lot of cases
#  Note: The catch comes from the logbooks but the effort is from the ais

library(tidyverse)
library(arrow)

# data -------------------------------------------------------------------------
vessels <- 
  open_dataset("data/vessels/vessels_iceland.parquet") |> 
  to_duckdb()
gears <- 
  open_dataset("data/gear/gear_mapping.parquet") |> 
  to_duckdb() |> 
  select(agf_gid = gid_agf, s1, s2, metier = met5)
ais <- 
  open_dataset("data/ais/trail") |> 
  to_duckdb() |> 
  filter(year %in% 2009:2024,
         (whack == FALSE | is.na(whack)),
         .cid > 0,                      # out of harbour
         !is.na(gid_trip)) |>           # a fishing trip
  select(vid, lon, lat, time, speed, .cid, dt, dd, year, month, gid_trip)
lb <- 
  open_dataset("data/logbooks/station-for-ais.parquet") |>
  to_duckdb() |> 
  filter(year(date) %in% 2009:2024) |> 
  filter(!is.na(agf_gid)) |> 
  select(vid, t1, t2, agf_gid, .sid, catch_total)

# missing kw -------------------------------------------------------------------
vessels <- 
  ais |> 
  group_by(vid) |> 
  summarise(n = n(),
            .groups = "drop") |> 
  left_join(vessels |> select(vid, loa, kw) |> mutate(in_registry = "yes"))  |> 
  collect() |> 
  mutate(in_registry = replace_na(in_registry, "no"))
vessels |> filter(in_registry == "no") |> knitr::kable(caption = "expect none")
vessels |> filter(loa == 0) |> knitr::kable(caption = "This needs to be checked")
vessels |> filter(is.na(kw)) |> knitr::kable(caption = "Fixed in next step")
vessels <-
  vessels |> 
  mutate(loar = round(loa)) |> 
  group_by(loar) |> 
  mutate(median = median(kw, na.rm = TRUE)) |> 
  ungroup() |> 
  mutate(kw = case_when(is.na(kw) ~ median,
                        .default = kw)) |> 
  select(vid, kw)
vessels |>  filter(is.na(kw)) |> knitr::kable(captoin = "expect none")
vessels <- vessels |> to_duckdb()
# analysis ---------------------------------------------------------------------
d <-
  ais |> 
  left_join(lb,
            by = join_by(vid, between(time, t1, t2))) |> 
  left_join(gears |> rename(gid_trip = agf_gid),
            by = join_by(gid_trip)) |> 
  left_join(vessels,
            by = join_by(vid)) |> 
  filter(speed >= s1,
         speed <= s2) |> 
  filter(!is.na(.sid)) |> 
  group_by(vid, year, .cid, .sid) |> 
  mutate(N = n()) |>    # to split the catch
  ungroup()
d |> count(gid_trip, metier) |> collect() -> tmp
tmp |> arrange(-n) |> knitr::kable()
  
## by ices rectangle -----------------------------------------------------------
dx <- 1
dy <- dx / 2
by_rectangle <- 
  d |> 
  filter(year %in% 2013:2023) |> 
  mutate(lon = lon %/% dx * dx + dx/2,
         lat = lat %/% dy * dy + dy/2) |> 
  group_by(year, month, metier, lon, lat) |>  
  summarise(effort_hours = sum(dt, na.rm = TRUE) / 3600,
            kwh = sum(dt / 3600 * kw, na.rm = TRUE),
            catch = sum(catch_total / N, na.rm = TRUE),  # just for checks
            .groups = "drop") |>
  collect()
## by 0.05 decimal grid --------------------------------------------------------
dx <- dy <- 0.05
by_005 <- 
  d |> 
  filter(year %in% 2015:2023) |> 
  mutate(lon = lon %/% dx * dx + dx/2,
         lat = lat %/% dy * dy + dy/2) |> 
  group_by(year, metier, lon, lat) |> 
  summarise(effort_hours = sum(dt, na.rm = TRUE) / 3600,
            kwh = sum(dt / 3600 * kw, na.rm = TRUE),
            catch = sum(catch_total / N, na.rm = TRUE),  # just for checks
            .groups = "drop") |> 
  collect()

## by 0.01x0.005 decimal grid --------------------------------------------------------
dx <-  0.005
dy <- dx / 2
by_0005 <- 
  d |> 
  filter(year %in% 2015:2023) |> 
  mutate(lon = lon %/% dx * dx + dx/2,
         lat = lat %/% dy * dy + dy/2) |> 
  group_by(year, metier, lon, lat) |> 
  summarise(effort_hours = sum(dt, na.rm = TRUE) / 3600,
            kwh = sum(dt / 3600 * kw, na.rm = TRUE),
            catch = sum(catch_total / N, na.rm = TRUE),  # just for checks
            .groups = "drop") |> 
  collect()

# checks -----------------------------------------------------------------------
metier_used <- 
  read_parquet("data/gear/gear_mapping.parquet") |> 
  filter(gid_agf %in% c(1:16, 21, 22)) |> 
  pull(met5) |> 
  unique()

by_005 |> 
  #filter(metier %in% metier_used) |> 
  group_by(year) |> 
  reframe(catch = sum(catch,na.rm = TRUE) / 1e6) |> 
  ggplot(aes(year, catch)) +
  geom_point() +
  geom_point(data = 
               lb |>
               collect() |> 
               mutate(year = year(t1)) |> 
               filter(year %in% 2015:2023) |> 
               group_by(year) |> 
               summarise(catch = sum(catch_total) / 1e6),
             colour = "red")
source("R/ramb_functions.R")
by_0005 |> 
  filter(year == 2023,
         metier == "OTB_DEF") |> 
  filter(between(lon, -30, -10)) |> 
  select(lon, lat, dt = catch) |> 
  rb_leaflet_raster()

# export -----------------------------------------------------------------------
by_rectangle |> 
  select(-catch) |> 
  filter(metier %in% metier_used) |> 
  arrange(year, month, metier) |> 
  write_csv("exports/91_datacall_buseful_2025-05-05_by_rectangle.csv")

by_005 |> 
  select(-catch) |> 
  filter(metier %in% metier_used) |> 
  arrange(year, metier) |> 
  write_csv("exports/91_datacall_buseful_2025-05-05_by_005.csv")

by_0005 |> 
  filter(metier %in% metier_used) |> 
  arrange(year, metier) |> 
  arrow::write_dataset(path = "~/stasi/fishydata/data/ais/grid",
                       format = "parquet",
                       existing_data_behavior = "overwrite")
