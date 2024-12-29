# TODO:
#  * include astd data via mmsi match
#    * question what to do with speed (and heading)
#  * test for whacky points - this could be done per mobileid
#  * standardize/correct harbour code
#  * points in
#     * harbour
#     * eez
#     * habitat
#     * ..
#  * create trips
#  * link to logbooks
#  * link to landings id
#  ...
library(arrow)
library(tidyverse)
stk <- 
  open_dataset("data/ais/stk-raw") |> 
  to_duckdb() |> 
  select(mid:io, year, month)
stk_vid <- 
  open_dataset("data/vessels/stk_vessel_match.parquet") |> to_duckdb() |> 
  select(mid, type, d1, d2, no, vid, mmsi) |> 
  filter(!is.na(vid))
q <- 
  stk |> 
  inner_join(stk_vid,
             by = join_by(mid, between(time, d1, d2))) |> 
  select(vid, everything()) |> 
  arrange(vid, time)
for(y in 2007:2024) {
    YEAR <- y
    print(YEAR)
    q |> 
      filter(year == YEAR) |> 
      collect() |> 
      # order maters here when using distinct below
      arrange(vid, time, hid, io) |> 
      distinct(vid, time, .keep_all = TRUE) |> 
      arrow::write_dataset("data/ais/stk-vid", format = "parquet",
                           partitioning = c("year"))
}
