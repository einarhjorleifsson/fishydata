# TODO:
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
v <- open_dataset("data/vessels/vessels_iceland.parquet") |> 
  to_duckdb() |> 
  select(vid, mmsi) |> 
  filter(!is.na(mmsi))
stk <- 
  open_dataset("data/ais/stk_raw") |> 
  to_duckdb() |> 
  select(mid:io, year, month)
stk_vid <- 
  open_dataset("data/vessels/stk_vessel_match.parquet") |> to_duckdb() |> 
  select(mid:glid, type, d1, d2, no, vid) |> 
  filter(!is.na(vid))
q <- 
  stk |> 
  inner_join(stk_vid,
             by = join_by(mid, between(time, d1, d2))) |> 
  select(vid, everything()) |> 
  left_join(v) |> 
  arrange(vid, time)
for(y in 2007:2024) {
    YEAR <- y
    print(YEAR)
    q |> 
      filter(year == YEAR) |> 
      collect() |> 
      arrow::write_dataset("data/ais/stk_vid", format = "parquet",
                           partitioning = c("year"))
}
