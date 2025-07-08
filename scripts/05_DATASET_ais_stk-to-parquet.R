library(tidyverse)
library(mar)
con <- connect_mar()
q <- 
  stk_trail(con) |> 
  left_join(tbl(con, dbplyr::in_schema("STK", "MOBILE")) |>
              select(mid = MOBILEID, loid = LOCALID, glid = GLOBALID)) |> 
  mutate(year = year(time),
         month = month(time)) |> 
  filter(time >= to_date("2007-06-01", "YYYY:MM:DD"),
         between(lon, -179.9, 179.9),
         between(lat, -89.9, 89.9))

#for(y in 2007:2025) {
for(y in 2025) {
  YEAR <- y
  print(YEAR)
  q |> 
    filter(year == YEAR) |> 
    collect() |> 
    select(mid, loid, glid, everything()) |> 
    mutate(month = as.integer(month)) |> 
    arrange(mid, time) |> 
    arrow::write_dataset("data/ais/stk-raw", format = "parquet",
                         existing_data_behavior = "overwrite",
                         partitioning = c("year"))
}

