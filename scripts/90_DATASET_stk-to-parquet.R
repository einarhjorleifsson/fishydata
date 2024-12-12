library(tidyverse)
library(mar)
con <- connect_mar()
q <- 
  stk_trail(con) |> 
  left_join(tbl(con, dbplyr::in_schema("STK", "MOBILE")) |>
              select(mid = MOBILEID, loid = LOCALID, glid = GLOBALID)) |> 
  mutate(year = year(time),
         month = month(time))

for(y in 2007:2024) {
  YEAR <- y
  print(YEAR)
  q |> 
    filter(year == YEAR) |> 
    collect() |> 
    arrow::write_dataset("data/ais/stk_raw", format = "parquet",
                       partitioning = c("year"))
}

  