library(duckdbfs)
library(tidyverse)
library(mar)

# Setup ------------------------------------------------------------------------

con <- connect_mar()

# Pull raw STK trail from Oracle -----------------------------------------------

q <-
  tbl_mar(con, "stk.trail") |>
  # poslon / poslat / heading are stored as radians in Oracle
  mutate(lon = poslon * 180/pi,
         lat = poslat * 180/pi, heading = heading * 180/pi,
         # speed in m/s → knots
         speed = speed * 3600/1852) |>
  select(.id = trailid, mid = mobileid,
         time = posdate, lon, lat, speed, heading, hid = harborid, io = in_out_of_harbor,
         recdate) |>
  mutate(.id = as.integer(.id),
         mid = as.integer(mid),
         year = year(recdate))

# Write by year ----------------------------------------------------------------

T1 <- seq.Date(from = ymd("2007-01-01"), to = ymd("2030-01-01"), by = "year")
# stop at current year — future boundaries are placeholders only
i <- year(T1) <= year(today())
T1 <- T1[i]
T2 <- T1 + years(1)
current_year <- max(year(T1))

for(i in 1:length(T1)) {

  path <- "data-raw/ais/stk"

  t1 <- T1[i]
  t2 <- T2[i]
  print(t1)
  # duckdbfs::write_dataset always writes data_0.parquet as the first file
  exists <- file.exists(paste0(path, "/year=", year(t1), "/data_0.parquet"))
  # current year is always re-fetched to pick up records added since last run
  if(!exists | year(t1) == current_year) {
    q |>
      # filter on recdate (DB receipt timestamp), not posdate —
      #. this is because recdate if intexed in the Oracle table
      # ensures each record falls in exactly one annual partition
      filter(recdate >= to_date(t1, "YYYY:MM:DD"),
             recdate <  to_date(t2, "YYYY:MM:DD")) |>
      collect() |>
      mutate(year = as.integer(year)) |>
      duckdbfs::write_dataset(path = path,
                              overwrite = TRUE,
                              partitioning = c("year"))
  }
}
