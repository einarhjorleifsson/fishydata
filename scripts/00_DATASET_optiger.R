library(tidyverse)
library(arrow)
"/home/haf/einarhj/stasi/hafriti/data-raw/o_atw_reg.csv" |> 
  read_csv2(guess_max = 1e6) |> 
  janitor::clean_names() |> 
  janitor::remove_empty(which = "cols") |> 
  rename(vid = skip_id,
         time = data_measure_time,
         speed = ship_speed,
         heading = ship_heading,
         doorspread = portside_door_dist_star_door) |> 
  write_parquet("data/gear/optigear.parquet")
  
