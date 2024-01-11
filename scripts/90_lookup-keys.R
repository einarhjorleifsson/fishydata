library(arrow)
library(tidyverse)

# STK - LOGBOOK station id -----------------------------------------------------
AIS <- 
  open_dataset("data/ais") |> 
  filter(vms == "yes")
# Note that the 
cid_sid <- 
  AIS |> 
  filter(!is.na(.sid)) |> 
  group_by(vid, .cid, .sid) |> 
  summarise(pings = n()) |> 
  collect()

# LOGBOOK - LANDINGS link ------------------------------------------------------
