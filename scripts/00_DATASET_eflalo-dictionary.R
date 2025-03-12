library(tidyverse)
tmp <- 
  tribble(~type, ~variable, ~code, ~format_unit, ~std_eh, ~mfri,
          "Fishing trip", "Departure datetime", "xx", NA, "T1", NA,
          "Fishing trip", "Arrival datetime", "xx", NA, "T2", NA,
          "Log event", "Start datetime", "xx", NA, "t1", NA,
          "Log event", "End datetime", "xx", NA, "t2", NA)
eformat <- 
  read_csv("data-raw/datamodel/efalo2_format.csv") |> 
  janitor::clean_names() |> 
  select(-x5) |> 
  fill(type) |> 
  # Einar's standards!
  mutate(std_eh = c("vid", "vcn", "VE_FLT", "loa", "kw", "gt", ".tid",
                    "cn1", "hid1", "D1", "HMS1", 
                    "cn2", "hid2", "D2", "HMS2",
                    ".sid", "d1", "hms1", "hms2",
                    "lat1", "lon1", "lat2", "lon2",
                    "gid", "mesch", 
                    "ir", "fao", "met",
                    NA, NA, NA, NA)) |> 
  # mfri agf database
  mutate(mfri = NA) |> 
  # add here datetime formats (code values as frequently used in ices-datacall
  bind_rows(tmp)
