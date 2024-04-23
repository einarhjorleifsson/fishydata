# Objective --------------------------------------------------------------------
# Match landings id and landings gear to logbooks
#
# Input:  Logbooks: data/logbooks/rds/station.rds
#         Catch:    data/logbooks/rds/catch.rds
#         Landings: data/landings/agf/stations.parquet
#                   data/landings/lods/stations.parquet
# Output: data/logbooks/rds/station_landings-merge.rds
#
# The matching is done by date not time. Landings data are hence consolidated
#  by date, the landings id is the lowest landings id value within a date
#
# Downstream usage: 02-3_logbooks-processing.R
#
# 
# Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < scripts/02-2_logbooks-landings-coupling.R --vanilla > lgs/02-2_logbooks-landings-coupling_2024-03-08.log &
library(tictoc)

tic()

lubridate::now()


# NOTE: If using years further back: need to double check that visir and station_id
#       are not the same.
YEARS <- 2024:2001

library(data.table)
library(tidyverse)
library(lubridate)
library(arrow)

# Read data from previous step -------------------------------------------------
LGS <- read_rds("data/logbooks/rds/station.rds")
CATCH <- read_rds("data/logbooks/rds/catch.rds")

LODS <- open_dataset("data/landings/lods/stations.parquet") |> collect()
AGF <-  open_dataset("data/landings/agf/stations.parquet") |> collect()

# 4. Add "target" species to each setting --------------------------------------
# Should really have this upstream in the code
#  Used downstream when attempting to correct gear
catch_target <- 
  CATCH |> 
  group_by(.sid) |> 
  mutate(total = sum(catch),
         p = catch / total) |> 
  # highest catch, lowest species number
  arrange(.sid, desc(p), sid) |> 
  slice(1) |> 
  ungroup() |> 
  select(.sid, sid_target = sid, p_target = p, catch_total = total)
print(paste0("Number of logbook records with no catch: ", 
             nrow(LGS) - nrow(catch_target),
             " (",
             100 * (1 - round(nrow(catch_target) / nrow(LGS), 3)),
             "%)"))
LGS <- 
  LGS |> 
  left_join(catch_target) |> 
  mutate(sid_target = replace_na(sid_target, 0),
         p_target = replace_na(p_target, 0),
         catch_total = replace_na(catch_total, 0))

# 5. Add landing id and gid from landings data ---------------------------------
## Landings data - agf ---------------------------------------------------------

### Nearest date match ---------------------------------------------------------
#### Function ------------------------------------------------------------------
match_nearest_date <- function(lb, ln) {
  
  lb.dt <-
    lb %>%
    select(vid, datel) %>%
    distinct() %>%
    setDT()
  
  ln.dt <-
    ln %>%
    select(vid, datel) %>%
    distinct() %>%
    mutate(dummy = datel) %>%
    setDT()
  
  res <-
    lb.dt[, date.ln := ln.dt[lb.dt, dummy, on = c("vid", "datel"), roll = "nearest"]] %>%
    as_tibble()
  
  lb %>%
    left_join(res,
              by = c("vid", "datel")) %>%
    left_join(ln %>% select(vid, date.ln = datel, gid_ln, .lid),
              by = c("vid", "date.ln"))
  
}

n_before_nearest_match <- nrow(LGS)
LGS <-
  LGS |> 
  match_nearest_date(AGF) |> 
  rename(date_ln = date.ln)
n_after_nearest_match <- nrow(LGS)
print(c(n_before_nearest_match, n_after_nearest_match))
LGS <- 
  LGS |> 
  rename(date_ln_agf = date_ln,
         gid_ln_agf = gid_ln,
         .lid_agf = .lid)
### Checks ---------------------------------------------------------------------
LGS |> 
  mutate(has.lid = !is.na(.lid_agf)) |> 
  count(has.lid) |> 
  mutate(p = n / sum(n)) |> 
  knitr::kable(caption = "Missing landings id")
LGS |> 
  mutate(dt = as.integer(difftime(datel, date_ln_agf, units = "days")),
         dt = ifelse(dt <= -5, -5, dt),
         dt = ifelse(dt >=  5,  5, dt)) |> 
  count(dt) |> 
  mutate(p = n / sum(n) * 100,
         pc = cumsum(p),
         p = round(p, 2),
         pc = round(pc, 2)) |> 
  knitr::kable(caption = "Difference in matched logbook and landings dates")

## Landings data - kvoti-lods --------------------------------------------------
n_before_nearest_match <- nrow(LGS)
LGS <-
  LGS |> 
  match_nearest_date(LODS) |> 
  rename(date_ln = date.ln)
n_after_nearest_match <- nrow(LGS)
print(c(n_before_nearest_match, n_after_nearest_match))
LGS <- 
  LGS |> 
  rename(date_ln_lods = date_ln,
         gid_ln_lods = gid_ln,
         .lid_lods = .lid)
### Checks ---------------------------------------------------------------------
LGS |> 
  mutate(has.lid = !is.na(.lid_lods)) |> 
  count(has.lid) |> 
  mutate(p = n / sum(n)) |> 
  knitr::kable(caption = "Missing landings id")
LGS |> 
  mutate(dt = as.integer(difftime(datel, date_ln_lods, units = "days")),
         dt = ifelse(dt <= -5, -5, dt),
         dt = ifelse(dt >=  5,  5, dt)) |> 
  count(dt) |> 
  mutate(p = n / sum(n) * 100,
         pc = cumsum(p),
         p = round(p, 2),
         pc = round(pc, 2)) |> 
  knitr::kable(caption = "Difference in matched logbook and landings dates")


# 6. Save ----------------------------------------------------------------------
LGS   |> write_rds("data/logbooks/rds/station_landings-merge.rds")

# 7. Info ----------------------------------------------------------------------
toc()

print(devtools::session_info())

