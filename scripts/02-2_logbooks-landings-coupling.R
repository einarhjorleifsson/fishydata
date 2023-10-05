# Objective --------------------------------------------------------------------
# Match landings id and gear to logbooks
#
# Input:  Logbooks: data/logbooks/station.rds
#         Catch:    data/logbooks/catch.rds
#         Landings: Oracle database
# Output: data/logbooks/logbooks_2009p.parguet
#         data/logbooks/catch_2009p.parquet
# Downstream usage: R/02-2_logbooks-gear-correction.R
#
# Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < scripts/02-1_logbooks-merge.R --vanilla > lgs/02-1_logbooks-merge_2023-09-09.log &
lubridate::now()


# NOTE: If using years further back: need to double check that visir and station_id
#       are not the same.
YEARS <- 2022:2009

library(data.table)
library(tidyverse)
library(lubridate)
library(omar)
con <- connect_mar()

# Read data from previous step -------------------------------------------------
LGS <- read_rds("data/logbooks/station.rds")
CATCH <- read_rds("data/logbooks/catch.rds")

# 4. Add "target" species to each setting --------------------------------------
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
LGS <- 
  LGS |> 
  left_join(catch_target)


# 5. Add landing id and gid from landings data ---------------------------------
## Landings data - agf ---------------------------------------------------------
LN_raw <- 
  omar::ln_agf(con) |> 
  filter(wt > 0,
         year(date) %in% YEARS) |> 
  rename(.lid = .id,
         hid_ln = hid,
         gid_ln = gid,
         datel = date) |> 
  collect(n = Inf) |> 
  filter(vid %in% c(2, 5:3699, 5000:9998)) |> 
  mutate(datel = as_date(datel),
         vid = as.integer(vid),
         gid_ln = as.integer(gid_ln),
         hid_ln = as.integer(hid_ln)) |> 
  arrange(vid, datel, .lid, gid_ln, sid) |>
  group_by(vid, datel) |> 
  mutate(.lid_min = min(.lid)) |> 
  ungroup()

### Checks ---------------------------------------------------------------------
checks <- 
  LN_raw |> 
  group_by(vid, datel) |> 
  summarise(n_harbours = n_distinct(hid_ln),
            n_gears = n_distinct(gid_ln),
            n_lid = n_distinct(.lid),
            .groups = "drop")
#### Same landings date, different harbours ------------------------------------
checks |> count(n_harbours) |> mutate(p = round(n / sum(n), 3))
#### Same landings date, different gear ----------------------------------------
checks |> count(n_gears) |> mutate(p = round(n / sum(n), 3))
#### Same landings date, different landings id ---------------------------------
checks |> count(n_lid) |> mutate(p = round(n / sum(n), 3))

### Date and gear only ---------------------------------------------------------
LN <-  
  LN_raw |> 
  select(vid, datel, gid_ln, .lid_min) |> 
  # here only keep one gid record and the lowest .lid wihtin a landings date
  distinct(vid, datel, .lid_min, .keep_all = TRUE)
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
    left_join(ln %>% select(vid, date.ln = datel, gid_ln, .lid_min),
              by = c("vid", "date.ln"))
  
}

n_before_nearest_match <- nrow(LGS)
LGS <-
  LGS |> 
  match_nearest_date(LN) |> 
  rename(date_ln = date.ln)
n_after_nearest_match <- nrow(LGS)
print(c(n_before_nearest_match, n_after_nearest_match))
LGS <- 
  LGS |> 
  rename(date_ln_agf = date_ln,
         gid_ln_agf = gid_ln,
         .lid_min_agf = .lid_min)
## Landings data - kvoti-lods --------------------------------------------------
LN_raw <- 
  omar::tbl_mar(con, "kvoti.lods_oslaegt") |> 
  filter(magn_oslaegt > 0,
         year(l_dags) %in% YEARS) |> 
  select(.lid = komunr,
         gid_ln = veidarf,
         datel = l_dags,
         vid = skip_nr) |> 
  collect(n = Inf) |> 
  filter(vid %in% c(2, 5:3699, 5000:9998)) |> 
  mutate(datel = as_date(datel),
         vid = as.integer(vid),
         gid_ln = as.integer(gid_ln)) |> 
  arrange(vid, datel, .lid, gid_ln) |>
  group_by(vid, datel) |> 
  mutate(.lid_min = min(.lid)) |> 
  ungroup()
### Date and gear only ---------------------------------------------------------
LN <-  
  LN_raw |> 
  select(vid, datel, gid_ln, .lid_min) |> 
  # here only keep one gid record and the lowest .lid wihtin a landings date
  distinct(vid, datel, .lid_min, .keep_all = TRUE)
n_before_nearest_match <- nrow(LGS)
LGS <-
  LGS |> 
  match_nearest_date(LN) |> 
  rename(date_ln = date.ln)
n_after_nearest_match <- nrow(LGS)
print(c(n_before_nearest_match, n_after_nearest_match))
LGS <- 
  LGS |> 
  rename(date_ln_lods = date_ln,
         gid_ln_lods = gid_ln,
         .lid_min_lods = .lid_min)

# 6. Save ----------------------------------------------------------------------
LGS   |> write_rds("data/logbooks/station_landings-merge.rds")
# CATCH |> write_rds("data/logbooks/catch.rds")

# 7. Info ----------------------------------------------------------------------
devtools::session_info()
