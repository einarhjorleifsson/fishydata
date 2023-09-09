# Objective --------------------------------------------------------------------
# Correct and "standardize" logbook records
#
# Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < scripts/02-3_logbooks-for-ais.R --vanilla > lgs/02-3_logbooks-for-ais_2023-09-09.log &
lubridate::now()

# Input:  xxx
#         xxx
# Output: xxx
# Downstream usage: R/...
## Brief summary ---------------------------------------------------------------
#

# Input ------------------------------------------------------------------------
library(tidyverse)
lb <- read_rds("data/logbooks/station-processing.rds")
ca <- read_rds("data/logbooks/catch.rds")

# Filter out records -----------------------------------------------------------

# Collapse records with missing t1 or t2 to daily records ----------------------
lb_t1_t2 <- 
  lb |> 
  filter(!is.na(t1) & !is.na(t2))
lb_rest <- 
  lb |> 
  filter(!.sid %in% lb_t1_t2$.sid)
# now for the catches
ca_t1_t2 <-
  ca |> 
  filter(.sid %in% lb_t1_t2$.sid)
ca_rest <- 
  lb_rest |> 
  select(.sid, vid, date) |> 
  left_join(ca |> 
              filter(!.sid %in% lb_t1_t2$.sid),
            relationship = "many-to-many") |> 
  group_by(vid, date, sid) %>%
  summarise(.sid = min(.sid),
            catch = sum(catch, na.rm = TRUE),
            .groups = "drop") |> 
  select(.sid, sid, catch)
ca <- 
  bind_rows(ca_t1_t2, ca_rest)


lb_rest <- 
  lb_rest |> 
  group_by(vid, date, gid) %>%
  # get here all essential variables that are needed downstream
  summarise(.sid = min(.sid),
            n.sids = n(),
            effort = sum(effort, na.rm = TRUE),
            catch_total = sum(catch_total, na.rm = TRUE),
            .groups = "drop") %>%
  mutate(t1 = ymd_hms(paste0(year(date), "-", month(date), "-", day(date),
                             " 00:00:00")),
         t2 = ymd_hms(paste0(year(date), "-", month(date), "-", day(date),
                             " 23:59:00")))
lb <-
  bind_rows(lb_rest, lb_t1_t2) |> 
  arrange(vid, t1)
lb |> glimpse()

# Save -------------------------------------------------------------------------
lb |> write_rds("data/logbooks/station-for-ais.rds")
ca |> write_rds("data/logbooks/catch-for-ais.rds")

# Info -------------------------------------------------------------------------
devtools::session_info()