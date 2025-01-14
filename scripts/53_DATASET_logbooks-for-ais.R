# Objective --------------------------------------------------------------------
# Should really cap effort upstream
#
# Approach:
#
#
#
# Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < scripts/02-3_logbooks-processing.R --vanilla > lgs/02-3_logbooks-processing_2024-03-08.log &
library(tictoc)
tic()
lubridate::now()

# Input:  data/logbooks/stations/part-0.parquet
#         data/logbooks/catch/part-0.parquet
#         data-aux/gear_codes.rds
# Output: data/logbooks/....
# Downstream usage: scripts/02-4_logbooks-for_ais.R

# Input ------------------------------------------------------------------------
library(arrow)
library(tidyverse)
lb <- arrow::read_parquet("data/logbooks/stations.parquet")
ca <- arrow::read_parquet("data/logbooks/catch.parquet")

# gears <- read_rds("data-aux/gear_codes.rds")

# effort units of corrected gears ----------------------------------------------
# pending, but not critical .....

# Effort units -----------------------------------------------------------------
lb |> 
  filter(date >= ymd("2008-01-01")) |> 
  mutate(effort_unit = ifelse(!is.na(effort_unit), effort_unit, "z_missing")) |> 
  #filter(!is.na(effort_unit)) |> 
  count(gid, effort_unit) |> 
  arrange(gid, -n) |> 
  spread(effort_unit, n) |> 
  knitr::kable(caption = "Effort units")

# Median effort if effort missing ----------------------------------------------
effort_q99 <- 
  lb |> 
  group_by(gid) |> 
  summarise(q99 = quantile(effort, 0.99, na.rm = TRUE))
lb |> 
  ggplot(aes(effort)) +
  geom_histogram() +
  geom_vline(data = effort_q99,
             aes(xintercept = q99),
             colour = "red") +
  facet_wrap(~ gid, scales = "free")
median.effort <- 
  lb %>% 
  group_by(gid) %>% 
  summarise(median = median(effort, na.rm = TRUE)) %>% 
  drop_na()
lb <- 
  lb %>% 
  left_join(median.effort, by = "gid") %>% 
  mutate(effort = ifelse(!is.na(effort), effort, median)) %>% 
  select(-median) %>% 
  # cap effort hours
  # NOTE: This should actually be done on t1 and t2 when available
  mutate(effort = case_when(effort > 12 & gid ==  6 ~ 12,
                            effort > 24 & gid ==  7 ~ 24,
                            effort > 15 & gid == 14 ~ 15,
                            TRUE ~ effort))
lb |> 
  filter(is.na(effort)) |> 
  count(gid) |> 
  knitr::kable(caption = "Missing effort (should be null")

# Cap on end time of setting ---------------------------------------------------
# Cap on the t2 so not overlapping with next setting
#    NOTE: Effort not adjusted accordingly
## Reporting of t1 and t2 ------------------------------------------------------
lb |> 
  select(gid, t1, t2) |> 
  mutate(w = case_when(!is.na(t1) & !is.na(t2) ~ "both",
                       !is.na(t1) &  is.na(t2) ~ "t1",
                       is.na(t1) & !is.na(t2) ~ "t2",
                       is.na(t1) &  is.na(t2) ~ "zNone")) |> 
  count(gid, w) |> 
  spread(w, n, fill = 0) |> 
  mutate(p_both = round(both / (both + t1 + zNone), 3)) |> 
  knitr::kable(caption = "Reporting of t1 and t2 and proportion both")
## t1-t2 overlaps --------------------------------------------------------------
lb |> 
  filter(!is.na(t1) & !is.na(t2)) |> 
  select(vid, gid, t1, t2) |> 
  arrange(vid, t1, t2) %>%
  group_by(vid) %>%
  mutate(overlap = if_else(t2 > lead(t1), "yes", "no", "no")) |> 
  ungroup() |> 
  count(gid, overlap) |> 
  spread(overlap, n, fill = 0) |> 
  mutate(p = round(no / (no + yes), 3)) |> 
  knitr::kable(caption = "Where t1 and t2 reported: Number and proportion of overlap")


lb <-
  lb |> 
  arrange(vid, t1) %>%
  group_by(vid) %>%
  mutate(overlap = if_else(t2 > lead(t1), TRUE, FALSE, NA),
         t22 = if_else(overlap,
                       lead(t1) - minutes(1), # need to subtract 1 minute but get the format right
                       t2,
                       as.POSIXct(NA)),
         t22 = if_else(overlap,
                       t22,
                       as.POSIXct(NA)),
         t2 = if_else(overlap & !is.na(t22), t22, t2, as.POSIXct(NA))) %>%
  ungroup() %>% 
  select(-t22) 
paste("Number of records:", nrow(lb))


# Gear width proxy -------------------------------------------------------------
# Should this be upstream, in 02-1?


# Cap gear width proxy ---------------------------------------------------------

## Proxy for missing gear width ------------------------------------------------
# Ideally this should be modelled based on z, lon and lat


# Determine what records to filter downstream ----------------------------------

# Save -------------------------------------------------------------------------
# lb |> write_rds("data/logbooks/station-processing.rds")

# X. Info ----------------------------------------------------------------------

toc()

print(devtools::session_info())





# Objective --------------------------------------------------------------------
# Correct and "standardize" logbook records
#
# Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < scripts/02-4_logbooks-for-ais.R --vanilla > lgs/02-4_logbooks-for-ais_2023-03-08.log &
lubridate::now()

# Input:  data/logbooks/station-processing.rds
#         data/logbooks/catch.rds
# Output: data/logbooks/station-for-ais.rds
#         data/logbooks/catch-for-ais.rds
#         data/logbooks/station-for-ais.parquet
#         data/logbooks/catch-for-ais.parquet
# Downstream usage: R/...
## Brief summary ---------------------------------------------------------------
#
library(tictoc)
tic()


# Input ------------------------------------------------------------------------
library(arrow)
library(tidyverse)
lb <- 
  lb |> 
  filter(between(year(date), 2009, 2023))
ca <- read_parquet("data/logbooks/catch.parquet")

# Filter out records -----------------------------------------------------------

# Collapse records with missing t1 or t2 to daily records ----------------------
# first an overview by gear
lb |> 
  mutate(flag = ifelse(!is.na(t1) & !is.na(t2), "has_t1_t2", "rest")) |> 
  count(gid, flag) |> 
  spread(flag, n) |> 
  mutate(total = has_t1_t2 + rest,
         p = has_t1_t2 / total) |> 
  knitr::kable()

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

lb |> write_parquet("data/logbooks/station-for-ais.parquet")
ca |> write_parquet("data/logbooks/catch-for-ais.parquet")

# Info -------------------------------------------------------------------------
toc()

print(devtools::session_info())

