# 0.  Objective --------------------------------------------------------------------
# Correct and "standardize" logbook records
#
# 1. Preamble ---------------------------------------------------------------------
# run this as:
#  nohup R < scripts/02-2_logbooks-processing.R --vanilla > lgs/02-2_logbooks-processing_2023-09-09.log &
lubridate::now()

# Input:  data/logbooks/station.parquet
#         data/logbooks/catch.parguet
# Output: data/stk/....
# Downstream usage: R/...
## Brief summary ---------------------------------------------------------------
#

# 3. Input ------------------------------------------------------------------------
library(omar)
library(tidyverse)
con <- connect_mar()

lb <- read_rds("data/logbooks/station.rds")
ca <- read_rds("data/logbooks/catch.rds")
gears <- read_rds("data-aux/gear_codes.rds")


# Gear correction -------------------------------------------------------------
# This is some legacy code that may need a review
# Basic procedure
# Question if this should not be moved to 02-1_logbooks-merge.R
lb <- 
  lb |> 
  # to align with the legacy code
  rename(gid.ln = gid_ln_lods,
         sid.target = sid_target) |> 
  mutate(dt = difftime(date, date_ln_lods, units = "days"),
         dt = as.integer(dt)) |> 
  mutate(gid.lb = gid) |> 
  left_join(gears %>% select(gid.lb = gid, gc.lb = gclass), by = "gid.lb") %>% 
  left_join(gears %>% select(gid.ln = gid, gc.ln = gclass), by = "gid.ln") %>% 
  select(.sid:gid.ln, gc.lb, gc.ln, everything()) %>% 
  mutate(gid = NA_integer_,
         gid.source = NA_character_) %>% 
  mutate(i = gid.lb == gid.ln,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.lb=gid.ln", gid.source),
         step = ifelse(i, 1L, NA_integer_)) %>% 
  mutate(i = is.na(gid) & gc.lb == gc.ln,
         gid = ifelse(i, gc.lb, gid),
         gid.source = ifelse(i, "gc.lb=gc.ln   -> gid.lb", gid.source),
         step = ifelse(i, 2L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 15L & gid.lb %in% c(5L, 6L),
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=15, gid.lb=5,6   -> gid.ln", gid.source),
         step = ifelse(i, 3L, step)) %>% 
  mutate(i = is.na(gid) & 
           gid.ln == 21 & 
           gid.lb == 6 &
           !sid.target %in% c(30, 36, 41),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6, sid.target != 30,36,41   -> gid.lb",
                             gid.source),
         step = ifelse(i, 4L, step)) %>% 
  mutate(i = is.na(gid) &
           gid.ln == 21 &
           gid.lb == 6 & 
           sid.target %in% c(22, 41),
         gid = ifelse(i, 14, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6, sid.target = 22,41   -> 14", gid.source),
         step = ifelse(i, 5L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 6 & gid.lb == 7 & sid.target %in% c(11, 19, 30:36),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=6, gid.lb=7, sid.target = 11,19,30:36   -> gid.lb", gid.source),
         step = ifelse(i, 6L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 5,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=5   -> gid.ln", gid.source),
         step = ifelse(i, 7L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 40,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=40   -> gid.lb", gid.source),
         step = ifelse(i, 8L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 14 & gid.lb == 6 & sid.target == 41,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=14, gid.lb=6, sid.target = 41   -> gid.ln", gid.source),
         step = ifelse(i, 9L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 7 & gid.lb == 6,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=7, gid.lb=6   -> gid.ln", gid.source),
         step = ifelse(i, 10L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 9 & gid.lb == 6,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=9, gid.lb=6   -> gid.ln", gid.source),
         step = ifelse(i, 11L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 18 & gid.lb == 38,
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=18, gid.lb=38   -> gid.ln", gid.source),
         step = ifelse(i, 12L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 6 & gid.lb == 14,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=6, gid.lb=14   -> gid.lb", gid.source),
         step = ifelse(i, 13L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 15 & gid.lb == 39,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=15, gid.lb=39   -> gid.lb", gid.source),
         step  = ifelse(i, 14L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 40 & gid.lb %in% c(5, 6),
         gid = ifelse(i, gid.ln, gid),
         gid.source = ifelse(i, "gid.ln=40, gid.lb=5,6   -> gid.ln", gid.source),
         step = ifelse(i, 15L, step)) %>% 
  mutate(i = is.na(gid) & is.na(gid.ln) & gid.lb %in% c(1:3),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "is.na(gid.ln), gid.lb=1:3   -> gid.lb", gid.source),
         step = ifelse(i, 16L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 21 & gid.lb == 6,
         gid = ifelse(i, 7, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=6   -> 7", gid.source),
         step = ifelse(i, 17L, step)) %>% 
  mutate(i = is.na(gid) & gid.ln == 21 & gid.lb == 14,
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "gid.ln=21, gid.lb=14   -> gid.lb", gid.source),
         step = ifelse(i, 18L, step)) |> 
  mutate(i = is.na(gid),
         gid = ifelse(i, gid.lb, gid),
         gid.source = ifelse(i, "rest", gid.source),
         step = ifelse(i, 19L, step))

# Lump some gears --------------------------------------------------------------
lb <-
  lb %>% 
  mutate(gid = case_when(gid %in% c(10, 12) ~ 4,   # purse seines
                         gid %in% c(18, 39) ~ 18,      # traps
                         TRUE ~ gid)) %>% 
  # make the rest a negative number
  mutate(gid = ifelse(is.na(gid), -666, gid)) %>% 
  # "skip" these also in downstream processing
  mutate(gid = ifelse(gid %in% c(4, 12, 42), -666, gid)) %>% 
  # lump dredges into one single gear
  mutate(gid = ifelse(gid %in% c(15, 37, 38, 40), 15, gid))

lb |> 
  filter(is.na(gid)) |> 
  count(gid.lb, gid.source, gid.ln) |> 
  arrange(-n) |> 
  knitr::kable(caption = "Expect a null table")
lb |> 
  count(gid.source) |> 
  arrange(-n) |> 
  mutate(p = (n / sum(n) * 100) |> round(2),
         cump = cumsum(p)) |> 
  knitr::kable(caption = "Derivation of correct gid")

# Gear class of corrected gid --------------------------------------------------
lb <- 
  lb %>% 
  left_join(gears %>% select(gid, gclass),
            by = "gid")
lb |> 
  count(gid, gclass) |> 
  left_join(gears |> select(gid, gclass, lysing, dcf4, dcf5)) |> 
  knitr::kable(captions = "Corrected gear and gear class")

# effort units of corrected gears ----------------------------------------------
# pending, but not critical .....

# Missing effort units ---------------------------------------------------------
lb |> 
  filter(!is.na(effort_unit)) |> 
  count(gid, effort_unit) |> 
  arrange(gid, -n) |> 
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
lb |> 
  mutate(has.t1 = !is.na(t1),
         has.t2 = !is.na(t2)) |> 
  count(gid, has.t1) |> 
  spread(has.t1, n)
lb2 <-
  lb |> 
  arrange(vid, t1) %>%
  group_by(vid) %>%
  mutate(overlap = if_else(t2 > lead(t1), TRUE, FALSE, NA),
         t22 = if_else(overlap,
                       lead(t1) - minutes(1), # need to subtract 1 minute but get the format right
                       t2,
                       as.POSIXct(NA)),
         t22 = if_else(overlap,
                       ymd_hms(format(as.POSIXct(t22, origin="1970-01-01", tz="UTC"))),
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
lb |> write_rds("data/logbooks/station-processing.rds")
# X. Info ----------------------------------------------------------------------
devtools::session_info()
