# Match the observers database with the landings database

# run this as:
#  nohup R < scripts/05_observer-trips.R --vanilla > lgs/05_observer-trips_2023-12-29.log &


library(data.table)
library(tidyverse)
#library(omar)
lh_date <- function(x) {
  if_else(str_detect(x, "\\."),
          dmy(x),
          janitor::excel_numeric_to_date(as.numeric(x)))
}

obs1 <-
  readxl::read_excel("data-external/SJOFERDIR_01012016_31122019.xlsx", col_types = "text") |>
  rename(vid = 1,
         gear = 2,
         t1 = 3,
         t2 = 4,
         .id = 5,
         datel = 6) |> 
  mutate(vid = as.integer(vid),
         t1 = lh_date(t1),
         t1 = if_else(t1 == ymd("2011-11-25"),
                      ymd("2017-11-25"),
                      t1),
         t2 = lh_date(t2),
         datel = lh_date(datel),
         .id = as.integer(.id)) |> 
  arrange(t1) |> 
  mutate(source = "dorothea")
obs2 <-
  read_csv("data-external/SJOFERDIR_01012017_25102022.csv") |> 
  rename(vid = 1,
         t1 = 2,
         t2 = 3) |> 
  mutate(vid = as.integer(vid),
         t1 = dmy(t1),
         t2 = dmy(t2)) |> 
  arrange(t1) |> 
  mutate(source = "dadi") |> 
  # Data frequency low prior to this date, also overlaps with that provided
  #  by Dorothea
  filter(t1 >= ymd("2020-01-01"))
obs3 <- 
  readxl::read_excel("data-external/2019_sjór_Fiskistofu_2021-09-19_from-vidar-olason.xlsx") |> 
  janitor::clean_names() |> 
  select(vid = skip,
         t1 = sjoferd_byrjar,
         t2 = sjoferd_endar,
         gear = veidarfaeri) |> 
  separate(vid, into = c("name", "vid"), sep = "\\(") |> 
  mutate(vid = str_remove(vid, "\\)"),
         vid = as.integer(vid),
         source = "Viðar") |> 
  select(-name)

obs <- 
  bind_rows(obs1, obs2) |> 
  arrange(vid, t1) |> 
  filter(vid > 0) |> 
  mutate(.rid = 1:n()) |> 
  select(.rid, everything())

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
    left_join(ln %>% select(vid, date.ln = datel, gid_ln = gid, .id),
              by = c("vid", "date.ln"))
  
}

# pretend observer data are logbook data
lb <- 
  obs |> 
  # use t2 as proxy for date landed (are pretending" we do not have it)
  rename(datel_org = datel, datel = t2, .id_org = .id)
ln <- 
  read_rds("data/landings/agf_stations.rds") |> 
  select(vid, 
         datel,
         gid = gid_ln,
         .id = .lid) |> 
  arrange(vid, datel)

m <- match_nearest_date(lb, ln)
obs <- 
  obs |> 
  left_join(m |> select(.rid, datel_agf = date.ln, gid_agf = gid_ln, .lid = .id))
obs |> 
  knitr::kable(caption = "Nearest date matching of observer trips vs AGF landings")

# how good is the match
obs |> 
  mutate(.lid = as.integer(.lid)) |> 
  mutate(what = case_when(.id == .lid ~ "same",
                          .id != .lid ~ "different",
                          is.na(.id) ~ ".id missing",
                          is.na(.lid) ~ ".lid missing",
                          .default = NA)) |> 
  count(source, what) |> 
  spread(source, n) |> 
  knitr::kable(caption = "landings id match (difference expected in some cases")
obs |> 
  mutate(dt = as.numeric(difftime(t2, datel_agf, units = "days"))) |> 
  count(source, dt) |> 
  spread(source, n) |> 
  knitr::kable()
# only use where match of day within +/- 1 day
obs <- 
  obs |> 
  mutate(dt = as.numeric(difftime(t2, datel_agf, units = "days")),
         use = if_else(between(dt, -1, 1), TRUE, FALSE, FALSE)) 
obs |> 
  count(source, use) |> 
  group_by(source) |> 
  mutate(p = n / sum(n)) |> 
  ungroup() |> 
  knitr::kable(caption = "Proportion of date matches within +/-1 day")
obs |> 
  filter(source == "dorothea") |> 
  filter(!use) |> 
  knitr::kable(caption = "Source Dorothea were date match outside +/- 1 day")
obs |> 
  filter(source == "dorothea",
         #.id == .lid,
         datel != datel_agf) |> 
  knitr::kable(caption = "Source Dorothea were her datel is not the same as datel_agf")

con <- omar::connect_mar()
gear <- 
  omar::tbl_mar(con, "agf.aflagrunnur_v") |> 
  select(starts_with("veidarfaeri")) |> 
  distinct() |> 
  collect() |> 
  arrange(veidarfaeri)
gear |> 
  left_join(obs |> 
  filter(source == "dorothea") |> 
  count(veidarfaeri = gid_agf, obs_gear_reported = gear)) |> 
  knitr::kable(caption = "AGF gears (first 3 columns and gear reported by observers")
  
obs |> 
  write_rds("data/landings/agf_observers.rds")


print(devtools::session_info())

