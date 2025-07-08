library(nanoparquet)
library(tidyverse)
library(mar)
con <- connect_mar()

res <- 
  les_skipaskra(con) |> 
  collect() |> 
  mutate(category=cut(length, breaks=c(-Inf, 6, 8, 10, 12, 15, 18, 24, 40 ,120), 
                      labels=c("a","b","c","d","e","f","g","h","j"))) |> 
  select(vid = vessel_id, category)

effort <- 
  read_parquet("/net/hafkaldi.hafro.is/export/home/haf/einarhj/stasi/fishydata/data/logbooks/stations.parquet") |> 
  select(-gid) |> 
  rename(gid = gid_lods) |> 
  # There are some errors here, not many records
  filter(effort > 0) |> 
  mutate(gear = case_when(gid %in% c(6, 59:63) ~ "Fish trawl",
                          gid %in% c(1, 71) ~ "Longline",
                          gid %in% c(2, 91, 92) ~ "Gillnet",
                          gid == 3 ~ "Jiggers",
                          gid == 5 ~ "Bottomseine",
                          gid == 7 ~ "Pelagic trawl",
                          gid == 9 ~ "Nephrops trawl",
                          gid == 10 ~ "Purse seine",
                          gid == 14 ~ "Shrimp trawl",
                          gid %in% c(15,37,38,40) ~ "Dredge",
                          gid %in% c(18,39) ~ "Trap",
                          .default = "Something else"),
         year = year(date)) |> 
  left_join(res)
effort_sum <-
  effort |> 
  group_by(year, gear) |> 
  reframe(effort = sum(effort, na.rm = TRUE),
          das = n_distinct(vid, date),
          trips = n_distinct(vid, datel))
effort_sum |> 
  ggplot(aes(year, das)) +
  geom_point() +
  facet_wrap(~ gear, scales = "free_y")
effort_sum |> 
  ggplot(aes(year, trips)) +
  geom_point() +
  facet_wrap(~ gear, scales = "free_y")


