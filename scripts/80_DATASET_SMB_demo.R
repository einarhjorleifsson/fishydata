# INCOMPLETE
# Objective: Generate a ais dataset with as many covariates as possible
#  This is limited to survey vessels (SMB, SMH, SMN, ...)
#  datasources: stk/astd, hafriti, hiti, optigear
library(staroddi)
library(arrow)
library(tidyverse)

# get seamon -------------------------------------------------------------------
fil <- 
  c("/u2/hiti/2024/Marsrall/A4_2024_marsrall_24D0211.DAT",
    "/u2/hiti/2024/Marsrall/B3_2024_18D0210.DAT",
    "/u2/hiti/2024/Marsrall/TB1_2024_6D0220.DAT",
    "/u2/hiti/2024/Marsrall/TG1_2024_16D0213.DAT")
d <- map(fil, read_dst)
names(d) <- c("A4-2024", "B3-2024", "TB1-2024", "TG1-2024")
seamon <- d |> bind_rows(.id = "cruise")
seamon |> glimpse()
seamon |> 
  filter(month(time) == 3,
         day(time) == 10) |> 
  filter(str_sub(cruise, 1, 1) %in% c("A", "B")) |> 
  mutate(depth = -depth) |> 
  gather(var, val, temp:depth) |> 
  ggplot(aes(time, val)) +
  geom_point(size = 0.1) +
  facet_grid(var ~ cruise, scales = "free_y")
