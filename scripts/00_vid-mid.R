# Code to generate the stk mobilid-localid vessel id
#  See: https://github.com/einarhjorleifsson/omar/blob/main/data-raw/00_SETUP_mobileid-vid-match.R

# NOTE: The datadump here is not used downstream, dump only made for "external usage"
library(tidyverse)
library(omar)
con <- connect_mar()
tbl_mar(con, "ops$einarhj.mobile_vid") |> 
  filter(!is.na(vid))  |> 
  collect(n = Inf) |> 
  write_rds("data-aux/mobile_vid.rds")
  