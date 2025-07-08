# nohup run --------------------------------------------------------------------
# run this in terminal as:
#  nohup R < scripts/05_DATASET_ais_astd-classB-to-parquet.R --vanilla > scripts/log/05_DATASET_ais_astd-classB-to-parquet_2025-06-23.log &

library(tidyverse)
library(arrow)
dir2 <- dir("/u3/geo/pame/classB", full.names = TRUE)
fil <- dir(dir2, full.names = TRUE)
# years <- str_sub(fil, 30, 33) |> unique()
years <- 2014:2024
months <- 1:12

files_to_skip <- 
  c("/u3/geo/pame/classB/date_utc=2019-02-12/part-00078-tid-5731395226879459904-0caff91b-bf61-402e-ae59-7270f66606c7-21910-10.c000.csv.gz",
    "/u3/geo/pame/classB/date_utc=2021-06-12/part-00162-tid-5731395226879459904-0caff91b-bf61-402e-ae59-7270f66606c7-21941-14.c000.csv.gz")

i <- fil %in% files_to_skip
fil <- fil[!i]
for(y in 1:length(years)) {
  print(years[y])
  fil_y <- fil[str_detect(fil, paste0("=", years[y]))]
  for(m in 1:length(months)) {
    str <- str_detect(fil_y, paste0("=", years[y], "-", str_pad(months[m], width = 2, pad = "0")))
    fil_m <- fil_y[str]
    res <- list()
    for(d in 1:length(fil_m)) {
      # print(fil_m[d])
      res[[d]] <- read_csv(fil_m[d], show_col_types = FALSE)
    }
    bind_rows(res) |> 
      mutate(year = as.integer(year(date_time_utc)),
             month = as.integer(month(date_time_utc))) |> 
      arrange(mmsi, date_time_utc) |> 
      arrow::write_dataset("data/ais/astd_classB", format = "parquet",
                           partitioning = c("year", "month"))
  }
}

devtools::session_info()

