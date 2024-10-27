# run this as:
#  nohup R < data-external/vessels/faero-scrape_vessel-info.R --vanilla > data-external/vessels/faero-scrape_vessel-info_2024-02-20.log &
library(tidyverse)
library(rvest)
for(v in 20001:40000) {
  print(v)
  url <- paste0("https://www.teyggjan.fo/Pages/Ship/Details.aspx?shipId=", v)
  page <-
    tryCatch(
      url %>%
        read_html(),
      error = function(e){NA}    # a function that returns NA regardless of what it's passed
    )
  if(!is.na(page)) {
    #print(v)
    tables <-
      page |>
      html_nodes("table") |>
      html_table(convert = FALSE)

    if(nrow(tables[[2]]) > 1) {

      tables[c(2, 4)] |>
        bind_rows() |>
        rename(variable = 1,
               value = 2) |>
        mutate(vid_id = v) |>
        mutate(variable = as.character(variable),
               variable = str_replace(variable, ",", "."),
               value = as.character(value),
               value = str_replace(value, ",", ".")) |>
        write_csv(paste0("data-external/vessels/færeyjar/vid-", str_pad(v, side = "left", pad = "0", width = 4), ".csv"))
      Sys.sleep(2)
    }
  }
}

