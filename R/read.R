fd_stk_arrow <- function(pth = "/net/hafkaldi.hafro.is/export/home/haf/einarhj/stasi/fishydata/data/") {
  sc_arrow <-
    tibble::tribble(~gid,  ~s1,  ~s2,
                    -199, 1.000, 3.000,
                    1,    0.375, 2.750,
                    2,    0.125, 2.500,
                    3,    0.025, 2.000,
                    5,    0.250, 3.250,
                    6,    2.625, 5.500,
                    7,    2.625, 6.000,
                    9,    2.375, 4.000,
                    12,    0.050, 2.250,
                    14,    1.750, 3.250,
                    15,    0.500, 5.500,
                    38,    0.250, 4.750,
                    40,    0.250, 6.000) |> 
    dplyr::mutate(gid = as.integer(gid)) |> 
    arrow::as_arrow_table()
  con <- 
    arrow::open_dataset(paste0(pth, "ais")) |> 
    dplyr::left_join(sc_arrow)
  return(con)
}


