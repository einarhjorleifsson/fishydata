format_vt_eflalo <- function(eflalo) {
  eflalo |> 
    dplyr::mutate(FT_DDAT = stringr::str_replace(FT_DDAT, "1800", "1803"),
                  FT_DDAT = stringr::str_replace(FT_DDAT, "1801", "1804"),
                  FT_LDAT = stringr::str_replace(FT_LDAT, "1800", "1803"),
                  FT_LDAT = stringr::str_replace(FT_LDAT, "1801", "1804"),
                  LE_CDAT = stringr::str_replace(LE_CDAT, "1800", "1803"),
                  LE_CDAT = stringr::str_replace(LE_CDAT, "1801", "1804")) |> 
    dplyr::rename(LE_MET = LE_MET_level6) |> 
    tidyr::unite(col = "FT_DDATIM", FT_DDAT, FT_DTIME) |> 
    tidyr::unite(col = "FT_LDATIM", FT_LDAT, FT_LTIME) |> 
    dplyr::mutate(FT_DDATIM = lubridate::dmy_hms(FT_DDATIM),
                  FT_LDATIM = lubridate::dmy_hms(FT_LDATIM),
                  LE_CDAT = lubridate::dmy(LE_CDAT)) |> 
    tibble::as_tibble()
}
