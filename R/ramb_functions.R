# Check that it doesn't match any non-letter
letters_only <- function(x) !grepl("[^A-Za-z]", x)
# Check that it doesn't match any non-number
numbers_only <- function(x) !grepl("\\D", x)

str_extract_between_parenthesis <- function(x) {
  str_match(x, "(?<=\\().+?(?=\\))")
}

str_extract_dmy_period <-
  function(x) {
    str_extract(x, "\\d+\\D+\\d+\\D+\\d+")
  }

#' Categories MMSI
#'
#'
#' See: https://en.wikipedia.org/wiki/Maritime_Mobile_Service_Identity
#'
#' @param mmsi MMSI
#'
#' @return A vector with mmsi categories
#' @export
#'
rb_mmsi_category <- function(mmsi) {

  category <-
    tibble::tibble(mmsi = mmsi) |>
    dplyr::mutate(mmsi = as.character(mmsi),
                  mmsi_candidate =
                    dplyr::case_when(
                      nchar(mmsi) == 9 & numbers_only(mmsi) ~ TRUE,
                      .default = FALSE)) |>
    dplyr::mutate(.mmsi_category =
                    dplyr::case_when(mmsi_candidate & stringr::str_sub(mmsi, 1, 1) == "0" ~ "coast station",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 1) == "1" ~ "SAR aircraft",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 1) %in% as.character(2:7) ~ "vessel",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 1) == "8" ~ "Handheld VHF transceiver",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 3) == "970" ~ "Search and Rescue Transponders",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 3) == "972" ~ "Man overboard",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 3) == "974" ~ "406 MHz EPIRBs fitted with an AIS transmitter",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 2) == "98" ~ "Child vessel",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 2) == "99" ~ "Navigational aid",
                                     mmsi_candidate & stringr::str_sub(mmsi, 1, 1) == "9" ~ "Starts with 9",
                                     .default = NA)) |>
    dplyr::pull(.mmsi_category)

  return(category)

}

rb_mmsi_flag <- function(mmsi, lookup) {

  # Check that it doesn't match any non-number
  numbers_only <- function(x) !grepl("\\D", x)

  tibble::tibble(mmsi = mmsi) |>
    dplyr::mutate(.mmsi_candidate =
                    dplyr::case_when(nchar(mmsi) == 9 & numbers_only(mmsi) ~ TRUE,
                                     .default = FALSE)) |>
    dplyr::mutate(.MID =
                    dplyr::case_when(
                      .mmsi_candidate == TRUE & stringr::str_sub(mmsi, 1, 1) %in% as.character(2:7) ~ stringr::str_sub(mmsi, 1, 3),
                      .mmsi_candidate & stringr::str_sub(mmsi, 1, 2) == "98" ~ stringr::str_sub(mmsi, 3, 5),
                      .default = NA)) |>
    dplyr::mutate(.MID = as.integer(.MID)) |>
    dplyr::left_join(lookup |> dplyr::mutate(MID = as.integer(MID)),
                     by = dplyr::join_by(.MID == MID)) |>
    dplyr::pull(flag)
}


rb_mapdeck <-
  function (d, radius = 400, col = "speed", tooltip = "speed", add_harbour = TRUE,
            no_lines = TRUE, highlight_colour = "black", stroke_colour = "cyan") {
    if (add_harbour)
      hb <- gisland::gl_read_is_harbours(trim = FALSE)
    if (!"sf" %in% class(d)) {
      d <- sf::st_as_sf(d, coords = c("lon", "lat"), crs = 4326)
    }
    if (!no_lines) {
      track <- sf::st_cast(summarise(d, do_union = FALSE),
                           "LINESTRING")
    }
    m <- mapdeck::mapdeck()
    if (add_harbour) {
      m <- mapdeck::add_polygon(m, data = hb, fill_colour = col2hex("pink"),
                                layer_id = "harbour")
    }
    if (!no_lines) {
      m <- mapdeck::add_path(m, data = track, layer_id = "track",
                             stroke_width = 300, width_min_pixels = 1, width_max_pixels = 5,
                             auto_highlight = TRUE, highlight_colour = paste0(col2hex(highlight_colour),
                                                                              "80"), update_view = FALSE, stroke_colour = paste0(col2hex(stroke_colour),
                                                                                                                                 "80"))
    }
    mapdeck::add_scatterplot(m, data = d, fill_colour = col,
                             radius = radius, tooltip = tooltip, layer_id = "points",
                             palette = "inferno", legend = TRUE)
  }
