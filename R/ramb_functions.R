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


#' Track time duration
#' 
#' Calculate time duration based on sequential difference of date-time input. 
#' The unit of time duration is seconds.
#' 
#' Function is a modification of traipse::track_time allowing for calculation
#' of time from previous points rather than by convention the time to next
#' point. It also allows for puting some weighing a.la. the datacall::intvTacsat
#' function.
#' 
#' @param date date-time in POSIXct
#' @param weight A numerical vector of length 2 weight to apply to calculation
#' of mean interval rate towards and away from ping
#' @param fill Boolean (default TRUE)
#'
#' @return A vector of duration in seconds
#' @export
#'
rb_track_time <- function (date, weight = c(0, 1), fill = TRUE) {
  
  # tests
  cls <- class(date)
  if (!inherits(date, "POSIXct")) {
    date <- try(as.POSIXct(date), silent = TRUE)
    if (inherits(date, "try-error")) {
      stop(sprintf("Cannot convert 'date' of class '%s' to POSIXct", 
                   paste(cls, collapse = ",")))
    }
  }
  # Check if 'weight' is a length 2 numeric vector
  if (length(weight) != 2) 
    stop("weight must be specified as a length 2 numeric vector")
  
  # Normalize 'weight' to sum to 1
  weight <- weight/sum(weight, na.rm = TRUE)
  
  if (any(weight == 0)) {
    if (weight[1] == 0) {
      int <- c(NA_real_, diff(unclass(date)))
      if(fill) int[1] <- int[2]
    }
    if (weight[2] == 0) {
      int <- c(diff(unclass(date)), NA_real_)
      if(fill) {
        l <- length(int)
        int[l] <- int[l-1]
      }
    }
  } else {
    int <- rowSums(cbind(c(NA_real_, diff(unclass(date))) * weight[1], c(diff(unclass(date)), NA_real_) * weight[2]))
    if(fill) {
      l <- length(int)
      int[1] <- int[2]
      int[l] <- int[l-1]
    }
  }
  
  return(int)
}


#' Points in polygon
#'
#' @param x object containin geometry POINT of class sf, sfc or sfg
#' @param y object cotaining geometry POLYGON of class sf, sfc or sfg
#'
#' @return Normally POINT object of class sf that are within object y
#' 
#' @note No serious tesing in done
#' @export
#'
rb_st_keep <- function(x, y) {
  i <- sf::st_intersects(x, y) |> lengths() > 0
  x <- x[i, ]
  return(x)
}


#' Points not in polygon
#'
#' @param x object containin geometry POINT of class sf, sfc or sfg
#' @param y object cotaining geometry POLYGON of class sf, sfc or sfg
#'
#' @return Normally POINT object of class sf that are within object y
#' 
#' @note No serious tesing is done
#' 
#' @export
#'
rb_st_remove <- function(x, y) {
  i <- lengths(st_intersects(x, y)) == 0
  x <- x[i, ]
  return(x)
}


rb_leaflet_raster <- function(g) {
  r <- 
    g |> 
    select(x = lon, y = lat, effort = dt) |> 
    mutate(effort = cap_value(effort)) |> 
    raster::rasterFromXYZ()
  raster::crs(r) <- "epsg:4326"
  inf <- viridis::inferno(12, alpha = 1, begin = 0, end = 1, direction = -1)
  pal <- leaflet::colorNumeric(inf, raster::values(r), na.color = "transparent")
  
  l <-
    leaflet::leaflet(options = leaflet::leafletOptions(minZoom = 4, maxZoom = 11)) %>%
    leaflet::addTiles(urlTemplate = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
             group = "Image",
             attribution = 'Data source: <a href="https://www.hafogvatn.is">Marine Rearch Institute</a>') %>%
    leaflet::setView(-18, 65.2, zoom = 6) %>%
    leaflet::addRasterImage(r, colors = pal, opacity = 1, group = "Humarvarpa",
                   maxBytes = Inf,
                   project = TRUE) 
  return(l)
}


rb_cap_value <- function(x, q = 0.975) {
  Q <- quantile(x, q)
  ifelse(x > Q, Q, x)
}


#' Not In Operator
#'
#' Negation of the %in% operator for convenience
#'
#' @param x vector or NULL: the values to be matched
#' @param table vector or NULL: the values to be matched against
#'
#' @return A logical vector indicating if there is no match for each element of x
#' @export
#'
`%!in%` <- function(x, table) {
  !(x %in% table)
}


#' Split Catch and Value Among VMS Pings (Version 2)
#' 
#' Distributes catch (in kg) and value (in euros) from logbook data (eflalo) 
#' among VMS ping records (tacsatp) based on time intervals. The function uses 
#' a hierarchical matching approach to allocate catches to pings, trying to 
#' match by trip, fishing method and date first, then progressively relaxing 
#' the matching criteria.
#' 
#' @details
#' The function performs a three-level hierarchical allocation:
#' 
#' 1. **Level 1**: Match by FT_REF (trip), LE_MET (fishing method), and SI_DATE
#'    - Finds logbook records matching trip, method and date
#'    - Distributes catch/value proportionally by time interval (INTV)
#' 
#' 2. **Level 2**: Match by FT_REF and LE_MET only
#'    - For remaining unallocated logbook records
#'    - Distributes among pings matching trip and method
#' 
#' 3. **Level 3**: Match by FT_REF only
#'    - For any remaining unallocated logbook records
#'    - Distributes among all pings in the same trip
#' 
#' The function filters to only fishing pings (SI_STATE == 1) and requires 
#' all pings to have valid time intervals (INTV). The distribution weight for 
#' each ping is calculated as its interval divided by the total interval within 
#' each matching group.
#' 
#' All columns in the eflalo data with "KG" or "EURO" in their names are 
#' treated as catch/value columns and are distributed accordingly.
#'
#' @param tacsatp A data.frame or data.table containing VMS ping data with at least:
#'   - FT_REF: Trip reference/identifier
#'   - LE_MET: Fishing method/metier
#'   - SI_STATE: Ping state (1 = fishing, 0 = non-fishing)
#'   - INTV: Time interval associated with each ping (in hours or minutes)
#'   - SI_DATE: Date of the ping (used for matching)
#' @param eflalo A data.frame or data.table containing logbook event data with at least:
#'   - FT_REF: Trip reference/identifier (matching tacsatp)
#'   - LE_MET: Fishing method/metier
#'   - LE_CDAT: Catch date
#'   - Columns with "KG" or "EURO" in the name containing catch weights and values
#'
#' @return A data.frame with the same structure as tacsatp but with catch (KG) 
#'   and value (EURO) columns added/updated. Each ping receives a proportion 
#'   of the catch/value based on the hierarchical matching and time interval 
#'   weighting scheme.
#'   
#' @section Warning:
#' The function will stop with an error if any NA values are found in the INTV 
#' column of tacsatp. All pings must have valid time intervals before calling 
#' this function.
#' 
#' @section Requirements:
#' The function requires the data.table package.
#'
#' @examples
#' \dontrun{
#' # Assuming tacsatp and eflalo are properly formatted:
#' result <- splitAmongPings2(tacsatp, eflalo)
#' 
#' # Check total catch is preserved (approximately)
#' sum(result$LE_KG_COD, na.rm = TRUE)
#' sum(eflalo$LE_KG_COD, na.rm = TRUE)
#' }
#'
#' @seealso \code{\link[vmstools]{splitAmongPings}} for the original vmstools implementation
#' 
#' @export
#'
splitAmongPings2 <- function(tacsatp, eflalo) {
  require(data.table)
  
  t <- data.table(tacsatp)[ SI_STATE == 1]
  e <- data.table(eflalo)
  
  if(any(is.na(t$INTV)))
    stop("NA values in intervals (INTV) in tacsatp, please add an interval to all pings")
  
  e[, SI_DATE := LE_CDAT] 
  #find all column names with KG or EURO in them
  kg_euro <- grep("KG|EURO", colnames(e), value = T)
  
  ### sum by FT_REF, LE_MET, SI_DATE
  
  n1 <- e[FT_REF %in% t$FT_REF,lapply(.SD,sum, na.rm = T),by=.(FT_REF, LE_MET, SI_DATE),
          .SDcols=kg_euro][, ide1 := 1:.N]
  
  setkey(t, FT_REF, LE_MET, SI_DATE)
  setkey(n1, FT_REF, LE_MET, SI_DATE)
  
  ts1 <- merge(t, n1)
  
  setkey(ts1, FT_REF, LE_MET, SI_DATE)
  ts1[,Weight:=INTV/sum(INTV, na.rm = T), by=.(FT_REF, LE_MET, SI_DATE)]
  ts1[,(kg_euro):= lapply(.SD, function(x) x * Weight), .SDcols=kg_euro]
  
  ### sum by FT_REF, LE_MET
  n2 <- n1[ide1 %!in% ts1$ide1, lapply(.SD,sum, na.rm = T),by=.(FT_REF, LE_MET),
           .SDcols=kg_euro][, ide2 := 1:.N]
  
  setkey(t, FT_REF, LE_MET)
  setkey(n2, FT_REF, LE_MET)
  
  ts2 <- merge(t, n2)
  
  setkey(ts2, FT_REF, LE_MET)
  ts2[,Weight:=INTV/sum(INTV, na.rm = T), by=.(FT_REF, LE_MET)]
  ts2[,(kg_euro):= lapply(.SD, function(x) x * Weight), .SDcols=kg_euro]
  
  
  ### sum by FT_REF
  n3 <- n2[ide2 %!in% ts2$ide2, lapply(.SD,sum, na.rm = T),by=.(FT_REF),
           .SDcols=kg_euro][, ide3 := 1:.N]
  
  setkey(t, FT_REF)
  setkey(n3, FT_REF)
  
  ts3 <- merge(t, n3)
  
  setkey(ts3, FT_REF)
  ts3[,Weight:=INTV/sum(INTV, na.rm = T), by=.(FT_REF)]
  ts3[,(kg_euro):= lapply(.SD, function(x) x * Weight), .SDcols=kg_euro]
  
  
  #Combine all aggregations
  ts <- rbindlist(list(t, ts1, ts2, ts3), fill = T)
  ts[ ,`:=`(Weight = NULL, ide1 = NULL, ide2 = NULL, ide3 = NULL)]
  diffs = setdiff(names(ts), kg_euro)
  
  out <- ts[,lapply(.SD,sum, na.rm = T),by=diffs,
            .SDcols=kg_euro]
  
  return(data.frame(out))
}

