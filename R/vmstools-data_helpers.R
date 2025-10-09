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


#' Split logbook catch and value data among VMS pings
#'
#' Distributes catch (in kilograms) and value (in euros) from logbook records 
#' (eflalo) to individual VMS ping records (tacsatp) using a hierarchical 
#' matching strategy. The function attempts to match records based on increasingly 
#' broader criteria when specific matches are not found.
#'
#' @details
#' The function performs distribution in three hierarchical steps:
#' 
#' **Step 1 - Match by Trip ID, Gear/Method, and Date:**
#' First attempts to match logbook records to pings using the combination of:
#' - FT_REF (trip reference/ID)
#' - LE_MET (metier/fishing method)  
#' - SI_DATE (date)
#' 
#' **Step 2 - Match by Trip ID and Gear/Method:**
#' For records not matched in Step 1, attempts matching using only:
#' - FT_REF (trip reference/ID)
#' - LE_MET (metier/fishing method)
#' 
#' **Step 3 - Match by Trip ID only:**
#' For records still unmatched, uses only:
#' - FT_REF (trip reference/ID)
#' 
#' **Weighting mechanism:**
#' Within each matching group, catch and value are distributed proportionally 
#' based on the time interval (INTV) associated with each ping. Pings with 
#' longer intervals receive proportionally more catch/value.
#' 
#' The formula used is: `Weight = INTV / sum(INTV)` for each group, then 
#' each KG/EURO column is multiplied by this weight.
#'
#' @param tacsatp A data.frame or data.table of VMS (Vessel Monitoring System) 
#'   ping data with fishing activity. Must contain:
#'   - FT_REF: Trip reference/ID
#'   - LE_MET: Metier/fishing method  
#'   - SI_DATE: Date of the ping
#'   - SI_STATE: Activity state (must equal 1 for fishing)
#'   - INTV: Time interval in hours associated with the ping (no NA values allowed)
#'   
#' @param eflalo A data.frame or data.table of logbook data containing catch 
#'   and value information. Must contain:
#'   - FT_REF: Trip reference/ID (must match tacsatp)
#'   - LE_MET: Metier/fishing method
#'   - LE_CDAT: Catch date
#'   - Columns with "KG" or "EURO" in their names containing catch weights and values
#'
#' @return A data.frame with the same structure as tacsatp but with catch (KG) 
#'   and value (EURO) columns added. Each ping receives its proportional share 
#'   of catch/value based on the hierarchical matching and time interval weighting.
#'   Records that could not be matched at any level will have NA values for the 
#'   catch/value columns.
#'
#' @note 
#' - Only pings with SI_STATE == 1 (fishing) are considered for distribution
#' - The function will stop with an error if any INTV values are NA in tacsatp
#' - The function uses data.table for efficient computation
#' - The '%!in%' operator must be defined (typically from vmstools package)
#'
#' @seealso 
#' This function is related to \code{vmstools::splitAmongPings} but uses a 
#' different matching strategy.
#'
#' @examples
#' \dontrun{
#' # Assuming you have tacsatp and eflalo datasets prepared
#' result <- splitAmongPings2(tacsatp, eflalo)
#' 
#' # Check total catch before and after
#' sum(eflalo$LE_KG_TOT, na.rm = TRUE)
#' sum(result$LE_KG_TOT, na.rm = TRUE)
#' }
#'
#' @export
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
