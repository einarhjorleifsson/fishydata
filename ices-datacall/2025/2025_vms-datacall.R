# TODO: 2025-08-30
## QC issues -------------------------------------------------------------------
# * Shrimp and nephrops missing as target (met 5)
#  * Why - Now the target is set as DEF in the gear table
#  * ACTION - Replace with CRU in the gear table
#  * DONE
# * Area swept missing for some mobile gear
#  * ACTION - In LB set a width when is.na(width) - use agf_gid as the conditional test
#   * Use some median/mean value for the missing values
#  * DONE
# * Check if capping has been applied
#  * Gear width in logbooks
#  * Maximum catch - logbooks and vms
#  * ...
# * VMS data
#   * In the QC report year 2023 and 2024 show no difference in effort
#   * ACTION: Seems like a coding error in the QC, no change in datacall code
#   * DONE




# Check out: https://github.com/quarto-dev/quarto-cli/discussions/13040
library(conflicted)
library(duckdbfs)
suppressWarnings(library(vmstools))
suppressMessages(library(tidyverse))
library(icesVocab)
library(gt)
library(here)
library(nanoparquet)
library(tictoc)
conflict_prefer(name = "filter", winner = "dplyr")
lubridate::now() |> print()

# data -------------------------------------------------------------------------
## CSquare resolution ----------------------------------------------------------
dx <- dy <- 0.05
## ICES vocabulary -------------------------------------------------------------
iv_ir <-     getCodeList("StatRec")$Key
iv_lclass <- 
  getCodeList("VesselLengthClass") |> 
  filter(Key %in% c("VL0006", "VL0608", "VL0810", "VL1012", "VL1215" ,"VL1518", "VL1824" ,"VL2440" ,"VL40XX")) |> 
  pull(Key) |> 
  sort()
iv_gear <-   getCodeList("GearType")$Key
iv_target <- getCodeList("TargetAssemblage")$Key
iv_met6 <-   getCodeList("Metier6_FishingActivity")$Key
iv_yesno <-  getCodeList("YesNoFields")$Key
iv_country <- getCodeList("ISO_3166")$Key
## native stuff ----------------------------------------------------------------
# There is confusion on metier5 vs target among different files
#  Needs to be fixed upstream
exclude_gear <- c("FPO", "GND", "LHP", "MIS")
exclude_met5 <- c("FPO_DEF", "GND_SPF", "LHP_FIF", "MIS_DWF")

vessels <- 
  open_dataset(here("data/vessels/vessels_iceland.parquet")) |> 
  mutate(loa_class = cut(loa,
                         breaks = c(0, 6, 8, 10, 12, 15, 18, 24, 40, 'inf' ),
                         right = FALSE,
                         include.lowest = TRUE,
                         labels = iv_lclass)) |> 
  select(vid, kw_total, loa_class, loa) |> 
  mutate(VE_ID = row_number(),
         VE_ID = as.character(VE_ID),
         VE_ID = paste0("IS", str_pad(VE_ID, pad = "0", width = 4)))
gear <- 
  open_dataset(here("data/gear/gear_mapping.parquet")) |> 
  select(agf_gid, gear, met5 = target, met6)
trail <- 
  open_dataset(here("data/ais/trail")) |> 
  filter(between(year, 2009, 2024),
         .cid > 0,
         whack == FALSE,
         !is.na(.sid),
         between(speed, s1, s2),
         between(time, t1, t2)) |> 
  # last minute change - also in logbooks
  #  Next time: No mets in preprocessed logbooks nor trail
  mutate(met6 = case_when(met6 == "OTB_DEF_>=40_0_0" & agf_gid == 7 ~ "OTB_CRU_90-99_1_120",
                          met6 == "OTB_DEF_>=40_0_0" & agf_gid == 8 ~ "OTB_CRU_40-54_0_0",
                          .default = met6))

midpoint <- 
  trail |> 
  select(.sid, lb_base, lon, lat) |> 
  group_by(.sid, lb_base) |> 
  mutate(.row_number = row_number(),
         total = n()) |> 
  ungroup() |> 
  filter(.row_number == floor(total/2)) |> 
  mutate(in_stk = "Y")
LB <- 
  open_dataset(here("data/logbooks/station-for-ais.parquet")) |>
  filter(between(year(date), 2009, 2024)) |>
  # this should be fixed upstream - about 0.05% of data
  distinct(vid, t1, t2, .keep_all = TRUE)


# Processsing ------------------------------------------------------------------

## Logbooks --------------------------------------------------------------------
### Processing -----------------------------------------------------------------

#### Gear width ----------------------------------------------------------------
# 1. Find median width of towed gear and use those values to replace missing values
#    Pelagic gear here excluded though
# 2. Cap gear width - winzoriztion

# 1. Missingness (NAs) - find median and use that for NA's
#    No width for pelagics - are made NA downstream
LB |> 
  filter(agf_gid %in% c(6:8, 15)) |> 
  mutate(has_width = ifelse(!is.na(width), 1, 0)) |> 
  group_by(agf_gid) |> 
  summarise(total = n(),
            has_width = sum(has_width),
            min = min(width, na.rm = TRUE),
            median = median(width, na.rm = TRUE),
            mean = mean(width, na.rm = TRUE),
            max = max(width, na.rm = TRUE)) |> 
  collect() |> 
  mutate(p = has_width / total,
         .after = has_width) |> 
  arrange(agf_gid)
width_agf_gid6 <- 100
width_agf_gid7 <- 45    # Rækjuvarpa - dubios, most likely larger for offshore
width_agf_gid8 <- 60    # Humarvarpa - larett opnun i fiskar
width_agf_gid15 <- 2
## 
LB <- 
  LB |> 
  mutate(width = case_when(agf_gid %in% c(6:8, 15) & !is.na(width) ~ width,
                           agf_gid == 6 & is.na(width) ~ width_agf_gid6,
                           agf_gid == 7 & is.na(width) ~ width_agf_gid7,
                           agf_gid == 8 & is.na(width) ~ width_agf_gid8,
                           agf_gid == 15 & is.na(width) ~ width_agf_gid15,
                           .default = NA))   # all other gear get an NA
# Capping
LB |> 
  group_by(agf_gid) |> 
  summarise(n = n(),
            max = max(width)) |> 
  collect() |> 
  drop_na() |> 
  arrange(agf_gid) |> 
  knitr::kable(caption = "Maximum reported gear width")
LB |> 
  filter(agf_gid %in% c(6:8, 15)) |> 
  collect() |> 
  ggplot(aes(width)) +
  geom_histogram() +
  facet_wrap(~ agf_gid, scales = "free")
LB <- 
  LB |> 
  mutate(width = case_when(agf_gid == 7 & width > 65 ~ 65,
                           agf_gid == 8 & width > 30 ~ 30,
                           .default = width))
LB |> 
  filter(agf_gid %in% c(6:8, 15)) |> 
  collect() |> 
  ggplot(aes(width)) +
  geom_histogram() +
  facet_wrap(~ agf_gid, scales = "free")
lb <- 
  LB |> 
  left_join(midpoint |> select(.sid, lb_base, lon_ais = lon, lat_ais = lat, in_stk),
            by = join_by(.sid, lb_base)) |> 
  mutate(in_stk = ifelse(is.na(in_stk), "N", in_stk),
         lon = case_when(!is.na(lon_ais) ~ lon_ais,
                         .default = lon),
         lat = case_when(!is.na(lat_ais) ~ lat_ais,
                         .default = lat)) |> 
  # missing loa: 4 "vessels" - 2 of them divers, other two with one record each
  left_join(vessels,
            by = join_by(vid)) |> 
  left_join(gear,
            by = join_by(agf_gid)) |> 
  group_by(vid, date) |> 
  mutate(fishing_days = 1 / n()) |> 
  ungroup() |> 
  mutate(kw_fishing_days = fishing_days * kw_total) |> 
  mutate(
    checks = 
      case_when(gear %in% exclude_gear ~ "00 excluded gear",
                is.na(catch_total) ~ "01 catch missing",
                is.na(agf_gid) ~ "02 agf gid missing",
                is.na(loa_class) & is.na(kw_total) ~ "03 loa and kw missing",
                is.na(loa_class) & !is.na(kw_total) ~ "04 loa missing",
                !is.na(loa_class) & is.na(kw_total) ~ "05 kw missing",
                is.na(gear) ~ "06 gear missing",
                is.na(met5) ~ "07 metier 5 missing",
                is.na(met6) ~ "08 metier 6 missing",
                is.na(lon) ~ "09 lon missing",
                is.na(lat) ~ "10 lat missing",
                !between(lon, -44, 69) ~ "11 lon outside range",
                !between(lat, 36, 85) ~ "11 lat outside range",
                loa == 0 ~ "12 Vessel zero length",
                .default = "ok"))

lb |> 
  count(checks) |> 
  collect() |> 
  arrange(checks) |> 
  mutate(p = round(n / sum(n, na.rm = TRUE) * 100, 3)) |> 
  knitr::kable()
lb <- 
  lb |> 
  filter(checks == "ok") |>
  select(-checks)

### Aggregate and summarise ----------------------------------------------------
dx_ir <- 1
dy_ir <- dx_ir / 2
lb_summary <- 
  lb |> 
  mutate(RecordType = "LE",
         CountryCode = "IS",
         Year = year(date),
         Month = month(date),
         # Midpoint of ICES rectangles
         lon = lon %/% dx_ir * dx_ir + dx_ir / 2,
         lat = lat %/% dy_ir * dy_ir + dy_ir / 2) |> 
  group_by(
    RecordType, 
    CountryCode, 
    Year, 
    Month,
    lon,
    lat,
    MetierL4 = gear,
    MetierL5 = met5,
    MetierL6 = met6,
    VesselLengthRange = loa_class, 
    VMSEnabled = in_stk) |> 
  summarise(
    FishingDays = sum(fishing_days, na.rm = TRUE),
    kWFishingDays = sum(kw_fishing_days, na.rm = TRUE),
    TotWeight = sum(catch_total, na.rm = TRUE),
    TotValue = NA,
    NoDistinctVessels = n_distinct(VE_ID, na.rm = TRUE),
    AnonymizedVesselID = ifelse(n_distinct(VE_ID) < 3, str_flatten(distinct(VE_ID), collapse = ";"), 'not_required'),
    .groups = "drop") |> 
  collect() |> 
  # already got midpoint of ICES rectangle above, no need to play "safe"
  mutate(ICESrectangle = ramb::rb_d2ir(lon, lat, safe = FALSE),
         .before = lon) |> 
  select(-c(lon, lat)) |> 
  relocate(NoDistinctVessels, AnonymizedVesselID, .before = ICESrectangle)

### Vocabulary checks ----------------------------------------------------------
lb_summary <- 
  lb_summary |> 
  mutate(
    checks = 
      case_when(!ICESrectangle %in% iv_ir ~ "1 ices_rectangle",
                !VesselLengthRange %in% iv_lclass ~ "2 length class",
                !MetierL4 %in% iv_gear ~ "3 gear (metier4)",
                !MetierL5 %in% iv_target ~ "4 target (metier5)",
                !MetierL6 %in% iv_met6 ~ "5 metier6",
                !VMSEnabled %in% iv_yesno ~ "6 vms enabled",
                !CountryCode %in% iv_country ~ "7 country code",
                .default = "ok")) 

lb_summary |> 
  count(checks) |> 
  mutate(p = round(n / sum(n) * 100, 3)) |> 
  knitr::kable(caption = "Logbooks vocabulary checks")

lb_summary <- 
  lb_summary |> 
  filter(checks == "ok") |> 
  select(-checks) |> 
  mutate(TotValue = NA_real_)

### Final checks ---------------------------------------------------------------

# Create the table to check fields formats and number of NA's
table2Save <- lb_summary |> as.data.frame()

# Print a summary table in Viewer
table_nas <- NULL
for ( nn in colnames(table2Save)) {
  table_na <- table(table2Save[, nn]%>%is.na() )
  row <- c(field = nn, is_na =  ifelse(is.na (table_na['TRUE']), 0, table_na['TRUE'] ), total_records =  table2Save[, nn]%>%length(), field_type =class(  table2Save[, nn]  ) )
  table_nas <- rbind(table_nas,  row)
}


gt(
  table_nas %>% as_tibble(),
  rowname_col = 'field'
) %>%
  tab_header(
    title = md('Summary of **Table 2**  number of NA and records types')
  ) %>%
  cols_label(  `is_na.NA`=  md('Number of  <br> NA\'s') ,
               total_records = md('Total <br> records'),
               field_type = md('Field <br> type')
  ) %>%
  tab_footnote(
    footnote = md('Non mandatory fields can include null values if not available'),
    locations = cells_stub( rows = c('TotValue'))
  )

### Expect checks --------------------------------------------------------------



### Export ---------------------------------------------------------------------
lb_summary |> 
  write.table(file = here("ices-datacall/2025/ICES_LE_IS.csv"),
              na = "",
              row.names = FALSE,
              col.names = TRUE,
              sep = ",",
              quote = FALSE)

## Trails ----------------------------------------------------------------------
### Processing -----------------------------------------------------------------

trail <- 
  trail |> 
  mutate(checks = case_when(met5 %in% exclude_met5 ~ "00 excluded gear",
                            is.na(loa) ~ "01 loa missing",
                            loa == 0 ~ "02 loa zero",
                            dt / (3600) > 2 ~ "03 interval greater than 2 hours",
                            dt < 5 ~ "03 interval less than 30 seconds", 
                            .default = "ok"))
trail |> 
  count(checks) |> 
  collect() |> 
  arrange(checks) |> 
  mutate(p = round(n / sum(n, na.rm = TRUE) * 100, 2)) |> 
  knitr::kable(caption = "Rows to be filtered")

trail <- 
  trail |> 
  filter(checks == "ok") |> 
  select(-c(checks))

### Aggregate and summarise ----------------------------------------------------
trail_summary <-
  trail |> 
  select(-c(loa)) |> 
  # Should be fixed/checked upstream
  filter(!is.na(dt), !is.na(dd)) |> 
  # Should also be fixed/checked upstream
  mutate(catch_total = ifelse(is.na(catch_total), 0, catch_total)) |> 
  left_join(vessels,
            by = join_by(vid)) |> 
  # met5 needs fixing upstream
  left_join(gear |> select(agf_gid, gear, met5_correct = met5),
            by = join_by(agf_gid)) |> 
  left_join(LB |> select(.sid, lb_base, width),
            by = join_by(.sid, lb_base)) |> 
  mutate(RecordType = "VE",
         CountryCode = "IS",
         lon = lon%/%dx * dx + dx/2,
         lat = lat%/%dy * dy + dy/2) |>
  group_by(.sid, lb_base) |> 
  mutate(catch = catch_total / n()) |> 
  ungroup() |> 
  group_by(RecordType,
           CountryCode,
           year,           # Can not do Year = year here, duckdb seems to be agnostic
           month,          #    ditto
           lon,            # 0.05 csquare midpoint 
           lat,            #    ditto
           MetierL4 = gear,
           MetierL5 = met5_correct,
           MetierL6 = met6,
           VesselLengthRange = loa_class,
           Habitat = .msfd_bbht, 
           Depth = .depth) |> 
  summarise(
    No_Records = n(),
    AverageFishingSpeed = mean(speed, na.rm = TRUE),
    FishingHour = sum(dt, na.rm = TRUE) / (60 * 60),       # dt is in seconds
    AverageInterval = mean(dt, na.rm = TRUE) / (60 * 60),  # hours!
    AverageVesselLength = mean(loa, na.rm = TRUE),         # meters
    AveragekW = mean(kw_total, na.rm = TRUE),
    kWFishingHour = sum(kw_total * dt, na.rm = TRUE) / (60 * 60),
    SweptArea = sum((dd / 1000) * (width / 1000), na.rm = TRUE), # [km2]
    TotWeight = sum(catch, na.rm = TRUE),                        # [kg]
    TotValue = NA_real_,
    NoDistinctVessels = n_distinct(vid, na.rm = TRUE),
    AnonymizedVesselID = ifelse(n_distinct(VE_ID) < 3, str_flatten(distinct(VE_ID), collapse = ";"), 'not_required'),
    AverageGearWidth = mean(width / 1000, na.rm = TRUE),         # km
    .groups = "drop"
  ) |> 
  collect() |> 
  rename(Year = year, Month = month) |> 
  mutate(Csquare = vmstools::CSquare(lon, lat, degrees = dx),
         .before = lon) |> 
  select(-c(lon, lat)) |> 
  relocate(NoDistinctVessels, AnonymizedVesselID, .before = Csquare)

### Vocabulary checks ----------------------------------------------------------
trail_summary <- 
  trail_summary |> 
  mutate(lonlat = vmstools::CSquare2LonLat(csqr = Csquare, degrees =  dx)) |> 
  unnest(lonlat) |> 
  mutate(checks = case_when(!between(SI_LATI, 30, 90) ~ "1 cquare out of bound",
                            !VesselLengthRange %in% iv_lclass ~ "2 length class",
                            !MetierL4 %in% iv_gear ~ "3 gear (metier4)",
                            !MetierL5 %in% iv_target ~ "4 target (metier5)",
                            !MetierL6 %in% iv_met6 ~ "5 metier6",
                            !CountryCode %in% iv_country ~ "7 country code",
                            .default = "ok"))
trail_summary |> 
  count(checks) |> 
  mutate(p = round(n / sum(n) * 100, 3))
  
trail_summary <- 
  trail_summary |> 
  filter(checks == "ok") |> 
  select(-c(SI_LATI, SI_LONG, checks))

### Final checks ---------------------------------------------------------------
table1Save <- trail_summary |> as.data.frame()
table_nas <- NULL
for ( nn in colnames(table1Save)) {
  table_na <- table(table1Save[, nn]%>%is.na() )
  row <- c(field = nn, is_na =  ifelse(is.na (table_na['TRUE']), 0, table_na['TRUE'] ), total_records =  table1Save[, nn]%>%length(), field_type =class(  table1Save[, nn]  ) )
  table_nas <- rbind(table_nas,  row)
}

# Print a summary table in Viewer
gt(
  table_nas%>%as_tibble(),
  rowname_col = 'field'
) %>%
  tab_header(
    title = md('Summary of **Table 1**  number of NA and records types')
  ) %>%
  cols_label(  `is_na.NA`=  md('Number of  <br> NA\'s') ,
               total_records = md('Total <br> records'),
               field_type = md('Field <br> type')
  ) %>%
  tab_footnote(
    footnote = md('Non mandatory fields can include null values if not available'),
    locations = cells_stub( rows = c( 'TotValue', 'AverageGearWidth', 'Habitat'))
  )

trail_summary |> filter(is.na(FishingHour)) 
trail_summary |> 
  filter(is.na(TotWeight)) |> 
  count()

trail_summary |> 
  filter(TotWeight == 0) |> 
  count(MetierL4)
trail_summary |> 
  filter(is.na(SweptArea)) |> 
  count(MetierL4)



trail_summary |> 
  filter(is.na(Depth)) |> 
  mutate(lonlat = vmstools::CSquare2LonLat(csqr = Csquare, degrees =  dx)) |> 
  unnest(lonlat) |> 
  rename(lon = SI_LONG, lat = SI_LATI) |> 
  ggplot(aes(lon, lat)) +
  geom_point() +
  coord_quickmap()


trail_summary |> 
  filter(is.na(Habitat)) |> 
  mutate(lonlat = vmstools::CSquare2LonLat(csqr = Csquare, degrees =  dx)) |> 
  unnest(lonlat) |> 
  rename(lon = SI_LONG, lat = SI_LATI) |> 
  ggplot(aes(lon, lat, colour = MetierL4)) +
  geom_point(size = 1) +
  coord_quickmap()

trail_summary |> 
  filter(TotWeight == 0) |> 
  mutate(lonlat = vmstools::CSquare2LonLat(csqr = Csquare, degrees =  dx)) |> 
  unnest(lonlat) |> 
  rename(lon = SI_LONG, lat = SI_LATI) |> 
  ggplot(aes(lon, lat, colour = MetierL4)) +
  geom_point(size = 1) +
  coord_quickmap()

trail_summary |> 
  mutate(zero = case_when(is.na(TotWeight) ~ "NA",
                          TotWeight == 0 ~ "yes",
                          TotWeight > 0 ~ "no",
                          .default = "Something else")) |> 
  count(MetierL4, zero) |>
  spread(zero, n, fill = 0) |> 
  mutate(total = yes + no,
         p = round(yes / total * 100, 2))
trail_summary |> 
  mutate(has_sa = case_when(is.na(SweptArea) ~ "NA",
                            SweptArea == 0 ~ "zero",
                            SweptArea > 0 ~ "positive",
                            .default = "something else")) |> 
  count(MetierL4, has_sa) |> 
  spread(has_sa, n)

### Export ---------------------------------------------------------------------
trail_summary |> 
  write.table(file = here("ices-datacall/2025/ICES_VE_IS.csv"), 
              na = "",
              row.names = FALSE,
              col.names = TRUE,
              sep = ",",
              quote = FALSE)

# Session info -----------------------------------------------------------------
devtools::session_info()
