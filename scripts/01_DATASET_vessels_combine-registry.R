# Compilation of vessel registries
#  Main emphasis (for now) is to consolidate the variables that ara of use
#  to identify the vessels in the stk-dataset
# TODO: ------------------------------------------------------------------------
#  * Check Icelandic vessels - some MMSI still missing - see Issues
#  * Add loa, kw and other things if available
#  * Check another source for norwegian vessels, one that has mmsi
# NEWS -------------------------------------------------------------------------
# 2024-11-09
#  * Moved Icelandic registry script to a separate R-file (00_vessels_iceland.R
#  *  File generated im that scrip now read in here
# 2024-10-27
#  * Icelandic registry
#    * Include the note, note_date and note_fate form mmsi into the vessel
#      registry
# 2024-10-26
#  * Icelandic vessels 3700:4999:
#    * Extract the cs from the vessel name
#    * Properly allocate a flag using info in the uid
# 2024-10-25
#  * consolidation, review and addition of older code
# OUTPUT -----------------------------------------------------------------------
# data-raw/vessels/vessel-registry.parquet
SAVE <- FALSE
library(nanoparquet)
library(arrow)
library(tidyverse)
library(readxl)
library(countrycode)
library(mar)
str_extract_between_parenthesis <- function(x) {
  str_match(x, "(?<=\\().+?(?=\\))")
}
options(knitr.kable.NA = '')
con <- connect_mar()

# Import vessel registries -----------------------------------------------------
## ISL -------------------------------------------------------------------------

vessel_ISL <-
  nanoparquet::read_parquet("data/vessels/vessels_iceland.parquet")

## FRO -------------------------------------------------------------------------
lh_remove_first_period <- function(x) {
  x |>
    str_locate_all("\\.") |>
    map(as_tibble) |>
    bind_rows(.id = ".id") |>
    mutate(.id = as.numeric(.id)) |>
    group_by(.id) |>
    summarise(n = n(),
              first = min(start)) |>
    mutate(x = x) |>
    mutate(x = case_when(n == 2 ~ paste0(str_sub(x, 1, first - 1),
                                         str_sub(x, first + 1)),
                         .default = x)) |>
    mutate(x = str_squish(x),
           x = as.numeric(x)) |>
    arrange(.id) |>
    pull(x)
}
fil <- dir("data-raw/vessels/FRO", full.names = TRUE)
vessel_FRO <-
  map_df(fil, read_csv, show_col_types = FALSE) |>
  drop_na() |>
  mutate(value = ifelse(value %in% "Einki ásett", NA, value)) |>
  spread(variable, value) |>
  janitor::clean_names() |>
  mutate(flag = "FRO",
         source = "FRO")
vessel_FRO <-
  vessel_FRO |>
  select(.vid = skrasetingarnr,
         mmsi,
         cs = kallimerki,
         imo = imo_nummar,
         loa = longd_yviralt_loa,
         gt = bruttotons,
         kw = motororka,
         #kw2,
         uid = havnakenningarnr,
         vessel = skipanavn,
         flag,
         source) |>
  mutate(imo = as.integer(imo),
         loa = str_remove(loa, "metrar"),
         loa = str_trim(loa),
         loa = lh_remove_first_period(loa),
         loa = as.numeric(loa),
         gt = str_trim(gt),
         gt = lh_remove_first_period(gt),
         gt = as.numeric(gt),
         kw = str_remove(kw, "KW"),
         kw = str_trim(kw),
         kw = lh_remove_first_period(kw),
         kw = as.numeric(kw))
## NOR -------------------------------------------------------------------------
# Biggest problem here is that there is no mmsi-data
fil <- "data-raw/vessels/NOR/norway_vessel-registry.xlsx"
sheet <- excel_sheets(fil)
sheet <- sheet[-c(1:2)]
res <- list()
for(i in 1:length(sheet)) {
  res[[i]] <-
    read_excel(fil, sheet[i]) |>
    janitor::clean_names()
}
names(res) <- sheet
vessel_NOR <-
  res |>
  bind_rows(.id = "year") |>
  mutate(year = as.integer(year),
         mmsi = NA_character_,
         imo = NA_integer_,
         gt = NA_real_,
         flag = "NOR",
         source = "NOR")
vessel_NOR <-
  vessel_NOR |>
  select(.vid = fartoy_id,
         mmsi,
         cs = radiokallesignal,
         imo,
         loa = storste_lengde,
         gt,
         kw = motorkraft,
         uid = registreringsmerke,   # this is guesswork
         vessel = fartoynavn,
         flag,
         source,
         year) |>
  arrange(-year) |>
  distinct(.vid, .keep_all = TRUE) |>
  select(-year)
## EU -------------------------------------------------------------------------
vessel_EU <-
  read_delim("data-raw/vessels/EU/vesselRegistryListResults.csv",
             delim = ";", guess_max = Inf) |>
  janitor::clean_names() |>
  mutate(source = "EU")
vessel_EU <-
  vessel_EU |>
  # last record?
  arrange(cfr, desc(event_end_date)) |>
  distinct(cfr, .keep_all = TRUE) |>
  select(.vid = cfr,
         mmsi,
         cs = ircs,
         imo = uvi,      # universal vessel identifier
         loa,
         gt = tonnage_gt,
         kw = power_of_main_engine,
         kw2 = power_of_auxiliary_engine, # Is this not in the Faero registry
         uid = external_marking,
         vessel = name_of_vessel,
         flag = country_of_registration,
         source) |>
  mutate(cs = str_trim(cs),
         uid = str_trim(uid),
         mmsi = str_trim(mmsi),
         mmsi = as.character(mmsi)) |>
  distinct(.vid, mmsi, .keep_all = TRUE) # Not many dual mmsi vessels
## ASTD ------------------------------------------------------------------------
vessel_ASTD <-
  open_dataset("data/ais/astd") |>
  select(mmsi, imo = imonumber, vessel, flag, time) |>
  group_by(mmsi, imo, vessel, flag) |>
  summarise(pings = n(),
            mmsi_t1 = min(time),
            mmsi_t2 = max(time),
            .groups = "drop") |>
  collect()

vessel_ASTD <-
  vessel_ASTD |>
  filter(!is.na(flag),
         pings >= 100,
         nchar(mmsi) == 9) |>
  arrange(mmsi, mmsi_t2) |>
  group_by(mmsi) |>
  fill(imo, vessel, .direction = "downup") |>
  ungroup() |>
  group_by(mmsi, imo, vessel, flag) |>
  summarise(pings = sum(pings),
            mmsi_t1 = min(mmsi_t1),
            mmsi_t2 = max(mmsi_t2)) |>
  ungroup() |>
  # keep the last information
  arrange(mmsi, desc(mmsi_t2)) |>
  distinct(mmsi, .keep_all = TRUE) |>
  mutate(mmsi = as.character(mmsi),
         cs = NA_character_,
         uid = NA_character_,
         source = "ASTD") |>
  select(mmsi, imo, cs, uid, vessel, mmsi_t1, mmsi_t2, flag, source, pings)
## GFW -------------------------------------------------------------------------
# This does not add much to the consolidation. Suggest not to include it
#  except possibly as a flag for .gfw_class as a variable in the compiled
#  vessel database
vessel_GFW <-
  read_csv("data-raw/vessels/gfw/fishing-vessels-v2.csv") |>
  mutate(source = "GFW") |>
  select(mmsi,
         flag = flag_gfw,
         source,
         .gfw_class = vessel_class_gfw) |>
  mutate(mmsi = as.character(mmsi))




## Export ---------------------------------------------------------------------
vessel_registry <-
  bind_rows(vessel_ISL |> mutate(imo = as.integer(imo),
                                 mmsi = as.character(mmsi),
                                 .id = as.character(.id)),
            vessel_FRO,
            vessel_NOR,
            vessel_EU,
            vessel_ASTD) |>
  left_join(vessel_GFW |> select(mmsi, gfw_class = .gfw_class),
            by = join_by(mmsi))
if(SAVE) {
vessel_registry |> nanoparquet::write_parquet("data/vessels/vessel-registry.parquet")
}
# Usage example ---------------------------------------------------------------
vessel_ASTD |>
  select(mmsi, vessel2 = vessel, flag2 = flag) |>
  inner_join(vessel_registry |>
               filter(source == "ISL") |>
               select(mmsi, cs, imo, uid, vessel, vid) |>
               drop_na(mmsi)) |>
  arrange(vid)
# Issues -----------------------------------------------------------------------
## Trials at consolidating informations ----------------------------------------
#  * Think about using hierarchical approach in "beliefs". Like trust
#    national records over others if there is a discrepancy
vessel_registry |>
  arrange(mmsi, desc(source)) |>
  filter(flag == "ISL",
         !is.na(mmsi)) |>
  group_by(mmsi) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  slice(1:600) |>
  knitr::kable()

### Check if still missing -----------------------------------------------------
# Some (non exclusive) mmsi picked up in astd that are note in mmsi registry
missing <-
  tribble(~mmsi, ~sknr, ~vessel_note,
          251396098, NA, "NIELS JONSSON EA106",
          251216100, NA, "GUNNA VALGEIRS",
          251857270, NA, "SIF",
          251139000, NA, "HUGINN",
          251513101, NA, NA,
          251068418, NA, NA)

vessel_registry |> filter(mmsi %in% as.character(missing$mmsi))

### ISL vessel - in landings?
# check if vessels that have landed have mmsi
vid_landings <-
  omar::ln_agf(con) |>
  mutate(year = year(date)) |>
  filter(between(year, 2008, 2024)) |>
  group_by(vid) |>
  summarise(n.landings = n_distinct(.id),
            last.landing = max(date),
            .groups = "drop") |>
  collect()
vessel_registry |>
  filter(source == "ISL") |>
  filter(!vid %in% 3700:4999) |>
  left_join(vid_landings) |>
  filter(is.na(mmsi) & !is.na(n.landings)) |>
  arrange(-desc(last.landing)) |>
  select(vid:vessel, n.landings, last.landing) |>
  knitr::kable(caption = "Vessels with no MMSI but have landings")


## Same MMSI, different IMO ----------------------------------------------------
# This is surprisingly rare
#  could try to run imo_check
vessel_registry |>
  filter(!is.na(mmsi)) |>
  arrange(flag, mmsi) |>
  group_by(mmsi) |>
  mutate(n.imo = n_distinct(imo, na.rm = TRUE)) |>
  filter(n.imo > 1) |>
  knitr::kable()

## Check on consolidating ASTD with NOR ----------------------------------------
# This will not work because the joining variabel is vessel
v_astd_nor <-
  vessel_ASTD |>
  filter(flag == "NOR",
         !is.na(imo)) |>
  select(mmsi, vessel, imo)
vessel_NOR |>
  select(-c(mmsi, imo)) |>
  filter(vessel %in% v_astd_nor$vessel) |>
  inner_join(v_astd_nor)
