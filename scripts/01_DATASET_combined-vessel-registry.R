# Compilation of vessel registries
#  Main emphasis (for now) is to consolidate the variables that ara of use
#  to identify the vessels in the stk-dataset
# TODO: ------------------------------------------------------------------------
#  * Check Icelandic vessels - some MMSI still missing - see Issues
#  * Add loa, kw and other things if available
#  * Check another source for norwegian vessels, one that has mmsi
# NEWS -------------------------------------------------------------------------
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

library(nanoparquet)
library(arrow)
library(tidyverse)
library(readxl)
library(mar)
str_extract_between_parenthesis <- function(x) {
  str_match(x, "(?<=\\().+?(?=\\))")
}
options(knitr.kable.NA = '')
con <- connect_mar()

# Import vessel registries -----------------------------------------------------
## ISL -------------------------------------------------------------------------
mmsi_ISL <-
  read_parquet("data/vessels/mmsi_ISL.parquet") |>
  filter(mmsi_cat == "vessel") |>
  filter(!is.na(sknr)) |>
  filter(!str_starts(sknr, "SKB")) |>
  rename(vid = sknr) |>
  mutate(vid = as.integer(vid)) |>
  arrange(vid)
# Test: Duplicate MMSI
mmsi_ISL |>
  group_by(mmsi) |>
  mutate(n.vid = n_distinct(vid)) |>
  ungroup() |>
  filter(n.vid > 1) |>
  arrange(mmsi, mmsi_t2) |>
  knitr::kable(caption = "Same MMSI on more than one vessel. Created in: 00_DATASET_mmsi_ISL.R")
mmsi_ISL |>
  group_by(vid) |>
  mutate(n.mmsi = n_distinct(mmsi)) |>
  ungroup() |>
  filter(n.mmsi > 1) |>
  knitr::kable(caption = "Vessel with more than one MMSI (expect) none")
vessel_ISL <-
  mar::tbl_mar(con, "vessel.vessel") |>
  dplyr::select(vid = registration_no,
                vessel = name,
                mclass = usage_category_no,   # mclass
                imo = imo_no,
                .vid = vessel_id) |>
  dplyr::left_join(tbl_mar(con, "vessel.vessel_identification") |>
                     select(uid = region_acronym,
                            uno = region_no,
                            cs = call_sign,
                            .vid = vessel_id),
                   by = dplyr::join_by(.vid)) |>
  collect() |>
  dplyr::mutate(uno_c = case_when(uno > 0 ~ str_pad(uno, 3, pad = "0"),
                                  .default = NA),
                uid = case_when(!is.na(uid) & !is.na(uno_c) ~ paste0(uid, uno_c),
                                .default = NA),
                source = "ISL",
                flag = "ISL") |>
  dplyr::select(cs,
                imo,
                uid,
                vessel,
                flag,
                vid,
                source,
                .id = .vid) |>
  filter(!vid %in% c(0))
# Extract the cs for foreign vessels from vessel name
vessel_ISL <-
  vessel_ISL |>
  mutate(vessel = str_squish(vessel),
         vessel = case_when(vessel == "Tuugaalik )OZBW)" ~ "Tuugaalik (OZBW)",
                            vessel == "Silver Fjord (3FWE9" ~ "Silver Fjord (3FWE9)",
                            .default = vessel),
         cs =
           case_when(
             source == "ISL" & vid %in% 3700:4999 & is.na(cs) ~ str_extract_between_parenthesis(vessel)[,1],,
             .default = cs),
         vessel =
           case_when(
             source == "ISL" & vid %in% 3700:4999 ~ str_remove(vessel, paste0("\\(", cs, "\\)")),
             .default = vessel),
         vessel = str_squish(vessel))
# classify foreign vessels
library(countrycode)
vessel_ISL <-
  vessel_ISL |>
  mutate(flag =
           case_when(
             vid %in% 3700:4999 & !is.na(uid) ~ countrycode(str_sub(uid, 1, 2),
                                                            origin = "iso2c",
                                                            destination = "iso3c"),
             .default = flag)) |>
  # Need to revisit this at a later stage
  mutate(flag =
           case_when(
             vid %in% 3700:4999 & flag == "ISL" ~ "UNK",
             vid %in% 3700:4999 & is.na(flag) ~ "UNK",
             .default = flag)) |>
  # drop the uid for foreign vessels
  mutate(uid =
           case_when(
             vid %in% 3700:4999 ~ NA,
             .default = uid))
# test
vessel_ISL |> filter(vid %in% 3700:4999 & flag == "ISL")
vessel_ISL |> filter(!vid %in% 3700:4999) |> count(flag)
vessel_ISL |>
  filter(vid %in% 3700:4999) |>
  filter(is.na(flag))

vessel_ISL <-
  vessel_ISL |>
  left_join(mmsi_ISL |>
              select(vid, mmsi, cs_mmsi = cs, mmsi_t1, mmsi_t2, zombie_no, note_date, note_fate),
            by = join_by(vid)) |>
  mutate(cs = case_when(is.na(cs) & !is.na(cs_mmsi) ~ cs_mmsi,
                        .default = cs)) |>
  select(-cs_mmsi) |>
  select(vid, mmsi, cs, imo, uid, vessel, flag, source, .id, everything())

## FRO ------------------------------------------------------------------------
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
  select(mmsi,
         cs = kallimerki,
         imo = imo_nummar,
         uid = havnakenningarnr,
         vessel = skipanavn,
         flag,
         source,
         .id = skrasetingarnr) |>
  mutate(imo = as.integer(imo))
## NOR ------------------------------------------------------------------------
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
         flag = "NOR",
         source = "NOR")
vessel_NOR <-
  vessel_NOR |>
  select(mmsi,
         cs = radiokallesignal,
         imo,
         uid = registreringsmerke,   # this is guesswork
         vessel = fartoynavn,
         flag,
         source,
         .id = fartoy_id,
         year) |>
  arrange(-year) |>
  distinct(.id, .keep_all = TRUE) |>
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
  select(mmsi,
         cs = ircs,
         imo = uvi,      # universal vessel identifier
         uid = external_marking,
         vessel = name_of_vessel,
         flag = country_of_registration,
         source,
         .id = cfr) |>
  mutate(cs = str_trim(cs),
         uid = str_trim(uid),
         mmsi = str_trim(mmsi),
         mmsi = as.character(mmsi))
## ASTD ------------------------------------------------------------------------
vessel_ASTD <-
  open_dataset("data/astd") |>
  select(mmsi, imo = imonumber, vessel, flag, time) |>
  group_by(mmsi, imo, vessel, flag) |>
  summarise(time = max(time),
            .groups = "drop") |>
  collect()
vessel_ASTD <-
  vessel_ASTD |>
  filter(!is.na(flag),
         nchar(mmsi) == 9) |>
  group_by(mmsi) |>
  fill(imo, vessel, .direction = "downup") |>
  ungroup() |>
  # keep the last information
  arrange(mmsi, desc(time)) |>
  distinct(mmsi, .keep_all = TRUE) |>
  mutate(mmsi = as.character(mmsi),
         cs = NA_character_,
         uid = NA_character_,
         source = "ASTD") |>
  select(mmsi, imo, cs, uid, vessel, flag, source)
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
vessel_registry |>
  nanoparquet::write_parquet("data/vessels/vessel-registry.parquet")

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
