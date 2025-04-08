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
  read_parquet("data/vessels/mmsi_iceland_archieves.parquet") |>
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
  knitr::kable(caption = "Same MMSI on more than one vessel. Created in: 00_DATASET_vessels_iceland-mmsi.R")
mmsi_ISL |>
  group_by(vid) |>
  mutate(n.mmsi = n_distinct(mmsi)) |>
  ungroup() |>
  filter(n.mmsi > 1) |>
  knitr::kable(caption = "Vessel with more than one MMSI (expect) none")
vessel_ISL <-
  mar::tbl_mar(con, "vessel.vessel_v") |> 
  dplyr::select(vid = registration_no,
                vessel = name,
                mclass = usage_category_no,   # mclass
                imo = imo_no,
                .vid = vessel_id,
                kw = power_kw,
                loa = max_length) |>
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
                loa,
                kw,
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
vessel_ISL <- 
  vessel_ISL |> 
  arrange(vid)

# add last year vessel in Hreiðar's dataset (last time updated 2021)
v <- 
  readxl::read_excel("~/stasi/hreidar/data-raw/HREIDAR_Islensk_skip.xlsx") |> 
  janitor::clean_names() |> 
  select(vid = skskr_nr,
         year = ar) |> 
  filter(!is.na(vid)) |> 
  group_by(vid) |> 
  reframe(yh1 = min(year),        # year hreiðar first
          yh2 = max(year)) |>     # year hreiðar last
  mutate(vid = as.integer(vid)) |> 
  mutate(yh1 = ifelse(yh1 == 9999, NA, yh1),
         yh2 = ifelse(yh2 == 9999, NA, yh2))

vessel_ISL <- 
  vessel_ISL |> 
  # Note: We will have vessels entering after 2021 which have NA year_last_hreidar
  left_join(v) |> 
  select(vid, yh1, yh2, everything())

# get rid of spaces in CS
vessel_ISL <- 
  vessel_ISL |> 
  mutate(cs = str_trim(cs),
         cs = str_remove(cs, " "))


# Save -------------------------------------------------------------------------
vessel_ISL |> 
  mutate(kw = ifelse(kw == 0, NA, kw)) |> 
  write_parquet("data/vessels/vessels_iceland.parquet")




# Issues -----------------------------------------------------------------------
## Trials at consolidating information ----------------------------------------
#  * Think about using hierarchical approach in "beliefs". Like trust
#    national records over others if there is a discrepancy
vessel_ISL |>
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

vessel_ISL |> filter(mmsi %in% as.character(missing$mmsi))

### ISL vessel - in landings?
# check if vessels that have landed have mmsi
vid_landings <-
  omar::ln_agf(con) |>
  mutate(year = year(date)) |>
  filter(between(year, 2007, 2024)) |>
  group_by(vid) |>
  summarise(n.landings = n_distinct(.id),
            last.landing = max(date),
            .groups = "drop") |>
  collect()
vessel_ISL |>
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
if(FALSE) {
  vessel_registry |>
  filter(!is.na(mmsi)) |>
  arrange(flag, mmsi) |>
  group_by(mmsi) |>
  mutate(n.imo = n_distinct(imo, na.rm = TRUE)) |>
  filter(n.imo > 1) |>
  knitr::kable()
}

## Check on consolidating ASTD with NOR ----------------------------------------
# This will not work because the joining variable is vessel
v_astd_nor <-
  vessel_ASTD |>
  filter(flag == "NOR",
         !is.na(imo)) |>
  select(mmsi, vessel, imo)
vessel_NOR |>
  select(-c(mmsi, imo)) |>
  filter(vessel %in% v_astd_nor$vessel) |>
  inner_join(v_astd_nor)
