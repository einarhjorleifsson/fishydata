# Classify the mobileid-localid-globalid in the stk-data
#  Focus on vessel data
#   * Assign MMSI to all Icelandic vessels
#   * Attempt to assign MMSI to foreign vessels
#   * Attempt to write the code flow so it can easily be updated
#     once new data are compiled
# TODO: ------------------------------------------------------------------------
#      ...
# NEWS -------------------------------------------------------------------------
# 202X-MM-DD
#
# 2024-10-24
#  * consolidation, review and addition of older code
# OUTPUT -----------------------------------------------------------------------
# data-raw/stk-mmsi-vessel-match/******.parquet

library(tidyverse)
options(knitr.kable.NA = '')
library(sf)
library(mapdeck)
set_token("pk.eyJ1IjoiZmlzaHZpY2UiLCJhIjoiY2p0MXQ5dThpMDZqeDQ5bHM0MGx4dHI5cyJ9.Fed_z7mv_TgTWDRjiavU3A")
library(mar)
source("R/ramb_functions.R")
con <- connect_mar()

# AUXILLARY DATA ---------------------------------------------------------------
## Maritime identification digits ----------------------------------------------
MID <-
  nanoparquet::read_parquet("data/lookups/maritime_identification_digits.parquet") |>
  # used when searching for child vessels
  mutate(MID_child = paste0("98", MID),
         MID_aid = paste0("99", MID))
## Call sign - flag state ------------------------------------------------------
CS <-
  nanoparquet::read_parquet("data/lookups/callsign_prefix.parquet")
#
vessels <-
  nanoparquet::read_parquet("data/vessels/vessel-registry.parquet")
isl_vid <-
  vessels |> filter(source == "ISL", !is.na(vid)) |> pull(vid) |> as.character()
# Note: only for those that have a mmsi
isl_cs <-
  vessels |> filter(source == "ISL", !is.na(vid), !is.na(mmsi)) |> pull(cs)
isl_mmsi <-
  vessels |> filter(source == "ISL", !is.na(vid), !is.na(mmsi)) |> pull(mmsi)
isl_uid <-
  vessels |> filter(source == "ISL", !is.na(vid), !is.na(mmsi), !is.na(uid)) |> pull(uid)
oth_uid <-
  vessels |> filter(source != "ISL", !is.na(uid)) |> pull(uid) |> unique()


# stk classifiction ------------------------------------------------------------
## import ----------------------------------------------------------------------
stk_gids <-
  tbl(con, dbplyr::in_schema("STK", "MOBILE")) |>
  select(mid = MOBILEID, loid = LOCALID, glid = GLOBALID) |>
  collect()
stk_summary <-
  tbl(con, dbplyr::in_schema("STK", "TRAIL")) |>
  mutate(YEAR = year(POSDATE),
         MONTH = month(POSDATE)) |>
  filter(YEAR >= 2007 &  MONTH >= 6) |>
  filter(YEAR <= 2024) |>
  group_by(MOBILEID) |>
  summarise(pings = n(),
            n_years = n_distinct(YEAR),
            t1 = min(POSDATE, na.rm = TRUE),
            t2 = max(POSDATE, na.rm = TRUE),
            .groups = "drop") |>
  collect() |>
  rename(mid = MOBILEID) |>
  mutate(t1 = as_date(t1),
         t2 = as_date(t2)) |>
  arrange(mid) |>
  left_join(stk_gids) |>
  select(mid, loid, glid, everything())

## define categorization keys --------------------------------------------------
fixed <-
  c("Surtseyja", "Straumnes", "Steinanes", "Haganes_K", "Bakkafjar",
    "Laugardal", "BorgfjE P", "Gemlufall", "Straumduf", "Eyrarhlid",
    "Hvalnes", "Straumm?l", "V_Blafj P", "Isafj.dju", "Rey?arfjo",
    "VidarfjAI", "KLIF AIS",  "VadlahAIS", "Hafell_AI", "TIND_AIS",  "Storhof?i",
    "Helguv", "Laugarb.f", "Grimseyja", "G.skagi",   "Grindavik", "Hornafjar",
    "Flateyjar", "Kogurdufl", "Blakkur.d", "Bakkafjor", "Hvalbakur", "SUGANDI_A",
    "TJALDANES",  "Snaefj1",
    "Snaefj2", "Lande", "Sjomsk", "TJALD.NES", "illvid_p", "BLAKKSNES", "V_Sfell B",
    "HOF", "Illvi?rah", "Miðfegg P", "BASE11", "Borgarfj ",
    "V_Hofsos", "V_Hofsos ", "Arnarfjor", "Trackw", "SUGANDAFJ",
    "BORGARÍS", "BORGARIS", "BORGARIS0", "BORGARIS1",
    "ThverfAIS",
    "TEST",
    "Hvannadal", "Tjaldanes", "BorglAIS", "HvalnAIS", "Midf_AIS",
    "Hellish A", "GreyAIS", "Berufjor?",
    "Baeir", "Frodarh_A", "Onundarfj", "HusavikAI", "Haukadals",
    "Drangsnes", "Hofdahusa", "Djupiv-AI", "Dyrafjor?", "Faskru?sf",
    "Fossfjor?", "Hvestudal", "Hringsdal", "Bakkafj-d", "Mulagong",
    "Grnipa P", "Haenuvk P", "Bolafj P", "Ennish P", "Grimsey P",
    "Frodarh P", "Haoxl B", "Hafell P", "Vidarfj P", "Djupiv P",
    "Blafj P", "Sigmundar", "Tjnes P", "Sfell P", "Hellish P",
    "Gvkurfj P", "Klif P", "Thverfj B", "Klif B", "Grimsey B",
    "Frodarh B", "Hvalnes P", "Haoxl P", "Grnipa B", "Illvidh P",
    "FLATEYRI_", "Hellish B", "Husavik B", "Hofsos P", "Faskra?sf",
    "Husavik P", "Tjornes P", "Thorbj B", "Borgarh-P", "Baeir B",
    "VadlahP", "Thverfj P", "Dalvik P", "Godat-P", "HafellAIS",
    "Bjolfur P", "Ennish B", "Thorbj P", "Hraunh P", "Gufusk P",
    "Lambhgi P", "Fri?rik A", "Baeir P", "Flatey du", "Fellsgg1P",
    "Fellsgg2P", "Akurtr B", "Midfell-P", "Horgargru", "Borgarl P",
    "Haenuvk B", "Gagnhdi P", "Hvalnes B", "HVestudal", "Gildruh B",
    "Sfell B", "Gagnhdi B", "BorgfjE B", "Spolur-P", "Klakkur P",
    "KOLBEINSE", "Stykkh P", "Tjnes B", "Kvigindis", "Dufl_GRV_",
    "Fell P", "Steinny-P", "Stokksn P", "Tjorn P", "Kopasker",
    "Akreyri P", "Grima P", "Dalatgi B", "ThverfjP", "Rifssker_",
    "Dalatgi P", "Tjorn B", "Kolmuli_K", "Vattarnes", "Thorbjorn",
    "Husavik", "Hafranes_", "Drangaj_P", "Hrisey", "Hofsos",
    "Midfegg P", "Midf P", "Gufunes P", "Mi?fegg P", "Dalvík P",
    "Dalvik", "Borgfj E", "Straumn-A", "Talknaf P",
    "Steinny", "TILK", "ThverfjP1", "Heidar-P", "Vadlaheid",
    "Talknaf B", "BLAKK_AIS", "Mork-P", "VPN_Bauja",
    "PF7567", "Daltat", "AEDEY AIS", "Borgfj E",
    # should really use the mobile id here, at least it is easier
    #  because that is how things are checked iteratively
    "2515036", "2311200", "2311400", "2573900", "2311500",
    "2515071", "25150051", "2314000",
    "251510120",    # Skipstjóraskólinn
    "231140005",
    "231140003",
    "251999898",
    "231140004",
    "231140006",
    "231140001",
    "231140002",
    "251513130",
    "xxx5",
    "103984",
    "Borgfj E",
    "Borgfj E ",
    "BLAKK_OLD",
    "Gufunes B",
    "Blondos P",
    "Mork P",
    "Va?lahei?"
  )
kvi <-
  c("Eyri_Kvi_", "Kvi_Skotu", "Kvi_Baeja", "Bjarg_Kvi", "Sjokvi-4", "Sjokvi-3",
    "Kvi-0 Hri", "Sjokvi-2", "Kvi_Sande", "Kvi_Fenri", "Sjokvi",
    "Y.Kofrady")
hafro <-
  c("Hafro_Str", "Hafro_O2_", "Hafro_CO2", "Hafro_H11", "Hafro_H20", "Hafro_Hva",
    "Hafro_duf", "AfmHafro_", "Afm_Hafro", "Hafro_W.V", "Hafro_W.V ", "afm_Hafro",
    "Rannsokn_")
net_glid <-
  c("9911378")

unknown_glid <- c("5200000")

## Classify the stk -------------------------------------------------------------
stk <-
  stk_summary |>
  # The order matters in the case_when
  mutate(.loid =
           case_when(
             loid %in% fixed ~ "fixed",
             loid %in% kvi ~ "kvi",
             numbers_only(loid) & nchar(loid) <= 4 ~ "vid",
             numbers_only(loid) & nchar(loid) == 9 ~ "mmsi",
             loid %in% isl_uid ~ "uid",
             loid %in% oth_uid ~ "uid",
             .default = NA)) |>
  mutate(.glid =
           case_when(
             glid %in% fixed ~ "fixed",
             glid %in% kvi ~ "kvi",
             glid %in% hafro ~ "hafro",
             glid %in% unknown_glid ~ "unknown",
             str_detect(tolower(glid), "_net") ~ "net",
             glid %in% net_glid ~ "net",
             numbers_only(glid) & nchar(glid) <= 4 ~ "vid",
             numbers_only(glid) & nchar(glid) == 9 ~ "mmsi",
             str_sub(glid, 1, 2) %in% CS$cs_prefix ~ "cs",
             str_sub(glid, 1, 5) %in% MID$MID_child ~ "child",
             str_sub(glid, 1, 5) %in% MID$MID_aid ~ "navigational aid",
             glid %in% isl_uid ~ "uid",
             glid %in% oth_uid ~ "uid",
             # This is dubious, should be the last check
             #  some elements here not callsigns, but pings few
             str_sub(glid, 1, 1) %in% CS$cs_prefix &
               !numbers_only(str_trim(glid)) &
               !str_starts(glid, "MOB_") ~ "cs1",
             .default = NA)) |>
  mutate(mmsi =
           case_when(
             .glid == "mmsi" ~ glid,
             .loid == "mmsi" ~ loid,
             .default = NA),
         cs =
           case_when(
             .glid == "cs" ~ glid,
             .loid == "cs" ~ loid,
             .default = NA),
         cs1 =
           case_when(
             .glid == "cs1" ~ glid,
             .loid == "cs1" ~ loid,
             .default = NA),
         vid =
           case_when(
             .glid == "vid" & .loid == "vid" & loid == glid ~ glid,
             .glid == "vid" & .loid == "cs" ~ glid,
             .loid == "vid" & .glid == "cs" ~ loid,
             .default = NA),
         uid =
           case_when(.glid == "uid" ~ glid,
                     .loid == "uid" ~ loid,
                     .default = NA)
  )
# add mmsi class if one has an mmsi candiate
stk <-
  stk |>
  mutate(mmsi_cat = case_when(!is.na(mmsi) ~ rb_mmsi_category(mmsi),
                              .default = NA))
stk |> group_by(mmsi_cat) |> summarise(n = n(), pings = sum(pings))

stk |> count(.loid, .glid)
### Crude filter first ---------------------------------------------------------
tmp <-
  stk |>
  mutate(
    drop =
      case_when(
        .glid %in% c("fixed", "hafro", "kvi") ~ .glid,
        .glid %in% c("navigational aid", "net") ~ .glid,
        .glid %in% c("unknown") ~ .glid,
        mmsi_cat != "vessel" ~ "mmsi",
        .default = NA),
    keep =
      case_when(
        .loid == "vid" & .glid == "vid" & loid == glid & glid %in% vid_isl ~ "vid_vid",
        .loid == "vid" & .glid == "cs" &
          loid %in% vid_isl & glid %in% isl_cs ~ "vid_cs_isl",
        .default = NA))

tmp |>
  group_by(drop, keep, .loid, .glid) |>
  reframe(n = n(), pings = sum(pings)) |>
  knitr::kable()

tmp |>
  filter(is.na(drop), is.na(keep)) |>
  filter(is.na(.loid), .glid == "cs") |>
  arrange(-pings)







# Isolate candiate vessels -----------------------------------------------------
vid_w.mmsi <-
  vessels |>
  filter(source == "ISL",
         !is.na(mmsi)) |>
  pull(vid) |>
  as.character()
mmsi_on_record <-
  c(vessels |> filter(source == "ISL", !is.na(mmsi)) |> pull(mmsi),
    arrow::open_dataset("data/astd") |> count(mmsi) |> collect() |> pull(mmsi)) |>
  unique()


stk2 <-
  stk |>
  mutate(
    keep =
      case_when(.glid %in% c("fixed", "hafro", "kvi", "navigational aid",
                             "net", "unknown") ~ FALSE,
                !is.na(mmsi) & mmsi_cat != "vessel" ~ FALSE,
                .loid == "vid" & .glid == "vid" & vid %in% vid_w.mmsi ~ TRUE,
                mmsi %in% mmsi_on_record ~ TRUE,
                .default = NA))
stk2 |>
  group_by(keep, .loid, .glid) |>
  reframe(n = n(),
          pings = sum(pings)) |>
  arrange(keep, -pings) |>
  knitr::kable()

stk2 |>
  filter(is.na(keep), .loid == "vid", .glid == "cs") |>
  knitr::kable()


  group_by(keep, .loid, .glid) |>
  reframe(n = n(),
          pings = sum(pings)) |>
  arrange(keep, -pings) |>
  knitr::kable()




## first exclude ---------------------------------------------------------------
#  What is done here is to use the exclusion approach with respect to what
#  is most likely not a vessel. In each step the part that is excluded is kept
#  separate and the remainder is used in in the next step
keep <- stk
keep |> count(.loid, .glid)
### drop fixed and things ------------------------------------------------------
drop <-
  keep |>
  filter() |>
  mutate(why = .glid)
keep <-
  keep |>
  filter(!mid %in% drop$mid)
keep |> count(.loid, .glid)
## drop non-vessel mmsi --------------------------------------------------------
drop <-
  bind_rows(drop,
            keep |>
              filter(!is.na(mmsi) & mmsi_cat != "vessel") |>
              mutate(why = "mmsi is not a vessel"))
drop |> count(why, mmsi_cat) # test
keep <-
  keep |>
  filter(!mid %in% drop$mid)
keep |> count(.loid, .glid)
## xxx -------------------------------------------------------------------------



keep <-
  stk |>
  filter(!mid %in% drop$mid)
keep |> count(.loid, .glid)


# remainder
stk_cat <-
  stk_cat |>
  filter(!mmsi %in% stk_mmsi$mmsi)




## iterative checks ------------------------------------------------------------
stk |> count(.loid, .glid)

stk |>
  filter(!is.na(cs1)) |>
  arrange(-pings) |>
  slice(1:1000) |>
  knitr::kable()
# mobileid that have lots of records but still not classifed
mid_to_check <- c(133897)


stk_trail(con) |>
  filter(mid == 113822) |>
  collect() |>
  ramb::rb_mapdeck(add_harbour = FALSE)

## match with vessel registry --------------------------------------------------






## export data -----------------------------------------------------------------

# issues -----------------------------------------------------------------------
