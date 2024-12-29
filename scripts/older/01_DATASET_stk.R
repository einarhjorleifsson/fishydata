# Classify the mobileid-localid-globalid in the stk-data
#  Focus on vessel data
#   * Assign MMSI to all Icelandic vessels
#   * Attempt to assign MMSI to foreign vessels
#   * Attempt to write the code flow so it can easily be updated
#     once new data are compiled (Not there yet)
# TODO: ------------------------------------------------------------------------
#      ...
# NEWS
# 202X-MM-DD
#
# 2024-10-24
#  * consolidation, review and addition of older code
# INPUT
#    oracle stk.trail
#    oracle stk.mobile
#    data/auxillary/maritime_identification_digits.parquet
#    data/vessels/vessels_iceland.parquet
#    older matches of mobileid and vessel registration number
#     see: ~/R/Pakkar2/omar/data-raw/00_SETUP_mobile-vid-match.R
#          last updated 2024-02-12
#          ops$einarhj.mobile_vid
#    
# OUTPUT
# data-raw/stk-mmsi-vessel-match/******.parquet

library(tidyverse)
library(nanoparquet)
options(knitr.kable.NA = '')
library(sf)
library(mapdeck)
set_token("pk.eyJ1IjoiZmlzaHZpY2UiLCJhIjoiY2p0MXQ5dThpMDZqeDQ5bHM0MGx4dHI5cyJ9.Fed_z7mv_TgTWDRjiavU3A")
library(mar)
source("R/ramb_functions.R")
con <- connect_mar()

# a stk summary helper function
lh <- function(d) {
  d |>
    group_by(.loid, .glid) |>
    reframe(n = n(), pings = sum(pings)) |>
    arrange(-n) |>
    mutate(cn = round(cumsum(n / sum(n)) * 100, 2),
           .after = n) |>
    mutate(pp = round(pings / sum(pings) * 100, 2)) |>
    knitr::kable()
}

# Data -------------------------------------------------------------------------

## Various auxillary informations ----------------------------------------------
## Maritime identification digits
MID <-
  nanoparquet::read_parquet("data/auxillary/maritime_identification_digits.parquet") |>
  # used for classifying likely incomplete mmsi-signals
  mutate(MID_child = paste0("98", MID),
         MID_aid = paste0("99", MID))
## Call sign prefix - flag state
cs.prefix <-
  nanoparquet::read_parquet("data/auxillary/callsign_prefix.parquet") |>
  # critical, lot of mess with TF in stk localid and globalid
  filter(cs_prefix != "TF")
## vessel registry
vessels <-
  nanoparquet::read_parquet("data/vessels/vessels_iceland.parquet")
vessels_mmsi <- 
  vessels |>
  filter(!is.na(mmsi))


## stk summary -----------------------------------------------------------------
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
rm(stk_gids)

# Older matches ----------------------------------------------------------------
older <- 
  tbl_mar(con, "ops$einarhj.mobile_vid") |>
  collect() |> 
  select(mid:vid, no) |> 
  # add loid and glid where missing
  left_join(stk_summary |> select(mid, loid_tmp = loid, glid_tmp = glid, t1, t2),
            by = join_by(mid)) |> 
  mutate(loid = ifelse(is.na(loid), loid_tmp, loid),
         glid = ifelse(is.na(glid), glid_tmp, glid)) |> 
  select(-c(loid_tmp, glid_tmp)) |> 
  arrange(mid) |> 
  group_by(mid) |> 
  mutate(n.mid = n()) |> 
  ungroup()
## Houston, we have a problem
older |> 
  filter(n.mid > 1) |> 
  knitr::kable()
# The problem:
#  In the earlier code we hadd some manual matches and then some
#   records that aimed at splitting mobileid on 2+ vessel if that
#   seemed to be the case. But here we have some indication that
#   * some "splits" did not have t1 and t2 specified
#   * some of the records are pure duplicates
#  What is done here is to save the data where the n.mid > 1 and
#  then to a manual corrections in libre office. Once done, data
#  is then read back in and merged with the data where n.mid = 1
if(FALSE) {
  older |> 
  filter(n.mid > 1) |> 
  write_csv("data-raw/stk_mobile_fix/older_duplicates.csv")
}
older.keep <- older |> filter(n.mid == 1)
older.fixed <- 
  readODS::read_ods("data-raw/stk_mobile_fix/older_duplicates.ods", na = "NA") |> 
  filter(keep == 1) |> 
  select(-c(keep, comments, n.mid))
older <- 
  bind_rows(older.keep |> select(-n.mid),
            older.fixed)

# Non-vessel localid or globalid ----------------------------------------------
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


# Classify the stk -------------------------------------------------------------
CS <-
  vessels |>
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
UID <-
  vessels |>
  filter(!is.na(uid)) |>
  pull(uid) |>
  unique()
VID <-
  vessels |>
  # NOTE: only vid's that have mmsi
  filter(!is.na(mmsi)) |> 
  filter(source == "ISL", !is.na(vid)) |>
  pull(vid) |>
  as.character()

stk <-
  stk_summary |>
  # The order matters in the case_when
  mutate(.loid =
           case_when(
             loid %in% fixed ~ "fixed",
             loid %in% kvi ~ "kvi",
             loid %in% VID ~ "vid",
             numbers_only(loid) & nchar(loid) == 9  ~ "mmsi",
             loid %in% CS ~ "cs",
             loid %in% UID ~ "uid",
             str_sub(loid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(loid)) &
               !str_starts(loid, "MOB_")  ~ "cs2",
             numbers_only(loid) & str_sub(loid, 1, 5) %in% MID$MID_child ~ "mmsi_other",
             numbers_only(loid) & str_sub(loid, 1, 5) %in% MID$MID_aid ~ "mmsi_other",
             .default = NA)) |>
  mutate(.glid =
           case_when(
             glid %in% fixed ~ "fixed",
             glid %in% kvi ~ "kvi",
             glid %in% hafro ~ "hafro",
             glid %in% unknown_glid ~ "unknown",
             str_detect(tolower(glid), "_net") ~ "net",
             glid %in% net_glid ~ "net",
             glid %in% VID ~ "vid",
             numbers_only(glid) & nchar(glid) == 9 ~ "mmsi",
             glid %in% CS ~ "cs",
             glid %in% UID ~ "uid",
             str_sub(glid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(glid)) &
               !str_starts(glid, "MOB_")  ~ "cs2",
             numbers_only(glid) & str_sub(glid, 1, 5) %in% MID$MID_child ~ "mmsi_other",
             numbers_only(glid) & str_sub(glid, 1, 5) %in% MID$MID_aid ~ "mmsi_other",
             .default = NA)) |>
  # temporary check what mmsi type - used in the drop below
  mutate(mmsi = case_when(.loid == "mmsi" ~ loid,
                          .glid == "mmsi" ~ glid,
                          .default = NA),
         mmsi_cat = case_when(!is.na(mmsi) ~ rb_mmsi_category(mmsi),
                              .default = NA),
         mmsi_cat = case_when(!is.na(mmsi_cat) & mmsi_cat == "vessel" ~ mmsi_cat,
                              !is.na(mmsi_cat) & mmsi_cat != "vessel" ~ "mmsi_other",
                              .default = NA)) |>
  select(-mmsi)
rm(CS, UID, VID, fixed, hafro, kvi, net_glid, unknown_glid)

stk |> lh()

# iterative trials ids that have not yet been classified
stk |> 
  filter(is.na(.loid), is.na(.glid)) |> 
  arrange(-pings) |> 
  slice(1:30) |> 
  knitr::kable(caption = "Unclassified IDs (first 30 by number of pings")

# Split the data ---------------------------------------------------------------
# The data is split up depending on if they are "stationary" and non-vessel type
#  of signal or if they are potential canditate for a proper vessel
stk <-
  stk |>
  mutate(
    drop =
      case_when(.loid == "fixed" & .glid == "fixed" ~ TRUE,
                .glid %in% c("fixed", "hafro", "kvi", "net", "unknown", "mmsi_other") ~ TRUE,
                mmsi_cat == "mmsi_other" ~ TRUE,
                .loid == "mmsi_other" ~ TRUE,
                .glid == "mssi_other" ~ TRUE,
                .default = FALSE))

drop <- stk |> filter(drop) |> select(-mmsi_cat)
keep <-
  stk |> filter(!drop) |>
  select(-drop, -mmsi_cat) |>
  mutate(vid =
           case_when(
             .glid == "vid" ~ glid,
             .loid == "vid" ~ loid,
             .default = NA),
         vid = as.integer(vid),
         mmsi =
           case_when(
             .glid == "mmsi" ~ glid,
             .loid == "mmsi" ~ loid,
             .default = NA),
         cs =
           case_when(
             .glid %in% c("cs", "cs2") ~ glid,
             .loid %in% c("cs", "cs2") ~ loid,
             .default = NA),
         uid =
           case_when(
             .glid == "uid" ~ glid,
             .loid == "uid" ~ loid,
             .default = NA),
  )

## Export ----------------------------------------------------------------------
#match |> nanoparquet::write_parquet("data/stk-isl-vessel-match.parquet")

# MATCH. Icelandic registered vessels only -------------------------------------
# Only create variables vid, cs and uid if
#   * vessel is icelandic
#   * vessel has mmsi
CS <-
  vessels |>
  filter(flag == "ISL", source == "ISL", !is.na(mmsi)) |>
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
UID <-
  vessels |>
  filter(flag == "ISL", source == "ISL", !is.na(mmsi)) |>
  filter(!is.na(uid)) |>
  pull(uid) |>
  unique()
VID <-
  vessels |>
  filter(flag == "ISL", source == "ISL", !is.na(mmsi)) |>
  filter(!is.na(vid)) |>
  pull(vid) |>
  as.character()
MMSI <-
  vessels |>
  filter(flag == "ISL", source == "ISL", !is.na(mmsi)) |>
  filter(!is.na(vid)) |>
  pull(mmsi) |>
  as.character()

keep <-
  stk |>
  filter(!drop) |>
  select(-drop, -mmsi_cat) |>
  mutate(vid =
           case_when(
             .glid == "vid" & glid %in% VID ~ glid,
             .loid == "vid" & loid %in% VID ~ loid,
             .default = NA),
         vid = as.integer(vid),
         mmsi =
           case_when(
             .glid == "mmsi" & glid %in% MMSI ~ glid,
             .loid == "mmsi" & loid %in% MMSI ~ loid,
             .default = NA),
         cs =
           case_when(
             .glid %in% c("cs", "cs2") & glid %in% CS ~ glid,
             .loid %in% c("cs", "cs2") & loid %in% CS ~ loid,
             .default = NA),
         uid =
           case_when(
             .glid == "uid" & glid %in% UID ~ glid,
             .loid == "uid" & loid %in% UID ~ loid,
             .default = NA),
  )

vessels.is <-
  vessels |>
  filter(!is.na(vid), !is.na(mmsi), source == "ISL", flag == "ISL") |>
  select(vid, yh1, yh2, mmsi, cs, uid, imo, vessel, mmsi_t1:note_fate)

keep.is <-
  keep |>
  filter(!is.na(vid) | !is.na(mmsi) | !is.na(cs) | !is.na(uid)) |>
  filter(pings > 144)  # pings at minimum equivalent 1 day (assuming dt is 10 minutes)
keep.is |> lh()

## Match vid-vid ---------------------------------------------------------------
match.vid.vid <-
  keep.is |>
  filter(.loid == "vid", .glid == "vid") |>
  select(mid:.glid, vid) |>
  inner_join(vessels.is) |>
  mutate(match = "vid.vid", .before = mid)
keep.is <- keep.is |> filter(!mid %in% match.vid.vid$mid)

## match vid-cs ----------------------------------------------------------------
keep.is |> lh()
match.vid.cs <-
  keep.is |>
  filter(.loid == "vid", .glid == "cs") |>
  select(mid:.glid, vid, cs) |>
  inner_join(vessels.is |> filter(!is.na(cs))) |>
  mutate(match = "vid.cs", .before = mid)
keep.is <- keep.is |> filter(!mid %in% match.vid.cs$mid)

## match uid-cs ----------------------------------------------------------------
keep.is |> lh()
match.uid.cs <-
  keep.is |>
  filter(.loid == "uid", .glid == "cs") |>
  select(mid:.glid, uid, cs) |>
  inner_join(vessels.is |> filter(!is.na(uid), !is.na(cs))) |>
  mutate(match = "uid.cs", .before = mid)
keep.is <- keep.is |> filter(!mid %in% match.uid.cs$mid)

## match NA-mmsi ---------------------------------------------------------------
keep.is |> lh()
match.NA.mmsi <-
  keep.is |>
  filter(is.na(.loid), .glid == "mmsi") |>
  select(mid:.glid, mmsi) |>
  inner_join(vessels.is) |>
  mutate(match = "NA.mmsi", .before = mid)
keep.is <- keep.is |> filter(!mid %in% match.NA.mmsi$mid)

## match NA-vid ----------------------------------------------------------------
keep.is |> lh()
# Things are getting more dubious
match.NA.vid <-
  keep.is |>
  filter(is.na(.loid), .glid == "vid") |>
  select(mid:.glid, vid) |>
  inner_join(vessels.is,
             by = join_by(vid))
keep.is <- keep.is |> filter(!mid %in% match.NA.vid$mid)

## match NA-cs -----------------------------------------------------------------
keep.is |> lh()
match.NA.cs <-
  keep.is |>
  filter(is.na(.loid), .glid == "cs") |>
  filter(!is.na(cs)) |>
  select(mid:.glid, cs) |>
  inner_join(vessels.is |> filter(!is.na(cs)),
             by = join_by(cs))
keep.is <- keep.is |> filter(!mid %in% match.NA.cs$mid)

match <-
  bind_rows(
    match.vid.vid |> mutate(match = "vid.vid", .before = mid),
    match.vid.cs  |> mutate(match = "vid.cs", .before = mid),
    match.uid.cs  |> mutate(match = "uid.cs", .before = mid),
    match.NA.mmsi |> mutate(match = "NA.mmsi", .before = mid),
    match.NA.vid  |> mutate(match = "NA.vid", .before = mid),
    match.NA.cs   |> mutate(match = "NA.cs", .before = mid)
  )

keep.is.store <- keep.is
keep.is |> lh()

# these are the remainders of glid-cs, loid can be different cats
#  Note here we first exclude vid already matched
match.XX.cs <-
  keep.is |>
  filter(.glid == "cs") |>
  filter(!is.na(cs)) |>
  select(mid:.glid, cs) |>
  inner_join(vessels.is |> filter(!is.na(cs)) |> filter(!vid %in% match$vid),
             by = join_by(cs))
keep.is <- keep.is |> filter(!mid %in% match.XX.cs$mid)

match <-
  bind_rows(
    match,
    match.XX.cs   |> mutate(match = "XX.cs", .before = mid)
  )


# The remainder ----------------------------------------------------------------
keep.is |> lh()
## Try manual approach
keep.is |> arrange(t2) |> knitr::kable()
keep.is |>
  pull(mid) ->
  MID
trail <-
  stk_trail(con) |>
  filter(mid %in% MID) |>
  collect()
trail |>
  mutate(speed = ifelse(speed > 15, 15, speed)) |>
  ggplot(aes(time, speed)) +
  geom_point(size = 0.2, alpha = 0.1) +
  facet_wrap(~ mid)
