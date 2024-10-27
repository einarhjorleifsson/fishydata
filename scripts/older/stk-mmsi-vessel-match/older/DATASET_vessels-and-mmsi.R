# Preamble ---------------------------------------------------------------------
# This code is a consolidation of some older codes, some e.g. residing in the
#  data-raw directory of the omar package. It deals with two things: Vessel
#  information (largely just getting the MMSI right) and the link to mobileid
#  in the stk-data
# The drive for this version is largely:
#   * To expand the final stk time series not only to Icelandic fishing vessels
#   * Assign the MMSI to each vessel including the time period when the MMSI
#     is applicable. this is because same MMSI may is some cases have been used
#     on two different vessels
#   * Assign the stk-mobile id to each vessel
# Getting the MMSI right allows for linking vessel data to the ASTD and GFW, where
#  the primary identifier is the MMSI

# Changelog --------------------------------------------------------------------
#  * 2024-06-05
#    * Seeding, but only focusing on Icelandic fishing vessels
#    * In order to not miss any, all mobileid are classified

# 0. libraries -----------------------------------------------------------------
library(arrow)
library(tidyverse)
library(omar)
con <- connect_mar()

# 1. Vessels -------------------------------------------------------------------
## Icelandic vessels -----------------------------------------------------------
#  One should really add the SKB vessels here
#   The call sign needs revisiting
vessel_is <-
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
  dplyr::left_join(tbl_mar(con, "vessel.vessel_mmsi") |>
                     filter(obj_type == "VES") |>
                     select(.vid = vessel_id,
                            mmsi_mmsi = mmsi_no,
                            cs_mmsi = call_sign),
                   by = dplyr::join_by(.vid)) |>
  collect() |>
  dplyr::mutate(cs = ifelse(is.na(cs), cs_mmsi, cs),
                uno_c = case_when(uno > 0 ~ str_pad(uno, 3, pad = "0"),
                                  .default = NA),
                uid = case_when(!is.na(uid) & !is.na(uno_c) ~ paste0(uid, uno_c),
                                .default = NA)) |>
  dplyr::select(vid,
                vessel,
                uid,
                uno,
                cs,
                imo,
                mmsi_mmsi,
                mclass) |>
  arrange(vid)

# 2. Icelandic mmsi (archives from Einar) --------------------------------------
# see 00_DATASET_mmsi-iceland.R

## Merge MMSI to vessel table --------------------------------------------------
nr1 <- nrow(vessel_is)
vessel_is <-
  vessel_is |>
  rename(mmsi0 = mmsi) |>
  left_join(mmsi_is,
            by = join_by(vid))
nr2 <- nrow(vessel_is)
if(nr1 != nr2) stop("CHECKS: When joining vessel and mmsi, get more rows than expected")

vessel_is |> glimpse()

### Checks: Vessels in landings but no MMSI (out of place here but informative)
ln <-
  tbl_mar(con, "agf.aflagrunnur") |>
  group_by(skip_numer) |>
  summarise(min = min(londun_hefst, na.rm = TRUE),
            max = max(londun_hefst, na.rm = TRUE),
            n_landings = n_distinct(londun_id)) |>
  collect() |>
  filter(!skip_numer %in% 3700:4999) |>
  rename(vid = skip_numer) |>
  mutate(min = as_date(min),
         max = as_date(max))
mmsi_is |>
  select(vid, mmsi) |>
  mutate(in.v = "yes") |>
  full_join(ln |>
              mutate(in.ln = "yes")) |>
  arrange(-n_landings) |>
  filter(is.na(in.v)) |>
  knitr::kable(caption = "Vessels with landings but no MMSI (Needs ckecking)")

# add leisure vessels???
mmsi_is |>
  mutate(vid = as.character(vid)) |>
  bind_rows(mmsi_skb |>
              select(-vessel) |>
              mutate(mmsi_t1 = ymd("2007-06-01"),
                     mmsi_t2 = ymd("2028-12-14"),
                     mmsi_no = 1))


# 3. STK mobilid - matching to vessel ------------------------------------------

# The ultimate goal is to assign an id (mmsi or national regisry) to all
#  STK signal. For now only focus on Icelandic vessels. Foreign info is
#  here only used for elimination, to make it easier to hunt the "last" likely
#  icelandic signals down.
# Note: There seems to be an evolution in the matching over time:
#  First local and globalid contained the vessel registration number, later
#   only one of the variables contain the vessel registration number (normally
#   the global id)

## Get foreign vessel call-signs & mmsi ----------------------------------------
### Faero vessels --------------------------------------------------------------
fil <- dir("~/stasi/fishydata/data-external/vessels/færeyjar", full.names = TRUE)
FO <-
  map_df(fil, read_csv, show_col_types = FALSE) |>
  filter(!is.na(variable),
         !is.na(value)) |>
  mutate(value = ifelse(value %in% "Einki ásett", NA, value)) |>
  spread(variable, value) |>
  janitor::clean_names() |>
  select(cs = kallimerki, mmsi) |>
  mutate(cs = str_trim(cs),
         mmsi = str_trim(mmsi)) |>
  distinct()
CS_FO <-
  FO |>
  select(cs) |>
  drop_na() |>
  pull(cs) |>
  unique()
MMSI_FO <-
  FO |>
  select(mmsi) |>
  drop_na() |>
  pull(mmsi) |>
  unique()
### Norwegian vessels ----------------------------------------------------------
library(readxl)
fil <- "~/stasi/fishydata/data-external/vessels/norway_vessel-registry.xlsx"
sheet <- excel_sheets(fil)
sheet <- sheet[-c(1:2)]
res <- list()
for(i in 1:length(sheet)) {
  res[[i]] <-
    read_excel(fil, sheet[i]) |>
    janitor::clean_names()
}
names(res) <- sheet
CS_NO <-
  res |>
  bind_rows(.id = "year") |> glimpse()
  mutate(year = as.integer(year)) |>
  select(cs = radiokallesignal) |>
  drop_na() |>
  pull(cs) |>
  unique()
### EU vessel ------------------------------------------------------------------
EU <-
  read_delim("~/stasi/fishydata/data-external/vessels/eu_fleet-registry/vesselRegistryListResults.csv", delim = ";", guess_max = Inf) |>
  janitor::clean_names() |>
  select(cs = ircs, mmsi) |>
  mutate(cs = str_trim(cs),
         mmsi = str_trim(mmsi)) |>
  distinct()
CS_EU <-
  EU |>
  select(cs) |>
  drop_na() |>
  pull(cs) |>
  unique()
MMSI_EU <-
  EU |>
  select(mmsi) |>
  drop_na() |>
  pull(mmsi) |>
  unique()
### Global fishing watch -------------------------------------------------------
# Not really used
GFW <-
  read_csv("data-raw/gfw/fishing-vessels-v2.csv")


## STK mobileid: import --------------------------------------------------------
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
            #cv_lon = sd(POSLON, na.rm = TRUE) / mean(POSLON, na.rm = TRUE),
            #cv_lat = sd(POSLAT, na.rm = TRUE) / mean(POSLAT, na.rm = TRUE),
            .groups = "drop") |>
  collect() |>
  rename(mid = MOBILEID) |>
  mutate(t1 = as_date(t1),
         t2 = as_date(t2)) |>
  arrange(mid)
stk_gids <-
  tbl(con, dbplyr::in_schema("STK", "MOBILE")) |>
  select(mid = MOBILEID, loid = LOCALID, glid = GLOBALID) |>
  collect()
stk_summary <-
  stk_summary |>
  left_join(stk_gids) |>
  select(mid, loid, glid, everything())

## STK mobileid: classification ------------------------------------------------
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
    "251513130"

  )
VIDc <-
  vessel_is$vid |>
  as.character()
UID <-
  vessel_is |>
  filter(!vid %in% 3700:4999) |>
  select(uid) |>
  drop_na() |>
  pull(uid)
# Addition to the above, obtained iteratively from STK
#  the two vectors below have a localid-globalid match
UID2 <- c("ÞH383", "ÁR020", "SF055", "HF24", "ÞH061", "IS808",   "RE",   "RE",   "RE")
CS2  <- c( "TFTV",  "TFXO",  "TFJI", "TFIS",  "TFEC",  "TFTC", "TFGA", "TFTA", "TFDA")

MT <-
  read_excel("data-external/marinetraffic/STK_MARINETRAFFIC_MATCTH.xlsx", guess_max = 1e5)
mt_match <-
  MT |>
  select(globalid) |>
  drop_na() |>
  pull(globalid)

# The primary purpose here is twofold
#  1. Create a crude classification of mid-loid-glid signal
#  2. Use that as a basis for matching mid to vessel id (icelandic) and mobileid (all vessels)
mob <-
  stk_summary |>
  mutate(link = case_when(glid %in% fixed ~ "fixed",
                          loid == glid & loid %in% VIDc ~ "vid_vid",
                          loid %in% VIDc & str_starts(glid, "TF") & nchar(glid) == 4 ~ "vid_cs",
                          loid %in% UID & str_starts(glid, "TF") & nchar(glid) == 4 ~ "uid_cs",
                          str_to_lower(loid) %in% c("unknown", "unkown") & str_detect(str_to_lower(glid), "kvi") ~ "unknown_kvi",
                          str_to_lower(loid) %in% c("unknown", "unkown") & str_detect(str_to_lower(glid), "net") ~ "unknown_net",
                          str_to_lower(loid) %in% c("unknown", "unkown") & glid %in% mmsi2 ~ "unknown_mmsi2",
                          str_to_lower(loid) %in% c("unknown", "unkown") & nchar(glid) == 9 & str_starts(glid, "992") ~ "unknown_mmsi-992",
                          glid %in% CS_FO ~ "NULL_cs-fo",
                          glid %in% CS_NO ~ "NULL_cs-no",
                          glid %in% CS_EU ~ "NULL_cs-eu",
                          glid %in% MMSI_FO ~ "NULL_mmsi-fo",
                          glid %in% MMSI_EU ~ "NULL_mmsi-eu",
                          glid %in% mt_match ~ "NULL_glid-MR-manual-match",
                          str_detect(str_to_lower(glid), "hafro") | glid == "Rannsokn_" ~ "NULL_research",
                          loid %in% UID2 & glid %in% CS2 ~ "uid2_cs2",
                          str_to_lower(loid) %in% c("unknown", "unkown") & str_starts(glid, "TF") & nchar(glid) == 4 & pings > 100 ~ "unknown_TF",
                          str_to_lower(loid) %in% c("unknown", "unkown") & glid %in% VIDc ~ "unknown_vid-some-dubious",
                          .default = "rest")) |>
  arrange(-pings)

# The question is if we are missing any Icelandic vessels (for now):
mob |>
  filter(link == "rest")

# 4. Mapping vessels by localid and globalid -----------------------------------

## vid_vid ---------------------------------------------------------------------
mob_vid_vid <-
  mob |>
  filter(link == "vid_vid") |>
  mutate(vid = as.integer(loid)) |>
  filter(vid %in% as.integer(VIDc))
## vid_cs ----------------------------------------------------------------------
mob_vid_cs <-
  mob |>
  filter(link == "vid_cs") |>
  mutate(vid = as.integer(loid),
         cs = glid) |>
  # inner join to make explicit that it is the vid-cs pair that matches
  inner_join(vessel_is |> select(vid, cs),
             by = join_by(vid, cs)) |>
  select(-cs)
# CHECKS: What vid_cs link gets dropped?
tmp <-
  mob |>
  filter(link == "vid_cs") |>
  filter(!mid %in% mob_vid_cs$mid)
tmp
MID <- tmp$mid
stk_trail(con) |>
  filter(mid %in% MID) |>
  collect() |>
  filter(between(year(time), 2007, 2024)) |>
  sample_n(1e5) |>
  filter(speed < 15) |>
  ggplot(aes(time, speed)) +
  geom_point(size = 0.05) +
  facet_wrap(~ mid)

## uid_cs --------------------------------------------------------------------
mob_uid_cs <-
  mob |>
  filter(link == "uid_cs") |>
  mutate(uid = loid,
         cs = glid) |>
  inner_join(vessel_is |> select(vid, uid, cs)) |>
  select(-c(uid, cs))
# CHECKS: What uid_cs link gets dropped?
tmp <-
  mob |>
  filter(link == "uid_cs") |>
  filter(!mid %in% mob_uid_cs$mid)
tmp
MID <- tmp$mid
stk_trail(con) |>
  filter(mid %in% MID) |>
  collect() |>
  filter(between(year(time), 2007, 2024)) |>
  sample_n(1e5) |>
  filter(speed < 15) |>
  ggplot(aes(time, speed)) +
  geom_point(size = 0.05) +
  facet_wrap(~ mid)




## Mapping vessels by localid and globalid -------------------------------------
### mapping of stk.mobile ------------------------------------------------------
mob <-
  tbl_mar(con, "stk.mobile") %>%
  select(mid = mobileid,
         loid = localid,
         glid = globalid) |>
  collect(n = Inf) |>
  arrange(mid) |>
  #full_join(stk_summary) |>
  arrange(mid) |>
  mutate(lid_what = case_when(as.integer(loid) %in% vessel_is$vid ~ "vid",
                              (nchar(loid) == 4 & str_starts(loid, "TF")) |
                                loid %in% vessel_is$cs[!is.na(vessel_is$cs)] ~ "cs",
                              loid %in% vessel_is$uid[!is.na(vessel_is$uid)] ~ "uid",
                              .default = NA),
         gid_what = case_when(as.integer(glid) %in% vessel_is$vid ~ "vid",
                              (nchar(glid) == 4 & str_starts(glid, "TF")) |
                                glid %in% vessel_is$cs[!is.na(vessel_is$cs)] ~ "cs",
                              glid %in% vessel_is$uid[!is.na(vessel_is$uid)] ~ "uid",
                              .default = NA),
         link = case_when(!is.na(lid_what) & !is.na(gid_what) ~ paste0(lid_what, "-", gid_what),
                          .default = NA_character_))
VID <- vessel_is$vid
mob_vid_vid <-
  mob |>
  filter(link == "vid-vid") |>
  mutate(vid = as.integer(loid)) |>
  filter(vid %in% VID)
mob_vid_cs <-
  mob |>
  filter(link == "vid-cs") |>
  mutate(vid = as.integer(loid),
         cs = glid) |>
  inner_join(vid |> select(vid, cs)) |>
  select(-cs)
mob_uid_cs <-
  mob |>
  filter(link == "uid-cs") |>
  mutate(uid = loid,
         cs = glid) |>
  inner_join(vid |> select(vid, uid, cs)) |>
  select(-c(uid, cs))

### mapping of stk.mobile_skip_v -----------------------------------------------
mob_mobile_skip_v <-
  tbl_mar(con, "stk.mobile_skip_v") %>%
  select(mid = mobileid,
         loid = localid,
         glid = globalid,
         vid = skip_nr) |>
  collect(n = Inf) |>
  filter(!vid %in% c(0, 9999)) |>
  filter(loid != vid) |>
  mutate(vid = as.integer(vid),
         link = "mobile_skip_v") |>
  arrange(mid)
### Merge the mapping ----------------------------------------------------------
mid.match.so.far <-
  c(mob_vid_vid$mid, mob_vid_cs$mid, mob_uid_cs$mid)
mob_final <-
  bind_rows(mob |> filter(!mid %in% mid.match.so.far),
            mob_vid_vid,
            mob_vid_cs,
            mob_uid_cs) |>
  mutate(midvid = ifelse(!is.na(vid), paste0(as.character(as.integer(mid)), "-", vid), NA)) |>
  arrange(mid)
mob_final <-
  mob_final |>
  bind_rows(mob_mobile_skip_v |>
              mutate(midvid = ifelse(!is.na(vid), paste0(as.character(as.integer(mid)), "-", vid), NA)) |>
              filter(!midvid %in% mob_final$midvid[!is.na(mob_final$midvid)])) |>
  arrange(mid, desc(link)) |>
  select(-midvid)
## Add auxillary informations --------------------------------------------------
lnd_last <-
  omar::ln_catch(con) |>
  filter(catch > 0) |>
  filter(vid > 0) |>
  group_by(vid) |>
  filter(date == max(date, na.rm = TRUE)) |>
  ungroup() |>
  select(vid, datel = date) |>
  distinct() |>
  collect(n = Inf) |>
  mutate(datel = as_date(datel))
MOBILE_VID <-
  mob_final |>
  left_join(stk_summary) |>
  left_join(lnd_last) |>
  select(mid:glid, vid, pings, n_years, datel, link, everything()) |>
  arrange(mid, vid, desc(link)) |>
  #  vessels still not in registry
  mutate(vid = case_when(mid == 146524 & is.na(vid) ~ 3038,
                         mid == 145278 & is.na(vid) ~ 3007,
                         mid == 132918 & is.na(vid) ~ 2969,
                         mid == 121166 & is.na(vid) ~ 2906,
                         mid == 121682 & is.na(vid) ~ 2871,
                         mid == 109624 & is.na(vid) ~ 2809,
                         mid == 141610 & is.na(vid) ~ 2983,
                         mid == 137136 & is.na(vid) ~ 7839,
                         mid == 137501 & is.na(vid) ~ 7837,
                         mid == 121845 & is.na(vid) ~ 7788,
                         mid == 118744 & is.na(vid) ~ 7787,
                         mid == 109644 & is.na(vid) ~ 7763,
                         mid == 124115 & is.na(vid) ~ 7744,
                         mid == 102216 & is.na(vid) ~ 6717,
                         mid == 103015 & is.na(vid) ~ 2702, # Ghandi, but only for years 2010-2014
                         #   taken care of below
                         mid == 101083 & is.na(vid) ~ 2654, # Háberg, but only for years 2009-2011
                         #   taken care of below
                         mid == 101115 & is.na(vid) ~ 1807, # Birtingur, loid is vid but glidi is cs as TMP_TFDP
                         mid == 102965 & is.na(vid) ~ 1337, # Skafti
                         mid == 102869 & vid == 6339 ~ 6155,


                         .default = vid))
# Mobileid takeover ------------------------------------------------------------
# NOTE: datel (last landing date) not included here
#  The "no" = 1 refers to the automatically derived match that is being
#   split up. This matters in the step below: "replace.vid" where the
#   records where no = 1 is replace by the split record.
manual <-
  tribble(~vid, ~mid, ~T1, ~T2, ~no,
          100, 100619, "2001-01-01", "2014-03-12", 1,
          2930, 100619, "2016-01-01", "2029-12-31", 2,

          124, 101548, "2001-01-01", "2009-01-01", 2,
          2929, 101548, "2016-01-01", "2029-12-31", 1,

          183, 101106, "2001-01-01", "2013-12-31", 2,
          2883, 101106, "2014-01-01", "2029-12-31", 1,

          219, 100056, "2001-01-01", "2014-12-31", 1,
          7749, 100056, "2020-01-01", "2029-12-31", 2,

          220, 101099, "2001-01-01", "2014-12-31", 1,   # here we have two matching
          2978, 101099, "2021-01-01", "2029-12-31", 1,   #  "vid-cs" and "mobile_skip_v"
          # Hence both matches get removed
          239, 100171, "2001-01-01", "2015-12-31", 1,
          3014, 100171, "2021-01-01", "2029-12-31", 2,

          243, 101030, "2001-01-01", "2008-12-31", 2,
          2952, 101030, "2019-01-01", "2029-12-31", 1,

          256, 101075, "2001-01-01", "2018-12-31", 2,
          2944, 101075, "2019-01-01", "2029-12-31", 1,

          733, 100340, "2001-01-01", "2013-12-31", 1,
          2938, 100340, "2014-01-01", "2029-12-31", 2,

          962, 103067, "2001-01-01", "2017-06-13", 2, # check these dates
          2936, 103067, "2017-06-14", "2029-12-31", 1,

          968, 101148, "2001-01-01", "2020-12-31", 1,
          3017, 101148, "2021-01-01", "2029-12-31", 2,

          971, 101223, "2001-01-01", "2015-12-31", 1,
          2718, 101223, "2016-01-01", "2029-12-31", 2,

          975, 101092, "2001-01-01", "2018-12-31", 1,
          NA, 101092, "2019-01-01", "2029-12-31", 2,

          1010, 101408, "2001-01-01", "2020-12-01", 1,
          2991, 101408, "2021-01-01", "2029-12-31", 2,

          1013, 101205, "2001-01-01", "2008-12-31", 1,
          1453, 101205, "2015-01-01", "2029-12-31", 1,

          1014, 100343, "2001-01-01", "2018-12-31", 1,
          2992, 100343, "2019-01-01", "2029-12-31", 2,

          1031, 101086, "2001-01-01", "2015-01-01", 1,
          2955, 101086, "2016-01-01", "2029-12-31", 2,

          1060, 101545, "2001-01-01", "2010-12-31", 1,
          2894, 101545, "2017-01-01", "2029-12-31", 1,

          1100, 102555, "2001-01-01", "2008-12-31", 2,
          2822, 102555, "2009-01-01", "2029-12-31", 1,

          1204, 100652, "2001-01-01", "2015-12-31", 1,
          3013, 100652, "2021-01-01", "2029-12-31", 2,

          1236, 100579, "2001-01-01", "2016-12-31", 1,
          7183, 100579, "2020-01-01", "2029-12-31", 2,  # Dubious match, not catch reported

          1244, 100716, "2001-01-01", "2010-12-31", 1,
          2999, 100716, "2021-01-01", "2029-12-31", 2,

          1270, 101079, "2001-01-01", "2017-12-31", 1,
          NA, 101079, "2021-01-01", "2029-12-31", 2,  # This is a fishing vessel (dredge??)

          1272, 102100, "2001-01-01", "2021-01-13", 1,
          2995, 102100, "2021-11-20", "2029-12-31", 2,

          1275, 101069, "2001-01-01", "2016-06-01", 2,
          1850, 101069, "2016-06-02", "2029-12-31", 1,

          1279, 101142, "2001-01-01", "2015-12-31", 1,
          3004, 101142, "2016-01-01", "2029-12-31", 2,

          1291, 101072, "2001-01-01", "2008-12-31", 1,
          NA, 101072, "2019-01-01", "2029-12-31", 2,  # útlendingur, cs skráning vitlaus

          1395, 101227, "2001-01-01", "2019-12-31", 1,
          3899, 101227, "2020-01-01", "2029-12-31", 2, # kalbakur flutningaskip?

          1401, 101119, "2001-01-01", "2022-04-30", 1,
          2730, 101119, "2022-05-01", "2029-12-31", 2, # vantar 2007-2013

          1612, 101065, "2001-01-01", "2012-12-31", 2,
          2888, 101065, "2014-01-01", "2029-12-31", 1,

          1562, 100109, "2001-01-01", "2008-12-31", 2,  # Jón á Hofi
          1890, 100109, "2009-01-01", "2029-12-31", 1,  # as dual mobileid

          1622, 100120, "2001-01-01", "2018-12-31", 1,
          2997, 100120, "2019-01-01", "2029-12-31", 2,

          1639, 100163, "2001-01-01", "2014-12-31", 1,
          2967, 100163, "2019-01-01", "2029-12-31", 2,

          1674, 101144, "2001-01-01", "2020-12-31", 1,
          3022, 101144, "2021-01-01", "2029-12-31", 2,

          2061, 102284, "2001-01-01", "2008-01-01", 2,
          2917, 102284, "2017-01-01", "2029-12-31", 1,

          2020, 101074, "2001-01-01", "2018-12-31", 1,
          3016, 101074, "2021-01-01", "2029-12-31", 2,

          2067, 101081, "2001-01-01", "2012-12-31", 1,
          NA, 101081, "2013-01-01", "2029-12-31", 2,

          2154, 101161, "2001-01-01", "2013-12-31", 2,
          2861, 101161, "2018-01-01", "2029-12-31", 1,

          2212, 101084, "2001-01-01", "2008-12-31", 2,
          2903, 101084, "2015-01-01", "2021-12-31", 1,
          2212, 101084, "2022-01-01", "2029-12-31", 2,

          2345, 101104, "2001-01-01", "2017-12-31", 1,
          3035, 101104, "2022-01-01", "2029-12-31", 2,

          2395, 100907, "2001-01-01", "2011-12-31", 1,
          NA, 100907, "2016-01-01", "2029-12-31", 2,

          2410, 101402, "2001-01-01", "2019-12-31", 2,
          2982, 101402, "2020-01-01", "2029-12-31", 1,

          2500, 103173, "2016-10-01", "2029-12-31", 1,  # check these corrections
          2549, 103173, "2001-01-01", "2014-01-01", 2,  # carefully

          2642, 102515, "2001-01-01", "2018-12-31", 1,
          3030, 102515, "2022-01-01", "2029-12-31", 2,

          2645, 102967, "2001-01-01", "2016-05-31", 1,
          NA, 102967, "2015-06-01", "2029-12-31", 2,

          2750, 103872, "2001-01-01", "2017-12-31", 1,
          3015, 103872, "2021-12-31", "2029-12-31", 2,

          2772, 104362, "2001-01-01", "2019-12-31", 1,
          3000, 104362, "2020-01-01", "2029-12-31", 2,

          2702, 103015, "2010-01-01", "2014-12-31", 1,    # Ghandi, see above
          NA, 103015, "2019-01-01", "2029-12-31", 2,

          2654, 101083, "2001-01-01", "2010-12-31", 1,    # Háberg, see above
          NA, 101083, "2011-01-01", "2029-12-31", 2     # This is a longliner
  )

# check again 1109, ...
# vessels with problem stk
# 1136  missing stk in the start of the series

# This is to be used downstream like:
MID <- manual$mid

mid_takeovers <-
  stk_trail(con) |>
  filter(mid %in% MID) |>
  collect()
mid_takeovers |>
  filter(speed < 15) |>
  filter(between(year(time), 2007, 2024)) |>
  sample_n(1e5) |>
  left_join(manual |> select(-vid) |> mutate(T1 = ymd(T1), T2 = ymd(T2)),
            by = join_by(mid, between(time, T1, T2))) |>
  ggplot(aes(time, speed, colour = factor(no))) +
  geom_point(size = 0.1) +
  facet_wrap(~ mid)

## I am here

MOBILE_VID <-
  MOBILE_VID |>
  mutate(datel = as.character(datel),
         t1_org = as.character(t1_org),
         t2_org = as.character(t2_org),
         T1 = as.character(T1),
         T2 = as.character(T2))

replace.vid <- manual |> filter(no == 1) |> pull(vid)

MOBILE_VID <-
  bind_rows(MOBILE_VID |> filter(!vid %in% replace.vid),
            manual |> mutate(link = "manual"))

MOBILE_VID |> glimpse()
