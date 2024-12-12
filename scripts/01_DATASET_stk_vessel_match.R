# Classify the mobileid-localid-globalid in the stk-data
#  Focus on vessel data
#   * Assign MMSI to all Icelandic vessels
#   * Attempt to assign MMSI to foreign vessels
#   * Attempt to write the code flow so it can easily be updated
#     once new data are compiled (Not there yet)
#
#  For the Excel keep this in mind:
#  * Dump ordered by mobileid, because they are (mostly) created consecutively
#    in time. This way, later updating should be easier.
#  * At minimum classify the localid and globalid
#  * Make an initial guess of the vid
#  * Try to flag where mobileid belongs to two vessels (using the historcial
#    dataset?)
#  * Suggest first to do a trial run, see how one would work within LibreOffice
#    or Excel


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
#  should really add skemmtibatar
vessels <-
  nanoparquet::read_parquet("data/vessels/vessels_iceland.parquet") |> 
  # Only vessels that have mmsi
  filter(!is.na(mmsi)) |> 
  mutate(vidc = as.character(vid))

## Known (via ad hoc) non-vessel localid or globalid ---------------------------
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

## Vessel info for local- and globalid classification --------------------------
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
  filter(source == "ISL", !is.na(vid)) |>
  pull(vid) |>
  as.character()

## stk summary -----------------------------------------------------------------
stk_ids <-
  tbl(con, dbplyr::in_schema("STK", "MOBILE")) |>
  select(mid = MOBILEID, loid = LOCALID, glid = GLOBALID) |>
  collect()
stk <-
  tbl(con, dbplyr::in_schema("STK", "TRAIL")) |>
  mutate(YEAR = year(POSDATE)) |>
  group_by(MOBILEID) |>
  summarise(pings = n(),
            n_years = n_distinct(YEAR),
            d1 = min(POSDATE, na.rm = TRUE),
            d2 = max(POSDATE, na.rm = TRUE),
            .groups = "drop") |>
  collect() |>
  rename(mid = MOBILEID) |>
  mutate(d1 = as_date(d1),
         d2 = as_date(d2)) |>
  arrange(mid) |>
  full_join(stk_ids,
            by = join_by(mid)) |>
  select(mid, loid, glid, everything()) |> 
  arrange(mid) |> 
  mutate(.rid = 1:n(),
         .before = mid)

# Classification ---------------------------------------------------------------
stk2 <-
  stk |>
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
             numbers_only(loid) & str_sub(loid, 1, 5) %in% MID$MID_child ~ "mmsi.other",
             numbers_only(loid) & str_sub(loid, 1, 5) %in% MID$MID_aid ~ "mmsi.other",
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
             numbers_only(glid) & str_sub(glid, 1, 5) %in% MID$MID_child ~ "mmsi.other",
             numbers_only(glid) & str_sub(glid, 1, 5) %in% MID$MID_aid ~ "mmsi.other",
             #numbers_only(glid) & nchar(glid) == 7 & !omar::vessel_valid_imo(glid) ~ "imo",
             .default = NA)) |> 
  mutate(type = paste0(replace_na(.loid, "NA"), "_", replace_na(.glid, "NA"))) |> 
  select(-c(.loid, .glid))

stk2 |> count(type) |> knitr::kable()

# Orphan -----------------------------------------------------------------------
if(FALSE) {
  stk2 |> 
    # temporary check what mmsi type - used in the drop below
    mutate(mmsi = case_when(.loid == "mmsi" ~ loid,
                            .glid == "mmsi" ~ glid,
                            .default = NA),
           mmsi_cat = case_when(!is.na(mmsi) ~ rb_mmsi_category(mmsi),
                                .default = NA),
           mmsi_cat = case_when(!is.na(mmsi_cat) & mmsi_cat == "vessel" ~ mmsi_cat,
                                !is.na(mmsi_cat) & mmsi_cat != "vessel" ~ "mmsi_other",
                                .default = NA),
           mmsi_mid = case_when(mmsi_cat == "vessel" ~ as.numeric(str_sub(mmsi, 1, 3)),
                                .default = NA)) |>
    select(-mmsi)
}

# Older matches ----------------------------------------------------------------
older <- 
  tbl_mar(con, "ops$einarhj.mobile_vid") |>
  collect() |> 
  select(mid:vid, no) |> 
  # add loid and glid where missing
  left_join(stk |> select(mid, loid_tmp = loid, glid_tmp = glid, d1, d2),
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
## merge stk with older --------------------------------------------------------
# Note, this will generate dual mobileid because of "overtake"
stk2 <- 
  stk2 |> 
  left_join(older |> select(mid, vid_older = vid, no, t1, t2)) |> 
  #janitor::get_dupes(.rid) |> 
  mutate(d1 = case_when(!is.na(t1) & !is.na(no) ~ t1,
                        .default = d1),
         d2 = case_when(!is.na(t2) & !is.na(no) ~ t2,
                        .default = d2)) |> 
  select(-c(t1, t2))

# New match --------------------------------------------------------------------
## vid_vid match ---------------------------------------------------------------
stk2 <- 
  stk2 |> 
  mutate(.vid = case_when(type == "vid_vid" & loid == glid ~ as.numeric(glid),
                          .default = NA)) |> 
  left_join(vessels |> select(.vid = vid),
            by = join_by(.vid)) |> 
  rename(vid_new = .vid)
## vid_cs match ----------------------------------------------------------------
# NOTE: ONLY USE vessels where mmsi is defined
stk2 <- 
  stk2 |> 
  mutate(.vid = case_when(type == "vid_cs" ~ as.numeric(loid),
                          .default = -9999),
         .cs = case_when(type == "vid_cs" ~ glid,
                         .default = "XXXX")) |> 
  left_join(vessels |> select(.vid = vid, .cs = cs),
            by = join_by(.vid, .cs)) |> 
  mutate(vid_new = case_when(!is.na(vid_new) ~ vid_new,
                             .vid != -9999 ~ .vid,
                             .default = vid_new)) |> 
  select(-c(.vid, .cs))

## NA_mmsi match: Icelandic vessels only ---------------------------------------
stk2 <- 
  stk2 |> 
  mutate(.mmsi = case_when(type == "NA_mmsi" & str_sub(glid, 1, 3) == "251" ~ glid,
                           .default = "XXXX")) |> 
  left_join(vessels |> filter(!is.na(mmsi)) |> select(.vid = vid, .mmsi = mmsi),
            by = join_by(.mmsi)) |> 
  mutate(vid_new = case_when(!is.na(vid_new) ~ vid_new,
                             !is.na(.vid) ~ .vid,
                             .default = vid_new)) |> 
  select(-c(.vid, .mmsi))
## uid_cs match ----------------------------------------------------------------
stk2 <- 
  stk2 |> 
  mutate(.uid = case_when(type == "uid_cs" ~ loid,
                          .default = "XXXX"),
         .cs = case_when(type == "uid_cs" ~ glid,
                         .default = "XXXX")) |> 
  left_join(vessels |> select(.vid = vid, .cs = cs, .uid = uid) |> drop_na(),
            by = join_by(.cs, .uid)) |> 
  mutate(vid_new = case_when(!is.na(vid_new) ~ vid_new,
                             !is.na(.vid) ~ .vid,
                             .default = vid_new)) |> 
  select(-c(.cs, .uid, .vid))
## NA_vid match ----------------------------------------------------------------
stk2 <-
  stk2 |> 
  mutate(.vid = case_when(type == "NA_vid" ~ glid,
                          .default = NA)) |> 
  left_join(vessels |> select(.vid = vidc),
            by = join_by(.vid)) |> 
  mutate(vid_new = case_when(!is.na(vid_new) ~ vid_new,
                             !is.na(.vid) ~ as.numeric(.vid),
                             .default = vid_new)) |> 
  select(-c(.vid))


# I AM HERE --------------------------------------------------------------------
stk3 <- 
  stk2 |> 
  mutate(vid = case_when(!is.na(vid_older) & !is.na(vid_new) & vid_older == vid_new ~ vid_older,
                         vid_older != vid_new & !is.na(no) ~ vid_older,
                         # vid_new superseeds vid_older
                         !is.na(vid_new) ~ vid_new,
                         !is.na(vid_older) ~ vid_older,
                         .default = NA))



stk4 <- 
  stk3 |>
  mutate(
    vid = 
      case_when(is.na(vid) & type == "NA_vid" & glid %in%  vessels$vidc ~ as.numeric(glid),
                .default = vid))

# Do later
stk4 <- 
  stk4 |> 
  left_join(vessels |> 
              select(vid, mmsi, mmsi_t1, mmsi_t2),
            by = join_by(vid))
stk4 |> 
  write_parquet("data/vessels/stk_vessel_match.parquet")

# TROUBLES ---------------------------------------------------------------------
v <- read_parquet("data/vessels/vessels_iceland.parquet")

# troubles, vid with no mmsi
stk4 |> 
  filter(!is.na(vid)) |> 
  filter(is.na(mmsi)) |> 
  filter(!is.na(pings)) |> 
  arrange(-pings) |> 
  filter(!vid %in% 3700:4999) -> 
  tmp
tmp |> 
  filter(pings > 10) |> 
  arrange(d2) |> 
  knitr::kable(caption = "Vessels with no MMSI")
lnd <- 
  read_parquet("data/landings/lods_stations.parquet") |> 
  filter(between(datel, ymd("2007-06-01"), ymd("2024-12-31"))) |> 
  group_by(vid) |> 
  reframe(n = n(),
          d1 = min(datel),
          d2 = max(datel))
lnd |> 
  left_join(stk3 |> select(mid, vid)) |> 
  mutate(has.mid = !is.na(mid)) |> 
  filter(!has.mid) |> 
  arrange(-n) |> 
  # Note: Only join if vid in registry
  inner_join(vessels |> select(vid:vessel)) |>  
  arrange(vid) |> 
  knitr::kable(caption = "Landings vessels with no stk: to hunt down")

# Table: Landings vessels with no stk: to hunt down
#   |  vid|   n|d1         |d2         | mid|has.mid |  yh1|  yh2|mmsi      |cs   |imo     |uid   |vessel             |
#   |----:|---:|:----------|:----------|---:|:-------|----:|----:|:---------|:----|:-------|:-----|:------------------|
#   | 1599| 165|2010-03-16 |2015-08-06 |    |FALSE   | 1982| 2020|251292840 |     |        |BA021 |ÖNGULL             |
#   | 2730| 345|2007-06-01 |2024-12-02 |    |FALSE   | 2006| 2013|251536000 |TFQF |9167928 |VE292 |GULLBERG           |
#   | 2917|  88|2017-06-16 |2024-11-19 |    |FALSE   | 2017| 2021|251718000 |TFYY |9774642 |ÓF001 |SÓLBERG            |
#   | 3018|  14|2024-08-20 |2024-12-06 |    |FALSE   |     |     |251534000 |TFBY |9951628 |ÁR067 |SIGURBJÖRG         |
#   | 3027|   2|2024-11-26 |2024-12-04 |    |FALSE   |     |     |251212000 |TFAU |9967732 |GK011 |HULDA BJÖRNSDÓTTIR |


# HUNTDOWN ---------------------------------------------------------------------
lh_speed <- function(MID) {
  stk_trail(con) |> 
    filter(mid == MID) |> 
    select(time, speed) |> 
    collect() |> 
    mutate(speed = ifelse(speed > 15, 15, speed)) |> 
    ggplot(aes(time, speed)) + geom_point(size = 0.1)
}
# Get all vessels, not filtered by availability of mmsi
v <- read_parquet("data/vessels/vessels_iceland.parquet")

## 1599 ------------------------------------------------------------------------
#   |  vid|   n|d1         |d2         | mid|has.mid |  yh1|  yh2|mmsi      |cs   |imo     |uid   |vessel             |
#   |----:|---:|:----------|:----------|---:|:-------|----:|----:|:---------|:----|:-------|:-----|:------------------|
#   | 1599| 165|2010-03-16 |2015-08-06 |    |FALSE   | 1982| 2020|251292840 |     |        |BA021 |ÖNGULL             |
v |> filter(vid == 1599)
v |> filter(mmsi == 251292840)
v |> filter(uid == "BA021")
stk4 |> filter(vid %in% c(2658, 6462))
lh_speed(101719)
lh_speed(103022)
stk4 |> filter(loid %in% c("BA21", "BA021") | loid %in% c("BA21", "BA021"))

## 2730 ------------------------------------------------------------------------
#   |  vid|   n|d1         |d2         | mid|has.mid |  yh1|  yh2|mmsi      |cs   |imo     |uid   |vessel             |
#   |----:|---:|:----------|:----------|---:|:-------|----:|----:|:---------|:----|:-------|:-----|:------------------|
#   | 2730| 345|2007-06-01 |2024-12-02 |    |FALSE   | 2006| 2013|251536000 |TFQF |9167928 |VE292 |GULLBERG           |
v |> filter(cs == "TFQF")
stk4 |> filter(vid == 1401)
lh_speed(101119)   # A dual vessel??
stk_trail(con) |> 
  mutate(year = year(time)) |> 
  filter(mid == 101119, year %in% c(2020, 2024)) |> 
  collect() |> 
  arrange(-speed) |> 
  ggplot() + geom_point(aes(lon, lat, colour = speed), size = 0.1) + facet_wrap(~ year) +
  scale_colour_viridis_c()
stk_trail(con) |> 
  mutate(year = year(time)) |> 
  filter(mid == 101119, year %in% c(2020)) |> 
  collect() |> 
  arrange(-speed) |>
  filter(between(lat, 66, 68)) |> 
  ggplot() + geom_point(aes(lon, lat, colour = speed), size = 0.1) + facet_wrap(~ year) +
  scale_colour_viridis_c()
# so switch from a longliner to a pelagic vessel
stk_trail(con) |> 
  filter(mid == 101119, year(time) == 2022) |> 
  ggplot(aes(time, speed)) + geom_point(size = 0.01)
# the switch occurs 2022-03-31



## 2917 ------------------------------------------------------------------------
#   |  vid|   n|d1         |d2         | mid|has.mid |  yh1|  yh2|mmsi      |cs   |imo     |uid   |vessel             |
#   |----:|---:|:----------|:----------|---:|:-------|----:|----:|:---------|:----|:-------|:-----|:------------------|
#   | 2917|  88|2017-06-16 |2024-11-19 |    |FALSE   | 2017| 2021|251718000 |TFYY |9774642 |ÓF001 |SÓLBERG            |
v |> filter(cs == "TFYY")
stk4 |> filter(vid == 2061)
lh_speed(102284)
stk4 |> filter(mid == 102284)  # The newer vessel is missing
stk4 <- 
  stk4 |> 
  add_row(stk4 |> filter(mid == 102284) |> 
            mutate(vid = 2917,
                   d1 = ymd("2015-01-01"),
                   # Check what to do here
                   d2 = ymd("2024-12-31")))

## 3018 ------------------------------------------------------------------------
#   |  vid|   n|d1         |d2         | mid|has.mid |  yh1|  yh2|mmsi      |cs   |imo     |uid   |vessel             |
#   |----:|---:|:----------|:----------|---:|:-------|----:|----:|:---------|:----|:-------|:-----|:------------------|
#   | 3018|  14|2024-08-20 |2024-12-06 |    |FALSE   |     |     |251534000 |TFBY |9951628 |ÁR067 |SIGURBJÖRG         |
v |> filter(cs == "TFBY")
stk4 |> filter(vid == 1351)
lh_speed(101196)
#stk4 |> 
#  add_row(stk4 |> filter(mid == 101196) |> 
#            mutate(vid == 3018,

## 3027 ------------------------------------------------------------------------
#   |  vid|   n|d1         |d2         | mid|has.mid |  yh1|  yh2|mmsi      |cs   |imo     |uid   |vessel             |
#   |----:|---:|:----------|:----------|---:|:-------|----:|----:|:---------|:----|:-------|:-----|:------------------|
#   | 3027|   2|2024-11-26 |2024-12-04 |    |FALSE   |     |     |251212000 |TFAU |9967732 |GK011 |HULDA BJÖRNSDÓTTIR |
v |> filter(cs == "TFAU") |> select(vid:uid)
stk4 |> filter(vid == 1115)
lh_speed(103600)

# Here wrong vessel is matched
stk4 |> 
  mutate(vid = case_when(mid == 103600 ~ 1115,
                         .default = vid))
#  but then what about 1115
stk4 |> filter(glid == "RE245")
lnd |> filter(vid == 1115)   # so not in landings

