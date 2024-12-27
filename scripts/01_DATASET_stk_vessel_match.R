# Classify the mobileid-localid-globalid in the stk-data
#  Focus on vessel data
#   * Assign MMSI, if possible to all Icelandic vessels
#   * Attempt to assign MMSI to foreign vessels
#   * Attempt to write the code flow so it can easily be updated
#     once new data are compiled (Not there yet)
#
#  The code includes many "tests", indended to capture possible match issues
#   as upstream as possible. The code is a result of iterative procedure where
#   overlaps (same mobileid on 2-3 vessels) are reported in the dual spreadsheet,
#   then whole process run a again, checking again for issues at each step.

# TODO: ------------------------------------------------------------------------
#   MOVE ALL MANUAL MATCHING TO THE DUAL DOCUMENT - done
# NEWS
# 202X-12-31
#
# 2024-10-24
#  * consolidation, review and addition of older matches
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
library(arrow)
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
lh_speed <- function(MID, trim = TRUE) {
  d <- 
    STK |> 
    filter(mid %in% MID) |> 
    select(mid, time, speed) |> 
    collect() |> 
    mutate(speed = ifelse(speed > 15, 15, speed))
  if(trim) {
    d <- d |> filter(time >= ymd_hms("2007-06-01 00:00:00"))
  }
  d |> 
    ggplot(aes(time, speed)) + geom_point(size = 0.1) +
    facet_wrap(~ mid, ncol = 1) + 
    scale_x_datetime(date_breaks = "2 year", date_labels = "%Y")
}
lh_overlaps <- function(d) {
  d |> 
    group_by(vid) |> 
    mutate(n = n()) |> 
    ungroup() |> 
    filter(n > 1, !is.na(vid)) |> 
    arrange(vid, d2) |> 
    group_by(vid) |> 
    mutate(dt = difftime(lead(d1), d2, units = "day"),
           dt = as.numeric(dt)) |> 
    mutate(overlap = case_when(dt < -1 ~ "yes",
                               .default = "no")) |> 
    filter(overlap == "yes") |> 
    pull(vid) |> 
    unique() ->
    vid_overlap
  d |> 
    filter(vid %in% vid_overlap) |> 
    arrange(vid, d2) |> 
    group_by(vid) |> 
    mutate(overlap = case_when(d2 > lead(d1) ~ "yes",
                               .default = "no"))
}
lh_overlaps_plot <- function(MID, stkX) {
  STK |> 
    filter(mid %in% MID) |>  # Need to check this
    select(mid, loid, glid, time, speed) |>
    mutate(speed = ifelse(speed > 15, 15, speed)) |> 
    collect() |> 
    left_join(stkX,
              by = join_by(mid == mid, loid == loid, glid == glid,
                           between(time, d1, d2))) |> 
    ggplot(aes(time, speed, colour = factor(mid))) +
    geom_point(size = 0.01) +
    facet_wrap(~ vid, ncol = 1) +
    scale_x_datetime(date_breaks = "2 year", date_labels = "%Y")
}

lh_mmsi_expectations <- function(d) {
  d |>
    filter(vid > 0) |>
    #filter(step != "duals") |>
    left_join(v_all |> select(vid, mmsi, yh1, yh2)) |>
    filter(is.na(mmsi)) |>
    arrange(desc(d2)) |>
    knitr::kable(caption = "Don't expect missing mmsi for 'recent' d2")
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
v_all <- 
  nanoparquet::read_parquet("data/vessels/vessels_iceland.parquet") |> 
  mutate(vidc = as.character(vid))
v_mmsi <-
  v_all |> 
  filter(!is.na(mmsi))

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
# suffix 2 is when mmsi not available
CS <-
  v_mmsi |>
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
CS2 <-
  v_all |>
  filter(!vid %in% 3700:4999) |> 
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
CS3 <- 
  v_all |>
  filter(vid %in% 3700:4999) |> 
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
UID <-
  v_mmsi |>
  filter(!is.na(uid)) |>
  pull(uid) |>
  unique()
UID2 <-
  v_all |>
  filter(!is.na(uid)) |>
  pull(uid) |>
  unique()
VID <-
  v_mmsi |>
  filter(source == "ISL", !is.na(vid)) |>
  pull(vid) |>
  as.character()
VID2 <-
  v_all |>
  filter(source == "ISL", !is.na(vid)) |>
  pull(vid) |>
  as.character()

lnd <- 
  read_parquet("data/landings/agf_stations.parquet") |> 
  group_by(vid) |> 
  reframe(n = n(),
          min = min(datel),
          max = max(datel))

## stk summary -----------------------------------------------------------------
STK <- 
  open_dataset("data/ais/stk-raw") |> 
  to_duckdb()
stk <- 
  STK |> 
  group_by(mid, year) |> 
  mutate(n = n()) |> 
  ungroup() |> 
  mutate(n = n()) |> 
  filter(n >= 10) |> 
  group_by(mid, loid, glid) |> 
  summarise(pings = n(),
            n_years = n_distinct(year),
            d1 = min(time, na.rm = TRUE),
            d2 = max(time, na.rm = TRUE),
            .groups = "drop") |> 
  collect() |>
  # these used when using join with between
  mutate(d1 = as_date(d1),
         d2 = as_date(d2) + ddays(1)) |>  # rather than 23:59:59 if time was used
  # this however includes the first second of next day
  arrange(mid, d1, d2) |> 
  mutate(.rid = 1:n(),
         .before = mid)

# Older matches ----------------------------------------------------------------
older <- 
  tbl_mar(con, "ops$einarhj.mobile_vid") |>
  collect() |> 
  select(mid:vid, no, t1, t2) |> 
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
# DUALS ------------------------------------------------------------------------
# Here we rely on some older work in addition to new work
#  This should NOT BE RUN AGAIN
#  Any future changes have to be done manually in:
#   data-raw/stk_mobile_fix/dual_mobileid.ods
if(FALSE) {
  duals_older <- 
    older |> 
    filter(!is.na(no)) |> 
    filter(n.mid > 1) |> 
    select(mid, loid, glid, vid, no, d1 = t1, d2 = t2)
  duals_new <- 
    tribble(~mid, ~loid, ~glid, ~d1, ~d2, ~vid, ~no,
            101092, "975",   "TFGT", "2007-06-04", "2019-12-31", 975, 1,
            101092, "975",   "TFGT", "2020-01-01", "2029-12-31", 3002, 2,
            101119, "1401",  "TFQF", "1999-11-24", "2022-03-31", 1401, 1,
            101119, "1401",  "TFQF", "2022-03-31", "2029-12-31", 2730, 2,
            108982, "2835",  "TFFL", "2012-09-07", "2018-12-24", 1173, 1,    # dubious, no MMSI
            108982, "2835",  "TFFL", "2020-01-01", "2029-12-31", 7830, 2,
            101015, "1052",  "TFRN", "1970-01-01", "2010-12-31", 1052, 1,
            101015, "1052",  "TFRN", "2020-01-01", "2029-12-31", 3021, 2,
            100135, "1838",  "TFJP", "2007-06-01", "2009-12-31", 2299, 1,  # this is a dummy
            100135, "1838",  "TFJP", "2019-01-01", "2029-12-31", 2988, 2,
            100603, "483",   "TFEG", "2008-06-26", "2014-12-31", 2722, 1,
            100603, "483",   "TFEG", "2015-01-01", "2029-12-31", 2910, 2,
            103251, "4322",  "TFDP", "2012-01-26", "2014-12-31", 1441, 1,
            103251, "4322",  "TFDP", "2015-01-01", "2029-12-31", 2948, 2
    ) #|> 
  #mutate(d1 = ymd(d1),
  #       d2 = ymd(d2))
  bind_rows(duals_older, duals_new) |> 
    arrange(mid) |> 
    left_join(v_mmsi |> select(vid, yh1, yh2)) |> 
    write_csv("data-raw/stk_mobile_fix/dual_mobileid.csv")
}

# test duals
if(FALSE) {
  duals <- 
    readODS::read_ods("data-raw/stk_mobile_fix/dual_mobileid.ods")
  STK |> 
    filter(mid %in% c(108982)) |>
    select(mid, loid, glid, time, speed) |>
    mutate(speed = ifelse(speed > 15, 15, speed)) |> 
    collect() |> 
    left_join(duals,
              by = join_by(mid == mid, loid == loid, glid == glid,
                           between(time, d1, d2))) |> 
    ggplot(aes(time, speed, colour = factor(vid))) +
    geom_point(size = 0.01) +
    facet_wrap(~ mid, ncol = 1) +
    scale_x_datetime(date_breaks = "2 year", date_labels = "%Y")
  v_mmsi |> filter(cs == "TFIS")
  stk2 |> filter(vid_older == 2870)
}

# Join duals -------------------------------------------------------------------
duals <- 
  readODS::read_ods("data-raw/stk_mobile_fix/dual_mobileid.ods") |> 
  select(mid:d2)

stk1 <-
  stk |> 
  left_join(duals |> 
              select(mid, vid, no, .d1 = d1, .d2 = d2)) |> 
  mutate(d1 = case_when(!is.na(.d1) ~ .d1,
                        .default = d1),
         d2 = case_when(!is.na(.d2) ~ .d2,
                        .default = d2)) |> 
  select(-c(.d1, .d2)) |> 
  mutate(step = case_when(!is.na(vid) ~ "duals",
                          .default = NA))


# Classification ---------------------------------------------------------------
stk2 <-
  stk1 |>
  # The order matters in the case_when
  mutate(.loid =
           case_when(
             loid %in% fixed ~ "fixed",
             loid %in% kvi ~ "kvi",
             loid %in% VID ~ "vid",
             loid %in% VID2 ~ "vid2",
             numbers_only(loid) & nchar(loid) == 9  ~ "mmsi",
             loid %in% CS ~ "cs",
             loid %in% CS2 ~ "cs2",
             loid %in% CS3 ~ "cs3",
             loid %in% UID ~ "uid",
             loid %in% UID2 ~ "uid2",
             str_sub(loid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(loid)) &
               !str_starts(loid, "MOB_")  ~ "cs4",
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
             glid %in% VID2 ~ "vid2",
             numbers_only(glid) & nchar(glid) == 9 ~ "mmsi",
             glid %in% CS ~ "cs",
             glid %in% CS2 ~ "cs2",
             glid %in% CS3 ~ "cs3",
             glid %in% UID ~ "uid",
             glid %in% UID2 ~ "uid2",
             str_sub(glid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(glid)) &
               !str_starts(glid, "MOB_")  ~ "cs4",
             numbers_only(glid) & str_sub(glid, 1, 5) %in% MID$MID_child ~ "mmsi.other",
             numbers_only(glid) & str_sub(glid, 1, 5) %in% MID$MID_aid ~ "mmsi.other",
             #numbers_only(glid) & nchar(glid) == 7 & !omar::vessel_valid_imo(glid) ~ "imo",
             .default = NA)) |> 
  mutate(type = paste0(replace_na(.loid, "NA"), "_", replace_na(.glid, "NA"))) |> 
  select(-c(.loid, .glid))

stk2 |> lh_overlaps() |> filter(vid > 0) |>  knitr::kable(caption = "Expect none")

# Join older -------------------------------------------------------------------
stk3 <- 
  stk2 |> 
  left_join(older |> filter(n.mid == 1) |> select(mid, vid_older = vid))
stk3 |> lh_overlaps() |> filter(vid > 0) |> knitr::kable(caption = "Expect none")
stk3 |> lh_mmsi_expectations()

## vid_vid ---------------------------------------------------------------------
stk4 <- 
  stk3 |> 
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "vid_vid" & loid == glid ~ as.numeric(glid),
                         .default = vid)) |> 
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "vid_vid",
                          .default = step))
stk4 |> 
  filter(type == "vid_vid") |> 
  filter(is.na(vid)) |> 
  knitr::kable(caption = "Missing vid for type vid_vid (expect none)")

# duals with overlaps
stk4 |> lh_overlaps() |> filter(vid > 0) |> knitr::kable(caption = "Expect vid 1511, 7807")
stk4 |> lh_mmsi_expectations()

## vid_cs match ----------------------------------------------------------------
stk5 <- 
  stk4 |> 
  mutate(.vid = case_when(type == "vid_cs" ~ as.numeric(loid),
                          .default = -9999),
         .cs = case_when(type == "vid_cs" ~ glid,
                         .default = "XXXX")) |> 
  left_join(v_mmsi |> select(.vid = vid, .cs = cs) |> mutate(type = "vid_cs"),
            by = join_by(.vid, .cs, type)) |> 
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "vid_cs" & vid_older == .vid & glid == .cs ~ vid_older,
                         type == "vid_cs" & is.na(vid_older) ~ .vid,
                         type == "vid_cs" & vid_older == 6643 ~ vid_older,  # single case
                         .default = vid)) |> 
  select(-c(.vid, .cs)) |> 
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "vid_cs",
                          .default = step))
stk5 |> 
  filter(type == "vid_cs") |> 
  filter(is.na(vid)) |> 
  knitr::kable(caption = "Missing vid for type vid_cs (expect none)")

stk5 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
if(FALSE) {
  lh_overlaps_plot(c(103851, 101223), stk5) # expected, vid 2718 has one mid inbetween another mid
}
stk5 |> lh_mmsi_expectations()
## uid_cs match ----------------------------------------------------------------
stk6 <- 
  stk5 |> 
  mutate(.uid = case_when(type == "uid_cs" ~ loid,
                          .default = "XXXX"),
         .cs = case_when(type == "uid_cs" ~ glid,
                         .default = "XXXX")) |> 
  left_join(v_mmsi |> select(.vid = vid, .cs = cs, .uid = uid) |> mutate(type = "uid_cs"),
            by = join_by(.cs, .uid, type)) |> 
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "uid_cs" & vid_older == .vid ~ vid_older,
                         type == "uid_cs" & !is.na(vid_older) ~ vid_older,
                         .default = vid)) |> 
  select(-c(.cs, .uid, .vid)) |> 
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "uid_cs",
                          .default = step))
stk6 |> 
  filter(type == "uid_cs") |> 
  filter(is.na(vid)) |> 
  knitr::kable(caption = "Missing vid for type uid_cs (expect none)")
stk6 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
stk6 |> lh_mmsi_expectations()

## NA_vid match ----------------------------------------------------------------
stk7 <-
  stk6 |> 
  mutate(.vid = case_when(type == "NA_vid" ~ glid,
                          .default = NA)) |> 
  left_join(v_mmsi |> select(.vid = vidc, mmsi) |> mutate(type = "NA_vid"),
            by = join_by(.vid, type)) |> 
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "NA_vid" & !is.na(mmsi) ~ as.integer(.vid),
                         .default = vid)) |> 
  select(-c(.vid, mmsi)) |> 
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "NA_vid",
                          .default = step))
stk7 |> 
  filter(type == "NA_vid") |> 
  filter(is.na(vid)) |> 
  knitr::kable(caption = "Missing vid for type NA_vid (expect none)")

stk7 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
if(FALSE) {
  lh_overlaps_plot(c(101081), stk7) # Still dubious, second period for 2067 may be a wrong allocation
}
stk7 |> lh_mmsi_expectations()

## NA_cs ------------------------------------------------------------------------
stk8 <- 
  stk7 |> 
  mutate(.cs = glid) |> 
  left_join(v_mmsi |> select(.cs = cs, .vid = vid, .uid = uid, yh1, yh2) |> mutate(type = "NA_cs")) |> 
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "NA_cs" & vid_older == .vid  ~ vid_older,
                         type == "NA_cs" & !is.na(.vid) ~ .vid,
                         .default = vid)) |> 
  select(-c(.cs, .vid, .uid, yh1, yh2)) |> 
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "NA_csts",
                          .default = step))
stk8 |> 
  filter(type == "NA_cs") |> 
  filter(is.na(vid)) |> 
  knitr::kable(caption = "Missing vid for type NA_cs (expect none)")
stk8 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
stk8 |> lh_mmsi_expectations()

## vid2_cs ---------------------------------------------------------------------
# Question if this should come next
stk9 <- 
  stk8 |> 
  mutate(.cs = glid) |> 
  left_join(v_mmsi |> 
              select(.cs = cs, .vid = vidc, .uid = uid, yh1, yh2) |> mutate(type = "vid2_cs") |> 
              # did this to not get many-to-many
              filter(yh2 > 2013)) |> 
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "vid2_cs" ~ as.numeric(.vid),
                         .default = vid)) |> 
  select(-c(.cs, .vid, .uid, yh1, yh2)) |> 
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "vid2_cs",
                          .default = step))
stk9 |> 
  filter(type == "vid2_cs") |> 
  filter(is.na(vid)) |> 
  knitr::kable()
stk9 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 1511, 2718, 7807")
stk9 |> lh_mmsi_expectations()


if(FALSE) {
  lnd |> 
    filter(!vid %in% c(stk9 |> select(vid) |> drop_na() |> pull(vid))) |> 
    arrange(desc(max)) |> 
    left_join(v_all) |> 
    knitr::kable(caption = "List of vessels in landings not in stk")
    
    # CHECK THIS
    v_all |> filter(vid == 3027)
    stk9 |> filter(vid == 1115)
    v_all |> filter(cs == "TFAU")
    v_all |> filter(uid == "RE245")
    v_all |> filter(uid == "GK011")
}

## VID_OLDER -------------------------------------------------------------------
# hail mary, step may be premature
stk10 <- 
  stk9 |> 
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         is.na(vid) & !is.na(vid_older) & pings > 100 ~ vid_older,
                         .default = vid)) |> 
  mutate(step = case_when(!is.na(vid) & is.na(step) & pings > 100 ~ "older",
                          .default = step))

stk10 |> filter(vid > 0) |> lh_overlaps() |> knitr::kable(caption = "Expect vid 396, 950, 1511, 2718, 7807")
if(FALSE) {
  lh_overlaps_plot(c(113343, 107082), stk10) # 396
  lh_overlaps_plot(c(107945, 100430), stk10) # 950
}
stk10 |> lh_mmsi_expectations()

## NA_mmsi ---------------------------------------------------------------------
v_mmsi_no_dupes <- 
  v_mmsi |> 
  arrange(mmsi, desc(mmsi_t2), desc(vid)) |> 
  group_by(mmsi) |> 
  slice(1) |> 
  ungroup()
stk11 <- 
  stk10 |> 
  mutate(.mmsi = glid) |> 
  left_join(v_mmsi_no_dupes |> select(.vid = vidc, .mmsi = mmsi, yh1, yh2) |> mutate(type = "NA_mmsi")) |> 
  mutate(vid = case_when(!is.na(vid) ~ vid,
                         type == "NA_mmsi" & pings >= 30 ~ as.numeric(.vid),
                         .default = vid)) |> 
  select(-c(.vid, .mmsi, yh1, yh2)) |> 
  mutate(step = case_when(!is.na(vid) & is.na(step) ~ "NA_mmsi",
                          .default = step))
stk11 |> 
  filter(type == "NA_mmsi") |> 
  filter(!is.na(vid)) |> 
  pull(vid) ->
  vids
stk11 |> 
  filter(vid %in% vids) |> 
  group_by(vid) |> 
  mutate(n = n()) |> 
  ungroup() |> 
  filter(n > 1) |> 
  arrange(vid, d2)
if(FALSE) {
  lh_overlaps_plot(c(120568, 100873), stk11)
  lh_overlaps_plot(c(143787, 102969), stk11)
}

# I AM HERE --------------------------------------------------------------------

## manual ----------------------------------------------------------------------
# Do this in the Libre Office Spreadsheet


lnd |> 
  filter(vid < 9900) |> 
  filter(!vid %in% c(stk11 |> select(vid) |> drop_na() |> pull(vid))) |> 
  arrange(desc(max)) |> 
  left_join(v_all) |> 
  knitr::kable(caption = "List of vessels in landings not in stk")

stk11 |> 
  filter(is.na(vid)) |> 
  count(type) |> 
  arrange(-n) |> 
  knitr::kable()
stk11 |> 
  filter(is.na(vid)) |> 
  filter(type == "vid2_vid2") |> 
  arrange(-pings)







stk10 |> 
  filter(vid > 0) |> 
  filter(pings > 10) |> 
  lh_overlaps() |> 
  knitr::kable(caption = "Expect vid 1511, 2718, 7807, 7830")
lh_overlaps_plot(c(120568, 100873), stk10)
lh_overlaps_plot(c(143787, 102969), stk10)
lh_overlaps_plot(c(106218, 100871), stk10)
lh_overlaps_plot(c(146481, 100643), stk10)
lh_overlaps_plot(c(140711, 101122), stk10)
lh_overlaps_plot(c(105694, 105294), stk10)
# seems like this merger is OK, but not fully tested

# any mmsi-vid match solely based on mmsi
stk10 |> 
  filter(step == "NA_mmsi") |> 
  arrange(-pings)
# ad hoc tests
stk10 |> filter(vid == 3003)
lh_overlaps_plot(c(101014, 140480, 141794), stk10)


# STATUS SO FAR ----------------------------------------------------------------
lnd |> 
  filter(!vid %in% c(stk10 |> select(vid) |> drop_na() |> pull(vid))) |> 
  arrange(desc(max)) |> 
  left_join(v_all) |> 
  knitr::kable(caption = "List of vessels in landings not in stk")
stk10 |> 
  mutate(has.vid = case_when(!is.na(vid) ~ "yes",
                             .default = "no")) |> 
  left_join(v_mmsi |> select(vid, mmsi) |> mutate(has.vid = "yes")) |> 
  filter(has.vid == "yes") |> 
  filter(is.na(mmsi)) |> 
  arrange(desc(d2)) |> 
  #filter(year(d2) >= 2021) |> 
  filter(vid > 0) |> 
  left_join(v_all |> select(vid, .cs = cs, .imo = imo, yh1, yh2)) |> 
  knitr::kable(capiton = "List of stk that have vid but no mmsi")

lh_speed(c(108963, 108963))
v_all |> filter(cs == "TFBZ")


# manual
stk12 <- 
  stk11 |> 
  mutate(vid = case_when(mid == 101499 ~ 2216,
                         mid == 101878 ~ 1578,
                         mid == 103600 ~ 3077,
                         .default = vid))
stk12 |> 
  filter(type != "NA_mmsi") |> 
  lh_overlaps() |> 
  knitr::kable(caption = "Expect vid 1511 and 2718, 7830 is dubious")

## vessels still with missing mmsi, but recent d2
stk12 |> 
  mutate(has.vid = case_when(!is.na(vid) ~ "yes",
                             .default = "no")) |> 
  left_join(v_mmsi |> select(vid, mmsi) |> mutate(has.vid = "yes")) |> 
  filter(has.vid == "yes") |> 
  filter(is.na(mmsi)) |> 
  arrange(desc(d2)) |> 
  filter(year(d2) >= 2021) |> 
  filter(vid > 0) |> 
  left_join(v_all |> select(vid, .cs = cs, .imo = imo, yh1, yh2)) |> 
  knitr::kable()

# the 3700:4999 vessels
lh_speed(c(102971))
v_all |> filter(cs %in% c("TFMG"))



stk11 |> filter(glid == "TFBC")
