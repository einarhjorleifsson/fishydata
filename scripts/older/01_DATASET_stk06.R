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
  nanoparquet::read_parquet("data/lookups/maritime_identification_digits.parquet") |>
  # used for classifying likely incomplete mmsi-signals
  mutate(MID_child = paste0("98", MID),
         MID_aid = paste0("99", MID))
## Call sign prefix - flag state
cs.prefix <-
  nanoparquet::read_parquet("data/lookups/callsign_prefix.parquet") |>
  # critical, lot of mess with TF in stk localid and globalid
  filter(cs_prefix != "TF")
## vessel registry
vessels <-
  nanoparquet::read_parquet("data/vessels/vessel-registry.parquet")


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

## Non-vessel localid or globalid ----------------------------------------------
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
             str_sub(loid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(loid)) &
               !str_starts(loid, "MOB_")  ~ "cs2",
             loid %in% UID ~ "uid",
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
             str_sub(glid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(glid)) &
               !str_starts(glid, "MOB_")  ~ "cs2",
             glid %in% UID ~ "uid",
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
stk |> filter(is.na(.loid), is.na(.glid)) |> arrange(-pings) |> slice(1:20) |> knitr::kable()

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


# MATCH ------------------------------------------------------------------------
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
  select(vid, mmsi, cs, uid, imo, vessel, mmsi_t1:note_fate)

keep.is <-
  keep |>
  filter(!is.na(vid) | !is.na(mmsi) | !is.na(cs) | !is.na(uid))
keep.is |> lh()

match.vid.vid <-
  keep.is |>
  filter(.loid == "vid", .glid == "vid") |>
  select(mid:.glid, vid) |>
  left_join(vessels.is) |>
  mutate(match = "vid.vid", .before = mid)
keep.is <- keep.is |> filter(!mid %in% match.vid.vid$mid)
keep.is |> lh()

match.vid.cs <-
  keep.is |>
  filter(.loid == "vid", .glid == "cs") |>
  select(mid:.glid, vid, cs) |>
  left_join(vessels.is) |>
  mutate(match = "vid.cs", .before = mid)
keep.is <- keep.is |> filter(!mid %in% match.vid.cs$mid)
keep.is |> lh()

match.uid.cs <-
  keep.is |>
  filter(.loid == "uid", .glid == "cs") |>
  select(mid:.glid, uid, cs) |>
  left_join(vessels.is) |>
  mutate(match = "uid.cs", .before = mid)
keep.is <- keep.is |> filter(!mid %in% match.uid.cs$mid)
keep.is |> lh()

match.NA.mmsi <-
  keep.is |>
  filter(is.na(.loid), .glid == "mmsi") |>
  select(mid:.glid, mmsi) |>
  left_join(vessels.is) |>
  mutate(match = "NA.mmsi", .before = mid)
keep.is <- keep.is |> filter(!mid %in% match.NA.mmsi$mid)
keep.is |> lh()


# OLDER BELOW ------------------------------------------------------------------






# ______________________________________________________________________________
# TEST: mid not yet classified
keep |> filter(is.na(.loid) & is.na(.glid)) |> arrange(-pings)
# for now suggest put these aside - not many icelandic vessels missing
# here is an initial attempt at filling in the blanks
#  there is actually no point in
tribble(
  ~mid, ~loid, ~glid, ~note, ~mmsi, ~vid,
  133897, "unkown", "-", "FRO - likely a buoy", NA, NA,
  100881, "0", "0A", "Likely a ferry - Baldur?", NA, NA,
  127245, "unkown", "MBDZ9", "Aquaculture service boat", 232008557, NA,
  101763, "9999", "9999", "An Icelandic longliner", NA, NA,
  100512, "2951X", "2951X", "An Icelandic jigger, possibly 2951", NA, 2951
)
unknowns <-
  keep |>
  filter(is.na(.loid) & is.na(.glid)) |>
  arrange(-pings) |>
  slice(1:999) |>
  pull(mid)
stk_trail(con) |>
  filter(mid %in% unknowns) |>
  collect() |>
  mutate(speed = ifelse(speed > 15, 15, speed)) |>
  rb_mapdeck(radius = 100, add_harbour = FALSE)
# TEST end _____________________________________________________________________

# Create variables from loid and glid ------------------------------------------
## vid-vid match ---------------------------------------------------------------
vessels.is <-
  vessels |>
  filter(source == "ISL" & flag == "ISL")
keep.vid.vid <-
  keep |>
  filter(.loid == "vid", .glid == "vid") |>
  filter(!is.na(vid)) |>
  #filter(pings > 10) |>
  select(mid:t2, vid) |>
  left_join(vessels.is) |>
  mutate(by = "vid", .before = mid) |>
  arrange(desc(t2))
# ______________________________________________________________________________
# TEST: Recent pings in stk but no MMSI are suspect
keep.vid.vid |>
  arrange(desc(t2)) |>
  filter(is.na(mmsi))
MID <- 101360
stk_trail(con) |>
  filter(mid %in% MID) |>
  collect() |>
  mutate(speed = ifelse(speed > 15, 15, speed)) |>
  rb_mapdeck(radius = 100, add_harbour = FALSE)
# TEST end _____________________________________________________________________
drop.vid.vid <-
  keep.vid.vid |>
  filter(pings < 400 & is.na(mmsi))
keep.vid.vid <-
  keep.vid.vid |>
  filter(!mid %in% drop.vid.vid$mid)
# TODO: Get the mmsi for these vessels
keep.vid.vid |>
  filter(is.na(mmsi))
## vid-cs match ----------------------------------------------------------------
keep.vid.cs <-
  keep |>
  filter(.loid == "vid", .glid == "cs") |>
  filter(!is.na(vid)) |>
  #filter(pings > 10) |>
  select(mid:t2, vid, cs_stk = cs) |>
  left_join(vessels.is) |>
  mutate(by = "vid", .before = mid)
keep.vid.cs.with.mmsi <-
  keep.vid.cs |>
  filter(!is.na(mmsi))

keep.vid.cs.without.mmsi <-
  keep.vid.cs |>
  filter(!mid %in% keep.vid.cs.with.mmsi$mid) |>
  select(-c(vid, mmsi:note_date)) |>
  rename(cs = cs_stk)

keep.vid.cs.without.mmsi |>
  left_join(vessels.is |> filter(!is.na(mmsi)) |> select(vid, cs, mmsi)) |>
  view()




## Here I am checking against having defined MMSI ------------------------------
# i.e. I am being very negative ------------------------------------------------
# join by vid

keep_vid |>
  # why do we not have mmsi for these:
  filter(is.na(mmsi)) |>
  arrange(desc(t2), -pings) |>
  slice(1:200) |>
  view()
stk_trail(con) |>
  filter(mid %in% 101673) |>
  collect() |>
  mutate(speed = ifelse(speed > 15, 15, speed)) |>
  rb_mapdeck(radius = 100, add_harbour = FALSE)

# joins by cs
keep_cs <-
  keep2 |>
  filter(str_starts(cs, "TF")) |>
  select(mid:t2, cs) |>
  left_join(vessels.is |>
              # now this is not really valid but still
              filter(!is.na(mmsi)),
            by = join_by(cs)) |>
  mutate(by = "cs", .before = mid)
keep_cs |>
  # why do we not have mmsi for these:
  filter(is.na(mmsi)) |>
  arrange(desc(t2), -pings) |>
  slice(1:30) |>
  knitr::kable()
# joins by uid
keep_uid <-
  keep2 |>
  filter(!is.na(uid)) |>
  select(mid:t2, uid) |>
  left_join(vessels.is |>
              # now this is not really valid but still
              filter(!is.na(mmsi)),
            by = join_by(uid))  |>
  mutate(by = "uid", .before = mid)

keep_uid |>
  # why do we not have mmsi for these:
  arrange(desc(t2), -pings) |>
  slice(1:30) |>
  knitr::kable()

# joins by mmsi
keep_mmsi <-
  keep2 |>
  filter(!is.na(mmsi)) |>
  filter(str_starts(mmsi, "251")) |>
  select(mid:t2, mmsi) |>
  left_join(vessels.is |>
              # now this is not really valid but still
              filter(!is.na(mmsi)),
            by = join_by(mmsi)) |>
  mutate(by = "mmsi", .before = mid)

keep_mmsi |>
  # why do we not have mmsi for these:
  arrange(desc(t2), -pings) |>
  slice(1:500) |>
  view()

### bind the stuff
bind_rows(keep_vid, keep_cs, keep_uid, keep_mmsi) |>
  group_by(mid) |>
  mutate(n.mid = n(),
         n.vid = n_distinct(vid, na.rm = TRUE)) |>
  ungroup() |>
  filter(n.mid > 1,
         n.vid > 1) |>
  arrange(mid) |>
  #slice(1:200) |>
  view()

stk_trail(con) |>
  filter(mid %in% 100013) |>
  collect() |>
  mutate(speed = ifelse(speed > 15, 15, speed)) |>
  ggplot(aes(time, speed)) +
  geom_point(size = 0.2)



## History explorations ---------------------------------------------------------
# seems like history of call sign is not in the data
history <- tbl_mar(con, "vessel.vessel_identification_hist") |> collect()
history |>
  select(vessel_iden_hist_id:home_port) |>
  group_by(vessel_id) |>
  summarise(n.cs = n_distinct(call_sign, na.rm = TRUE)) |>
  ungroup() |>
  filter(n.cs > 1)
vessel <-
  tbl_mar(con, "vessel.vessel") |>
  select(.id = vessel_id, vid = registration_no, vessel = name, imo = imo_no, status) |>
  collect() |>
  arrange(vid)
hist <-
  tbl_mar(con, "vessel.vessel") |> glimpse()
  select(.id = vessel_id,
         vid = registration_no) |>
  left_join(tbl_mar(con, "vessel.vessel_identification_hist") |>
              select(.id = vessel_id,
                     .hid = vessel_iden_hist_id,
                     t1 = valid_from,
                     t2 = valid_to,
                     uch = region_acronym,
                     uno = region_no,
                     cs = call_sign,
                     hb = home_port)) |>
  collect() |>
  arrange(vid, .hid) |>
  mutate(t1 = as_date(t1),
         t2 = as_date(t2))


## May be of use ----------------------------------------------------------------
read_csv("~/prj2/vms/data-dumps/2019-03_from-jrc/vesselRecap-2019-03-25.csv")
v <-
  readxl::read_excel("~/stasi/hreidar/data-raw/HREIDAR_Islensk_skip.xlsx", guess_max = Inf) |>
  janitor::clean_names()


fil <- here("data-raw/vessels/marinetraffic/mmsi_2024-10-27_web-copy.txt")
mt <-
  read_delim(fil,
             col_names = c("var", "val"), comment = "#",
             delim = "\t") |>
  mutate(id = ifelse(var == "Name", 1, 0),
         id = cumsum(id),
         var = tolower(var),
         var = str_replace_all(var, " ", "_"),
         val = str_trim(val),
         val = ifelse(val == "-", NA, val)) |>
  spread(var, val) |>
  select(id,
         name,
         mmsi,
         imo,
         cs = call_sign,
         flag,
         mobileid,
         vessel_class = general_vessel_type,
         vessel_type = detailed_vessel_type,
         ais_class = ais_transponder_class,
         vessels_local_time = `vessel's_local_time`,
         date_of_retrieval)
