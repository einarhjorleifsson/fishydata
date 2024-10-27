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
  # used for classifying likely incomplete mmsi-signals
  mutate(MID_child = paste0("98", MID),
         MID_aid = paste0("99", MID))
## Call sign - flag state ------------------------------------------------------
cs.prefix <-
  nanoparquet::read_parquet("data/lookups/callsign_prefix.parquet") |>
  # critical, lot of mess with TF in stk localid and globalid
  filter(cs_prefix != "TF")

#
vessels <-
  nanoparquet::read_parquet("data/vessels/vessel-registry.parquet")
# NOTE: The ISL source contains also foreign flags
isl.vid <-
  vessels |> filter(source == "ISL", !is.na(vid)) |> pull(vid) |> as.character()
cs.vessel <-
  vessels |>
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()
cs.isl <-
  vessels |>
  filter(source == "ISL") |>
  filter(!is.na(cs)) |>
  filter(nchar(cs) %in% 4:7) |>
  pull(cs) |>
  unique()

mmsi.vessels <- vessels |> filter(!is.na(mmsi)) |> pull(mmsi) |>
  unique() |> as.character()
mmsi.isl <-
  vessels |> filter(source == "ISL", flag == "ISL", !is.na(vid), !is.na(mmsi)) |> pull(mmsi)
uid.vessel <-
  vessels |> filter(!is.na(uid)) |> pull(uid) |> unique()


# stk classification ------------------------------------------------------------
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

## Classify the stk ------------------------------------------------------------
stk <-
  stk_summary |>
  # The order matters in the case_when
  mutate(.loid =
           case_when(
             loid %in% fixed ~ "fixed",
             loid %in% kvi ~ "kvi",
             loid %in% isl.vid ~ "vid",
             numbers_only(loid) & nchar(loid) == 9  ~ "mmsi",
             loid %in% cs.vessel ~ "cs",
             str_sub(loid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(loid)) &
               !str_starts(loid, "MOB_")  ~ "cs2",
             loid %in% uid.vessel ~ "uid",
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
             glid %in% isl.vid ~ "vid",
             numbers_only(glid) & nchar(glid) == 9 ~ "mmsi",
             glid %in% cs.vessel ~ "cs",
             str_sub(glid, 1, 2) %in% cs.prefix$cs_prefix &
               !numbers_only(str_trim(glid)) &
               !str_starts(glid, "MOB_")  ~ "cs2",
             glid %in% uid.vessel ~ "uid",
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
stk |> lh()

# iterative trials ids that have not yet been classified
stk |> filter(is.na(.loid), is.na(.glid)) |> arrange(-pings) |> slice(1:10) |> knitr::kable()


# Drop data --------------------------------------------------------------------
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
keep <- stk |> filter(!drop) |> select(-drop, -mmsi_cat)

# what is yet not known --------------------------------------------------------
keep |> lh()
## Neither loid nor glid classified --------------------------------------------
keep |> filter(is.na(.loid) & is.na(.glid)) |> arrange(-pings)
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

# Create variables from loid and glid ------------------------------------------
keep |> lh()
keep2 <-
  keep |>
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


# Icelandic vessels ------------------------------------------------------------
vessels.is <-
  vessels |>
  filter(source == "ISL" & flag == "ISL")

## Here I am checking against having defined MMSI ------------------------------
# i.e. I am being very negative ------------------------------------------------
# join by vid
keep2 |>
  filter(!is.na(vid)) |>
  select(mid:t2, vid) |>
  left_join(vessels.is) |>
  # why do we not have mmsi for these:
  filter(is.na(mmsi)) |>
  arrange(desc(t2), -pings) |>
  slice(1:30) |>
  knitr::kable()
# joins by cs
keep2 |>
  filter(str_starts(cs, "TF")) |>
  select(mid:t2, cs) |>
  left_join(vessels.is |>
              # now this is not really valid but still
              filter(!is.na(mmsi)),
            by = join_by(cs)) |>
  # why do we not have mmsi for these:
  filter(is.na(mmsi)) |>
  arrange(desc(t2), -pings) |>
  slice(1:30) |>
  knitr::kable()
# joins by uid
keep2 |>
  filter(!is.na(uid)) |>
  select(mid:t2, uid) |>
  left_join(vessels.is |>
              # now this is not really valid but still
              filter(!is.na(mmsi)),
            by = join_by(uid)) |>
  # why do we not have mmsi for these:
  arrange(desc(t2), -pings) |>
  slice(1:30) |>
  knitr::kable()
