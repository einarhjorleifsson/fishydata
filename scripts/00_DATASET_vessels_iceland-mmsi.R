# Historical mmsi datasets
#
# I expect that this script should not be rerun unless some bug is found or
#   correction is needed.
#   Corrections needed, see ISSUES
# If new/additional data are obtained from fjarskiptastofa it would be
#  best have that in a separate script that starts by reading the output
#  generated here.
#
# 2024-12-12
# * More additions of mmsi
# 2024-10-30:
# * Add likely missing vessels
# * Did corrections on existing mmsi
# 2024-10-25:
# * The code consolidate older archives of data obtained over time from
#   https://www.fjarskiptastofa.is
# * Why not relay solely on tbl_mar(con, "vessel.vessel_mmsi") ?
#   * Does not contain all the data in the fjarskiptastofa file
#   * That dataset is a recent, but not quite up to date snapshot of the
#     data from https://www.fjarskiptastofa.is.
#   * It seems that the snapshots do not contain all the archive MMSI numbers.
#   * We have mmsi takeovers - newer vessel can take over an mmsi number
#   * The child mmsi (starting with 99MID [99251]) migrate between
#     mother mmsi over time
#
# OUTPUT -----------------------------------------------------------------------
# /u3/haf/fishydata/ais/vessels/data/XXXXX

library(tidyverse)
library(arrow)
library(here)
library(mar)
source("R/ramb_functions.R")
con <- connect_mar()

## Current dataset -------------------------------------------------------------
database <-
  tbl_mar(con, "vessel.vessel_mmsi") |>
  collect() |>
  rename(sknr = registration_no,
         cs = call_sign,
         mmsi = mmsi_no,
         type = obj_type) |>
  mutate(sknr = as.character(sknr),
         mmsi = as.character(mmsi)) |>
  select(mmsi, sknr, cs, note, type)
# TEST - is mmsi unique?
database |>
  group_by(mmsi) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  knitr::kable(caption = "Duplicate MMSI (expect none)")

## Current (2025-05-08) from fjarskiptastofa -----------------------------------
# Download:
if(FALSE) {
  url <- "https://www.fjarskiptastofa.is/library?itemid=7ec1e44c-5464-46e7-a9ec-eb093d0b78ef&type=xlsx"
  tmpfile <- "~/stasi/fishydata/data-raw/vessels/ISL/mmsi-iceland_fjarskiptastofa_2025-04-10.xlsx"
  download.file(url, destfile = tmpfile)
}
## Current (2024-10-22) from fjarskiptastofa -----------------------------------
# The current url (2024-10-22) is here:
#    https://www.fjarskiptastofa.is/fjarskiptastofa/fjarskiptainnvidir/skipa-og-flugfjarskipti
#     Here it is stated: "Númeraskrárnar (Excel skjöl) uppfærðar síðast: 21.08.2024"
#    https://www.fjarskiptastofa.is/page/e8a4e3e3-0460-4288-9262-6f585ed196cb
# Download:
if(FALSE) {
  url <- "https://www.fjarskiptastofa.is/library?itemid=2782f880-8bff-43b5-b1f3-c50bc9378973"
  tmpfile <- "/u3/haf/fishydata/ais/vessels/data-raw/ISL/mmsi-iceland_fjarskiptastofa_2024-08-21.xlsx"
  download.file(url, destfile = tmpfile)
}
tmpfile <- "data-raw/vessels/ISL/mmsi-iceland_fjarskiptastofa_2024-08-21.xlsx"
current <-
  readxl::read_excel(tmpfile) |>
  janitor::clean_names() |>
  select(mmsi = mmsi_nr,
         sknr,
         cs = kallm,
         everything()) |>
  rename(note = athugasemdir)
# even newer file
tmpfile2 <- "data-raw/vessels/ISL/Númer - Skip - MMSI númeraröð_07112024.xlsx"
current2 <-
  readxl::read_excel(tmpfile2) |>
  janitor::clean_names() |>
  select(mmsi = mmsi_nr,
         sknr,
         cs = kallm,
         everything()) |>
  rename(note = athugasemdir)
# more new
tmpfile3 <- "data-raw/vessels/ISL/mmsi-iceland_fjarskiptastofa_2025-04-10.xlsx"
current3 <- 
  readxl::read_excel(tmpfile3) |>
  janitor::clean_names() |>
  select(mmsi = mmsi_nr,
         sknr,
         cs = kallm,
         everything()) |>
  rename(note = athugasemdir)
# combine the two
current <- 
  bind_rows(current3, current2, current) |> 
  # because order above, below means we retain the newest
  distinct(mmsi, .keep_all = TRUE)
current <-
  current |>
  # assume this is a typo:
  mutate(mmsi = ifelse(mmsi == "261860840", "251860840", mmsi)) |>
  # keyboarding gone amok
  mutate(skip = case_when(mmsi == "251860940" ~ str_sub(skip, 1, 11),
                          .default = skip)) |> 
  # belti og axlabönd
  distinct(mmsi, .keep_all = TRUE)
# TEST - is mmsi unique?
current |>
  group_by(mmsi) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  knitr::kable(caption = "Duplicate MMSI (expect none)")
# TEST - is current in database
vessel_current <- current |> pull(mmsi)
database |>
  mutate(in.current = ifelse(mmsi %in% vessel_current,
                             TRUE, FALSE)) |>
  count(in.current)
database |>
  mutate(in.current = ifelse(mmsi %in% vessel_current,
                             TRUE, FALSE)) |>
  filter(!in.current) |>
  knitr::kable(caption = "Vessels that are no longer in the latest xlsx-sheet")
# TEST - is database in current
mmsi_in_database <- database |> pull(mmsi)
current |>
  mutate(in.database = ifelse(mmsi %in% mmsi_in_database,
                              TRUE, FALSE)) |>
  filter(!in.database) |>
  mutate(mmsi_cat = rb_mmsi_category(mmsi), .before = mmsi) |>
  arrange(mmsi_cat, sknr) |>
  knitr::kable(caption = "What is not in the oracle database")
# add the records in the database that is not in current
current <-
  bind_rows(
    current,
    database |>
      filter(!mmsi %in% mmsi_in_database)
  )
# TEST: any duplicate MMSI
current |> count(mmsi) |> filter(n > 1) |> knitr::kable(caption = "Duplicate mmsi (expect none)")

current <-
  current |>
  select(mmsi, sknr, cs, nafn = skip, note, everything())

## Historical archieves --------------------------------------------------------
# These were data obtained on an adhoc basis through time.
# The time here indicate when the table was archived (and replaced by a new
#  file), not the time the data was downloaded from fjarskiptastofa
# NOTE: NOW include "baujur" (i.e. not only mmsi starting with 251)
#       This was set as vid2 in the historical archieves
hist4 <-
  omar::mmsi_icelandic_registry(con) |> collect() |> mutate(source = 4) |>
  select(mmsi, sknr, cs, nafn = name, note = athugasemdir, source, everything())
hist3 <-
  omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20220405") |> collect() |> mutate(source = 3) |>
  select(mmsi, sknr, cs, nafn = vessel, note = athugasemdir, source, everything())
hist2 <-
  omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20201215") |> collect() |> mutate(source = 2) |>
  select(mmsi, sknr, cs, nafn = name, source, everything())
hist1 <-
  omar::tbl_mar(con, "ops$einarhj.vessel_mmsi_20190627") |> collect() |> mutate(source = 1) |>
  select(mmsi, sknr, cs, nafn = name, source, everything())
history <-
  bind_rows(hist4,
            hist3,
            hist2,
            hist1) |>
  # notes where not stored in the earliest two history files - use more recent for those
  group_by(mmsi, sknr) |>
  fill(note, .direction = "downup") |>
  ungroup() |>
  # likely typo
  mutate(mmsi = case_when(mmsi == "992151462" ~ "992511462",
                          .default = mmsi))
# TEST: MMSI not in current
history |>
  filter(str_starts(mmsi, "251")) |>
  filter(!mmsi %in% current$mmsi) |>
  arrange(source, sknr) |>
  select(mmsi:vid2) |>
  knitr::kable(caption = "Vessels no longer in current")

## Up to date dataset ----------------------------------------------------------
# Will only check for MMSI being on more than one sknr for vessel proper
#  For the rest, only use the last "registration"
d <-
  bind_rows(current |> mutate(source = 5, .after = nafn),
            history)
d_rest <-
  d |>
  filter(!str_starts(mmsi, "251")) |>
  arrange(-source) |>
  distinct(mmsi, .keep_all = TRUE) |>
  select(mmsi, sknr, cs, nafn, note, source)

d <- d |> filter(str_starts(mmsi, "251"))

## Correction -------------------------------------------------------------------
# should possibly be done more upstream
d <-
  d |>
  # These need verification from Fjarskiptastofa
  mutate(mmsi = case_when(mmsi == "251439000" ~ "251139000",  # 2411-Huginn: based on ASTD - verification pending
                          mmsi == "251216110" ~ "251216100",  # 2935-Gunna Valgeirs:   ditto
                          mmsi == "251454110" ~ "251396098",  # 1357-Níels Jónsson":   ditto
                          .default = mmsi))
## Additions -------------------------------------------------------------------
# should possibly be done more upstream
add <-
  tribble(~mmsi, ~sknr, ~cs, ~nafn, ~note,
          "251066110", "84",   "TFQZ", "Kristbjörg", "manual add - marine traffic",
          "251434110", "2855", "TFQG", "Hugur", "manual add - marine traffic",
          #"251700001", "2905",     NA, "Eskey", "manual add - marine traffic - could be vid = 2905, but that also found under mmsi = 251849340",
          "251857270", "3002", "TFGT", "Sif", "manual add - marine traffic")
d <-
  bind_rows(d,
            add)
d |>
  select(mmsi, sknr, cs, nafn, note, source) |>
  distinct(mmsi, sknr, .keep_all = TRUE) |>
  group_by(mmsi) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  arrange(mmsi, sknr) |>
  knitr::kable(caption = "Duplicate MMSI - expect zero (but are not)")

### Vessel with more than one MMSI ---------------------------------------------
d |>
  filter(!is.na(vid)) |>
  select(vid, mmsi, source) |>
  arrange(-source) |>
  distinct(vid, mmsi, .keep_all = TRUE) |>
  group_by(vid) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  arrange(vid) |>
  #mutate(mmsi = as.integer(mmsi)) |>
  left_join(history) |>
  knitr::kable(caption = "Vessels with more than one MMSI (expect none)")

#### Vessel 2988 ---------------------------------------------------------------
d |> filter(sknr == "2988") |> select(mmsi:vid)
# Comment
#  VID: 2988 (a sailing boat)
#   * MMSI: 251573110 in ASTD dataset from 2019 onwards
#   * MMSI: 251573000 not in ASTD, not in marine traffic
#           Only appeared in source = 1, the oldest sourc
#   * SUGGEST: remove vid = 2988 and mmsi = 251573000 from the dataset
d <-
  d |>
  filter(!(sknr == "2988" & mmsi == "251573000"))
#### Vessel 7326 ---------------------------------------------------------------
d |> filter(sknr == "7236") |> select(mmsi:vid)
d |>
  filter(mmsi %in% c("251857870", "251354640")) |>
  glimpse()
#  VID: 7236 (a fishing vessel)
#   * First appears in the stk and lb/ln-records in 2021
#   * MMSI: 251857870 in marine traffic, there stated built in 1990
#   * MMSI: 251354640 this number appears also under a different vessel vid = 1815
#       as seen here
#       Most likely that vid = 7236 and vid = 1815 are the same vessel judging
#       by "athugasemdir"
#   * SUGGEST: Remove vid = 7236 where mmsi = 251354640
d <-
  d |>
  filter(!(vid == 7236 & mmsi == "251354640"))

#### Check if any remaining vessels with more than one MMSI --------------------
# NOT checking baujur
d |>
  filter(!is.na(vid)) |>
  select(vid, mmsi, source) |>
  arrange(-source) |>
  distinct(vid, mmsi, .keep_all = TRUE) |>
  group_by(vid) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  arrange(vid) |>
  #mutate(mmsi = as.integer(mmsi)) |>
  left_join(history) |>
  knitr::kable(caption = "Vessels with more than one MMSI (expect none)")



### MMSI on more than one vessel -----------------------------------------------
# This can happen but not at the same time
d |>
  select(mmsi, sknr, source) |>
  distinct(mmsi, sknr, .keep_all = TRUE) |>
  group_by(mmsi) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  select(-n) |>
  spread(source, sknr) |>
  mutate(.row = 1:n(), .before = mmsi) |>
  knitr::kable(caption = "Same MMSI - different sknr")

### Set period over which an MMSI is valid for a vessel ------------------------
#  The period were partially determined by exploring the stk data interactively
#  Takeovers

mmsi_takeovers <-
  # mmsi_no: think of it like the version stuff in the DST data
  tribble(~sknr, ~mmsi, ~mmsi_t1, ~mmsi_t2, ~zombie_no,
          2450, 251146240, "2007-06-01", "2010-12-31", 1,
          7839, 251146240, "2011-01-01", "2028-12-24", 2,
          1337, 251158000, "2007-06-01", "2011-12-31", 1,
          3000, 251158000, "2012-01-01", "2028-12-24", 2,
          2299, 251172110, "2007-06-01", "2007-06-01", 1,   # CHECK, mmsi assigned to vessel but never used?
          3014, 251172110, "2007-06-02", "2028-12-24", 2,
          2154, 251175000, "2007-06-01", "2013-12-31", 1,
          3017, 251175000, "2014-01-01", "2028-12-24", 2,
          1903, 251179000, "2007-06-01", "2014-12-31", 1,
          3009, 251179000, "2015-01-01", "2028-12-24", 2,
          2288, 251201000, "2007-06-01", "2007-06-01", 1,   # CHECK, mmsi assigned to vessel but never used?
          3013, 251201000, "2007-06-02", "2028-12-24", 2,
          2605, 251206000, "2007-06-01", "2008-12-31", 1,
          3035, 251206000, "2009-01-01", "2028-12-24", 2,
          1159, 251308110, "2007-06-01", "2020-12-31", 1,
          3021, 251308110, "2021-01-01", "2028-12-24", 2,
          1773, 251345740, "2007-06-01", "2017-12-31", 1,
          7107, 251345740, "2018-01-01", "2028-12-24", 2,
          2219, 251354110, "2007-06-01", "2014-12-31", 1,
          3022, 251354110, "2015-01-01", "2028-12-24", 2,
          1930, 251374840, "2007-06-01", "2009-12-31", 1,
          3006, 251374840, "2010-01-01", "2028-12-24", 2,
          2719, 251380110, "2007-06-01", "2007-06-01", 1,  # CHECK, mmsi assigned to vessel but never used?
          7858, 251380110, "2007-06-02", "2028-12-24", 2,
          2371, 251430000, "2007-06-01", "2007-06-01", 1,  # CHECK, mmsi assigned to vessel but never used?
          2992, 251430000, "2007-06-02", "2028-12-24", 2,
          2549, 251458000, "2007-06-01", "2014-12-31", 1,
          2983, 251458000, "2015-01-01", "2028-12-24", 2,
          2600, 251463000, "2007-06-01", "2014-12-31", 1,
          3016, 251463000, "2015-01-01", "2028-12-24", 2,
          2318, 251490840, "2007-06-01", "2014-12-31", 1,
          7856, 251490840, "2015-01-01", "2028-12-24", 2,
          2395, 251520640, "2007-06-01", "2011-12-31", 1,
          6658, 251520640, "2012-01-01", "2028-12-24", 2,
          2654, 251528000, "2007-06-01", "2010-12-31", 1,
          2982, 251528000, "2011-01-01", "2028-12-24", 2,
          2702, 251532000, "2007-06-01", "2013-12-31", 1,
          3015, 251532000, "2014-01-01", "2028-12-24", 2,
          7649, 251549110, "2007-06-01", "2007-06-01", 1,   # CHECK, mmsi assigned to vessel but never used?
          9844, 251549110, "2007-06-02", "2028-12-24", 2,
          2480, 251551540, "2007-06-01", "2009-12-31", 1,
          7831, 251551540, "2010-01-01", "2028-12-24", 2,
          2608, 251764110, "2007-06-01", "2016-12-31", 1,
          7873, 251764110, "2017-01-01", "2028-12-24", 2,
          7769, 251846940, "2007-06-01", "2014-12-31", 1,
          7764, 251846940, "2015-01-01", "2028-12-24", 2) |> 
          # WRONG
          # 1351, 251079000, "2007-06-01", "2019-12-31", 1,     # added 2024-12-11
          # 3018, 251534000, "2024-01-01", "2028-12-24", 2) |>  #   ditto
  mutate(sknr = as.character(sknr),
         mmsi = as.character(mmsi),
         mmsi_t1 = ymd(mmsi_t1),
         mmsi_t2 = ymd(mmsi_t2))

# get auxillary information for the takeover vessels
nrow1 <- nrow(mmsi_takeovers)
mmsi_takeovers <-
  mmsi_takeovers |>
  left_join(d |>
              arrange(-source) |>
              select(mmsi, sknr, cs, nafn, note, source) |>
              distinct(mmsi, sknr, .keep_all = TRUE),
            by = join_by(sknr, mmsi))
if(nrow(mmsi_takeovers) != nrow1) stop("Check code")

mmsi_takeovers |>
  ggplot() +
  geom_segment(aes(x = mmsi_t1, xend = mmsi_t2,
                   y = mmsi, yend = mmsi,
                   colour = factor(zombie_no))) +
  coord_cartesian(xlim = c(ymd("2007-01-01"), ymd("2024-12-31")))

# create a valid time set for non-takeover vessels
d <-
  d |>
  filter(!mmsi %in% mmsi_takeovers$mmsi) |>
  arrange(-source) |>
  select(mmsi, sknr, cs, nafn, note, source) |>
  distinct(mmsi, sknr, .keep_all = TRUE) |>
  mutate(
    mmsi_t1 = ymd("2007-06-01"),   # The real start of STK
    mmsi_t2 = ymd("2028-12-24"),   # Maximum working life span :-)
    zombie_no = 1)
# add the takeovers
d <-
  d |>
  bind_rows(mmsi_takeovers)

### Check vessel-mmsi overlaps -------------------------------------------------
#  One way to do this is by using a self-join to check if any start-end interval
#  overlap with another
d |>
  inner_join(d, join_by(overlaps(mmsi_t1, mmsi_t2, mmsi_t1, mmsi_t2), mmsi)) |>
  filter(sknr.x != sknr.y) |>
  knitr::kable(caption = "Vessel-MMSI overlap (expect none)")
if(d |>
   inner_join(d, join_by(overlaps(mmsi_t1, mmsi_t2, mmsi_t1, mmsi_t2), mmsi)) |>
   filter(sknr.x != sknr.y) |>
   nrow() > 0) stop("We do not expect vid-mmsi overlap")

### Merge the vessel dataset with the auxillary dataset ------------------------
d_rest |> count(mmsi) |> filter(n > 1) # quick test
data <-
  bind_rows(d,
            d_rest |>
              mutate(
                mmsi_t1 = ymd("2007-06-01"),   # The real start of STK
                mmsi_t2 = ymd("2028-12-24"),   # Maximum working life span :-)
                zombie_no = 1))

## Classify MMSI ---------------------------------------------------------------
data <-
  data |>
  arrange(mmsi, sknr) |>
  mutate(mmsi_cat = rb_mmsi_category(mmsi),
         .after = mmsi) |>
  # some overwrite of default classification
  mutate(mmsi_cat =
           case_when(
             mmsi == "251991540" ~ "SAR aircraft",                  # Flugvél gæslunnar
             note == "V/prófana á fjarskiptabúnaði" ~ "testing",
             nafn %in% c("Radíómiðun hf", "Brimrún hf",
                         "Stefja ehf Ánanaustum 15") ~ "testing",
             .default = mmsi_cat))



## Extract likely date information from note -----------------------------------
data <-
  data |>
  mutate(
    note_date = str_extract(note, "\\d+\\D+\\d+\\D+\\d+"),
    note_date = dmy(note_date),
    .after = note) |>
  mutate(note_fate =
           case_when(
             str_detect(tolower(note), "brota") ~ "Niðurrif",
             str_detect(tolower(note), "niður") ~ "Niðurrif",
             str_detect(tolower(note), "seld") ~ "Seldur",
             str_detect(tolower(note), "selt") ~ "Seldur",
             str_detect(tolower(note), "afsk") ~ "Afskráður",
             str_detect(tolower(note), "afsráð") ~ "Afskráður",
             str_detect(tolower(note), "afrskráð") ~ "Afskráður",
             str_starts(tolower(note), "afmáð") ~ "Afskráður",
             str_starts(tolower(note), "teki") ~ "Úr rekstri",
             .default = NA),
         .after = note_date)

## Script demonstrating likely usage downstream --------------------------------


# The idea is then to do something like this when working with the raw data
#  One would be doing this on the raw STK data, not on the processed data
#  like is thrown up here as an example
if(FALSE) {
  tmp_vid <- mmsi_takeovers$sknr |> as.integer()
  ais <-
    open_dataset("/home/haf/einarhj/stasi/fishydata_2024-11-09/data/ais_2024-05-27") |>
    filter(vid %in% tmp_vid,
           vms == "yes",
           ref == "lhg") |>
    collect()
  ais |>
    select(vid, time, speed) |>
    left_join(mmsi_takeovers |> rename(vid = sknr) |> mutate(vid = as.integer(vid)),
              by = join_by(vid, between(time, mmsi_t1, mmsi_t2)), unmatched = "drop") |>
    filter(speed < 15) |>
    ggplot(aes(time, speed, colour = factor(zombie_no))) +
    geom_point(size = 0.1) +
    facet_wrap(~ mmsi) +
    scale_colour_brewer(palette = "Set1")
}

## SAVE ------------------------------------------------------------------------
data |>
  arrange(mmsi_cat, mmsi) |>
  nanoparquet::write_parquet("data/vessels/mmsi_iceland_archieves.parquet")

## ISSUES ----------------------------------------------------------------------

### Why is this guy here? ------------------------------------------------------
# Not icelandic MID, not on marine traffic
data |> filter(mmsi %in% c("291990101")) |> glimpse()

### Icelandic MMSI in ASTD that are not in fjarskiptastofa registry ------------
# This done at 2024-10-28
fs <-
  readxl::read_excel("data-raw/vessels/ISL/mmsi-iceland_fjarskiptastofa_2024-08-21.xlsx") |>
  janitor::clean_names()
astd <-
  open_dataset("data/ais/astd") |>
  filter(flag == "ISL") |>
  group_by(mmsi, vessel) |>
  summarise(pings = n(),
            t1 = min(time),
            t2 = max(time),
            .groups = "drop") |>
  collect()
astd_missing <-
  astd |>
  filter(pings > 100) |>
  mutate(mmsi = as.character(mmsi)) |>
  arrange(mmsi, vessel) |>
  group_by(mmsi) |>
  fill(vessel) |>
  group_by(mmsi, vessel) |>
  reframe(pings = sum(pings),
          t1 = min(t1),
          t2 = max(t2)) |>
  filter(!mmsi %in% c(fs$mmsi_nr, data$mmsi)) |>
  arrange(mmsi)

astd_missing |>
  knitr::kable(caption = "ASTD ISL vessels not in MMSI registry (more than 100 pings)")

#  Some copy-paste from marinetraffic
fil <- "data-raw/vessels/marinetraffic/mmsi_2024-10-27_web-copy.txt"
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
         vessel_class = general_vessel_type,
         vessel_type = detailed_vessel_type,
         ais_class = ais_transponder_class,
         vessels_local_time = `vessel's_local_time`)
mt

astd_missing |>
  left_join(mt)

data |> 
  filter(mmsi == "251857270")
data |> 
  filter(is.na(sknr)) |> 
  filter(mmsi_cat == "vessel") |> 
  knitr::kable()
mt |> 
  filter(!mmsi %in% data$mmsi) |> 
  filter(flag == "Iceland")
mt |> 
  filter(str_starts(name, "S"))

