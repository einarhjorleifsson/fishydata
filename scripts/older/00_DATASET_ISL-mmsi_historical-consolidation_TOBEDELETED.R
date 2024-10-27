# Historical mmsi datasets
# 2024-10-25:
# * The code consolidate older archives of data obtained over time from
#   https://www.fjarskiptastofa.is
#
# OUTPUT -----------------------------------------------------------------------
# ....

library(tidyverse)
library(arrow)
library(here)
library(mar)
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
  knitr::kable(caption = "Duplicate MMSI - expect zero")

## Current (2024-10-22) from fjarskiptastofa -----------------------------------
# The current url (2024-10-22) is here:
#    https://www.fjarskiptastofa.is/page/e8a4e3e3-0460-4288-9262-6f585ed196cb
# Download:
if(FALSE) {
  url <- "https://www.fjarskiptastofa.is/library?itemid=2782f880-8bff-43b5-b1f3-c50bc9378973"
  tmpfile <- here("vessels/data-raw/ISL/mmsi-iceland_fjarskiptastofa_2024-10-22.xlsx")
  download.file(url, destfile = tmpfile)
}
tmpfile <- here("vessels/data-raw/ISL/mmsi-iceland_fjarskiptastofa_2024-10-22.xlsx")
current <-
  readxl::read_excel(tmpfile) |>
  janitor::clean_names() |>
  select(mmsi = mmsi_nr,
         sknr,
         cs = kallm,
         everything()) |>
  rename(note = athugasemdir)
current <-
  current |>
  # assume this is a typo:
  mutate(mmsi = ifelse(mmsi == "261860840", "251860840", mmsi))
# TEST - is mmsi unique?
current |>
  group_by(mmsi) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  knitr::kable(caption = "Duplicate MMSI - expect zero")
# TEST - is current in database
vessel_current <- current |> pull(mmsi)
database |>
  mutate(in.current = ifelse(mmsi %in% vessel_current,
                             TRUE, FALSE)) |>
  count(in.current)
# TEST - is database in current
mmsi_in_database <- database |> pull(mmsi)
current |>
  mutate(in.database = ifelse(mmsi %in% mmsi_in_database,
                              TRUE, FALSE)) |>
  count(in.database)

# add the records in the database that is not in current
current <-
  bind_rows(
    current,
    database |>
      filter(!mmsi %in% mmsi_in_database)
  )
# TEST: any duplicate MMSI
current |> count(mmsi) |> filter(n > 1)
# TEST: any types not supposed to be here:
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
  ungroup()

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

d |>
  select(mmsi, sknr, cs, nafn, note, source) |>
  distinct(mmsi, sknr, .keep_all = TRUE) |>
  group_by(mmsi) |>
  mutate(n = n()) |>
  ungroup() |>
  filter(n > 1) |>
  arrange(mmsi, sknr) |>
  knitr::kable(caption = "Duplicate MMSI - expect zero")

### Vessel with more than one MMSI ---------------------------------------------

#### Vessel 2988 ---------------------------------------------------------------
d |> filter(sknr == "2988") |> select(mmsi:vid)
# Comment
#  VID: 2988 (a sailing boat)
#   * MMSI: 251573110 in ASTD dataset from 2019 onwards
#   * MMSI: 251573000 not in ASTD, not in marine traffic
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
  arrange(mmsi) |>
  filter(!mmsi %in% c("291990101", "992151462")) |>
  mutate(mmsi_class =
           case_when(
             str_starts(mmsi, "00251") ~ "coast station",
             str_starts(mmsi, "111251") ~ "SAR",
             mmsi == "251991540" ~ "SAR",                  # Flugvél gæslunnar
             str_starts(mmsi, "251") & !is.na(sknr) ~ "vessel",
             str_starts(mmsi, "98251") ~ "child vessel",
             str_starts(mmsi, "99251") ~ "aids",
             note == "V/prófana á fjarskiptabúnaði" ~ "testing",
             nafn %in% c("Radíómiðun hf", "Brimrún hf",
                         "Stefja ehf Ánanaustum 15") ~ "testing",
             .default = "óflokkað"))

## Extract likely date information from note -----------------------------------
data <-
  data |>
  mutate(
    date_note = str_extract(note, "\\d+\\D+\\d+\\D+\\d+"),
    date_note = dmy(date_note),
    .after = note) |>
  mutate(fate_note =
           case_when(
             str_detect(tolower(note), "brota") ~ "niðurrif",
             str_detect(tolower(note), "niður") ~ "niðurrif",
             str_detect(tolower(note), "seld") ~ "Seldur",
             str_detect(tolower(note), "selt") ~ "Seldur",
             str_detect(tolower(note), "afsk") ~ "Afskráður",
             str_detect(tolower(note), "afsráð") ~ "Afskráður",
             str_detect(tolower(note), "afrskráð") ~ "Afskráður",
             str_starts(tolower(note), "afmáð") ~ "Afskráður",
             str_starts(tolower(note), "teki") ~ "Úr rekstri",
             .default = NA),
         .after = date_note)

## Script demonstrating likely usage downstream --------------------------------


# The idea is then to do something like this when working with the raw data
#  One would be doing this on the raw STK data, not on the processed data
#  like is thrown up here as an example
if(FALSE) {
  tmp_vid <- mmsi_takeovers$sknr |> as.integer()
  ais <-
    open_dataset("/home/haf/einarhj/stasi/fishydata/data/ais_2024-05-27") |>
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
data |> nanoparquet::write_parquet("setups/data-raw/mmsi-iceland.parquet")

## ISSUES ----------------------------------------------------------------------

### Icelandic MMSI in ASTD that are not in mmsi-registry compiled above
astd <-
  nanoparquet::read_parquet(here::here("setups/data-raw/vessels-asdt.parquet"))
is <-
  nanoparquet::read_parquet(here::here("setups/data-raw/mmsi-iceland.parquet")) |>
  filter(mmsi_class == "vessel") |>
  mutate(mmsi = as.integer(mmsi))
is |> filter(is.na(sknr))
astd |>
  filter(flag == "ISL") |>
  left_join(is) |>
  filter(is.na(sknr)) |>
  arrange(-pings) |>
  slice(1:200) |>
  select(mmsi, vessel, pings, t1, t2) |>
  slice(1:20)
library(patchwork)

lh <- function(q, MMSI) {
  tmp <-
    q %>%
    filter(mmsi == MMSI) %>%
    collect()
  if(nrow(tmp) > 1e5) {
    tmp <-
      tmp |>
      sample_n(1e5)
  } else {

  }
  p1 <-
    tmp |>
    filter(between(lon, -30, 0)) |>
    ggplot(aes(lon, lat)) + geom_point(size = 0.5, alpha = 0.2)+
    coord_quickmap()
  p2 <-
    tmp |> filter(between(speed, 0.25, 20)) |> ggplot() + geom_histogram(aes(speed))
  p3 <-
    tmp |> filter(speed < 20) |> ggplot(aes(time, speed)) + geom_point(size = .5)
  return(p2 + p3 + p1 + plot_layout(ncol = 2))
}

q <-  open_dataset("astd")

astd |>
  filter(flag == "ISL") |>
  left_join(is) |>
  filter(is.na(sknr)) |>
  arrange(-pings) |>
  slice(1:200) |>
  select(mmsi, vessel, pings, t1, t2) |>
  slice(1:20)

q |> lh(251010000)



