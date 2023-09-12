---
  title: "Matching ais and landings"
echo: true
warning: false
message: false
code-fold: true
---
  
  WORK IN PROGRESS


## Preamble

The objective here is to match ais-trails and landings based on time. Both data source have some record of time, albeit at a different resolution and accuracy.


Landings are normally reported as date of landing while in the ais time is reported at arrival to port. One could of course convert the landing date to time, but that would be time at midnight (yyyy-mm-dd 00:00:00). Here an appoximation of the time of landings is derived based on the time of arrival to port in the ais data.

## The approach:

* Use the same landing / harbour site identifier for both data sources, here a 3 letter "standard" acronym.
* Inititally allocate approximate date-time of landings to the landings data by:
  * For all records where ais-habour arrival "date" and ais-harbour id match with those of landings: allocate time of landing as the time record in the ais-data plus 5 minutes.
* Issues: We may get multiple matches by date-harbour, for now just use the first match record. The multiple maches may indicate double landings on the site or consequitve landings in different harbours. Should be explored later.
* For records where no date-harbour match found, add 1 minute to the landings date as the assumed landing date-time. This occurence will come up in cases when arrival time is at least a day before the actual reported landings time.
* Bind the two datasources and arrange by date-time and data source - thus ais records preceed landings records
* Create unique identifier based on the patterns of trips endings with landings. 
* In the ideal world, where all trips result in landings one would have a consequive pattern of "al, al, al, ...", the identifier for these two events then being 1, 2, 3, ...
* In the real world we would also have "aal", "aaal", aaaal", ....
* Also possibly a pattern that does not end with a landings event, e.g. "a", "aa", "aaa". This would typically occur at the end of the time-chunk.

## The assumptions:

* Landing site of vessel is accurately reported in the landings database - we can say that that is very likely
* Landing date is accurately reported in the landings database - this may be less likely
* Accuracy of the upstream code used to match vessel id and ais-data

## Codes

### Upstream code

The upstream code (not shown here) is basically:

* [matching of the vessel registry to the ais mobileid](https://github.com/einarhjorleifsson/omar/blob/main/data-raw/00_SETUP_mobileid-vid-match.R)

```{r}
# A little peek
omar::tbl_mar(omar::connect_mar(), "ops$einarhj.mobile_vid") |> dplyr::glimpse()
```

* [Creation of ais-trails](https://github.com/einarhjorleifsson/AIS_TRAIL/blob/main/rscripts/01_stk-trails.R) - includes:
    * finding points in harbour
    * creating trips from ais pings based on harbour-to-harbour
    * algorithm defining "whacky points.

The end result is the ais data (a peek below) with added variables:
  
  * vessel identification (skipaskrárnúmer), labelled "vid"
* trip identification, labelled ".cid"
* point classification, aka if e.g. "whacky point"

### The match

See the description of the approach above

```{r}
library(tidyverse)
library(omar)
library(patchwork)
con <- connect_mar()

YEAR <- 2017 # Year to process

gid_agf <- 
  tbl_mar(con, "agf.aflagrunnur_v") |> 
  select(starts_with("veidarfaeri")) |> 
  distinct() |> 
  collect() |> 
  select(gid = veidarfaeri,
         veiðarfæri = veidarfaeri_heiti)
gid_agf |> glimpse()
hid_abbreviations <- 
  read_csv("~/stasi/gis/AIS_TRAIL/data-raw/stk_harbours.csv") |> 
  select(starts_with("hafnarnumer"), hid_std) |> 
  distinct() |> 
  arrange(hafnarnumer_id) |> 
  select(hid = hafnarnumer,
         hid_abb = hid_std) |> 
  filter(!is.na(hid))
hid_abbreviations |> glimpse()
# Get the vessels that have trails - will only work on those downstream
fil <- dir("~/stasi/gis/AIS_TRAIL/trails", full.names = TRUE)
fil <- fil[str_detect(fil, paste0("y", YEAR))]
n <- nchar(fil[1])
VIDS_now <- str_sub(fil, n - 7, n-4) |> as.integer()

lnd <- 
  omar::ln_agf(con) |> 
  filter(year(date) %in% YEAR) |> 
  collect(n = Inf) |> 
  filter(!between(vid, 3700, 4999)) |>   # only icelandic vessels
  filter(vid > 0, vid != 9999) |> 
  filter(wt > 0) |>   # check this, wt if only ungutted weight
  mutate(date = as_date(date)) |> 
  group_by(vid, date, hid) |> 
  summarise(n.landings = n_distinct(.id),
            .id = min(.id),
            gid = min(gid),
            catch = sum(wt, na.rm = TRUE) / 1e3,
            .groups = "drop") |> 
  left_join(hid_abbreviations) |> 
  select(-hid) |> 
  rename(hid = hid_abb) |> 
  # only vessels with trails
  filter(vid %in% VIDS_now)
lnd |> glimpse()
trail <- 
  map(fil, read_rds) |> 
  bind_rows() |> 
  # landings data for Njarðvík are registiered as Keflavík in the landings database
  mutate(hid_dep = case_when(hid_dep == "NJA" ~ "KEF",
                             .default = hid_dep),
         hid_arr = case_when(hid_arr == "NJA" ~ "KEF",
                             .default = hid_arr))
trail |> glimpse()
trip <- 
  trail |> 
  filter(v %in% c("end_location", "not", "short")) |> 
  arrange(vid, time) |> 
  mutate(dd = traipse::track_distance(lon, lat)) |> 
  group_by(vid, .cid) |> 
  reframe(pings = n(),
          T1 = min(time),
          T2 = max(time),
          dt = round(as.numeric(difftime(T2, T1, units = "hours")), 2),
          dd = round(sum(dd, na.rm = TRUE) / 1852, 2),
          hid1 = hid_dep[1],
          hid2 = hid_arr[1],
          lon1 = lon[1],            # departure coordinates
          lat1 = lat[1],
          lon2 = lon[time == T2],   # arrival coordinates
          lat2 = lat[time == T2],
          .groups = "drop") |> 
  # use only these trips downstream
  #  this causes trouble when settings are very close to harbour
  mutate(use = case_when(.cid > 0 & pings > 5 & dt >= 1 & dd > 1 ~ TRUE,
                         .default = FALSE))
trip |> glimpse()
# Create an approximate time of landings ---------------------------------------
#  Idea here is:
#   1. Get the time of entering harbour when there is date-match between lnd and
#       ais.
#      - Add five minutes to that time as an approximation for landing time
#      - In cases landings in not on the same date as entering harbour assume
#        landings occur just after midnight
# 

## Some issues -----------------------------------------------------------------
# we are adding some rows (~ 1800+. ~3.5% of landings) here, multiple matches
#   quo vadis?
lnd_tmp <- 
  lnd |> 
  left_join(trip |> 
              filter(use) |> 
              mutate(date = as_date(T2)) |> 
              rename(hid = hid2))
# reasons are most likely additional trips witin a date

## lets move forward and just take the first left_join match -------------------
lnd <- 
  lnd |> 
  left_join(trip |>
              filter(use) |> 
              mutate(date = as_date(T2)) |> 
              rename(hid = hid2) |> 
              select(vid, date, hid, T2)) |> 
  group_by(.id) |> 
  slice(1) |> 
  ungroup() |> 
  arrange(vid, date) |> 
  mutate(time = if_else(is.na(T2), 
                        ymd_hms(paste0(as.character(date), " 00:01:00")),
                        T2 + dminutes(5),
                        NA))

## Match -----------------------------------------------------------------------

match <- 
  trip |>
  filter(use,
         vid %in% unique(lnd$vid)) |> 
  mutate(date = as_date(T2)) |> 
  select(vid, .cid, date, time = T2, hid1, hid2) |>
  # s - source, a - ais
  mutate(s = "a") |> 
  bind_rows(lnd |> 
              select(vid, .lid = .id, date, time, hid, gid) |> 
              # l - landings
              mutate(s = "l")) |> 
  arrange(vid, date, time, s) |> 
  mutate(hid2 = case_when(is.na(hid2) & !is.na(hid) & s == "l" ~ hid,
                          .default = hid2)) |> 
  mutate(.id = ifelse(!is.na(.cid), .cid, .lid)) |> 
  select(-c(.cid, .lid, date, hid)) |> 
  group_by(vid) |> 
  mutate(ll = case_when(s == "l"  ~ TRUE,
                        .default = FALSE),
         g = cumsum(ll),
         g = ifelse(s == "l", g - 1, g)) |> 
  ungroup() |> 
  select(-ll) |> 
  group_by(vid, g) |> 
  # call this p (not s) standing for patterns
  mutate(seq = paste(s, collapse = '')) |> 
  ungroup()
```

## Results

### A peek at the principal datatables

```{r}
trail |> glimpse()
lnd |> glimpse()
match |> glimpse()
```

### Number of landings not matched with a preceeding trip

```{r}
match |> 
  filter(s == "l") |> 
  summarise(total = n(),
            missing = sum(seq == "l")) |> 
  mutate(proportion = round(missing / total * 100, 2)) |> 
  gather(Variable, Value) |> 
  knitr::kable(caption = "Number of landings, number and proportion of landings with missing trip (ais)")
```

So we have ~2% of landings that are preceeded with an earlier landing, not a trip (ais - signal).

By gear we have:
  
  ```{r}
match |> 
  filter(s == "l") |> 
  group_by(gid) |> 
  summarise(total = n(),
            missing = sum(seq == "l")) |> 
  group_by(gid) |> 
  mutate(proportion = round(missing / total * 100, 2)) |> 
  ungroup() |> 
  left_join(gid_agf) |> 
  select(gid, veiðarfæri, everything()) |> 
  knitr::kable()
```

We observe that the highest reporting of landings with no preceeding (missing) trips is in handfæri and sjóstöng. This is kind of expected. The most likely reason may be that we cut off trips with few pings, short duration or distance in the code above, something to check further. There may of course be myriad of other causes.

### Time difference between arrival and landings

```{r}
m <- 
  match |> 
  group_by(vid, g) |> 
  mutate(dt = case_when(lag(s) == "a" & s == "l" ~ as.numeric(difftime(time, lag(time), units = "days")),
                        .default = NA)) |> 
  ungroup()
m |>
  mutate(dt = ifelse(dt > 10, 10, dt),
         dt = floor(dt)) |> 
  count(dt) |> 
  drop_na() |> 
  mutate(Percent = n / sum(n) * 100 |> round(1),
         cPercent = cumsum(Percent),
         Percent = round(Percent, 2),
         cPercent = round(Percent, 2)) |> 
  arrange(dt) |> 
  rename(Days = dt) |> 
  knitr::kable(caption = "Time difference [days] between arrival to landings")
```

So in most cases we have a reported landings within the same date as arrival to harbour.

### A summary of the patterns

#### Patterns that end with a landing

```{r}
match |> 
  select(vid, g, seq) |> 
  distinct() |> 
  mutate(landings = ifelse(str_detect(seq, "l"),
                           TRUE, 
                           FALSE),
         
         seq = case_when(str_ends(seq, "l") & str_starts(seq, "a") ~ paste0("a", str_pad(nchar(seq) - 1, 3, pad = "0"), "_l"),
                         str_ends(seq, "a") & str_starts(seq, "a") ~ paste0("a", nchar(seq)),
                         .default = seq)) |> 
  filter(landings) |> 
  count(seq) |> 
  mutate(p = n / sum(n) * 100,
         pc = cumsum(p),
         p = round(p, 2),
         pc = cumsum(p)) |>
  rename(Pattern = seq, Number = n, Percent = p, cPercent = pc) |> 
  knitr::kable(caption = "Event sequence. a stands for ais and l for landings. Numerical values after a is the number of trips preceeding a landing event (l). l alone stands for landings that were preceeded by another landing.")
```

In ~83% of cases the sequence of patterns is "al", that is a trip is followed by a landing event. In ~11% of cases we have two trips followed by a landing event ("aal") and ~3% where landings is preceeded by thee trips ("aaal").

The above patterns are kind of expected. However it is expected that in some of the trips we actually may have a fishing event but no landings. For static gear that is kind of expected but for mobile gear not. Would be of interest to classify each trip as if a fishing event occurs or not.

There are however some ~2% (922 landings) which are actually preceeded with another landings, i.e. the trip inbetween is missing. Alternatively, one may have dual landings from the same trip, again something that needs further exploration.

### ....

... notes pending

```{r}
## Landing records that do not match ais harbour -------------------------------
match |> 
  filter(seq %in% "al") |> 
  group_by(vid) |> 
  mutate(x = case_when(s == "l" & hid2 == lag(hid2) ~ "ok",
                       s == "l" & hid2 != lag(hid2) ~ "not ok",
                       .default = NA)) |>
  ungroup() |> 
  filter(x == "not ok")
```

Remarkable - only 82 landing records out of around 50000 (less than 0.2%)



## Digging into some mobile data

```{r}
## Only vessels that only used mobile gear -------------------------------------
GIDs <- c(6, 7, 8, 9, 11)
match |> 
  filter(s == "l") |> 
  filter(gid %in% GIDs) |> 
  pull(vid) |> 
  unique() ->
  VIDs
match |> 
  filter(s == "l",
         vid %in% VIDs) |> 
  mutate(gid = ifelse(gid %in% GIDs, TRUE, FALSE)) |> 
  group_by(vid) |> 
  summarise(N = n(),
            n = sum(gid)) |> 
  filter(N == n) |> 
  pull(vid) ->
  VIDs
mobile <- 
  match |> 
  filter(vid %in% VIDs) |> 
  group_by(vid) |> 
  mutate(n = 1:n()) |> 
  ungroup() |> 
  mutate(s = ifelse(s == "a", "ais", "landing"))
```


### Visual display of ais-landing sequence

Here we only display sequence for vessels that only used mobile gear within the year:
  
  ```{r, fig.height = 12}
mobile |> 
  ggplot(aes(n, reorder(factor(vid), n), fill = s)) +
  geom_tile() +
  labs(x = "Event", y = "Vessel id",
       fill = "Source")
```

### Case example

* Landing event: orange
* Logbook event: green

```{r, echo = FALSE}
lgs <- 
  omar::lb_mobile(con) |> 
  filter(vid == 1043,
         year == 2017) |> 
  collect(n = Inf) |> 
  arrange(date, t1) |> 
  mutate(date = as_date(date))
tmp <- 
  match |> 
  filter(vid == 1043) |> 
  mutate(.cid = .id) |> 
  left_join(trip |> 
              select(-c(hid1, hid2))) |> 
  rename(time2 = time) |> 
  left_join(trail |> select(vid, .cid, time, lon, lat, speed))
```

```{r}
seqg <- unique(tmp$g)

for(i in seqg) {
  
  tmp2 <- 
    tmp |> 
    filter(g == i)
  
  G <- unique(tmp2$g)[1]
  
  timer <- range(tmp2$time, na.rm = TRUE)
  lgs2 <-
    lgs |> 
    filter(between(t1, timer[1], timer[2]))
  
  xlim <- range(c(tmp2$time2, tmp2$time), na.rm = TRUE)
  p1 <- 
    ggplot() +
    theme_bw() +
    geom_vline(data = tmp2 |> filter(s == "l"),
               aes(xintercept = time2),
               colour = "orange", lwd = 1) +
    geom_vline(data = lgs2,
               aes(xintercept = t1),
               colour = "darkgreen") +
    geom_line(data = tmp2 |> filter(s == "a"),
              aes(time, speed, group = factor(.cid)),
              colour = "grey") +
    geom_point(data = tmp2 |> filter(s == "a"),
               aes(time, speed, colour = factor(.cid))) +
    coord_cartesian(ylim = c(0, 14),
                    xlim = xlim) +
    theme(legend.position = c(0, 1))
  
  xlim <- range(tmp2$lon, na.rm = TRUE)
  ylim <- range(tmp2$lat, na.rm = TRUE)
  p2 <- 
    ggplot() +
    theme_bw() +
    geom_path(data = geo::bisland, aes(lon, lat)) +
    geom_path(data = tmp2 |> filter(s == "a"),
              aes(lon, lat,
                  group = factor(.cid)),
              colour = "grey",
              lwd = 0.1) +
    geom_point(data = tmp2 |> filter(s == "a"),
               aes(lon, lat,
                   colour = speed)) +
    scale_colour_viridis_c(option = "B") +
    coord_quickmap(xlim = xlim, ylim = ylim) +
    theme(legend.position = "none") +
    facet_wrap(~ .cid) +
    labs(x = NULL, y = NULL, caption = G)
  
  print(p1 + p2)
  
}
```

```{r, echo = FALSE}
omar::ln_agf(con) |> 
  filter(year(date) %in% YEAR,
         vid == 1043) |> 
  collect(n = Inf) |>
  group_by(.id, date, hid, gid) |> 
  summarise(wt = sum(wt),
            gt = sum(gt),
            magn = sum(magn),
            .groups = "drop") |> 
  knitr::kable()
```

