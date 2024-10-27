library(tidyverse)

### Faero vessels --------------------------------------------------------------
fo_mmsi <-
  stk_cat |>
  inner_join(FO2 |> drop_na(mmsi) |> select(-cs),
             by = join_by(mmsi))
fo_cs <-
  stk_cat |>
  inner_join(FO2 |> drop_na(cs) |> select(-mmsi),
             by = join_by(cs))
bind_rows(fo_mmsi, fo_cs) |> arrange(-pings)

### Norwegian vessels ----------------------------------------------------------

no_cs <-
  stk_cat |>
  inner_join(NO2 |> drop_na(cs),
             by = join_by(cs))
no_cs |> arrange(-pings)


### EU vessel ------------------------------------------------------------------

eu_cs <-
  stk_cat |>
  inner_join(EU2 |> drop_na(cs) |> select(-mmsi),
             by = join_by(cs))
eu_cs |> arrange(-pings)
eu_mmsi <-
  stk_cat |>
  inner_join(EU2 |> drop_na(mmsi) |> select(-cs),
             by = join_by(mmsi))
eu_mmsi |> arrange(-pings)

### Global fishing watch -------------------------------------------------------
# Not really used
GFW <- read_csv("~/stasi/fishydata/data-raw/gfw/fishing-vessels-v2.csv")
gfw_mmsi <-
  stk_cat |>
  inner_join(GFW |> select(mmsi, flag_gfw) |> mutate(mmsi = as.character(mmsi)))
gfw_mmsi |> arrange(-pings) |> count(flag_gfw) |> arrange(-n) |> knitr::kable()

### ASTD registry --------------------------------------------------------------
astd_summary <-
  nanoparquet::read_parquet("setups/data-raw/vessels-asdt.parquet")
astd_mmsi <-
  stk_cat |>
  inner_join(astd_summary |>
               mutate(mmsi = as.character(mmsi)) |>
               select(mmsi, flag, astd_imo = imo, astd_vessel = vessel,
                      astd_cat, astd_pings = pings))
astd_mmsi |>
  arrange(-pings)



# Get foreign vessel call-signs & mmsi

### Faero vessels --------------------------------------------------------------

fo_mmsi <-
  stk_cat |>
  inner_join(FO2 |> drop_na(mmsi) |> select(-cs),
             by = join_by(mmsi))
fo_cs <-
  stk_cat |>
  inner_join(FO2 |> drop_na(cs) |> select(-mmsi),
             by = join_by(cs))
bind_rows(fo_mmsi, fo_cs) |> arrange(-pings)

### Norwegian vessels ----------------------------------------------------------

no_cs <-
  stk_cat |>
  inner_join(NO2 |> drop_na(cs),
             by = join_by(cs))
no_cs |> arrange(-pings)


### EU vessel ------------------------------------------------------------------

eu_cs <-
  stk_cat |>
  inner_join(EU2 |> drop_na(cs) |> select(-mmsi),
             by = join_by(cs))
eu_cs |> arrange(-pings)
eu_mmsi <-
  stk_cat |>
  inner_join(EU2 |> drop_na(mmsi) |> select(-cs),
             by = join_by(mmsi))
eu_mmsi |> arrange(-pings)

### Global fishing watch -------------------------------------------------------
# Not really used
GFW <- read_csv("~/stasi/fishydata/data-raw/gfw/fishing-vessels-v2.csv")
gfw_mmsi <-
  stk_cat |>
  inner_join(GFW |> select(mmsi, flag_gfw) |> mutate(mmsi = as.character(mmsi)))
gfw_mmsi |> arrange(-pings) |> count(flag_gfw) |> arrange(-n) |> knitr::kable()

### ASTD registry --------------------------------------------------------------
astd_summary <-
  nanoparquet::read_parquet("setups/data-raw/vessels-asdt.parquet")
astd_mmsi <-
  stk_cat |>
  inner_join(astd_summary |>
               mutate(mmsi = as.character(mmsi)) |>
               select(mmsi, flag, astd_imo = imo, astd_vessel = vessel,
                      astd_cat, astd_pings = pings))
astd_mmsi |>
  arrange(-pings)
