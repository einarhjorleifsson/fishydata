# On gears
#  The primary gear code used in the ais/vms analysis is that reported in the
#  gafl/landings database.
#
# TODO:
#  * Add criterion for maximum duration
#  * Add criterion for maximum distance

library(tidyverse)
library(omar)
con <- connect_mar()

m4_ices <- 
  icesVocab::getCodeList("GearType") |> 
  as_tibble() |> 
  janitor::clean_names()
m5_ices <- 
  icesVocab::getCodeList("TargetAssemblage") |> 
  as_tibble() |> 
  janitor::clean_names()
m6_ices <- 
  icesVocab::getCodeList("Metier6_FishingActivity") |> 
  as_tibble() |> 
  janitor::clean_names()

met5 <- 
  m6_ices |> 
  select(key, description) |> 
  mutate(description = str_remove(description, ", see the code reg. mesh size and selectivity device")) |> 
  mutate(met5 = case_when(str_sub(key, 8, 8) == "_" ~ str_sub(key, 1, 7),
                           str_sub(key, 7, 7) == "_" ~ str_sub(key, 1, 6),
                           .default = "should not be here")) |> 
  select(met5, description) |> 
  distinct(met5, .keep_all = TRUE) |> 
  arrange(met5) 


speed_criterion <- 
  tribble(~gid_agf, ~s1, ~s2,
          1, 0, 2.500,
          2, 0, 1.700,
          3, 0, 1.700,
          4, 0, 1.700,
          5, 0.05, 1.125,
          6, 2.625, 4.700,
          7, 2.375, 3.700,
          8, 1.750, 3.000,
          9, 2.625, 6.000,
          10, 0, 1.7,
          11, 0.250, 2.9,
          12, 0.375, 2.750,
          13, 0.375, 2.750,
          14, 0.025, 1.7,
          15, 1.2, 2.9,
          16, 0.1, 2,
          19, 0.125, 1.25,
          21, 0.1, 2.2,
          22, 0.05, 1.4,
          24, 0, 1.4,
          25, 2.3, 4) 

gid_agf <- 
  tbl_mar(con, "agf.aflagrunnur_v") |> 
  select(starts_with("veidar")) |> 
  distinct() |> 
  select(gid_agf = veidarfaeri, veiðarfæri_agf = veidarfaeri_heiti) |> 
  collect() |> 
  arrange(gid_agf) |> 
  mutate(gid_orri = 
           case_when(gid_agf == 1 ~ 91,
                     gid_agf == 2 ~  2,
                     gid_agf == 3 ~ 25,
                     gid_agf == 4 ~ 29,
                     gid_agf == 5 ~ 11,
                     gid_agf == 6 ~ 6,
                     gid_agf == 7 ~ 9,
                     gid_agf == 8 ~ 14,
                     gid_agf == 9 ~ 7,
                     gid_agf == 10 ~ 10,   # Multiple choices
                     gid_agf == 11 ~ 5,    # Multiple choices, here lowest value
                     gid_agf == 12 ~ 1,
                     gid_agf == 13 ~ 71,
                     gid_agf == 14 ~ 3,
                     gid_agf == 15 ~ 15,   # Multiple choices
                     gid_agf == 16 ~ 16,   # Multiple choices
                     gid_agf == 17 ~ 99,   # Óskráð veiðarfæri
                     gid_agf == 18 ~ NA,   # Eldiskví
                     gid_agf == 19 ~ 43,   # Veiðistöng
                     gid_agf == 20 ~ 42,
                     gid_agf == 21 ~ 1,    # Línutrekt - think the old system did not have that
                     gid_agf == 22 ~ 92,
                     gid_agf == 23 ~ 41,   # Ígulkerakafari in the old system
                     gid_agf == 24 ~ NA,   # Sláttuprammi
                     gid_agf == 25 ~ NA,   # Þaraplógur
                     .default = NA)) |> 
  mutate(met5 = case_when(gid_agf ==  1 ~ "GNS_DEF",  # Skötuselsnet
                          gid_agf ==  2 ~ "GNS_DEF",  # Þorskfisknet
                          gid_agf ==  3 ~ "GNS_DEF",  # Grásleppunet
                          gid_agf ==  4 ~ "GNS_DEF",  # Rauðmaganet
                          gid_agf ==  5 ~ "GND_SPF",  # Reknet - check if small pelagics
                          gid_agf ==  6 ~ "OTB_DEF",  # Botnvarpa
                          gid_agf ==  7 ~ "OTB_MCD",  # Humarvarpa
                          gid_agf ==  8 ~ "OTB_MCD",  # Rækjuvarpa
                          gid_agf ==  9 ~ "OTM_SPF",  # Flotvarpa
                          gid_agf == 10 ~ "PS_SPF",   # Nót
                          gid_agf == 11 ~ "SDN_DEF",  # Dragnót - CHECK
                          gid_agf == 12 ~ "LLS_DEF",  # Lína
                          gid_agf == 13 ~ "LLS_DEF",  # Landbeitt lína
                          gid_agf == 14 ~ "LHM_DEF",  # Handfæri
                          gid_agf == 15 ~ "DRB_DES",  # Plógur
                          gid_agf == 16 ~ "FPO_DEF",  # Gildra - CHECK main species
                          gid_agf == 17 ~ "MIS_DWF",  # Annað
                          gid_agf == 18 ~ NA,         # Eldiskví
                          gid_agf == 19 ~ "LHP_FIF",  # Sjóstöng
                          gid_agf == 20 ~ NA,         # Kræklingalína
                          gid_agf == 21 ~ "LLS_DEF",  # Línutrekt
                          gid_agf == 22 ~ "GNS_DWS",  # Grálúðunet
                          gid_agf == 23 ~ "DIV_DES",  # Kafari
                          gid_agf == 24 ~ "HMS_SWD",  # Sláttuprammi
                          gid_agf == 25 ~ "HMS_SWD",  # Þarapógur
                          .default = NA)) |> 
  left_join(met5) |> 
  left_join(speed_criterion)


gid_agf |> nanoparquet::write_parquet("data/gear/gear_mapping.parquet")

