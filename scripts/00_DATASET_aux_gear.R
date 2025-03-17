library(tidyverse)
library(omar)
con <- connect_mar()

orri <- 
  omar::gid_orri(con) |> 
  collect() |> 
  arrange(gid)
lods <- 
  omar::ln_landing(con) |> 
  count(gid) |> 
  collect() |> 
  arrange(gid)
gear <-
  orri |> 
  full_join(lods) |> 
  mutate(in.lods = !is.na(n),
         .after = gid) |> 
  mutate(gear = case_when(gid == 92 ~ "Ghalibut net",
                          .default = gear))

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
                     .default = NA))




gid_agf |> nanoparquet::write_parquet("data/auxillary/gear_mapping.parquet")
nanoparquet::read_parquet("data/auxillary/agf_gear.parquet")

