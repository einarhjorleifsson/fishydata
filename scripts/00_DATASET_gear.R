library(tidyverse)
library(omar)
con <- connect_mar()

gid_agf <- 
  tbl_mar(con, "agf.aflagrunnur_v") |> 
  select(starts_with("veidar")) |> 
  distinct() |> 
  select(gid_ln = veidarfaeri, vf_agf = veidarfaeri_heiti) |> 
  collect() |> 
  arrange(gid_ln)
gear <- gid_orri_plus(con) |> arrange(gid) |> collect()
# map gid_agf to orri
gid_agf <- 
  gid_agf |> 
  mutate(gid = 
           case_when(gid_ln == 1 ~ 91,
                     gid_ln == 2 ~  2,
                     gid_ln == 3 ~ 25,
                     gid_ln == 4 ~ 29,
                     gid_ln == 5 ~ 11,
                     gid_ln == 6 ~ 6,
                     gid_ln == 7 ~ 9,
                     gid_ln == 8 ~ 14,
                     gid_ln == 9 ~ 7,
                     gid_ln == 10 ~ NA,   # Multiple choices
                     gid_ln == 11 ~ 5,    # Multiple choices, here lowest value
                     gid_ln == 12 ~ 1,
                     gid_ln == 13 ~ 71,
                     gid_ln == 14 ~ 3,
                     gid_ln == 15 ~ NA,   # Multiple choices
                     gid_ln == 16 ~ NA,   # Multiple choices
                     gid_ln == 17 ~ 20,   # Óskráð veiðarfæri. check this
                     gid_ln == 18 ~ NA,
                     gid_ln == 19 ~ 43,   # Veiðistöng
                     gid_ln == 20 ~ 42,
                     gid_ln == 21 ~ 1,    # Línutrekt - think the old system did not have that
                     gid_ln == 22 ~ 92,
                     gid_ln == 23 ~ 41,   # Ígulkerakafari in the old system
                     gid_ln == 24 ~ NA,   # Sláttuprammi
                     gid_ln == 25 ~ NA,   # Þaraplógur
                     .default = NA))

gid_agf |> nanoparquet::write_parquet("data/auxillary/gear_mapping.parquet")
nanoparquet::read_parquet("data/auxillary/agf_gear.parquet")

