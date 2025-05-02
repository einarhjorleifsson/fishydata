# Port identification and shapes
#  The ports_table is generated from the stk trail harbour data where io == "I"
#  This table is joined with the ports_kvoti table via port name to generate a 
#  link with the numerical harbour value (hid) found in the logbooks and landings
#  tables.
#  Inputs:
#    trail_stk (database) where values are in io
#    kvoti.stadur (database) containing numerical code for harbours and harbour
#    names
#  Outputs:
#    data/auxillary/ports.gpkg  - to be used to classify points in harbours
#    data/auxillary/prots.parquet - a lookup to be used in conjunction with
#                                   logbooks and landings table
#
# Comments:
#  

library(sf)
library(tidyverse)
library(arrow)
library(omar)
con <- connect_mar()

## Create one master table -----------------------------------------------------
#  
ports_kvoti <- 
  tbl_mar(con, "kvoti.stadur") |> 
  collect() |> 
  select(stad_nr, port = heiti) |> 
  arrange(stad_nr) |> 
  filter(stad_nr > 0) |> 
  mutate(port = str_trim(port),
         port = case_when(port == "Vík í Mýrdal" ~ "Vík",
                          port == "Miðsandur, Hvalfirði" ~ "Miðsandur",
                          port == "Borgarfjörður Eystri" ~ "Borgarfjörður",
                          port == "Núpsskóli, Dýrafirði" ~ "Alviðruvör",
                          port == "Núpskatla" ~ "Núpskatla",
                          stad_nr == 102 ~ "Litli Árskógssandur",
                          #pid == 103 ~ "Árskógsströnd",
                          #port == "XXX Arnarstapi ekki nota" ~ NA,
                          port == "Svalbarðsströnd" ~ "Svalbarðseyri",
                          #port == "Reyðarfjörður" ~ "Búðareyri",
                          port == "Skútustaðahreppur" ~ "Mývatn",
                          port == "Flatey á Breiðafirði" ~ "Flatey",
                          .default = port)) |> 
  filter(!stad_nr %in% c(150, 153, 155:212, 300:999)) |> 
  add_row(stad_nr = 140, port = "Grundartangi")
ports_kvoti |> count(port) |> filter(n > 1) |> knitr::kable(caption = "Expect none")
ports_table <- 
  tribble(~pid,       ~port,          ~unlocode,
          'FO-ANR', 'Ánirnar',             'no',
          'FO-EID', 'Eiði',                'yes',
          'FO-FUG', 'Fuglafjörður',        'yes',    
          'FO-GLY', 'Glyvrar',             'yes',         
          'FO-HUS', 'Husevig',             'yes',         
          'FO-HVA', 'Hvalba',              'no',              
          'FO-HVS', 'Hvannasund',          'no',              
          'FO-KOL', 'Kollafjörður',        'yes',    
          'FO-KVI', 'Klakksvík',           'yes',       
          'FO-LOP', 'Lopra',               'yes',           
          'FO-LVK', 'Leirvík',             'no',
          'FO-MID', 'Miðvágur',            'no',
          'FO-NLS', 'Nólsoy',              'yes',          
          'FO-OYR', 'Oyri',                'no',      
          'FO-RVK', 'Rúnavík',             'yes',         
          'FO-SGV', 'Við Hólkin',          'no',  
          'FO-SKA', 'Skáli',               'yes',           
          'FO-SKO', 'Skopun',              'no', 
          'FO-SND', 'Sandur',              'no',
          'FO-SRV', 'Sorvágur',            'yes',        
          'FO-SVK', 'Sandvík',             'no',
          'FO-SYN', 'Steymnes',            'yes',        
          'FO-TOF', 'Tóftir',              'yes',          
          'FO-TOR', 'Tórshavn',            'no',
          'FO-TVO', 'Tvoeyri',             'yes',         
          'FO-VAG', 'Vágur',               'yes',           
          'FO-VBR', 'Velbastadur',         'yes',     
          'FO-VES', 'Vestmanhafn',         'yes',     
          'IS-AED', 'Æðey',                'no',
          'IS-AKE', 'Akureyjar',           'no',
          'IS-AKR', 'Akranes',             'yes',      
          #'IS-AKR', 'Akranes',             'yes',        
          'IS-AKU', 'Akureyri',            'yes',        
          'IS-ASS', 'Árskógssandur',       'yes',   
          'IS-AST', 'Arnarstapi',          'no',
          'IS-BAK', 'Bakkafjörður',        'yes',
          'IS-BGJ', 'Borgarfjörður',       'yes',   
          #'IS-BGJ', 'Borgarfjörður',       'yes',  
          'IS-BIL', 'Bíldudalur',          'yes',      
          'IS-BLO', 'Blönduós',            'yes',        
          'IS-BOR', 'Borgarnes',           'yes',       
          'IS-BRE', 'Breiðdalsvík',        'yes',    
          'IS-BRL', 'Brjánslækur',         'no', 
          'IS-BRO', 'Brokey',              'no',
          'IS-BOL', 'Bolungarvík',         'no',
          'IS-BUD', 'Búðardalur',          'yes',      
          'IS-DAL', 'Dalvík',              'yes',          
          'IS-DJU', 'Djúpivogur',          'yes',
          #'IS-DJU', 'Djúpivogur',          'yes',     
          'IS-DPV', 'Djúpavík',            'yes',        
          'IS-DRA', 'Drangsnes',           'yes',       
          #'IS-DRA', 'Drangsnes',           'yes',      
          'IS-DRE', 'Drangar',             'no',
          'IS-DYR', 'Dyrhólaey',           'no',
          'IS-ESK', 'Eskifjörður',         'yes',
          'IS-EYB', 'Eyrarbakki',          'no',
          'IS-FAS', 'Fáskrúðsfjörður',     'yes', 
          'IS-FBR', 'Flatey',              'no',
          'IS-FLA', 'Flateyri',            'no',
          'IS-FSK', 'Flatey á Skjálfanda', 'no',
          #  þetta er Bessataðahreppur
          'IS-GHV', 'Bessastaðahreppur',  'no',
          'IS-GJO', 'Gjögur',              'no',
          'IS-GRB', 'Garðabær',            'yes',        
          'IS-GRD', 'Garður',              'yes',          
          'IS-GRE', 'Grenivík',            'yes',        
          'IS-GRF', 'Grundarfjörður',      'yes',  
          'IS-GRT', 'Grundartangi',        'yes',    
          'IS-GRV', 'Grindavík',           'no',
          'IS-GRY', 'Grímsey',          'yes',         
          'IS-GUF', 'Gufunes',          'yes',         
          'IS-HAF', 'Hafnarfjörður',    'yes',   
          'IS-HAU', 'Hauganes',         'no',
          'IS-HBV', 'Haukabergsvaðall', 'no',    
          'IS-HEL', 'Helguvík',         'yes',        
          'IS-HLF', 'Hellisfjörður',    'no',
          'IS-HFN', 'Hornafjörður',     'yes',    
          'IS-HGV', 'Haganesvík',       'no',
          'IS-HJA', 'Hjalteyri',        'yes',       
          'IS-HNR', 'Hafnir',           'yes',          
          'IS-HOF', 'Hofsós',           'yes',          
          #'IS-HOL', 'Hólmavík',         'no',
          'IS-HRI', 'Hrísey',           'yes',          
          'IS-HST', 'Hesteyrar',        'no',          
          'IS-HUS', 'Húsavík',          'yes',         
          'IS-HVV', 'Hvammsvík',        'no',
          'IS-HVE', 'Hvalseyjar',       'yes',      
          'IS-HVK', 'Hólmavík',         'yes',        
          'IS-HVL', 'Hvallátur',        'no',           
          'IS-HVM', 'Hvammstangi',      'yes',     
          'IS-HVR', 'Miðsandur',        'yes',     
          'IS-ING', 'Ingólfsfjörður',   'no',    
          'IS-ISA', 'Ísafjörður',       'yes',      
          'IS-JON', 'Jónsnes',          'no',
          'IS-KDN', 'Kaldrananes',      'no',         
          'IS-KEV', 'Keflavík',         'yes',        
          #'IS-KEV', 'Keflavík',         'yes',       
          'IS-KOP', 'Kópasker',         'yes',        
          'IS-KOV', 'Kópavogur',        'yes',       
          #'IS-KOV', 'Kópavogur',        'yes',      
          'IS-KRS', 'Kristnes',         'no',
          'IS-LAN', 'Landeyjarhöfn',    'yes',   
          'IS-LEY', 'Langey',           'no',
          'IS-LAT', 'Látravík',         'no',
          'IS-LEH', 'Leirhöfn',         'no',
          'IS-LSA', 'Litli sandur',     'yes',     
          'IS-MAN', 'Mánavík',          'no',
          'IS-MJF', 'Mjóifjörður',      'no',
          'IS-MJH', 'Mjóeyri',          'yes',         
          'IS-NES', 'Neskaupstaður',    'yes',   
          'IS-NJA', 'Njarðvík',         'yes',        
          'IS-NOU', 'Norðurfjörður',    'yes',   
          'IS-NUK', 'Í Breiðarfirði',   'no',      
          'IS-NUP', 'Núpskatla',        'no',   
          'IS-NYB', 'Nýjabúð',          'no',      
          'IS-OLF', 'Ólafsfjörður',     'yes',    
          'IS-OLV', 'Ólafsvík',         'yes',        
          'IS-PAP', 'Papey',            'no',
          'IS-PAT', 'Patreksfjörður',   'yes',  
          'IS-PUR', 'Purkey',           'no',
          'IS-RAU', 'Raufarhöfn',       'yes',      
          'IS-RES', 'Reykjaströnd',     'no',
          'IS-REY', 'Reykjavík',        'yes',       
          #'IS-REY', 'Reykjavík',        'yes',      
          'IS-RFJ', 'Reyðarfjörður',    'yes',   
          'IS-RHA', 'Reykhólar',        'yes',       
          'IS-RIF', 'Rif',              'yes',             
          'IS-RKF', 'Reykjafjörður',    'no',
          'IS-RKN', 'Reykjanes',        'no',
          'IS-SAN', 'Sandgerði',        'yes',       
          'IS-SAU', 'Sauðárkrókur',     'yes',    
          'IS-SEL', 'Selfoss',          'yes',         
          'IS-SEY', 'Seyðisfjörður',    'yes',   
          'IS-SIG', 'Siglufjörður',     'yes',    
          'IS-SKA', 'Skagaströnd',      'yes',     
          'IS-SKR', 'Skarðsstöð',       'no',
          'IS-SKE', 'Skáleyjar',        'no',
          'IS-SKY', 'Skákarey',        'no',
          'IS-SKM', 'Skálmarnes',       'no',
          'IS-SNF', 'Snarfarahöfn',     'no',
          'IS-STA', 'Stafnes',          'no',
          'IS-STD', 'Stöðvarfjörður',   'yes',  
          'IS-STK', 'Stokkseyri',       'yes',     
          'IS-STN', 'Seltjarnarnes',    'no',               
          #'IS-STO', 'Stöðvarfjörður',   'no',              
          'IS-STU', 'Klauf',  'no',               
          'IS-STV', 'Straumsvík',       'no',               
          'IS-STY', 'Stykkishólmur',    'yes',
          #'IS-STY', 'Stykkishólmur',    'yes',  
          'IS-SUD', 'Suðureyri',        'yes',       
          #'IS-SUD', 'Suðureyri',        'yes',      
          'IS-SUV', 'Súðavík',          'yes',    
          'IS-SVA', 'Svalbarðseyri',    'yes',   
          'IS-SVD', 'CHECK',            'no',         
          'IS-TAL', 'Tálknafjörður',    'yes',   
          'IS-TEY', 'Þingeyri',         'yes',        
          'IS-THH', 'Þorlákshöfn',      'yes',     
          'IS-THO', 'Þórshöfn',         'yes',        
          #'IS-TMY', 'Traðir',           'no',          
          'IS-TRD', 'Traðir',                'no',             
          'IS-VES', 'Vestmannaeyjar',   'yes',  
          'IS-VGR', 'Vigur',            'no',
          'IS-VID', 'Viðey',            'no',  
          'IS-VFF', 'Viðfjörður',       'no',
          'IS-VOG', 'Vogar',            'yes',           
          'IS-VPN', 'Vopnafjörður',     'yes',
          # added
          'IS-VIK', 'Vík', 'no',
          'IS-HLL', 'Hellnar', 'no',     # Hellnar
          'IS-HLS', 'Hellissandur', 'no',     # Hellisandur
          'IS-ALV', 'Alviðruvör', 'no',    # Alviðruvör, Núpur
          'IS-HDL', 'Hnífsdalur', 'no',     # Hnífsdalur
          'IS-OGV', 'Ögurvík', 'no')
ports_table |> count(port) |> filter(n > 1) |> knitr::kable(caption = "Expect none")

ports_table <- 
  ports_table |> 
  full_join(ports_kvoti)
ports_table |> count(port) |> filter(n > 1) |> knitr::kable(caption = "Expect none")

## The polygons for the ports --------------------------------------------------
#   First generate an additional table of some ports in kvoti.stadur that is
#   not in the stk data. This is just done for sake of completeness, expect
#   in the end few points to fall into these "areas"
ports_add <- 
  tribble(~pid,    ~port, ~lat, ~lon,
          'IS-VIK', 'Vík', 63.413191, -19.005441,
          'IS-HLL', 'Hellnar', 64.751359, -23.643993,     # Hellnar
          'IS-HLS', 'Hellisandur', 64.751359, -23.643993,     # Hellisandur
          'IS-ALV', 'Aviðruvör',  65.926529, -23.610718,    # Alviðruvör, Núpur
          'IS-HDL', 'Hnífsdalur', 64.751359, -23.643993,     # Hnífsdalur
          'IS-OGV', 'Ögurvík', 66.043098, -22.732611) |> 
          #'IS-BKK', 'Bakki', 65.741300, -23.821559, # Dýrafjörður
          #'IS-BIR', 'Bæjir', 66.087940, -22.553233,         # Bæjir, Snæfjallaströnd
          #NA, 'Grunnavik', 66.245726, -22.875815,
          #NA, 'Latrar', 66.389450, -23.039549) |>  # 
  st_as_sf(coords = c("lon","lat"),
           crs = 4326) |> 
  st_transform(crs = 3857) |> 
  st_buffer(dist = 500) |> 
  st_transform(crs = 4326) |> 
  select(-port)

# Besides standardization and some correction code here we may in some cases
# split up the io="I" points to generate two polygons for some harbours. These
# will be merged into a multipolygon downstream, so we end up with each pid
# being unique
ports <-
  open_dataset("data/ais/stk-raw") |> 
  filter(!is.na(hid)) %>%
  collect(n = Inf) |> 
  filter(io == "I") %>%
  select(stk = hid, lon, lat) |>
  mutate(lon = round(lon, 4),
         lat = round(lat, 4)) |> 
  distinct() |> 
  filter(!stk %in% c("ISL", "DNG",  "BER", "REYF", "RYH", "LSGUFA", "GYH", "IYH",
                     "AYH", "GRYYH", "R", "MJO", "MJOIF")) |> 
  filter(!(stk == "THH" & lat > 65)) |> 
  filter(!(stk == "THO" & lat < 65)) |> 
  filter(!(stk == "SEY" & lon < -20)) |> 
  filter(!(stk == "FLA" & lon < 65.963447)) |> 
  filter(!(stk == "RES" & lat < 65)) |> 
  filter(!(stk == "KRS" & lat < 64)) |> 
  filter(!(stk == "HUS" & lon > -16)) |> 
  filter(!(stk == "FA" & lat < 64.872063)) |> 
  filter(!(stk == "DRA" & lat > 65.866120)) |> 
  filter(!(stk == "HVF" & lon > -21.416204)) |> 
  filter(!(stk == "KRS" & lat < 65.699448)) |> 
  filter(!(stk == "FA" & lat < 64.916849)) |> 
  mutate(stk = case_when(stk == "H" ~ "HNR",
                         stk == "BU" ~ "BOL",
                         stk == "SNG" ~ "SAN",
                         stk == "G" ~ "GRD",
                         stk %in% c("KF", "KEF") ~ "KEV",
                         stk == "NJ" ~ "NJA",
                         stk == "VO" ~ "VOG",
                         stk == "HF" ~ "HAF",
                         stk == "GB" ~ "GRB",
                         stk == "KV" ~ "KOV",
                         stk == "HVF" ~ "HVR", 
                         stk %in% c("AK", "AKR2") ~ "AKR",
                         stk == "HVAM" ~ "HVV",
                         stk == "BG" ~ "BOR",
                         stk == "GF" ~ "GRF",
                         stk == "OFS" ~ "OLV",
                         stk %in% c("STM", "STY2") ~ "STY",
                         stk == "HVAL" ~ "HVL",
                         stk == "PURK" ~ "PUR",
                         stk == "REYK" ~ "RHA",
                         stk == "STAD" ~ "STU",
                         stk == "PT" ~ "PAT",
                         stk == "TF" ~ "TAL",
                         stk == "BD" ~ "BIL",
                         stk == "THI" ~ "TEY",
                         stk %in% c("SUD2", "SUG") ~ "SUD",
                         stk == "FL" ~ "FLA",
                         stk == "IS" ~ "ISA",
                         stk == "SK" ~ "SUV",
                         stk == "NFJ" ~ "NOU",
                         stk == "GJ" ~ "GJO",
                         stk == "DRN" ~ "DRA",
                         stk == "HK" ~ "HOL",
                         stk == "HVT" ~ "HVM",
                         stk == "BL" ~ "BLO",
                         stk == "SR" ~ "SAU",
                         stk == "HFS" ~ "HOF",
                         stk == "SG" ~ "SIG",
                         stk == "OF" ~ "OLF",
                         stk == "DL" ~ "DAL",
                         stk == "AR" ~ "ASS",
                         stk == "HG" ~ "HAU",
                         stk == "HJE" ~ "HJA",
                         stk == "A" ~ "AKU",
                         stk == "SVB" ~ "SVA",
                         stk == "GK" ~ "GRE",
                         stk == "HRY" ~ "HRI",
                         stk == "HU" ~ "HUS",
                         stk == "KP" ~ "KOP",
                         stk == "RH" ~ "RAU",
                         stk %in% c("BAK2", "BKF") ~ "BAK",
                         stk == "VP" ~ "VPN",
                         stk == "BF" ~ "BGJ",
                         stk == "SF" ~ "SEY",
                         stk == "NK" ~ "NES",
                         stk == "HELL" ~ "HLF",
                         stk == "RF" ~ "RFJ",
                         stk == "FA" ~ "FAS",
                         stk == "STF" ~ "STD",
                         stk == "BRK" ~ "BRE",
                         stk == "DP" ~ "DJU",
                         stk == "VM" ~ "VES",
                         stk == "EB" ~ "EYB",
                         stk == "GEST" ~ "GHV",
                         stk == "LANG" ~ "LEY",
                         stk == "VIDF" ~ "VFF",
                         stk == "LH" ~ "LEH",
                         stk == "SKEY" ~ "SKY",
                         stk == "SKAR" ~ "SKR",
                         stk == "TRAD" ~ "TRD",
                         .default = stk)) |> 
  # Split some harbours - older harbour retains name, newer harbour gets 2 added
  mutate(stk = case_when(stk == "KEV" & between(lat, 64.002491, 64.007134) ~ NA,
                         stk == "KEV" & lat > 64.007134 ~ "KEV2",
                         stk == "KOV" & lon >= -21.932025 ~ "KOV2",
                         stk == "AKR" & lon < -22.091226 ~ "AKR2",
                         stk == "STY" & lon <  -22.732919 ~ "STK2",
                         stk == "SUD" & lon <  -23.537276 ~ "SUD2",     # Not really a harbour?
                         stk == "DRA" & lon <   -21.471696  ~ "DRA2",
                         stk == "AKU" & lat > 65.697073 ~ "KRS",        # relabelling
                         stk == "BAK" & lat > 66.029713 ~ NA,           # really not a harbour
                         stk == "BGJ" & lon > -13.787222 ~ "BGJ2",
                         stk == "DJU" & between(lon, -14.290093, -14.284439) ~ NA,
                         stk == "DJU" & lon < -14.290093 ~ "DJU2",
                         stk == "REY" & lat >= 64.159826 & lon > -21.859520 ~ "VID",
                         stk == "REY" & lon < -21.929564 ~ stk,
                         stk == "REY" & lon >  -21.881557 & lat >  64.136520 ~ "REY2",
                         stk == "REY" ~ NA,
                         
                         .default = stk)) |> 
  filter(!is.na(stk)) |> 
  mutate(pid = case_when(str_starts(stk, "FO-") ~ stk,
                         .default = paste0("IS-", stk))) |> 
  st_as_sf(coords = c("lon", "lat"),
           crs = 4326) |> 
  select(pid) |> 
  group_by(pid) |> 
  summarise(do_union = FALSE) |> 
  st_convex_hull() |> 
  rename(pid2 = pid) |> 
  mutate(pid = str_remove(pid2, "2")) |> 
  select(pid2, pid) |> 
  select(-pid2) |> 
  group_by(pid) |> 
  summarise(do_union = FALSE) |> 
  ungroup() |> 
  bind_rows(ports_add)

ports |> st_drop_geometry() |> count(pid) |> filter(n > 1) |> knitr::kable(caption = "Expect none")
## Merge the final data and export ---------------------------------------------
ports <- 
  ports |> 
  # inner join drops "ports" on land and some others
  inner_join(ports_table) |> 
  select(pid, port, hid = stad_nr, everything())

ports |> st_drop_geometry() |> count(pid) |> filter(n > 1) |> knitr::kable(caption = "Expect none")
ports |> st_drop_geometry() |> count(port) |> filter(n > 1) |> knitr::kable(caption = "Expect none")


ports |>  
  st_write("data/auxillary/ports.gpkg", append = FALSE)
