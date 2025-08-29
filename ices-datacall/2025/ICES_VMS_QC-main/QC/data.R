# packages
library(RODBC)
library(icesTAF)
library(jsonlite)
library(dplyr)

# settings
config <- read_json("QC/config.json", simplifyVector = TRUE)

# create directories
mkdir(config$data_dir)

# connect to DB
conn <- odbcDriverConnect(connection = config$db_connection)

# LE (Logbook) Data Extraction
for (country in config$countries) {
  msg("downloading LE data for ... ", country)
  
  # parse LE_session IDs
  LE_session_ids <- paste(config$LE_session_ID, collapse = ", ")
  
  # LE SQL query - extract all required fields in correct order
  sqlq <- sprintf("SELECT 
                          [RecordType],
                          [CountryCode],
                          [Year],
                          [Month],
                          [NoDistinctVessels],
                          [AnonymizedVesselID],
                          [ICESrectangle],
                          [MetierL4],
                          [MetierL5],
                          [MetierL6],
                          [VesselLengthRange],
                          [VMSEnabled],
                          [FishingDays],
                          [kWFishingDays],
                          [TotWeight],
                          [TotValue]
                          FROM [DATSU].dbo.tbl_145_21928_LE 
                          WHERE countryCode = '%s' AND sessionID in (%s)", 
                  country, LE_session_ids)
  
  fname <- paste0(config$data_dir, "/ICES_LE_", country, ".csv")
  
  # fetch LE data
  out <- sqlQuery(conn, sqlq)
  
  # save LE data to file
  write.table(out, file = fname, row.names = FALSE, col.names = FALSE, sep = ",")
}

# VE (VMS) Data Extraction  
for (country in config$countries) {
  msg("downloading VMS data for ... ", country)
  
  # parse VE_session IDs
  VE_session_ids <- paste(config$VE_session_ID, collapse = ", ")
  
  # VE SQL query - extract ALL required fields
  sqlq <- sprintf("SELECT
                          [RecordType],
                          [CountryCode],
                          [Year],
                          [Month],
                          [NoDistinctVessels],
                          [AnonymizedVesselID],
                          [C-square],
                          [MetierL4],
                          [MetierL5],
                          [MetierL6],
                          [VesselLengthRange],
                          [HabitatType],
                          [DepthRange],
                          [NumberOfRecords],
                          [AverageFishingSpeed],
                          [FishingHour],
                          [AverageInterval],
                          [AverageVesselLength],
                          [AveragekW],
                          [kWFishingHour],
                          [SweptArea],
                          [TotWeight],
                          [TotValue],
                          [AverageGearWidth]
                          FROM [DATSU].dbo.tbl_145_21928_VE 
                          WHERE countryCode = '%s' AND SessionID in (%s)", 
                  country, VE_session_ids)
  
  fname <- paste0(config$data_dir, "/ICES_VE_", country, ".csv")
  
  # fetch VE data
  out <- sqlQuery(conn, sqlq)
  
  # save VE data to file
  write.table(out, file = fname, row.names = FALSE, col.names = TRUE, sep = ",")
}

# disconnect
odbcClose(conn)
