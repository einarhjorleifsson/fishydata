library(rmarkdown)
library(icesTAF)
library(jsonlite)
library(markdown)



# settings
config <- read_json("QC/config.json", simplifyVector = TRUE)

# create report directory
mkdir(config$report_dir)

# loop over countries
for (country in config$countries) {
  
  msg("Running QC for ... ", country)
  
  render("report-QC_format2025editNC.Rmd", 
         output_dir = "QC/reports_2025/", 
         output_file = paste0("QC_", country, format(Sys.time(), "_%Y-%m-%d"),".html"),
         params = list(
           country = country,
           ve_id = as.character(config$VE_session_ID),
           le_id = as.character(config$LE_session_ID),
           data_dir = config$data_dir
         ))
  
  msg("Done ... ", country)
}
