# https://www.fiskeridir.no/statistikk-tall-og-analyse/data-og-statistikk-om-yrkesfiske/apne-data-fangstdata-seddel-koblet-med-fartoydata

fil <- 
  paste0("https://register.fiskeridir.no/uttrekk/fangstdata_",
  2000:2024,
  ".csv.zip")
base <- basename(fil)
for(i in 1:length(fil)) {
  print(base[i])
  download.file(fil[i], destfile = paste0("data-raw/norway/fangstdata/", base[i]), method = "wget")
}
