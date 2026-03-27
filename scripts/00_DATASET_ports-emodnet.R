library(emodnet.wfs)
library(sf)
library(dplyr)

wfs_ha <- emodnet_init_wfs_client(service = "human_activities")
# sort(emodnet_get_wfs_info(wfs_ha)$layer_name)
emodnet <- emodnet_get_layers(
  wfs    = wfs_ha,
  layers = "portlocations",
  simplify = TRUE
)

emodnet |>
  write_sf("data/ports/ports_emodnet.gpkg")
