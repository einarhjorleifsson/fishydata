library(leaflet)
# https://gis.hafogvatn.is/geoserver/hafro/wms
m <-
  leaflet() |>
  addTiles(group = "base") %>%
  setView(-20, 64, zoom = 7) |>
  addWMSTiles(baseUrl = "https://gis.hafogvatn.is/geoserver/hafro/wms",
              layers = "hafro:trawl_3857_byte",
              group = "Fish trawl",
              options = WMSTileOptions(format = "image/jpeg",
                                       transparent  = FALSE,
                                       crs = "EPSG:3057",
                                       opacity = 0.7
              )
  ) |>
  addLayersControl(baseGroups = c("base"),
                   overlayGroups = c("Fish trawl"),
                   options = layersControlOptions(collapsed = FALSE))
m

leaflet() |>
  addTiles(group = "base") %>%
  setView(-20, 64, zoom = 7) |>
  addWMSTiles(baseUrl = "https://gis.hafogvatn.is/geoserver/hafro/wms",
              layers = "hafro:smx",
              group = "Fish trawl",
              options = WMSTileOptions(format = "image/jpeg",
                                       transparent  = FALSE,
                                       crs = "EPSG:3057",
                                       opacity = 0.5
              )
  )
# minimum
leaflet() |>
  addTiles(group = "base") %>%
  setView(-20, 64, zoom = 7) |>
  addWMSTiles(baseUrl = "https://gis.hafogvatn.is/geoserver/hafro/wms",
              layers = "hafro:smx",
              group = "Fish trawl",
              options = WMSTileOptions(
                                       #transparent  = TRUE,
                                       opacity = 0.5
              )
  )


# from local server
## raster
leaflet() |>
  addTiles(group = "base") %>%
  setView(-25, 66, zoom = 6) |>
  addWMSTiles(baseUrl = "http://localhost:8080/geoserver/einar_workspace/wms",
              layers = "einar_workspace:harbours",
              group = "Fish trawl",
              options = WMSTileOptions(format = "image/png",
                                       transparent  = FALSE,
                                       #crs = "EPSG:3057",
                                       opacity = 0.7              )
  ) |>
  addLayersControl(baseGroups = c("base"),
                   overlayGroups = c("Fish trawl"),
                   options = layersControlOptions(collapsed = FALSE))



"einar_workspace:harbours"
leaflet() |>
  addTiles(group = "base") %>%
  setView(-20, 64, zoom = 7) |>
  addWMSTiles(baseUrl = "http://localhost:8080/geoserver/einar_workspace/wms",
              layers = "einar_workspace:harbours",
              group = "Fish trawl",
              options = WMSTileOptions(format = "image/jpeg",
                                       transparent  = FALSE,
                                       crs = "EPSG:4326",
                                       opacity = 1)
  )
