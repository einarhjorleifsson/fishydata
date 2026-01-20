library(tidyverse)
library(tmap)
library(leaflet)
library(sf)
# colour name to hex - for leaflet
col2hex <- function(x) {
  rgb(t(col2rgb(x)), maxColorValue = 255)
}

# data -------------------------------------------------------------------------
images <- nanoparquet::read_parquet("data/hafmynd_coral.parquet")
tillogur <- read_sf("data/sa_kantur_tillogur.gpkg")
korall <- read_sf("data/reglugerdir_korall.gpkg")
z <- read_sf("data/depth_lines.gpkg")
gid <- nanoparquet::read_parquet("data/gear.parquet")
g01 <- raster::raster("data/raster/2015-2024_effort-Lína.tif")
g02 <- raster::raster("data/raster/2015-2024_effort-Þorskfisknet.tif")
g06 <- raster::raster("data/raster/2015-2024_effort-Botnvarpa.tif")
p01 <-
  leaflet::colorBin(palette = viridis::inferno(12, alpha = 1, begin = 0, end = 1, direction = -1),
                    domain = raster::values(g01),
                    # bins = c(6, 10, 15, 20, 30, 40, 50, 60, 80, 90, 101),
                    na.color = "transparent")
p02 <-
  leaflet::colorBin(palette = viridis::inferno(12, alpha = 1, begin = 0, end = 1, direction = -1),
                    domain = raster::values(g02),
                    # bins = c(6, 10, 15, 20, 30, 40, 50, 60, 80, 90, 101),
                    na.color = "transparent")
p06 <-
  leaflet::colorBin(palette = viridis::inferno(12, alpha = 1, begin = 0, end = 1, direction = -1),
                    domain = raster::values(g06),
                    # bins = c(6, 10, 15, 20, 30, 40, 50, 60, 80, 90, 101),
                    na.color = "transparent")
# Map --------------------------------------------------------------------------
## base layers -----------------------------------------------------------------
l <-
  leaflet() %>%
  #setView(lng = pt.centroid$lon,
  #        lat = pt.centroid$lat,
  #        zoom = 7) %>%
  addTiles(urlTemplate = "https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}",
           #group = "Hnöttur",
           attribution = 'Data source: <a href="https://www.hafogvatn.is">Marine & Freshwater Research Institute</a>') |>
  addTiles(urlTemplate = "https://heima.hafro.is/~einarhj/tiles/haf/050m/{z}/{x}/{-y}.png",
           group = "Botnlag",
           options = tileOptions(minZoom = 5, maxZoom = 10)) |>
  addTiles(urlTemplate = "https://heima.hafro.is/~einarhj/tiles/haf/020m/{z}/{x}/{-y}.png",
           group = "Botnlag",
           options = tileOptions(minZoom = 5, maxZoom = 10)) |>
  addTiles(urlTemplate = "https://heima.hafro.is/~einarhj/tiles/lhg/{z}/{x}/{-y}.png",
           group = "Botnlag",
           options = tileOptions(minZoom = 5, maxZoom = 10)) |>
  addTiles(urlTemplate = "https://heima.hafro.is/~einarhj/tiles/vestfirdir_dpi/{z}/{x}/{-y}.png",
           group = "Botnlag",
           options = tileOptions(minZoom = 5, maxZoom = 10))
## add raster ------------------------------------------------------------------
l <-
  l |>
  addRasterImage(g01,
                 colors = p01,
                 opacity = 1,
                 group = "Lína",
                 maxBytes = Inf) |>
  addRasterImage(g02,
                 colors = p02,
                 opacity = 1,
                 group = "Þorskfiskanet",
                 maxBytes = Inf) |>
  addRasterImage(g06,
                 colors = p06,
                 opacity = 1,
                 group = "Fiskibotnvarpa",
                 maxBytes = Inf)
## Add rest --------------------------------------------------------------------
l <-
  l |>
  addPolylines(data = tillogur|> filter(tillaga == "A"),
               opacity = 1,
               color = col2hex("green"),
               group = "Tillaga A") |>
  addPolylines(data = tillogur|> filter(tillaga == "B"),
               opacity = 1,
               color = col2hex("blue"),
               group = "Tillaga B") |>
  addPolylines(data = tillogur|> filter(tillaga == "C"),
               opacity = 1,
               color = col2hex("red"),
               group = "Tillaga C") |>
  addPolygons(data = korall,
              opacity = 1,
              weight = 1,
              fillOpacity = 0.3,
              fillColor = col2hex("cyan"),
              color = col2hex("cyan"),
              group = "Reglugerðir")
## add contols -----------------------------------------------------------------
l <-
  l |>
  addLayersControl(baseGroups = c("Hnöttur", "Botnlag"),
                   overlayGroups = c("Lína", "Þorskfiskanet", "Fiskibotnvarpa",
                                     "Tillaga A", "Tillaga B", "Tillaga C",
                                     "Reglugerðir"),
                   options = layersControlOptions(collapsed = FALSE)) |>
  hideGroup(c("Lína", "Þorskfiskanet", "Tillaga B", "Tillaga C", "Reglugerðir"))
l

## save stuff ------------------------------------------------------------------
l |> write_rds("data/leaflet_sa_kantur_fridun.rds")
if(FALSE) {
  # Need to find a proper locations
  htmlwidgets::saveWidget(l,
                          file="/net/hafri.hafro.is/export/home/hafri/einarhj/public_html/vernd2/index.html",
                          selfcontained = FALSE)
  system("chmod -R a+rx /net/hafri.hafro.is/export/home/hafri/einarhj/public_html/vernd2")
  #system("chmod -R a+rx /net/hafri.hafro.is/export/home/www/public/hafro/skip/index_files")
}


