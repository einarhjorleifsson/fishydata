library(leaflet)
image <- nanoparquet::read_parquet("data/hafmynd_coral.parquet")
l <-
  leaflet() |>
  addTiles() |>
  addTiles(urlTemplate = "https://heima.hafro.is/~einarhj/tiles/haf/050m/{z}/{x}/{-y}.png",
           group = "050m",
           options = tileOptions(minZoom = 5, maxZoom = 16)) |>
  addTiles(urlTemplate = "https://heima.hafro.is/~einarhj/tiles/haf/020m/{z}/{x}/{-y}.png",
           group = "020m",
           options = tileOptions(minZoom = 5, maxZoom = 16)) |>
  addTiles(urlTemplate = "https://heima.hafro.is/~einarhj/tiles/lhg/{z}/{x}/{-y}.png",
           group = "lhg",
           options = tileOptions(minZoom = 0, maxZoom = 16)) |>
  leaflet::addMarkers(lng = image$lon,
                      lat = image$lat,
                      popup = image$content,
                      group = "vernd",
                      clusterOptions = markerClusterOptions(spiderfyOnMaxZoom = 6))
htmlwidgets::saveWidget(l,
                        file="/home/hafri/einarhj/public_html/data/hafmynd/index.html",
                        selfcontained = FALSE)
system("chmod -R a+rx /home/hafri/einarhj/public_html/data/hafmynd")
