library(raster)
library(tidyverse)
library(arrow)
pth <- "/net/hafkaldi.hafro.is/export/home/haf/einarhj/stasi/fishydata/data/"
hyp <- 
  tidyterra:::extract_pal(tidyterra::hypsometric_tints_db, 
                          palette = "etopo1_hypso") |> 
  pull(hex)
raster_helper <- function(g, cap_percentile) {
  if(!missing(cap_percentile)) {
    r <- 
      g |> 
      mutate(z = ifelse(z > quantile(z, 0.99, na.rm = TRUE), quantile(z, 0.99, na.rm = TRUE), z)) |> 
      raster::rasterFromXYZ(crs = "EPSG:3857")
  } else {
    r <- 
      g |>
      raster::rasterFromXYZ(crs = "EPSG:3857")
  }
  return(r)
}


raster_effort <- function(ais, GID = 6, dx = 300, xlim = c(-30, -10), ylim = c(62, 68.5),
                          rasterize = TRUE, scale = TRUE) {
  q <- 
    ais |> 
    filter(between(lon, xlim[1], xlim[2]),
           between(lat, ylim[1], ylim[2]),
           speed >= s1, speed <= s2,
           gid %in% GID) |> 
    mutate(x = x %/% dx * dx + dx/2,
           y = y %/% dx * dx + dx/2,
           z = 1L) |> 
    group_by(x, y) |> 
    summarise(z = sum(z),
              .groups = "drop")
  
  if(rasterize) {
    q <- 
      q |> 
      collect()
    if(scale) {
      q <- 
        q |> 
        mutate(z = scales::rescale(z, na.rm = TRUE))
    }
    q <- 
      q |> 
      raster_helper(cap_percentile = 0.99)
  }
  return(q)
}

sc_arrow <-
  tribble(~gid,  ~s1,  ~s2,
          -199, 1.000, 3.000,
          1,    0.375, 2.750,
          2,    0.125, 2.500,
          3,    0.025, 2.000,
          5,    0.250, 3.250,
          6,    2.625, 5.500,
          7,    2.625, 6.000,
          9,    2.375, 4.000,
          12,    0.050, 2.250,
          14,    1.750, 3.250,
          15,    0.500, 5.500,
          38,    0.250, 4.750,
          40,    0.250, 6.000) |> 
  mutate(gid = as.integer(gid)) |> 
  arrow::as_arrow_table()
# parquet files:
# LB <- open_dataset(paste0(pth, "logbooks/station-for-ais.parquet"))
# CT <- open_dataset(paste0(pth, "logbooks/catch-for-ais.parquet"))
con_ais <- 
  open_dataset(paste0(pth, "ais")) |> 
  left_join(sc_arrow)
r01 <- con_ais |> raster_effort(GID = 1, dx = 400)
r02 <- con_ais |> raster_effort(GID = 2)
r03 <- con_ais |> raster_effort(GID = 3)
r05 <- con_ais |> raster_effort(GID = 5, dx = 600)
r06 <- con_ais |> raster_effort(GID = 6)
r07 <- con_ais |> raster_effort(GID = 7, xlim = c(-40, 10), ylim = c(30, 68.5), dx = 1200)
r09 <- con_ais |> raster_effort(GID = 9)
r14 <- con_ais |> raster_effort(GID = 14)