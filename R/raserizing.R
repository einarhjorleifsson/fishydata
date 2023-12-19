
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
                          rasterize = TRUE) {
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
      collect() |> 
      raster_helper(cap_percentile = 0.99)
  }
  return(q)
}
