# Various datapreps
#  All but the gridding of the ais data
library(omar)
library(stars)
library(terra)
library(sf)
library(tidyverse)
library(magick)

rb_cap <- function(x, Q = 0.975) {
  q = quantile(x, Q)
  ifelse(x > q, q, x)
}

# Images -----------------------------------------------------------------------
## Import: Searches for all images where substrates starts with Coral ----------
con <- connect_mar()
prefix <- "/u5/hafmynd/Myndgrunnur/Myndir"
image <-
  tbl_mar(con, "hafmynd.cruise") |>
  select(cruise_id = id, cruise = name) |>
  inner_join(tbl_mar(con, "hafmynd.station") |>
               select(cruise_id, station_id = id, station = station_number, lon = longitude, lat = latitude)) |>
  inner_join(tbl_mar(con, "hafmynd.image") |>
               rename(image_id = id)) |>
  filter(!is.na(lon), !is.na(lat)) |>
  filter(between(lon, -180, 180)) |>
  left_join(tbl_mar(con, "hafmynd.image_substrate") |>
              select(substrate_id, image_id) |>
              left_join(tbl_mar(con, "hafmynd.substrate") |>
                          select(substrate_id = id, substrate = name))) |>
  collect() |>
  as_tibble() |>
  mutate(from = paste0(prefix, file_path),
         to = paste0("/home/hafri/einarhj/public_html/data/hafmynd/myndir/", basename(file_path)),
         link = paste0("https://heima.hafro.is/~einarhj/data/hafmynd/myndir/", basename(from))) |>
  mutate(exists = fs::file_exists(from)) |>
  filter(exists == TRUE) |>
  filter(str_starts(substrate, "Coral")) |>
  mutate(
    content =
      paste(sep = "<br/>",
            paste0("<img src = ", link, ">"),
            substrate,
            file_path)
  )
image |> nanoparquet::write_parquet("data/hafmynd_coral.parquet")
## Read images, standardize format and save to web-space -----------------------
for(i in 1:length(image$from)) {
  print(i)
  gc()
  from <- image$from[i]
  to <- image$to[i]
  from |>
    image_read() |>
    image_scale("250x") |>
    image_write(to)
}

# Current coral closures -------------------------------------------------------
gisland::gl_fs_virkar_reglugerdir() |>
  filter(ath == "Verndun kóralsvæða.") |>
  write_sf("data/reglugerdir_korall.gpkg")
# Depth lines ------------------------------------------------------------------
gisland::gl_lhg_dypislinur() |>
  filter(minz %in% c(300, 400, 500)) |>
  write_sf("data/depth_lines.gpkg")
# Survey stations --------------------------------------------------------------
smb <-
  read_csv2("/home/haf/einarhj/cronjobs/skip/smbstodvar_feb2025_2.csv",
            show_col_types = FALSE) %>%
  janitor::clean_names() |>
  rename(lat1 = n_5,
         lon1 = v_6,
         lat2 = n_7,
         lon2 = v_8) |>
  mutate(.rid = 1:n(),
         lon1 = -geo::geoconvert.1(lon1),
         lat1 =  geo::geoconvert.1(lat1),
         lon2 = -geo::geoconvert.1(lon2),
         lat2 =  geo::geoconvert.1(lat2))
smb <-
  smb %>%
  left_join(bind_rows(smb %>% dplyr::select(.rid, x = lon1, y = lat1),
                      smb %>% dplyr::select(.rid, x = lon2, y = lat2)) %>%
              st_as_sf(coords = c("x", "y"),
                       crs = 4326,
                       remove = FALSE) %>%
              group_by(.rid) %>%
              summarise(do_union = FALSE,
                        .groups = "drop") %>%
              st_cast("LINESTRING")) |>
  st_as_sf()
smh <-
  read_csv("/home/haf/einarhj/cronjobs/skip/extas/2023_SMH.csv",
           show_col_types = FALSE) %>%
  select(reitur,
         tognumer,
         smareitur,
         lat1 = kastad_b,
         lon1 = kastad_l,
         lat2 = hift_b,
         lon2 = hift_l,
         vid = skip.skr.nr.) |>
  mutate(.rid = 1:n(),
         lon1 = -geo::geoconvert.1(lon1),
         lat1 =  geo::geoconvert.1(lat1),
         lon2 = -geo::geoconvert.1(lon2),
         lat2 =  geo::geoconvert.1(lat2))
smh <-
  smh %>%
  left_join(bind_rows(smh %>% dplyr::select(.rid, x = lon1, y = lat1),
                      smh %>% dplyr::select(.rid, x = lon2, y = lat2)) %>%
              st_as_sf(coords = c("x", "y"),
                       crs = 4326,
                       remove = FALSE) %>%
              group_by(.rid) %>%
              summarise(do_union = FALSE,
                        .groups = "drop") %>%
              st_cast("LINESTRING")) |>
  st_as_sf()
smb |> write_sf("data/smb.gpkg")
smh |> write_sf("data/smh.gpkg")
# Gear -------------------------------------------------------------------------
arrow::read_parquet("/u3/geo/fishydata/data/gear/older/agf_gear.parquet") |>
  #read_parquet("~/stasi/fishydata/data/gear/older/agf_gear.parquet") |>
  mutate(veidarfaeri = case_when(gid %in% 3:4 ~ "Grásleppunet",
                                 gid %in% c(12, 13, 21) ~ "Lína",
                                 .default = veidarfaeri)) |>
  filter(!gid %in% c(5, 16, 17, 18, 19, 20, 23:25)) |>
  select(gid_trip = gid, veidarfaeri) |>
  arrow::write_parquet("data/gear.parquet")
# grid and save as raster ------------------------------------------------------
dx <- 0.005
dy <- dx / 2   # grid resolution approximately 275 x 275 meters
g <-
  duckdbfs::open_dataset(paste0(fishy_path, "/data/ais/trail")) |>
  # Not in harbour
  filter(.cid > 0,                           # .cid positive -> not in harbour
         (whack == FALSE | is.na(whack))) |> # need to fix this
  filter(gid_trip %in% c(2, 6, 12, 13, 21)) |>
  # Fishing occurred on date
  filter(between(year, 2015, 2024)) |>
  #filter(gid_trip %in% c(6, 14)) |>    # fishtrawl & jiggers
  filter(between(speed, s1, s2)) |>    # use what is already in the data
  filter(between(time, t1, t2)) |>
  mutate(x = lon %/% dx * dx + dx/2,
         y = lat %/% dy * dy + dy/2) |>
  group_by(gid_trip, x, y) |>
  summarise(dt = sum(dt, na.rm = TRUE) / 60,
            .groups = "drop") |>
  collect() |>
  inner_join(gid) |>
  group_by(veidarfaeri, x, y) |>
  summarise(dt = sum(dt),
            .groups = "drop") |>
  filter(dt > 1,              # at least 1 minutes of effort per pixel
         between(x, -30, -10),
         between(y, 62, 68.5)) |>
  group_by(veidarfaeri) |>
  mutate(dt = rb_cap(dt, 0.99)) |>
  ungroup() |>
  mutate(dt = sqrt(dt))
vf <- g$veidarfaeri |> unique() |> sort()
for(i in 1:length(vf)) {
  g |>
    filter(veidarfaeri == vf[i]) |>
    select(x, y, dt) |>
    rast(type = "xyz",
         crs = "epsg:4326") |>
    st_as_stars() |>
    stars::write_stars(paste0("data/raster/2015-2024_effort-", vf[i], ".tif"))
}

