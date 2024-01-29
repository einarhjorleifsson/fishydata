match_nearest_date <- function(x, y, var) {
  
  x.dt <-
    x %>%
    select(vid, datel) %>%
    distinct() %>%
    setDT()
  
  y.dt <-
    y %>%
    select(vid, datel) %>%
    distinct() %>%
    mutate(dummy = datel) %>%
    setDT()
  
  res <-
    x.dt[, date.y := y.dt[x.dt, dummy, on = c("vid", "datel"), roll = "nearest"]] %>%
    as_tibble()
  
  x %>%
    left_join(res,
              by = c("vid", "datel")) %>%
    left_join(y %>% select(vid, date.y = datel, gid_y, {{ var }}),
              by = c("vid", "date.y"))
  
}
