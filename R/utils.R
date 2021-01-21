library(magrittr)
library(dplyr)
library(lubridate)
library(purrr)
library(stringr)
library(rstac)

if_null <- function(x, y) {
  if (is.null(x))
    return(y)
  x
}

.set_url_query_string <- function(url, query_string = NULL) {
  if (is.null(query_string) || !stringr::str_detect(file, "^(https?://)"))
    return(url)
  
  ensurer::ensure_that(query_string, all(nzchar(names(.))))
  sep <- "?"
  if (stringr::str_detect(url, "^(https?://.+/.*\\?.*)")) sep <- "&"
  paste0(url, sep, paste0(names(query_string), "=", 
                          query_string, collapse = "&"))
}

.set_url_gdal_vsicurl <- function(url) {
  stringr::str_replace(url, "^(https?://)", "/vsicurl/\\1")
}

get_bdc_access_key <- function() {
  list(access_token = Sys.getenv("BDC_ACCESS_KEY"))
}

.get_gdal_info <- function(file, http_query = NULL) {
  file <- file[[1]]
  file <- path.expand(file)
  file <- .set_url_query_string(file, http_query)
  file <- .set_url_gdal_vsicurl(file)
  do.call(gdalUtils::gdalinfo, list(datasetname = file, proj4 = TRUE))
}
file <- "http://brazildatacube.dpi.inpe.br/data/d006/Mosaic/CB4_64_16D_STK/v001/022025/2020-07-27_2020-08-11/CB4_64_16D_STK_v001_022025_2020-07-27_2020-08-11_EVI.tif"
info <- .get_gdal_info(file, http_query = get_bdc_access_key())

# test <- sf:::CPL_read_gdal(
#   fname = .set_url_gdal_vsicurl(.set_url_query_string(file, get_bdc_access_key())),
#   options = "", driver = "GTiff", read_data = TRUE, NA_value = -9999,
#   RasterIO_parameters = list(nXOff = 1, nYOff = 1, nXSize = 10, nYSize = 10))

.get_raster_size <- function(info) {
  info %>% 
    stringr::str_subset("Size is ") %>%
    ensurer::ensure_that(length(.) == 1) %>% 
    stringr::str_remove("^.*Size is ") %>% 
    stringr::str_split(", ", n = 2) %>%
    unlist() %>% 
    as.integer() %>%
    ensurer::ensure_that(length(.) == 2) %>% 
    magrittr::set_names(c("n_cols", "n_rows"))
}
.get_raster_size(info = info)

.get_block_size <- function(info) {
  info %>% 
    stringr::str_subset("Block=") %>% 
    ensurer::ensure_that(length(.) > 0) %>% 
    stringr::str_remove("^.*Block=") %>% 
    stringr::str_remove(" Type=.*$") %>% 
    stringr::str_split("x", n = 2) %>%
    unlist() %>% 
    as.integer() %>% 
    ensurer::ensure_that(length(.) == 2) %>% 
    magrittr::set_names(c("n_cols", "n_rows"))
}
.get_block_size(info = info)

.get_origin <- function(info) {
  info %>% 
    stringr::str_subset("Origin = \\(") %>% 
    ensurer::ensure_that(length(.) == 1) %>% 
    stringr::str_remove("^Origin = \\(") %>% 
    stringr::str_remove("\\)$") %>% 
    stringr::str_split(",", n = 2) %>%
    unlist() %>% 
    as.numeric() %>%
    ensurer::ensure_that(length(.) == 2) %>%
    magrittr::set_names(c("x", "y"))
}
.get_origin(info = info)

.get_resolution <- function(info) {
  info %>% 
    stringr::str_subset("Pixel Size = \\(") %>% 
    ensurer::ensure_that(length(.) == 1) %>% 
    stringr::str_remove("^Pixel Size = \\(") %>% 
    stringr::str_remove("\\)$") %>% 
    stringr::str_split(",", n = 2) %>%
    unlist() %>% 
    as.numeric() %>%
    ensurer::ensure_that(length(.) == 2) %>%
    magrittr::set_names(c("res_x", "res_y"))
}
.get_resolution(info = info)

.get_n_layers <- function(info) {
  info %>% 
    stringr::str_subset("Block=") %>% 
    ensurer::ensure_that(length(.) > 0) %>% 
    length()
}
.get_n_layers(info = info)

.get_proj4_string <- function(info) {
  info %>% 
    stringr::str_subset("\\+proj=") %>% 
    ensurer::ensure_that(length(.) == 1) %>% 
    stringr::str_remove_all("'")
}
.get_proj4_string(info = info)

open_rc <- function(file, datetime, asset_name, tile, ..., 
                    .http_query = NULL, .max_connections = 1,
                    .quiet = FALSE) {

  stopifnot(length(file) == length(datetime))
  stopifnot(length(file) == length(asset_name))
  stopifnot(length(file) == length(tile))

  images <- dplyr::tibble(file = file, 
                          datetime = lubridate::as_datetime(datetime), 
                          asset_name = asset_name, 
                          tile = tile) %>% 
    dplyr::nest_by(tile, datetime, .key = "assets") 
  # %>% 
  #   dplyr::ungroup()
  
  # set progress bar
  pb <- NULL
  if (!.quiet) {
    pb <- utils::txtProgressBar(min = 0, max = nrow(images) + 1, style = 3)
    on.exit({
      utils::setTxtProgressBar(pb, length(file) + 1)
      close(pb)
    })
  }
  
  .rc_tbl <- function(tile, datetime, assets) {
    # update progress bar
    if (!is.null(pb)) 
      utils::setTxtProgressBar(pb, utils::getTxtProgressBar(pb) + 1)
    
    # call gdal info
    .info <- .get_gdal_info(assets$file[[1]], http_query = .http_query)
    
    size <- .get_raster_size(.info)
    origin <- .get_origin(.info)
    resolution <- .get_resolution(.info)
    
    assets <- dplyr::mutate(assets, n_layers = .get_n_layers(.info)) %>%
      magrittr::set_class(c("rc_assets_tbl", class(.)))
    
    dplyr::tibble(tile = tile,
                  datetime = datetime,
                  n_cols = size[["n_cols"]],
                  n_rows = size[["n_rows"]],
                  origin_x = origin[["x"]],
                  origin_y = origin[["y"]],
                  res_x = resolution[["res_x"]],
                  res_y = resolution[["res_y"]],
                  proj4 = .get_proj4_string(.info),
                  assets = list(assets)) %>%
      magrittr::set_class(c("rc_tbl", class(.))) %>% 
      list() %>% 
      magrittr::set_class(c("rc_lst", class(.)))
  }

  images %>%  
    dplyr::mutate(rc_obj = .rc_tbl(tile, datetime, assets)) %>%
    dplyr::select(tile, datetime, rc_obj)
}

#---- TEST 1 ----
items <- rstac::stac("http://brazildatacube.dpi.inpe.br/stac/") %>%
  rstac::stac_search(collections = "CB4_64_16D_STK-1",
                     bbox = c(-47.02148, -12.98314, 
                              -42.53906, -17.35063),
                     limit = 1000) %>%
  rstac::get_request() %>% 
  rstac::items_fetch()

files <- c(items %>% rstac::items_reap("assets", "EVI", "href"),
           items %>% rstac::items_reap("assets", "NDVI", "href"))
dates <- c(items %>% rstac::items_reap("properties", "datetime"),
           items %>% rstac::items_reap("properties", "datetime"))
tiles <- c(items %>% rstac::items_reap("properties", "bdc:tiles"),
           items %>% rstac::items_reap("properties", "bdc:tiles"))
#cloud_cov <- items %>% rstac::items_reap("properties", "eo:cloud_cover")
bands <- rep(c("EVI", "NDVI"), each = length(files) / 2)

rc1 <- open_rc(file = files, datetime = dates, asset_name = bands, 
               tile = tiles, cloud_cover = cloud_cov, 
               .http_query = get_bdc_access_key())

#---- PROTOTYPE ----
rc_intersects <- function(rc_obj, bbox) UseMethod("rc_intersects", rc_obj)
rc_intersects.rc_tbl <- function(rc_obj, bbox) {
  stopifnot(inherits(rc_obj, "rc_tbl"))
  stopifnot(inherits(bbox, "bbox"))
  
  ###
  rc_obj$tile == "022025"
}
rc_intersects.rc_lst <- function(rc_obj, bbox) {
  stopifnot(inherits(rc_obj, "rc_lst"))
  stopifnot(inherits(bbox, "bbox"))
  purrr::map_lgl(rc_obj, rc_intersects.rc_tbl, bbox = bbox)
}
rc_crop <- function(rc_obj, bbox) UseMethod("rc_crop", rc_obj)
rc_crop.rc_tbl <- function(rc_obj, bbox) {
  stopifnot(inherits(rc_obj, "rc_tbl"))
  stopifnot(inherits(bbox, "bbox"))
  list(rc_obj) %>% 
    magrittr::set_class(c("rc_lst", class(.)))
}
rc_crop.rc_lst <- function(rc_obj, bbox) {
  stopifnot(inherits(rc_obj, "rc_lst"))
  stopifnot(inherits(bbox, "bbox"))
  rc_obj
}

bbox <- st_bbox(c(xmin = 16.1, xmax = 16.6, ymax = 48.6, ymin = 47.9), 
                crs = st_crs(4326))
rc1 %>%
  dplyr::filter(rc_intersects(rc_obj, bbox))

rc1 %>% 
  dplyr::mutate(rc_cropped = rc_crop(rc_obj, bbox))


rc_get_intervals <- function(rc_obj,
                             start_date = "2000-02-18",
                             end_date = "2021-01-20",
                             period = lubridate::period(1, "month")) UseMethod("rc_get_intervals", rc_obj)

rc_get_intervals.rc_tbl <- function(rc_obj,
                                    start_date = "2000-02-18",
                                    end_date = "2021-01-20",
                                    period = lubridate::period(1, "month")) {
  
  start_date <- lubridate::as_datetime(start_date)
  end_date <- lubridate::as_datetime(end_date)

  x <- rc_obj$datetime

  breaks <- seq(from = start_date, to = end_date + 1, by = "1 month")

  cut(x = x, breaks = breaks, labels = FALSE) %>% 
    as.factor()
}

rc_get_intervals.rc_lst <- function(rc_obj,
                                    start_date = "2000-02-18",
                                    end_date = "2021-01-20",
                                    period = lubridate::period(1, "month")) {
  
  start_date <- lubridate::as_datetime(start_date)
  end_date <- lubridate::as_datetime(end_date)
  
  x <- purrr::map(rc_obj, "datetime") %>% 
    unlist() %>% 
    lubridate::as_datetime()
  
  breaks <- seq(from = start_date, to = end_date + 1, by = "1 month")

  cut(x = x, breaks = breaks, labels = FALSE) %>% 
    as.factor()
}

rc_get_intervals(rc_obj, start_date = "2000-01-01")

rc1 %>% dplyr::mutate(interval = rc_get_intervals(rc_obj, 
                                                  start_date = "2000-01-01"))

rc1 %>% 
  dplyr::group_by(tile, interval = rc_get_intervals(rc_obj, 
                                                    start_date = "2000-01-01"))


dplyr::mutate(rc_obj = rc::rc_chunks(rc_obj,
                                       size = c(512, 512),
                                       overlap = c(10, 10))) %>%
  dplyr::mutate(chunk = rc::rc_map(chunk, fun = function(NDVI) { NDVI })) %>%
  dplyr::mutate(file_out = rc::rc_merge(chunk))


rc2$origin

rc_chunks <- function(rc, n_cols, n_rows) {
  
  size <- rc$raster_size
  cols <- c(seq(1, size[["n_cols"]] - 1, by = n_cols), size[["n_cols"]] + 1)
  rows <- c(seq(1, size[["n_rows"]] - 1, by = n_rows), size[["n_rows"]] + 1)
  
  offset <- expand.grid(x_off = cols[seq_len(length(cols) - 1)], 
                        y_off = rows[seq_len(length(rows) - 1)])
  
  size <- expand.grid(x_size = diff(cols), y_size = diff(rows))
  
  slide_data(cbind(offset, size))
}

rc$block_size
x <- rc_chunks(rc, n_cols = 512, n_rows = 512 * 4)

dplyr::as_tibble(rc$metadata)

rc_option_env <- new.env()

rc_option <- function(...) {
  dots <- list(...)
  if (length(dots) == 0)
    return(as.list(rc_option_env))
  stopifnot(length(dots) == 1)
  key <- names(dots)
  if (is.null(key)) {
    stopifnot(is.character(dots[[1]]))
    return(rc_option_env[[dots[[1]]]])
  }
  if (is.null(dots[[key]])) {
    if (key %in% names(rc_option_env)) 
      rm(list = key, envir = rc_option_env)
  } else
    rc_option_env[[key]] <- dots[[key]]
  invisible(NULL)
}
rc_option()
rc_option(extension = ".tif")
rc_option("extension")

rc_chunk <- function(x_off, y_off, x_size, y_size) {
  structure(list(
    x_off = x_off,
    y_off = y_off,
    x_size = x_size,
    y_size = y_size
  ), class = "rc_chunk")
}

crop.rc_chunk <- function(chunk, file, options, out_file = NULL) {
  
  out_file <- if_null(out_file, 
                     tempfile(fileext = rc_option("extension")))
  
  gdalUtils::gdal_translate(src_dataset = file,
                            dst_dataset = out_file,
                            )
  
}



split_clusterR <- function(x, n_tiles, pad_rows, fun,
                           args = NULL, export = NULL, cl = NULL, ...) {
  
  stopifnot(n_tiles > 0)
  stopifnot(pad_rows >= 0)
  
  breaks <- ceiling(seq(1, nrow(x) + 1, length.out = n_tiles + 1))
  breaks <- mapply(list,
                   r1 = ifelse(breaks - pad_rows < 0, 1,
                               breaks - pad_rows)[seq_len(n_tiles)],
                   r2 = ifelse(breaks + pad_rows - 1 > nrow(x), nrow(x),
                               breaks + pad_rows - 1)[-1:0], SIMPLIFY = FALSE,
                   orig1 = ifelse(breaks - pad_rows < 0, 1,
                                  pad_rows + 1)[seq_len(n_tiles)],
                   orig2 = ifelse(breaks - pad_rows < 0,
                                  breaks[-1:0] - breaks[seq_len(n_tiles)],
                                  breaks[-1:0] - breaks[seq_len(n_tiles)]
                                  + pad_rows))
  
  if (is.null(cl)) {
    
    cl <- raster::getCluster()
    on.exit(raster::returnCluster(), add = TRUE)
    stopifnot(!is.null(cl))
  }
  
  # export
  
  if (!is.null(export)) {
    
    parallel::clusterExport(cl, export)
  }
  
  # start process cluster
  pb <- txtProgressBar(max = length(breaks) + 1, style = 3)
  
  .arg_fun <- function(i) {
    
    setTxtProgressBar(pb, i)
    c(list(b = breaks[[i]]), x = x, fun = fun, args)
  }
  
  .io_fun <- function(b, x, fun, ...) {
    
    # crop adding pads
    x <- raster::crop(x, raster::extent(x, r1 = b$r1, r2 = b$r2,
                                        c1 = 1, c2 = ncol(x)))
    
    # process it
    res <- fun(x, ...)
    stopifnot(inherits(res, c("RasterLayer", "RasterStack", "RasterBrick")))
    
    # crop removing pads
    res <- raster::crop(res, raster::extent(res, r1 = b$orig1, r2 = b$orig2,
                                            c1 = 1, c2 = ncol(res)))
    
    # export to temp file
    filename <- tempfile(fileext = ".tif")
    raster::writeRaster(res, filename = filename, overwrite = TRUE)
    
    filename
  }
  
  tmp_tiles <- snow::dynamicClusterApply(cl = cl, fun = .io_fun,
                                         n = length(breaks),
                                         argfun = .arg_fun)
  
  setTxtProgressBar(pb, length(breaks) + 1)
  close(pb)
  on.exit(unlink(tmp_tiles))
  # end process cluster
  
  # merge to save final result with '...' parameters
  message("Merging files...", appendLF = TRUE)
  do.call(raster::merge, c(lapply(tmp_tiles, raster::brick), list(...)))
}


library(rstac)
library(magrittr)
library(purrr)


items %>%
  purrr::pluck("features") %>% 
  purrr::map(function(x) {
    purrr::pluck(x, "properties") %>% 
      purrr::map(function(x) {
        if (length(x) == 1) return(x)
        list(x)
      }) %>% 
      dplyr::as_tibble()
  }) %>% 
  dplyr::bind_rows()

items %>% purrr::pluck("features", 1, "assets") %>% names()
files <- items %>% rstac::items_reap("assets", "EVI", "href")

items$features[[1]]$properties$`bdc:metadata`
