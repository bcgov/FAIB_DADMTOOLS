#' Percent of each cell covered by a polygon layer
#'
#' Creates a raster on the gr_skey grid where each cell holds the percentage
#' (0-100) of its area covered by ANY polygon in the layer, regardless of the
#' polygons' attribute values. Overlapping polygons are not double counted:
#' two polygons covering the same half of a cell give 50, not 100.
#'
#' Coverage is calculated exactly with exactextractr. By default the layer is
#' processed in square tiles; within each tile the polygons are clipped to the
#' tile and dissolved into one shape before measuring, which is what prevents
#' double counting. If the layer is known to have no overlaps, set
#' overlap_free = TRUE to skip the clipping and dissolving. Output is written
#' one band of tiles at a time, so memory use depends on tile_size, not on the
#' size of the output grid.
#'
#' @param src_sf sf/sfc polygon object, or path to a vector file (gdb,
#'   geopackage, shapefile) with src_lyr naming the layer
#' @param template_tif Path to the gr_skey template raster (or a SpatRaster)
#'   defining the grid
#' @param out_tif_path Directory of the output tif
#' @param out_tif_name Name of the output tif, e.g. 'thlb_pct_cover.tif'
#' @param crop_extent OPTIONAL - extent of output raster c(xmin, xmax, ymin,
#'   ymax) in EPSG:3005, e.g. c(273287.5, 1870587.5, 367787.5, 1735787.5).
#'   Defaults to the full template extent.
#' @param src_lyr OPTIONAL - layer name when src_sf is a file path
#' @param where OPTIONAL - attribute filter when src_sf is a file path, e.g.
#'   "fire_year = 2023"
#' @param datatype Output datatype. 'INT1U' (default) stores whole percents
#'   0-100, rounded to the nearest percent, so cells less than 0.5 percent
#'   covered become 0. Use 'FLT4S' to keep decimals.
#' @param nodata Value written for cells with no coverage. Defaults to 0, which
#'   matches the other dadmtools rasters (0 = no data).
#' @param overlap_free Set to TRUE only if you know no two polygons in the
#'   layer overlap (e.g. VRI, or a layer already dissolved/flattened). The
#'   polygons are then measured as they are, with no clipping or dissolving in
#'   R, and each cell's shares are added together. This is exact for overlap
#'   free data. If the data does overlap, overlapping areas are counted more
#'   than once (each cell is still capped at 100), so the result is too high.
#'   Defaults to FALSE, which dissolves and is always correct.
#' @param tile_size Tile height and width in metres, rounded to whole cells.
#'   Defaults to 50000 (50 km). Smaller tiles use less memory and make
#'   dissolving cheaper. With overlap_free = TRUE only the height is used: each
#'   band of rows is measured in one pass.
#'
#' @return Path of the output tif
#' @export
#'
#' @examples
#' \dontrun{
#' ## percent of each hectare inside a park, from a gdb layer
#' rasterize_pct_cover(
#'   src_sf       = 'C:/data/parks.gdb',
#'   src_lyr      = 'parks',
#'   template_tif = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
#'   crop_extent  = c(273287.5, 1870587.5, 367787.5, 1735787.5),
#'   out_tif_path = 'C:/projects/output',
#'   out_tif_name = 'parks_pct_cover.tif'
#' )
#' }

rasterize_pct_cover <- function(src_sf,
                                src_lyr     = NULL,
                                where       = NULL,
                                out_tif_path,
                                out_tif_name,
                                datatype    = 'INT1U',
                                template_tif = "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif",
                                crop_extent = c(273287.5,1870587.5,367787.5,1735787.5),
                                nodata      = 0,
                                overlap_free = FALSE,
                                tile_size   = 50000)
{
  start_time <- Sys.time()
  dest_tif <- file.path(out_tif_path, out_tif_name)
  if (file.exists(dest_tif)) file.remove(dest_tif)

  # ---------------------------------------------------------------------------
  # Polygons: keep geometry only (attribute values do not matter)
  # ---------------------------------------------------------------------------
  if (is.character(src_sf)) {
    print(glue("Reading polygons from {src_sf} | layer = {src_lyr}"))
    if (!is_blank(where)) {
      lyr <- if (is_blank(src_lyr)) sf::st_layers(src_sf)$name[1] else src_lyr
      geoms <- sf::st_geometry(sf::st_read(src_sf, query = glue("SELECT * FROM \"{lyr}\" WHERE {where}"), quiet = TRUE))
    } else if (!is_blank(src_lyr)) {
      geoms <- sf::st_geometry(sf::st_read(src_sf, layer = src_lyr, quiet = TRUE))
    } else {
      geoms <- sf::st_geometry(sf::st_read(src_sf, quiet = TRUE))
    }
  } else {
    geoms <- sf::st_geometry(src_sf)
  }
  if (length(geoms) == 0) stop("No polygons to rasterize.")
  if (is.na(sf::st_crs(geoms))) {
    geoms <- sf::st_set_crs(geoms, 3005)
  } else if (!isTRUE(sf::st_crs(geoms) == sf::st_crs(3005))) {
    geoms <- sf::st_transform(geoms, 3005)
  }
  geoms <- geoms[!sf::st_is_empty(geoms)]
  gtypes <- unique(as.character(sf::st_geometry_type(geoms)))
  if (!all(gtypes %in% c("POLYGON", "MULTIPOLYGON"))) {
    stop(glue("rasterize_pct_cover needs polygons; found geometry type(s): {paste(gtypes, collapse = ', ')}"))
  }

  # Bounding box of every polygon, for quick tile selection
  bb <- do.call(rbind, lapply(geoms, function(g) as.numeric(sf::st_bbox(g))))
  colnames(bb) <- c("xmin", "ymin", "xmax", "ymax")

  # ---------------------------------------------------------------------------
  # Output grid
  # ---------------------------------------------------------------------------
  tmpl <- if (inherits(template_tif, "SpatRaster")) template_tif[[1]] else terra::rast(template_tif)[[1]]
  if (!is.null(crop_extent)) {
    tmpl <- terra::crop(tmpl, terra::ext(crop_extent[1], crop_extent[2], crop_extent[3], crop_extent[4]))
  }
  grid  <- terra::rast(tmpl)                       # geometry only, no values in memory
  nr    <- terra::nrow(grid); nc <- terra::ncol(grid)
  res   <- terra::res(grid)
  e     <- as.list(setNames(as.vector(terra::ext(grid)), c("xmin", "xmax", "ymin", "ymax")))  # plain numbers
  tile_cells_x <- max(1, round(tile_size / res[1]))
  tile_cells_y <- max(1, round(tile_size / res[2]))
  n_bands <- ceiling(nr / tile_cells_y); n_cols <- ceiling(nc / tile_cells_x)

  full <- 1000000L                                 # shares stored as millionths of a cell
  print(glue("Percent cover: {length(geoms)} polygons, grid {nr} x {nc} cells, ",
             "{n_bands * n_cols} tiles of {tile_cells_x * res[1] / 1000} km (overlap_free = {overlap_free})"))

  out <- terra::rast(grid)
  terra::writeStart(out, dest_tif, overwrite = TRUE, datatype = datatype, NAflag = nodata,
                    gdal = c("COMPRESS=LZW"), names = "pct_cover")

  for (b in seq_len(n_bands)) {
    r0 <- (b - 1) * tile_cells_y + 1               # first row of the band
    r1 <- min(b * tile_cells_y, nr)
    band_rows <- r1 - r0 + 1
    band_cov  <- integer(band_rows * nc)          # coverage in millionths, row-major
    y_top <- e$ymax - (r0 - 1) * res[2]
    y_bot <- e$ymax - r1 * res[2]
    in_band <- which(bb[, "ymax"] > y_bot & bb[, "ymin"] < y_top)

    if (length(in_band) > 0 && isTRUE(overlap_free)) {
      # Overlap-free: no clipping or dissolving. The band's polygons are just
      # bundled into one multipolygon (st_combine does no geometry work) and
      # measured in a single pass; with no overlaps, each cell's coverage is the
      # sum of its parts. Cell numbers of the band raster are positions in
      # band_cov directly.
      band_rast <- terra::init(terra::rast(terra::ext(e$xmin, e$xmax, y_bot, y_top),
                                           resolution = res, crs = terra::crs(grid)), 0)
      ex <- exactextractr::exact_extract(band_rast, sf::st_sf(geometry = sf::st_combine(geoms[in_band])),
                                         include_cell = TRUE, progress = FALSE)
      cell <- unlist(lapply(ex, `[[`, "cell"), use.names = FALSE)
      if (length(cell) > 0) {
        w   <- as.integer(round(unlist(lapply(ex, `[[`, "coverage_fraction"), use.names = FALSE) * full))
        tot <- rowsum(w, cell, reorder = FALSE)
        pos <- as.integer(rownames(tot))
        band_cov[pos] <- pmin(as.integer(tot[, 1]), full)
      }
      rm(ex, band_rast)
    } else if (length(in_band) > 0) {
      for (t in seq_len(n_cols)) {
        c0 <- (t - 1) * tile_cells_x + 1
        c1 <- min(t * tile_cells_x, nc)
        x_left  <- e$xmin + (c0 - 1) * res[1]
        x_right <- e$xmin + c1 * res[1]
        idx <- in_band[bb[in_band, "xmax"] > x_left & bb[in_band, "xmin"] < x_right]
        if (length(idx) == 0) next

        tile_ext <- terra::ext(x_left, x_right, y_bot, y_top)
        tile_box <- sf::st_as_sfc(sf::st_bbox(c(xmin = x_left, ymin = y_bot, xmax = x_right, ymax = y_top),
                                              crs = sf::st_crs(3005)))
        tile_geoms <- clip_and_dissolve(geoms[idx], tile_box)
        if (length(tile_geoms) == 0) next

        tile_rast <- terra::init(terra::rast(tile_ext, resolution = res, crs = terra::crs(grid)), 0)
        ex <- exactextractr::exact_extract(tile_rast, sf::st_sf(geometry = tile_geoms),
                                           include_cell = TRUE, progress = FALSE)
        cell <- unlist(lapply(ex, `[[`, "cell"), use.names = FALSE)
        if (length(cell) == 0) next
        w <- as.integer(round(unlist(lapply(ex, `[[`, "coverage_fraction"), use.names = FALSE) * full))
        tot <- rowsum(w, cell, reorder = FALSE)    # dissolved, so normally one piece per cell
        tcell <- as.integer(rownames(tot))

        # tile cell -> position in this band
        tnc <- c1 - c0 + 1
        tr  <- (tcell - 1L) %/% tnc                # 0-based row within tile (= within band)
        tc  <- (tcell - 1L) %%  tnc                # 0-based col within tile
        pos <- tr * nc + (c0 - 1L) + tc + 1L
        band_cov[pos] <- pmin(band_cov[pos] + as.integer(tot[, 1]), full)
      }
    }

    pct <- band_cov / (full / 100)                 # 0-100
    if (datatype %in% c("INT1U", "INT2U", "INT2S", "INT4U", "INT4S")) pct <- round(pct)
    pct[pct == 0] <- NA                            # written as nodata
    terra::writeValues(out, pct, r0, band_rows)
    if (b %% 5 == 0 || b == n_bands) print(glue("  rows {r1} of {nr} done"))
  }
  terra::writeStop(out)
  terra::tmpFiles(remove = TRUE)

  print(glue("Percent cover raster written: {dest_tif} ({round(difftime(Sys.time(), start_time, units = 'mins'), 1)} min)"))
  return(dest_tif)
}


#' Clip polygons to a tile and dissolve them into one shape
#' @keywords internal
clip_and_dissolve <- function(g, tile_box) {
  clip <- function(x) suppressWarnings(sf::st_intersection(x, tile_box))
  out <- tryCatch(clip(g), error = function(e) clip(sf::st_make_valid(g)))
  out <- out[!sf::st_is_empty(out)]
  if (length(out) == 0) return(out)
  ## intersections can return lines/points along the tile edge; keep areas only
  if (!all(sf::st_geometry_type(out) %in% c("POLYGON", "MULTIPOLYGON"))) {
    out <- suppressWarnings(sf::st_collection_extract(out, "POLYGON"))
  }
  if (length(out) == 0) return(out)
  if (length(out) > 1) {
    out <- tryCatch(sf::st_union(out), error = function(e) sf::st_union(sf::st_make_valid(out)))
  }
  out
}
