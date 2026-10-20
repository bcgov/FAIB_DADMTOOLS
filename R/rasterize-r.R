#' Rasterize spatial data using Terra
#'
#' @param src_sf Simple feature object
#' @param field field to be rasterized
#' @param template_tif path of template raster
#' @param crop_extent OPTIONAL - extent of output raster <xmin>,<ymax>,<ymin>,<xmax> default c(273287.5,1870587.5,367787.5,1735787.5)
#' @param src_lyr OPTIONAL - input data layer e.g. 'veg_comp_poly'
#' @param out_tif_path path of out TIF defualt D:/Projects/provDataProject
#' @param out_tif_name  name of out TIF
#' @param datatype  default Int64
#' @param nodata No data value, defaults to 0
#' @param touches OPTIONAL - if TRUE, every cell touched by a line or polygon is
#'   burned in. If FALSE (default), a polygon only gets a cell when it covers the
#'   cell's center point. Has no effect on point data.
#' @param largest_area OPTIONAL - if TRUE, each cell gets the value of whatever
#'   covers the largest area of it, where the part of the cell not covered by
#'   any polygon counts as nodata and competes like a polygon. For example, a
#'   cell covered 49% by polygon A, 25% by polygon B and 26% by nothing gets
#'   A's value; a cell covered 26% by A, 25% by B and 49% by nothing is set to
#'   nodata. No single share has to be more than half. Coverage is calculated
#'   exactly with exactextractr. On an exact tie between two polygons, the one
#'   that comes first in the data wins; on an exact tie between a polygon and
#'   nodata, the polygon wins. Polygons only. Cannot be combined with touches.
#'   Takes roughly 1 ms per polygon (about 1-1.5 hours for the full VRI).
#' @param chunk_size OPTIONAL - number of polygons processed per batch when
#'   largest_area = TRUE. Lower it if you run short of memory. Default 50000.
#'
#' @return output tif name
#' @export
#'
#' @examples coming soon

rasterize_r <- function(src_sf,
                          field,
                          template_tif,
                          crop_extent = NULL,
                          src_lyr = NULL,
                          out_tif_path = NULL,
                          out_tif_name = NULL,
                          datatype ='INT4S',
                          nodata = 0,
                          touches = FALSE,
                          largest_area = FALSE,
                          chunk_size = 50000)
{

  if (isTRUE(touches) && isTRUE(largest_area)) {
    stop("Choose either touches = TRUE or largest_area = TRUE, not both.")
  }

  dest_tif <- file.path(out_tif_path, out_tif_name)

  # Check if the file exists
  if (file.exists(dest_tif)) {
    # If exists, delete the file
    file.remove(dest_tif)
  }

  if( !is.null(src_lyr)) {
    in_vect <- terra::vect(x = src_sf, layer = src_lyr)
  } else {
    in_vect <- terra::vect(x = src_sf)

  }


  template_rast <- terra::rast(template_tif)

  if (isTRUE(largest_area)) {
    print(glue("Rasterizing field: {field} using datatype: {datatype} (largest area, nodata competes)"))
    in_sf <- if (inherits(src_sf, "sf") && is.null(src_lyr)) src_sf else sf::st_as_sf(in_vect)
    rast_band <- rasterize_largest_area(in_sf, template_rast, field, crop_extent, chunk_size)
  } else {
    print(glue("Rasterizing field: {field} using datatype: {datatype} (touches = {touches})"))
    rast_band <- terra::rasterize(in_vect, template_rast, field = field, touches = touches, wopt = list(datatype = datatype))
  }

  if( !is.null(crop_extent)) {
    rast_band <-  terra::crop(rast_band, crop_extent, datatype = datatype)
    }
  print(glue("Writing raster: {dest_tif} using datatype: {datatype}"))
  terra::writeRaster(rast_band, dest_tif, datatype = datatype, overwrite = TRUE, NAflag = nodata)

  terra::tmpFiles(remove=TRUE)
  print('Raster created successfully.')
  return(dest_tif)
}


#' Largest-area rasterization where uncovered area competes as nodata
#'
#' exactextractr::exact_extract() gives, for each polygon, every cell it
#' overlaps and the exact fraction of that cell it covers. For each cell this
#' tracks the polygon with the largest fraction and the total fraction covered
#' by all polygons. The uncovered share is 1 - total. A cell keeps its largest
#' polygon only if that polygon's share is at least the uncovered share;
#' otherwise it is set to NA. Polygons are processed in chunks, so memory
#' depends on the number of raster cells rather than the number of polygons.
#'
#' @keywords internal
rasterize_largest_area <- function(in_sf, template_rast, field, crop_extent = NULL, chunk_size = 50000) {

  tmpl <- template_rast[[1]]
  # Crop first so only the output area is processed
  if (!is.null(crop_extent)) tmpl <- terra::crop(tmpl, crop_extent)
  # exact_extract needs cell values to read; a bare grid gets filled with 0
  if (!terra::hasValues(tmpl)) tmpl <- terra::init(tmpl, 0)

  if (!field %in% names(in_sf)) stop(glue("Field {field} not found in the input data."))
  vals <- in_sf[[field]]
  if (!is.numeric(vals)) stop(glue("Field {field} must be numeric for largest-area rasterization."))

  # Shares are stored as integer millionths of a cell to keep memory down
  full <- 1000000L
  n_cell <- terra::ncell(tmpl)
  best_w  <- integer(n_cell)   # largest single-polygon share so far
  best_id <- integer(n_cell)   # row number of the polygon holding it (0 = none)
  total_w <- integer(n_cell)   # share covered by all polygons combined

  n <- nrow(in_sf)
  starts <- seq(1, n, by = chunk_size)
  for (k in seq_along(starts)) {
    idx <- starts[k]:min(starts[k] + chunk_size - 1, n)
    print(glue("  largest area: polygons {min(idx)}-{max(idx)} of {n}"))

    ex <- exactextractr::exact_extract(tmpl, in_sf[idx, ], include_cell = TRUE, progress = FALSE)
    lens <- vapply(ex, nrow, integer(1))
    if (sum(lens) == 0) next
    cell <- unlist(lapply(ex, `[[`, "cell"), use.names = FALSE)
    w    <- as.integer(round(unlist(lapply(ex, `[[`, "coverage_fraction"), use.names = FALSE) * full))
    id   <- rep(idx, lens)
    rm(ex)

    # Add this chunk's coverage to each cell's total (capped at a full cell)
    tot <- rowsum(w, cell, reorder = FALSE)
    tc  <- as.integer(rownames(tot))
    total_w[tc] <- pmin(total_w[tc] + as.integer(tot[, 1]), full)

    # Best polygon per cell within this chunk (earlier polygons win ties)
    o <- order(cell, -w, id)
    cell <- cell[o]; w <- w[o]; id <- id[o]
    keep <- !duplicated(cell)
    cell <- cell[keep]; w <- w[keep]; id <- id[keep]

    # Keep it only if it beats what earlier chunks found for that cell
    upd <- w > best_w[cell]
    best_w[cell[upd]]  <- w[upd]
    best_id[cell[upd]] <- id[upd]
    rm(cell, w, id, o, keep, upd, tot, tc)
  }

  # Nodata wins where the uncovered share is larger than the best polygon's share
  nodata_w <- full - total_w
  best_id[best_id == 0L | best_w < nodata_w] <- NA_integer_
  rm(best_w, total_w, nodata_w)

  out <- terra::rast(tmpl)
  terra::values(out) <- vals[best_id]
  out
}
