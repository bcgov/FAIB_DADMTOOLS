
#' Validate the coordinate reference system (CRS) of a spatial data source
#'
#' Checks whether a spatial data source uses the expected EPSG coordinate
#' reference system before import. Supports raster, shapefile, GeoPackage,
#' and file geodatabase sources.
#'
#' Oracle sources from the corporate database are assumed to use EPSG:3005
#' and bypass direct CRS inspection.
#'
#' @param src_type Type of source data. Supported values are "raster",
#'   "shp", "shapefile", "gpkg", "geopackage", "gdb", and "oracle".
#' @param src_path Path to the source file or data source identifier.
#' @param src_lyr Optional source layer name for vector data sources.
#' @param expected_epsg Expected EPSG code. Defaults to 3005 (BC Albers).
#'
#' @return Invisibly returns TRUE if CRS validation succeeds.
#' @details An error if the source type is unsupported, the CRS cannot be
#'   determined, or the CRS does not match the expected EPSG code.
#'
#' @keywords internal
#' @export

check_source_crs <- function(
    src_type,
    src_path,
    src_lyr = NULL,
    expected_epsg = 3005
) {
  src_type <- tolower(trimws(src_type))

  # Oracle corporate data is assumed to be in EPSG:3005.
  if (src_type == "oracle") {
    message("Oracle source trusted, assumed to be EPSG:", expected_epsg)
    return(invisible(TRUE))
  }

  if (src_type == "raster") {

    src <- terra::rast(src_path)
    source_crs <- sf::st_crs(terra::crs(src))

  } else if (src_type %in% c(
    "shp", "shapefile", "gpkg", "geopackage", "gdb"
  )) {

    src <- sf::st_read(
      dsn = src_path,
      layer = src_lyr,
      quiet = TRUE
    )

    source_crs <- sf::st_crs(src)

  } else {
    stop(
      "Unsupported source type for CRS validation: ",
      src_type
    )
  }

  source_epsg <- source_crs$epsg

  if (is.na(source_epsg)) {
    stop(
      "Could not determine the source EPSG code for ",
      src_path, " (layer: ", src_lyr, "). Import cancelled."
    )
  }

  if (source_epsg != expected_epsg) {
    stop(
      "Invalid source CRS: EPSG:", source_epsg,
      ". Expected EPSG:", expected_epsg,
      ". Import cancelled for ", src_path,
      " (layer: ", src_lyr, ").",
      "Please reproject the source data to EPSG:3005 before importing."
    )
  }

  message("Source CRS validated: EPSG:", source_epsg)

  invisible(TRUE)
}
