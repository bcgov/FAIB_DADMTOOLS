#' Check for MULTISURFACE geometry in a File Geodatabase feature class.
#'
#' Reads a feature class directly from a File Geodatabase using
#' `sf::st_read()` and checks the geometry types of the features.
#' This function is used to identify `MULTISURFACE` geometries, which
#' are not supported by the GDAL import workflow used by `dadmtools`.
#'
#' The layer is read directly rather than querying a specific geometry
#' column name. This allows the GDAL/OpenFileGDB driver to handle the
#' geometry field appropriately.
#'
#' @param gdb_path Character. The full path to the File Geodatabase (.gdb).
#' @param layer_name Character. The name of the feature class (layer) within
#'   the geodatabase to check.
#' @param nrow Integer. The maximum number of features to inspect.
#'   Defaults to 1000.
#'
#' @return Logical. Returns `TRUE` if a `MULTISURFACE` geometry is detected
#'   within the features inspected, and `FALSE` otherwise.
#'
#' @export
#'
#' @examples
#' in_gdb <- "E:/data/indata.gdb"
#' in_fc  <- "parks"
#'
#' check_geom <- check_multisurface_gdb(
#'   gdb_path = in_gdb,
#'   layer_name = in_fc,
#'   nrow = 1000
#' )
#'
#' if (!check_geom) {
#'   message(glue::glue(
#'     "No MULTISURFACE geometry found in {in_gdb}/{in_fc}"
#'   ))
#' } else {
#'   warning(glue::glue(
#'     "MULTISURFACE geometry detected in {in_gdb}/{in_fc}. ",
#'     "Data may not import correctly."
#'   ))
#' }



check_multisurface_gdb <- function(gdb_path, layer_name, nrow = 1000) {

  result <- try(
    st_read(
      dsn = gdb_path,
      layer = layer_name,
      quiet = TRUE
    ),
    silent = TRUE
  )

  if (inherits(result, "try-error")) {
    stop(
      "WARNING: Could not read layer '",
      layer_name,
      "' from GDB: ",
      gdb_path
    )
  }

  # Only inspect the first nrow features
  if (nrow(result) > nrow) {
    result <- result[seq_len(nrow), ]
  }

  geom_types <- unique(st_geometry_type(result))

  print(
    glue(
      "Geometry type(s) found in {layer_name}: ",
      paste(geom_types, collapse = ", ")
    )
  )

  return(any(geom_types == "MULTISURFACE"))
}
