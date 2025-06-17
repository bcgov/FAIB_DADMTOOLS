#' Check for MULTISURFACE or other unsupported geometry types in a File Geodatabase.
#' @param gdb_path Character. The full file path to the File Geodatabase (.gdb).
#' @param Character. The name of the feature class (layer) within the geodatabase to check.
#' @param nrow Logical. Returns `TRUE` if unsupported geometry types like `MULTISURFACE` are detected; `FALSE` otherwise.
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
#'   message(glue::glue("No MULTISURFACE geometry found in {in_gdb}/{in_fc}"))
#' } else {
#'   warning(glue::glue("MULTISURFACE geometry detected in {in_gdb}/{in_fc}. Data may not import correctly."))
#' }
check_multisurface_gdb <- function(gdb_path, layer_name, nrow = 1000) {
  geom_candidates <- c("Shape", "Geom","GEOMETRY","GEOM","Geometry","SHAPE")

  for (geom_col in geom_candidates) {
    sql_query <- sprintf("SELECT %s FROM \"%s\" limit %s", geom_col, layer_name, nrow)

    # Try reading using this geometry column
    result <- try(
      st_read(dsn = gdb_path, query = sql_query, quiet = TRUE),
      silent = TRUE
    )

    if (inherits(result, "sf")) {
      # Success! Now check for MultiSurface
      return(any(st_geometry_type(result) == "MULTISURFACE"))
    }
  }

  stop("WARNING: Could not find known geometry column names (shape, geom, geometry) in layer. Unable to check layer for invalid geometry. Review input feature class with geotiff specified in <out_tif_path>.")
}
