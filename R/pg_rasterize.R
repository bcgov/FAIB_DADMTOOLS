

#' Rasterize a PostgreSQL Attribute to GeoTIFF
#'
#' Rasterizes a numeric attribute from a PostgreSQL table to a compressed
#' GeoTIFF using GDAL. Source attributes are joined to polygon geometries
#' in `whse.all_bc_gr_skey` using the `gr_skey` key.
#'
#' The raster is generated directly from a PostgreSQL SQL query, avoiding
#' the need to load the full vector dataset into R. The output grid is
#' defined by the requested coordinate reference system, pixel resolution,
#' and spatial extent.
#'
#' @param table Character string specifying the source PostgreSQL table in
#'   `schema.table` format. The table must contain `gr_skey` and the
#'   attribute specified by `attribute`.
#'
#' @param attribute Character string specifying the source column
#'   to burn into the raster cells. The column should contain numeric
#'   values suitable for representation in the output raster data
#'   type. Numeric suitability is ultimately determined by GDAL.
#'
#' @param output Character string specifying the destination GeoTIFF path,
#'   including the filename and `.tif` extension. Local paths and accessible
#'   network paths are supported. The parent directory must already exist.
#'   The function checks whether an existing output can be overwritten,
#'   but does not explicitly test directory writability before rasterization.
#'
#' @param local_keyring Character string specifying the keyring name passed
#'   to `get_pg_conn_list()` to retrieve PostgreSQL connection settings.
#'   Defaults to `"localpsql"`.
#'
#' @param where Optional character string containing a SQL `WHERE`
#'   condition, without the `WHERE` keyword. Use this to restrict the
#'   source records before rasterization. For example,
#'   `"thlb_fact > 0"`. The condition is inserted directly into the
#'   SQL query and must be valid, trusted SQL for PostgreSQL.
#'
#' @param resolution Positive numeric value specifying the raster pixel
#'   size in the units of the output coordinate reference system.
#'   A single value is used for both the horizontal and vertical
#'   resolution. Defaults to `100`.
#'
#' @param extent Numeric vector of length four specifying the output
#'   raster extent as `c(xmin, ymin, xmax, ymax)`, in the units of the
#'   output coordinate reference system. Defaults to the standard
#'   EPSG:3005 extent used by this function.
#'
#' @param crs Character string specifying the spatial reference assigned
#'   to the output raster, using a GDAL-compatible CRS identifier such as
#'   `"EPSG:3005"`. Defaults to `"EPSG:3005"`. This parameter does not
#'   reproject source geometries or transform the supplied extent; those
#'   must already be expressed in the target CRS.
#'
#' @param nodata Numeric value written as the output raster's NoData
#'   marker. Defaults to `-9999`. Ensure this value does not conflict
#'   with valid attribute values.
#'
#' @param overwrite Logical indicating whether an existing output file
#'   may be replaced. Defaults to `FALSE`.
#'
#' @return Invisibly returns the output file path as a character string.
#'   The function writes the GeoTIFF to disk as a side effect.
#'
#' @details
#' PostgreSQL connection settings are retrieved using
#' `get_pg_conn_list()` and validated using `.pg_transfer_connect()`.
#' GDAL then connects directly to PostgreSQL to execute the rasterization
#' query. The connection settings must include the host, port, database
#' name, username, and password required by GDAL.
#'
#' Polygon geometries are obtained from `whse.all_bc_gr_skey`, which must
#' contain `gr_skey` and a geometry column named `geom`. The source table
#' is joined to this geometry table using `gr_skey`.
#'
#' The output uses LZW compression. Rasterization burns the selected
#' attribute value into each covered cell. Where multiple features
#' contribute to the same raster cell, the resulting value can depend
#' on feature processing order.
#'
#' The source geometries, spatial extent, and CRS must be compatible.
#' This function assigns the requested output CRS but does not perform
#' coordinate transformation. Use a separate reprojection workflow when
#' the source geometries are not already expressed in the target CRS.
#'
#' The GDAL installation available to `sf` must support the PostgreSQL
#' driver. Database permissions must allow access to the source and
#' geometry tables.
#'
#' @examples
#' \dontrun{
#' # Rasterize THLB factors greater than zero using the default settings
#' pg_rasterize(
#'   table = "whse.thlb_tsr_blend_with_proxy",
#'   attribute = "thlb_fact",
#'   output = "D:/Data/thlb_fact.tif",
#'   where = "thlb_fact > 0"
#' )
#' }
#'
#' @seealso [sf::gdal_utils()], [get_pg_conn_list()]
#'
#' @export

pg_rasterize <- function(
    table,
    attribute,
    output,
    local_keyring = "localpsql",
    where = NULL,
    resolution = 100,
    extent = c(
      159587.5, 173787.5,
      1881187.5, 1748187.5
    ),
    crs = "EPSG:3005",
    nodata = -9999,
    overwrite = FALSE
) {

  # ----------------------------------------------------------
  # VALIDATE INPUTS
  # ----------------------------------------------------------

  validate_name <- function(x, name) {
    if (
      !is.character(x) ||
      length(x) != 1L ||
      is.na(x) ||
      !nzchar(trimws(x))
    ) {
      stop(name, " must be a non-empty name.", call. = FALSE)
    }
  }

  validate_path <- function(x, name) {
    if (
      !is.character(x) ||
      length(x) != 1L ||
      is.na(x) ||
      !nzchar(trimws(x))
    ) {
      stop(name, " must be a non-empty path.", call. = FALSE)
    }
  }

  validate_name(table, "table")
  validate_name(attribute, "attribute")
  validate_path(output, "output")
  validate_name(local_keyring, "local_keyring")
  validate_name(crs, "crs")

  if (!grepl(
    "^[A-Za-z_][A-Za-z0-9_]*\\.[A-Za-z_][A-Za-z0-9_]*$",
    table
  )) {
    stop(
      "table must use schema.table format.",
      call. = FALSE
    )
  }

  if (!grepl("^[A-Za-z_][A-Za-z0-9_]*$", attribute)) {
    stop(
      "attribute must be a simple SQL column name.",
      call. = FALSE
    )
  }

  if (
    !is.numeric(resolution) ||
    length(resolution) != 1L ||
    !is.finite(resolution) ||
    resolution <= 0
  ) {
    stop(
      "resolution must be a positive finite number.",
      call. = FALSE
    )
  }

  if (
    !is.numeric(extent) ||
    length(extent) != 4L ||
    any(!is.finite(extent)) ||
    extent[1] >= extent[3] ||
    extent[2] >= extent[4]
  ) {
    stop(
      "extent must be c(xmin, ymin, xmax, ymax).",
      call. = FALSE
    )
  }

  if (
    !is.numeric(nodata) ||
    length(nodata) != 1L ||
    !is.finite(nodata)
  ) {
    stop("nodata must be a finite number.", call. = FALSE)
  }

  if (
    !is.logical(overwrite) ||
    length(overwrite) != 1L ||
    is.na(overwrite)
  ) {
    stop("overwrite must be TRUE or FALSE.", call. = FALSE)
  }

  if (!is.null(where)) {
    if (
      !is.character(where) ||
      length(where) != 1L ||
      is.na(where) ||
      !nzchar(trimws(where))
    ) {
      stop(
        "where must be NULL or a non-empty SQL condition.",
        call. = FALSE
      )
    }
  }

  if (file.exists(output) && !overwrite) {
    stop(
      "Output already exists. Set overwrite = TRUE to replace it: ",
      output,
      call. = FALSE
    )
  }

  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("Package DBI is required.", call. = FALSE)
  }

  if (!requireNamespace("sf", quietly = TRUE)) {
    stop("Package sf is required.", call. = FALSE)
  }

  # ----------------------------------------------------------
  # CONNECT TO POSTGRESQL
  # ----------------------------------------------------------

  local_cfg <- get_pg_conn_list(
    keyring_name = local_keyring
  )

  required <- c(
    "driver",
    "host",
    "port",
    "dbname",
    "user",
    "password"
  )

  missing <- required[
    !vapply(
      required,
      function(x) {
        !is.null(local_cfg[[x]]) &&
          length(local_cfg[[x]]) == 1L &&
          !is.na(local_cfg[[x]])
      },
      logical(1)
    )
  ]

  if (length(missing) > 0L) {
    stop(
      "Missing database connection settings: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  # Own and close only the connection created by this function.
  local_con <- NULL

  on.exit({
    if (!is.null(local_con)) {
      try(
        DBI::dbDisconnect(local_con),
        silent = TRUE
      )
    }
  }, add = TRUE)

  local_con <- .pg_transfer_connect(local_cfg)

  if (!DBI::dbIsValid(local_con)) {
    stop(
      "The local database connection is not valid.",
      call. = FALSE
    )
  }

  local_db <- DBI::dbGetQuery(
    local_con,
    "SELECT current_database() AS database_name"
  )$database_name[[1]]

  message("Connected to database: ", local_db)

  # ----------------------------------------------------------
  # BUILD GDAL POSTGRESQL CONNECTION STRING
  # ----------------------------------------------------------

  # Escape values for a libpq-style connection string.
  quote_conn_value <- function(x) {
    x <- as.character(x)
    x <- gsub("\\\\", "\\\\\\\\", x)
    x <- gsub("'", "\\\\'", x, fixed = TRUE)
    paste0("'", x, "'")
  }

  connection <- paste0(
    "PG:host=", quote_conn_value(local_cfg$host),
    " port=", quote_conn_value(local_cfg$port),
    " dbname=", quote_conn_value(local_cfg$dbname),
    " user=", quote_conn_value(local_cfg$user),
    " password=", quote_conn_value(local_cfg$password)
  )

  # ----------------------------------------------------------
  # BUILD SQL QUERY
  # ----------------------------------------------------------

  sql <- sprintf(
    paste(
      "SELECT a.%s AS %s, b.geom",
      "FROM %s a",
      "JOIN whse.all_bc_gr_skey b",
      "ON a.gr_skey = b.gr_skey"
    ),
    attribute,
    attribute,
    table
  )

  if (!is.null(where)) {
    sql <- paste(sql, "WHERE", where)
  }

  # ----------------------------------------------------------
  # PREPARE OUTPUT
  # ----------------------------------------------------------

  output_dir <- dirname(output)

  if (!dir.exists(output_dir)) {
    stop(
      "Output directory does not exist: ",
      output_dir,
      call. = FALSE
    )
  }

  if (file.exists(output) && overwrite) {
    if (!file.remove(output)) {
      stop(
        "Could not remove existing output: ",
        output,
        call. = FALSE
      )
    }
  }

  # ----------------------------------------------------------
  # RASTERIZE
  # ----------------------------------------------------------

  options <- c(
    "-a_srs", crs,
    "-tr",
    as.character(resolution),
    as.character(resolution),
    "-te",
    as.character(extent[1]),
    as.character(extent[2]),
    as.character(extent[3]),
    as.character(extent[4]),
    "-sql", sql,
    "-a", attribute,
    "-a_nodata", as.character(nodata),
    "-co", "COMPRESS=LZW"
  )

  message("Rasterizing ", table, ".", attribute)
  message("Output: ", output)

  sf::gdal_utils(
    util = "rasterize",
    source = connection,
    destination = output,
    options = options,
    quiet = FALSE
  )

  if (!file.exists(output)) {
    stop(
      "GDAL did not create the expected output file.",
      call. = FALSE
    )
  }

  message("Raster created successfully: ", output)

  invisible(output)
}
