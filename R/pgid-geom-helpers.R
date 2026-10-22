#' Read pgid + geometry from Postgres into an sf object
#'
#' Runs `sql` (which must return `pk` and one geometry column, and may return
#' overlap group fields) and returns it as sf in EPSG:3005, ordered by `pk`.
#' Ordering by pgid makes largest-area ties deterministic (lowest pgid wins).
#' Only the columns the query selects are read, so keep it to pgid + geometry
#' to limit RAM.
#'
#' @param sql SQL returning `pk` and a geometry column
#' @param conn Open DBI connection to Postgres
#' @param pk Name of the id field. Defaults to "pgid"
#' @return sf object
#' @keywords internal
read_pgid_geom <- function(sql, conn, pk = "pgid") {
  sql <- glue("SELECT * FROM ({sql}) AS q ORDER BY {pk}")
  out <- sf::st_read(conn, query = sql, crs = 3005, quiet = TRUE)
  if (nrow(out) == 0) stop("No features returned from the query; nothing to rasterize.")
  for (cast_type in c("MULTIPOLYGON", "MULTIPOINT", "MULTILINESTRING")) {
    cast_sf <- tryCatch(sf::st_cast(out, cast_type), error = function(e) NULL)
    if (!is.null(cast_sf)) return(cast_sf)
  }
  out
}


#' Build a WHERE clause selecting one overlap group
#'
#' Turns one row of overlap group values into
#' "q.col = 2021 AND q.name = 'O''Brien' AND q.code IS NULL".
#' Columns are prefixed with the alias `q`.
#'
#' @param row One-row data.frame of group field values
#' @return character SQL condition
#' @keywords internal
build_overlap_where <- function(row) {
  parts <- vapply(names(row), function(col) {
    value <- row[1, col]
    if (is.na(value)) {
      glue("q.{col} IS NULL")
    } else if (is.character(value)) {
      glue("q.{col} = '{gsub(\"'\", \"''\", value)}'")
    } else {
      glue("q.{col} = {value}")
    }
  }, character(1))
  paste(parts, collapse = " AND ")
}


#' Create the non-spatial attribute table from a staging table with geometry
#'
#' Copies every column except geometry (or only `flds_to_keep` + `pk`) from the
#' staging table into a new table, so the attribute table holds no hidden
#' geometry and pgid values are carried over unchanged.
#'
#' @param stage_schema Schema of the staging table
#' @param stage_tbl Name of the staging table (has pk and geometry)
#' @param dst_schema Schema of the output attribute table
#' @param dst_tbl Name of the output attribute table
#' @param pk Primary key field. Defaults to "pgid"
#' @param pg_conn_param Keyring object of Postgres credentials
#' @param flds_to_keep Optional comma separated fields to keep. Blank keeps all
#' @param tbl_comment COMMENT ON TABLE statement for the output table
#' @return None
#' @keywords internal
stage_to_attr_tbl <- function(stage_schema,
                              stage_tbl,
                              dst_schema,
                              dst_tbl,
                              pk = "pgid",
                              pg_conn_param,
                              flds_to_keep = NULL,
                              tbl_comment = NULL)
{
  cols <- sql_to_df(glue("SELECT column_name, udt_name
                          FROM information_schema.columns
                          WHERE table_schema = '{stage_schema}' AND table_name = '{stage_tbl}'
                          ORDER BY ordinal_position;"), pg_conn_param)

  ## never copy geometry, or the geometry length/area style fields ogr_to_tbl drops
  drop_cols <- c("shape", "geometry", "geom", "_ogr_geometry_", "geometry_l", "geometry_length")
  attr_cols <- cols$column_name[cols$udt_name != "geometry" & !(cols$column_name %in% drop_cols)]

  if (!is_blank(flds_to_keep)) {
    keep <- trimws(strsplit(gsub(",$", "", flds_to_keep), ",")[[1]])
    missing <- setdiff(keep, attr_cols)
    if (length(missing) > 0) {
      stop(glue("Field(s) not found in staging table {stage_schema}.{stage_tbl}: {paste(missing, collapse = ', ')}"))
    }
    ## ogc_fid is kept for parity with ogr_to_tbl's non-spatial import
    attr_cols <- unique(c(intersect("ogc_fid", attr_cols), keep, pk))
  }
  ## pgid last, matching ogr_to_tbl's column order
  attr_cols <- c(setdiff(attr_cols, pk), pk)
  select_cols <- paste0('"', attr_cols, '"', collapse = ", ")

  run_sql_r(glue("DROP TABLE IF EXISTS {dst_schema}.{dst_tbl} CASCADE;"), pg_conn_param)
  run_sql_r(glue("CREATE TABLE {dst_schema}.{dst_tbl} AS
                  SELECT {select_cols} FROM {stage_schema}.{stage_tbl} ORDER BY {pk};"), pg_conn_param)
  run_sql_r(glue("ALTER TABLE {dst_schema}.{dst_tbl} ADD PRIMARY KEY ({pk});"), pg_conn_param)
  if (!is_blank(tbl_comment)) run_sql_r(tbl_comment, pg_conn_param)
  run_sql_r(glue("ANALYZE {dst_schema}.{dst_tbl};"), pg_conn_param)
}
