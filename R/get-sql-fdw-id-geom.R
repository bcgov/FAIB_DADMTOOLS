#' Returns a SQL statement including the spatial field and primary key field of a Foreign Data Wrapper table.  Can be used in gdal rasterize statement
#' @param dst_tbl coming soon
#' @param dst_schema coming soon
#' @param ora_tbl coming soon
#' @param pk coming soon
#' @param pg_conn_param coming soon
#' @param fdw_schema Defaults to "load"
#' @param where Optionalm defaults to NULL
#' @return sql string
#' @export
#'
#' @examples coming soon


get_sql_fdw_id_geom<- function(dst_tbl,
                          dst_schema,
                          ora_tbl,
                          pk,
                          pg_conn_param,
                          fdw_schema     = 'load',
                          where          = NULL
                          )
{
  if (grepl("\\.", ora_tbl)) {
    ora_tbl <- strsplit(ora_tbl, "\\.")[[1]][[2]]
  } else {
    ora_tbl <- ora_tbl
  }

  ## retrieve the geometry name
  geom_name <- dadmtools::get_pg_geom_name(fdw_schema, ora_tbl, pg_conn_param)
    ## post process the where statement
  if(!is.null(where)) {
    if(startsWith(trimws(where), "where")) {
      where <- substr(trimws(where), 6, nchar(where))
    }
    if (where == '') {
      where <- glue("WHERE {geom_name} <> ''" )
    } else {
      where <- glue::glue("WHERE {where} AND {geom_name} <> ''" )
    }
  }


  con <-  DBI::dbConnect(pg_conn_param["driver"][[1]])

  sqlstmt <- glue("
  WITH fdw_w_geom_and_where_clause AS (
  SELECT
    objectid,
    {geom_name}
  FROM
     {fdw_schema}.{ora_tbl}
  {where}
  )
  SELECT
    non_spatial.{pk},
    fdw_w_geom.{geom_name}
  FROM
    {dst_schema}.{dst_tbl} non_spatial
  JOIN
    fdw_w_geom_and_where_clause fdw_w_geom
  ON
    fdw_w_geom.objectid::integer = non_spatial.objectid::integer")

  return(sqlstmt)
}
