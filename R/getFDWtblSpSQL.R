#' Returns a SQL statement including the spatial field and primary key field of a Foreign Data Wrapper table.  Can be used in gdal rasterize statement
#'
#' @param oratable coming soon
#' @param pk coming soon
#' @param connList coming soon
#' @param fdwSchema coming soon
#' @param where coming soon
#' @return sql string
#' @export
#'
#' @examples coming soon


getFDWtblSpSQL<- function(dst_table_name, dst_schema_name, oratable, pk, connList, fdwSchema = 'load', where = NULL) {
  if (grepl("\\.", oratable)) {
    oraTblNameNoSchema <- unlist(strsplit(oratable, split = "[.]"))[-1]
  } else {
    oraTblNameNoSchema <- oratable
  }

  ## retrieve the geometry name
  geomName <- faibDataManagement::getGeomNamePG(fdwSchema, oraTblNameNoSchema, connList)
    ## post process the where statement
  if(!is.null(where)) {
    if(startsWith(trimws(where), "where")) {
      where <- substr(trimws(where), 6, nchar(where))
    }
    if (where == '') {
      where <- glue("WHERE {geomName} <> ''" )
    } else {
      where <- glue::glue("WHERE {where} AND {geomName} <> ''" )
    }
  }


  con <-  DBI::dbConnect(connList["driver"][[1]])

  sqlstmt <- glue("
  WITH fdw_w_geom_and_where_clause AS (
  SELECT
    objectid,
    {geomName}
  FROM
     {fdwSchema}.{oraTblNameNoSchema}
  {where}
  )
  SELECT
    non_spatial.{pk},
    fdw_w_geom.{geomName}
  FROM
    {dst_schema_name}.{dst_table_name} non_spatial
  JOIN
    fdw_w_geom_and_where_clause fdw_w_geom
  ON
    fdw_w_geom.objectid::integer = non_spatial.objectid::integer")

  return(sqlstmt)
}
