#' Returnes a sql staement including the spatial field and primary key field of a Foreign Data Wrapper table.  Can be used in gdal rasterize statemenr
#'
#' @param oratable coming soon
#' @param outTblName coming soon
#' @param pk coming soon
#' @param schema coming soon
#' @param connList coming soon
#' @return None
#' @export
#'
#' @examples coming soon


getFDWtblSpSQL<- function(oratable,pk,connList,fdwSchema = 'load'){
  if (grepl("\\.", oratable)) {
    oraTblNameNoSchema <- unlist(strsplit(oratable, split = "[.]"))[-1]
  }else {oraTblNameNoSchema <- fdwTblName}
  oraTblNameNoSchema <- oraTblNameNoSchema
  geomName <- faibDataManagement::getGeomNamePG(fdwSchema,oraTblNameNoSchema,connList)
  con <- con <- DBI::dbConnect(connList["driver"][[1]])
  sql <-  glue::glue_sql("SELECT 'SELECT ' || array_to_string(ARRAY(SELECT 'o' || '.' || c.column_name
                                            FROM information_schema.columns As c
                                            WHERE table_name = {oraTblNameNoSchema}  and
                                            table_schema = {fdwSchema}
                                            AND  c.column_name IN({geomName},{pk})  ), ',')
                     || ' FROM load.{`oraTblNameNoSchema`} As o' As sqlstmt",.con= con)
  sqlstmt <- (faibDataManagement::getTableQueryPG(sql,connList))$sqlstmt
  return(sqlstmt)
}
