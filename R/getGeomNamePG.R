#' Get name of geometry name is PG
#'
#' @param schema Name of table's schema
#' @param tablename Name of pg table
#' @param connList Named list with the following parameters Driver,host,user,dbname,password,port,schema
#'
#' @return returns field name of geometry field
#' @export
#'
#' @examples coming soon

getGeomNamePG <- function(schema, tablename, connList ) {
  geomName <- (getTableQueryPG(paste0("select column_name
        FROM information_schema.columns As c
            WHERE table_schema = '",schema,"' and table_name = '",tablename,"' and udt_name = 'geometry'"), connList))$column_name
  return(geomName)
}
