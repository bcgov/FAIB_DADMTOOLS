#' Get name of geometry name is PG
#'
#' @param schema Name of table's schema
#' @param tablename Name of pg table
#' @param pg_conn_param Named list with the following parameters Driver,host,user,dbname,password,port,schema
#'
#' @return returns field name of geometry field
#' @export
#'
#' @examples coming soon

get_pg_geom_name <- function(schema, tablename, pg_conn_param ) {
  geom_name <- (sql_to_df(glue("SELECT 
                                  column_name
                                FROM 
                                  information_schema.columns AS c
                                WHERE 
                                  table_schema = '{schema}' 
                                AND 
                                  table_name = '{tablename}' 
                                AND
                                  udt_name = 'geometry'"), pg_conn_param))$column_name
  return(geom_name)
}
