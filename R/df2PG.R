#' Write a PG Table from Dataframe
#'
#' @param pg_tbl Name of output PG Table
#' @param in_df input dataframe
#' @param pg_conn_param Named list with the following parameters Driver,host,user,dbname,password,port,schema
#' @param overwrite overwrite output T or F
#' @param append append output T or F
#'
#' @return no return
#' @export
#'
#' @examples df2PG('pgTableName',myDF,'postgres','myDB','mypassword',5432,'prod','T','F')


df2PG<-function(pg_tbl, 
                in_df, 
                pg_conn_param, 
                overwrite = TRUE, 
                append = FALSE
                )
{
  in_df <- dfColNames2lower(in_df)
  conn <- dbConnect(pg_conn_param["driver"][[1]],
                  host = pg_conn_param["host"][[1]],
                  user = pg_conn_param["user"][[1]],
                  dbname = pg_conn_param["dbname"][[1]],
                  password = pg_conn_param["password"][[1]],
                  port = pg_conn_param["port"][[1]])
  on.exit(RPostgres::dbDisconnect(conn))
  RPostgres::dbWriteTable(conn, pg_tbl, value = in_df, overwrite = overwrite, append = append, row.names = FALSE)}
