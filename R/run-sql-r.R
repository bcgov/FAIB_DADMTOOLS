#' Send sql statement to PG
#'
#' @param sql A sql string
#' @param pg_conn_param Named list with the following connection parameters Driver,host,user,dbname,password,port,schema
#'
#' @return no return
#' @export
#'
#' @examples run_sql_r('Create table tester as select * from test1','postgres','myDB','mypassword',5432,'prod')



run_sql_r <- function(sql, pg_conn_param){
  conn <- dbConnect(pg_conn_param["driver"][[1]],
                  host = pg_conn_param["host"][[1]],
                  user = pg_conn_param["user"][[1]],
                  dbname = pg_conn_param["dbname"][[1]],
                  password = pg_conn_param["password"][[1]],
                  port = pg_conn_param["port"][[1]])
  on.exit(RPostgres::dbDisconnect(conn))
  RPostgres::dbExecute(conn, statement = sql)
}
