#' Query PG and return results to a dataframe
#'
#' @param sql A sql string
#' @param pg_conn_param Named list with the following parameters Driver,host,user,dbname,password,port,schema
#'
#' @return a dataframe of the returned query
#' @export
#'
#' @examples sql_to_df('select * from table','localhost','postgres','myDB','mypassword',5432,'prod')

sql_to_df <- function(sql, pg_conn_param){
  conn <- dbConnect(pg_conn_param["driver"][[1]],
                  host = pg_conn_param["host"][[1]],
                  user = pg_conn_param["user"][[1]],
                  dbname = pg_conn_param["dbname"][[1]],
                  password = pg_conn_param["password"][[1]],
                  port = pg_conn_param["port"][[1]])
  on.exit(RPostgres::dbDisconnect(conn))
  RPostgres::dbGetQuery(conn, sql)
  }
