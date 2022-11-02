#' Send sql statement to PG
#'
#' @param sql A sql string
#' @param connList Named list with the following connection parameters Driver,host,user,dbname,password,port,schema
#'
#' @return no return
#' @export
#'
#' @examples sendSQLstatement('Create table tester as select * from test1','postgres','myDB','mypassword',5432,'prod')



sendSQLstatement<-function(sql,connList){
  conn<-dbConnect(connList["driver"][[1]],
                  host = connList["host"][[1]],
                  user = connList["user"][[1]],
                  dbname = connList["dbname"][[1]],
                  password = connList["password"][[1]],
                  port = connList["port"][[1]])
  on.exit(RPostgres::dbDisconnect(conn))
  RPostgres::dbExecute(conn, statement = sql)
}
