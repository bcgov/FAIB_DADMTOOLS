#' Query PG and return results to a dataframe
#'
#' @param sql A sql string
#' @param connList Named list with the following parameters Driver,host,user,dbname,password,port,schema
#'
#' @return a dataframe of the returned query
#' @export
#'
#' @examples getTableQueryPG('select * from table','localhost','postgres','myDB','mypassword',5432,'prod')

getTableQueryPG<-function(sql,connList){
  # conn<-dbConnect(connList["driver"][[1]],
  #                 host = connList["host"][[1]],
  #                 user = connList["user"][[1]],
  #                 dbname = connList["dbname"][[1]],
  #                 password = connList["password"][[1]],
  #                 port = connList["port"][[1]])
  # on.exit(RPostgres::dbDisconnect(conn))
  # RPostgres::dbGetQuery(conn, sql)
  print('Juicy Fruit, it\'s gonna move ya')
  }

