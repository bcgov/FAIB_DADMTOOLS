#' Write a PG Table from Dataframe
#'
#' @param pgTblName Name of output PG Table
#' @param inDF input dataframe
#' @param connList Named list with the following parameters Driver,host,user,dbname,password,port,schema
#' @param overwrite overwrite output T or F
#' @param append append output T or F
#'
#' @return no return
#' @export
#'
#' @examples df2PG('pgTableName',myDF,'postgres','myDB','mypassword',5432,'prod','T','F')


df2PG<-function(pgTblName, inDF, connList, overwrite=T, append=F){
  inDF <- dfColNames2lower(inDF)
  column_names <- colnames(inDF)
  inDF[[column_names[column_names != "gr_skey"]]] <- as.integer(inDF[[column_names[column_names != "gr_skey"]]])
  conn<-dbConnect(connList["driver"][[1]],
                  host = connList["host"][[1]],
                  user = connList["user"][[1]],
                  dbname = connList["dbname"][[1]],
                  password = connList["password"][[1]],
                  port = connList["port"][[1]])
  on.exit( RPostgres::dbDisconnect(conn))
  RPostgres::dbWriteTable(conn, pgTblName, value = inDF, overwrite = overwrite, append = append, row.names = FALSE)}
