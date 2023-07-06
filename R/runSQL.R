#' Run a external sql file
#'
#' @param sqlVar vector of variables to use in sql file e.g. c("schema='dbName',xwalkprev='inFC')
#' @param sqlFile path and file name of sql file
#' @param pgDB pg database name
#' @param host host of db e.g. 'localhost'
#'
#' @return no return
#' @export
#'
#' @examples sqlVar <- c("schema"='dbName',xwalkprev='inFC') \cr
#' sqlFile <- C:/sql/file.sql \cr
#' runSQL(sqlVar,sqlFile,pgDB)



runSQL <- function(sqlVar,sqlFile,pgDB, host=NULL){
  if (is.null(host)){host<-'localhost'}
  cmd <-  c("-d",pgDB,"-f",sqlFile,"-h",host)
  for(i in sqlVar){
    cmd <- append(cmd,"-v")
    cmd <- append(cmd,i)
  }
  print(cmd)
  print(system2("psql",args=cmd,wait=TRUE,stdout=TRUE,stderr=TRUE))
}
