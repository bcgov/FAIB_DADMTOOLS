#' Run a external sql file
#'
#' @param sql_var vector of variables to use in sql file e.g. c("schema='dbName',xwalkprev='inFC'), when set to NULL, runs sql_file without variable replacement
#' @param sql_file path and file name of sql file
#' @param pg_db pg database name
#' @param host host of db e.g. 'localhost', defaults to NULL
#'
#' @return no return
#' @export
#'
#' @examples sql_var <- c("schema"='dbName',xwalkprev='inFC') \cr
#' sql_file <- C:/sql/file.sql \cr
#' run_sql_psql(sql_var,sql_file,pg_db)



run_sql_psql <- function(sql_var, sql_file, pg_db, host=NULL){
  if (is.null(host)){
    host<-'localhost'
  }

  cmd <-  c("-d", pg_db, "-f", sql_file, "-h", host)
  if (is.null(sql_var)){
    print(cmd)
    print(system2("psql", args = cmd, wait = TRUE, stdout = TRUE, stderr = TRUE))
  } else {
    for (i in sql_var){
      cmd <- append(cmd, "-v")
      cmd <- append(cmd, i)
    }
  print(cmd)
  print(system2("psql", args = cmd, wait = TRUE, stdout = TRUE, stderr = TRUE))
  }
}
