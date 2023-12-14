#' Query PG and return list of table names that are date based (named)
#'
#' @param pattern An text string for the table pattern to look for date named related tables
#' @param schema  The schema to search in
#' @param connList Named list with the following parameters Driver,host,user,dbname,password,port,schema
#' @param depth The number of matches with dates to return. default is 2
#'
#' @return a dataframe of the list of matching tables
#' @export
#'
#' @examples getDatePatternTableList('techpanel_scenarios','localhost','postgres','myDB','mypassword',5432,'prod')

#install.packages("D:\\Projects\\FAIB_Data_Management\\FAIB_DATA_MANAGEMENT",repos=NULL, type="source")

#devtools::install("D:\\Projects\\FAIB_Data_Management\\FAIB_DATA_MANAGEMENT", dependencies = TRUE, INSTALL_opts = '--no-lock')
#devtools::install("D:\\Projects\\FAIB_Data_Management\\FAIB_DATA_MANAGEMENT")
#load_all(".");
#uninstall(pkg = ".", unload = TRUE, quiet = FALSE, lib = .libPaths()[[1]])

#library(faibDataManagement)



getDatePatternTableList<-function(pattern,schema,connList,depth=2){
  sql <- "select table_name
  from information_schema.tables t
  where table_schema = 'x_x'
  and table_name ~ 'y_y_\\d+$'
  order by table_name desc
  limit z_z"

  sql <- gsub('x_x',schema,sql)
  sql <- gsub('y_y',pattern,sql)
  sql <- gsub('z_z',depth,sql)


  conn<-dbConnect(connList["driver"][[1]],
                  host = connList["host"][[1]],
                  user = connList["user"][[1]],
                  dbname = connList["dbname"][[1]],
                  password = connList["password"][[1]],
                  port = connList["port"][[1]])
  on.exit(RPostgres::dbDisconnect(conn))
  RPostgres::dbGetQuery(conn, sql)
}


