#' Create Foreign Data Wrapper Table in Postgres
#'
#' @param dbserver coming soon
#' @param oraUser coming soon
#' @param oraPassword coming soon
#' @param oratable coming soon
#'
#' @return None
#' @export
#'
#' @examples coming soon


createOracleFDWpg <- function(dbserver, oraUser, oraPassword, oratable,connList,fdwSchema){

  if (grepl("\\.", oratable)) {
    oraTblNameNoSchema <- unlist(strsplit(oratable, split = "[.]"))[-1]
    oraSchema <- toupper(unlist(strsplit(oratable, split = "[.]"))[1])

  }else {
    oraSchema <- ''
    oraTblNameNoSchema <- fdwTblName
  }
  sendSQLstatement(glue('drop foreign table if exists {fdwSchema}.{oraTblNameNoSchema};'),connList)
  sendSQLstatement('drop user mapping if exists for postgres server oradb;',connList)
  sendSQLstatement('drop server if exists oradb cascade;',connList)
  sendSQLstatement(glue('drop schema if exists {fdwSchema} cascade;'),connList)

  sendSQLstatement(glue('create schema if not exists {fdwSchema};'),connList)
  sendSQLstatement(  paste0("create server oradb foreign data wrapper oracle_fdw options (dbserver '", dbserver, "' );"),connList)
  sendSQLstatement(  paste0("create user mapping for postgres server oradb options (user '", oraUser, "', password '", oraPassword,"');"),connList)

  sendSQLstatement(  glue('import foreign schema "', oraSchema,'" limit to (', oratable, ') FROM SERVER oradb INTO {fdwSchema};'),connList)
  print(paste0('created fdw for ', oratable ))
  return(glue("{fdwSchema}.{oraTblNameNoSchema}"))


}
