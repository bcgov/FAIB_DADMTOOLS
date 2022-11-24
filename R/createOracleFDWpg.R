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


createOracleFDWpg <- function(dbserver, oraUser, oraPassword, oratable,connList){

  if (grepl("\\.", oratable)) {
    oraTblNameNoSchema <- unlist(strsplit(oratable, split = "[.]"))[-1]
    oraSchema <- toupper(unlist(strsplit(oratable, split = "[.]"))[1])

  }else {
    oraSchema <- ''
    oraTblNameNoSchema <- fdwTblName
  }
  sendSQLstatement( paste0('drop foreign table if exists load.', oraTblNameNoSchema,';'),connList)
  sendSQLstatement(  paste0('drop user mapping if exists for postgres server oradb;'),connList)
  sendSQLstatement(  paste0('drop server if exists oradb cascade;'),connList)
  sendSQLstatement(  paste0('drop schema if exists load cascade;'),connList)

  sendSQLstatement(  paste0('create schema if not exists load;'),connList)
  sendSQLstatement(  paste0("create server oradb foreign data wrapper oracle_fdw options (dbserver '", dbserver, "' );"),connList)
  sendSQLstatement(  paste0("create user mapping for postgres server oradb options (user '", oraUser, "', password '", oraPassword,"');"),connList)

  sendSQLstatement(  paste0('import foreign schema "', oraSchema,'" limit to (', oratable, ') FROM SERVER oradb INTO load;'),connList)
  print(paste0('created fdw for ', oratable ))
  return(glue("load.{oraTblNameNoSchema}"))


}
