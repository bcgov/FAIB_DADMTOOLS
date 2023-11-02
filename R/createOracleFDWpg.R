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

FDWServerExists <- function(srvName, pgConnList){
  sql <- glue('select * from information_schema.foreign_servers f where foreign_server_name = \'{srvName}\'')
  r <- getTableQueryPG(sql, pgConnList)
  if(nrow(r)>0){
    return(TRUE)
  }
  else{FALSE}
}

FDWUserMappingExists <- function(srvName, userName, pgConnList){
  sql <- glue('select * from information_schema.user_mappings f where foreign_server_name = \'{srvName}\' and authorization_identifier = \'{userName}\'')
  r <- getTableQueryPG(sql, pgConnList)
  if(nrow(r)>0){
    return(TRUE)
  }
  else{FALSE}
}


createOracleFDWpg <- function(dbserver, oraUser, oraPassword, oratable,connList){

  if (grepl("\\.", oratable)) {
    oraTblNameNoSchema <- unlist(strsplit(oratable, split = "[.]"))[-1]
    oraSchema <- toupper(unlist(strsplit(oratable, split = "[.]"))[1])

  }else {
    oraSchema <- ''
    oraTblNameNoSchema <- fdwTblName
  }

  sendSQLstatement(glue('drop foreign table if exists {fdwSchema}.{oraTblNameNoSchema};'),connList)
  sendSQLstatement(glue('create schema if not exists {fdwSchema};'),connList)

  if(!(FDWServerExists('oradb', connList))){
    sendSQLstatement(paste0("create server oradb foreign data wrapper oracle_fdw options (dbserver '", dbserver, "' );"),connList)
  }
  if(!(FDWUserMappingExists('oradb', 'postgres', connList))){
    sendSQLstatement(  paste0("create user mapping for postgres server oradb options (user '", oraUser, "', password '", oraPassword,"');"),connList)
  }

  sendSQLstatement(  paste0('import foreign schema "', oraSchema,'" limit to (', oratable, ') FROM SERVER oradb INTO load;'),connList)
  print(paste0('created fdw for ', oratable ))
  return(glue("load.{oraTblNameNoSchema}"))


}
