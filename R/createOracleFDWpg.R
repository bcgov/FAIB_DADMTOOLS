#' Create Oracle Foreign Data Wrapper Table in Postgres
#'
#' @param intable oracle table to import (e.g. WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY )
#' @param oraConnList  named list of  oracle connection parameters (see get_ora_conn_list() function for more details)
#' @param pgConnList named list of  postgres connection parameters (see get_pg_conn_list() function for more details)
#' @param outServerName name of FDW Server that will be created
#' @param outSchema name of schema where foreign table will be outputted to
#'
#' @return string of output foreign table (schema.intable)
#' @export
#'
#' @examples coming soon


createOracleFDWpg <- function(intable,oraConnList,pgConnList,outServerName,outSchema){


  oraServer <- oraConnList["server"][[1]]
  idir <- oraConnList["user"][[1]]
  orapass <- oraConnList["password"][[1]]
  print("create Foreign Table in pG")

  FDWServerExists <- function(outServerName, pgConnList){
    sql <- glue('select * from information_schema.foreign_servers f where foreign_server_name = \'{outServerName}\'')
    r <- getTableQueryPG(sql, pgConnList)
    if(nrow(r)>0){
      return(TRUE)
    }
    else{FALSE}
  }

  if (grepl("\\.", intable)) {
    oraTblNameNoSchema <- unlist(strsplit(intable, split = "[.]"))[-1]
    oraSchema <- toupper(unlist(strsplit(intable, split = "[.]"))[1])

  }else {
    oraSchema <- ''
    oraTblNameNoSchema <- intable
  }


  faibDataManagement::sendSQLstatement(glue('drop foreign table if exists {outSchema}.{oraTblNameNoSchema};'),pgConnList)
  faibDataManagement::sendSQLstatement(glue('create schema if not exists {outSchema};'),pgConnList)

  if(!(FDWServerExists(outServerName, pgConnList))){
    faibDataManagement::sendSQLstatement(paste0("create server ", outServerName,
                                                " foreign data wrapper oracle_fdw options (dbserver '", oraServer, "' );"),pgConnList)
    faibDataManagement::sendSQLstatement(paste0("create user mapping for postgres server ", outServerName," options (user '",idir,"', password '",orapass,"');"),pgConnList)

    print(paste0("create user mapping for postgres server ", outServerName," options (user '",idir,"', password '",orapass,"');"))

  }


  faibDataManagement::sendSQLstatement(glue('import foreign schema "', oraSchema,'" limit to (', oraTblNameNoSchema, ') FROM SERVER {outServerName} INTO {outSchema};'),pgConnList)
  print(paste0('created fdw for ', intable ))
  return(glue("{outSchema}.{oraTblNameNoSchema}"))


}
