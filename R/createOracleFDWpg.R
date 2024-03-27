#' Create Oracle Foreign Data Wrapper Table in Postgres
#'
#' @param in_table oracle table to import (e.g. WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY )
#' @param ora_conn_param  named list of  oracle connection parameters (see get_ora_conn_list() function for more details)
#' @param pg_conn_param named list of  postgres connection parameters (see get_pg_conn_list() function for more details)
#' @param out_server_name name of FDW Server that will be created
#' @param out_schema name of schema where foreign table will be outputted to
#'
#' @return string of output foreign table (schema.in_table)
#' @export
#'
#' @examples coming soon


createOracleFDWpg <- function(in_table, ora_conn_param, pg_conn_param, out_server_name, out_schema) {

  ora_server <- ora_conn_param["server"][[1]]
  idir <- ora_conn_param["user"][[1]]
  orapass <- ora_conn_param["password"][[1]]

  FDWServerExists <- function(out_server_name, pg_conn_param){
    sql <- glue('select * from information_schema.foreign_servers f where foreign_server_name = \'{out_server_name}\'')
    r <- getTableQueryPG(sql, pg_conn_param)
    if(nrow(r)>0) {
      return(TRUE)
    }
      else{FALSE}
    }

  if (grepl("\\.", in_table)) {
    ora_tbl_no_schema <- unlist(strsplit(in_table, split = "[.]"))[-1]
    ora_schema <- toupper(unlist(strsplit(in_table, split = "[.]"))[1])
  } else {
    ora_schema <- ''
    ora_tbl_no_schema <- in_table
  }

  
  faibDataManagement::sendSQLstatement(glue('drop foreign table if exists {out_schema}.{ora_tbl_no_schema};'), pg_conn_param)
  faibDataManagement::sendSQLstatement(glue('create schema if not exists {out_schema};'), pg_conn_param)

  if(!(FDWServerExists(out_server_name, pg_conn_param))) {
    faibDataManagement::sendSQLstatement(paste0("create server ", out_server_name,
                                                " foreign data wrapper oracle_fdw options (dbserver '", ora_server, "' );"), pg_conn_param)
    faibDataManagement::sendSQLstatement(paste0("create user mapping for postgres server ", out_server_name," options (user '",idir,"', password '",orapass,"');"), pg_conn_param)

    print(paste0("create user mapping for postgres server ", out_server_name," options (user '",idir,"', password '" ,orapass,"');"))

  }

  print(glue('Creating PG FDW table: {out_schema}.{ora_tbl_no_schema} from Oracle table: {in_table}'))
  faibDataManagement::sendSQLstatement(glue('import foreign schema "', ora_schema,'" limit to (', ora_tbl_no_schema, ') FROM SERVER {out_server_name} INTO {out_schema};'), pg_conn_param)
  return(glue("{out_schema}.{ora_tbl_no_schema}"))

}
