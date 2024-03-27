#' Get named oracle connection string
#' @param in_tbl_name Oracle Table Name
#' @param schema the database schema default is NULL and implies the shcma name will be in the the input table name
#' @param host  database host. Default is oracle keyring value.
#' @param user database user. Default is oracle keyring value.
#' @param service_name  database service name. Default is oracle keyring value.
#' @param password database password. Default is oracle keyring value.
#' @param port database port. Default '1521'.
#'
#' @return returns named list of connection parameters
#' @export
#'
#' @examples get_oracle_conn_string(schema = 'whse', dbname = 'test')

get_oracle_conn_string<-function(in_tbl_name, schema = NULL, host = NULL, user = NULL, service_name = NULL, password = NULL, port = '1521'){
  schema <- if (is.null(schema)) '' else glue::glue("{schema}.")
  host <- if (is.null(host)) keyring::key_get('dbhost', keyring = 'oracle') else host
  user <- if (is.null(user)) keyring::key_get('dbuser', keyring = 'oracle') else user
  service_name <- if (is.null(service_name)) keyring::key_get('dbservicename', keyring = 'oracle') else service_name
  password <- if (is.null(password)) keyring::key_get('dbpass', keyring = 'oracle') else password
  tableName <- glue("{schema}{in_tbl_name}")
  outString <- glue("OCI:\"{user}/{password}@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST={host})(PORT={port})))(CONNECT_DATA=(SERVICE_NAME={service_name}))):{tableName}\"")
  return(outString)}

