#' Get named list of  postgres connection parameters
#' @param driver the database driver. Default is keyring value. E.g. dbDriver("PostgreSQL")
#' @param host pg database host. Default is localpsql keyring value.
#' @param user pg database user. Default is localpsql keyring value.
#' @param dbname  pg database name. Default is localpsql keyring value.
#' @param password pg database password. Default is localpsql keyring value.
#' @param port pg database port. Default '5432'.
#'
#' @return returns named list of connection parameters
#' @export
#'
#' @examples get_pg_conn_list(schema = 'whse', dbname = 'test')


get_pg_conn_list<-function(driver= NULL,host=NULL,user=NULL,dbname=NULL, password=NULL,port='5432'){
  driver <- if (is.null(driver)) RPostgres::Postgres() else driver
  host <- if (is.null(host)) keyring::key_get('dbhost', keyring = 'localpsql') else host
  user <- if (is.null(user)) keyring::key_get('dbuser', keyring = 'localpsql') else user
  dbname <- if (is.null(dbname)) 'prov_data' else dbname
  password <- if (is.null(password)) keyring::key_get('dbpass', keyring = 'localpsql') else password
  connList <- list(driver=driver,host=host,user=user,dbname=dbname,password=password,port=port)
  return(connList)}
