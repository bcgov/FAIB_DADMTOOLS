#' Get named list of  oracle connection parameters
#' @param host oracle database host. Default is oracle keyring value.
#' @param user oracle database user. Default is oracle keyring value.
#' @param service_name  oracle service name. Default is oracle keyring value.
#' @param password pg database password. Default is oracle keyring value.
#' @param port pg database port. Default '1521'.
#'
#' @return returns named list of connection parameters
#' @export
#'
#' @examples get_ora_conn_list(schema = 'whse', serviceName = 'test')


get_ora_conn_list <- function(host = NULL, user = NULL, service_name = NULL, server = NULL, password = NULL, port = '1521'){
  host <- if (is.null(host)) keyring::key_get('dbhost', keyring = 'oracle') else host
  user <- if (is.null(user)) keyring::key_get('dbuser', keyring = 'oracle') else user
  service_name <- if (is.null(service_name)) keyring::key_get('dbservicename', keyring = 'oracle') else service_name
  server <- if (is.null(server)) keyring::key_get('dbserver', keyring = 'oracle') else server
  password <- if (is.null(password)) keyring::key_get('dbpass', keyring = 'oracle') else password
  conn_list <- list(host = host, user = user, service_name = service_name, server = server, password = password, port = port)
  return(conn_list)
}
