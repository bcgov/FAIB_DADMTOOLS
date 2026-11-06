
#' Get named list of PostgreSQL connection parameters
#'
#' @param driver Database driver. Defaults to RPostgres::Postgres().
#' @param host PostgreSQL host. Defaults to the selected keyring.
#' @param user PostgreSQL user. Defaults to the selected keyring.
#' @param dbname PostgreSQL database name. Defaults to the selected keyring.
#' @param password PostgreSQL password. Defaults to the selected keyring.
#' @param port PostgreSQL port. Defaults to "5432".
#' @param keyring_name Character scalar identifying the keyring
#'   used to retrieve any connection parameters not explicitly
#'   supplied. Defaults to "localpsql".
#'
#' @return A named list of PostgreSQL connection parameters.
#' @export
#'
#' @examples
#' \dontrun{
#' local_cfg <- get_pg_conn_list()
#' central_cfg <- get_pg_conn_list(keyring_name = "provpsql")
#' }
get_pg_conn_list <- function(
    driver = NULL,
    host = NULL,
    user = NULL,
    dbname = NULL,
    password = NULL,
    port = "5432",
    keyring_name = "localpsql"
) {
  driver <- if (is.null(driver)) {
    RPostgres::Postgres()
  } else {
    driver
  }

  host <- if (is.null(host)) {
    keyring::key_get("dbhost", keyring = keyring_name)
  } else {
    host
  }

  user <- if (is.null(user)) {
    keyring::key_get("dbuser", keyring = keyring_name)
  } else {
    user
  }

  dbname <- if (is.null(dbname)) {
    keyring::key_get("dbname", keyring = keyring_name)
  } else {
    dbname
  }

  password <- if (is.null(password)) {
    keyring::key_get("dbpass", keyring = keyring_name)
  } else {
    password
  }

  list(
    driver = driver,
    host = host,
    user = user,
    dbname = dbname,
    password = password,
    port = port
  )
}
