#' Transfer Datasets Between PostgreSQL Databases
#'
#' Transfers configured datasets from a central PostgreSQL database
#' to a local PostgreSQL database using an existing foreign data
#' wrapper (FDW) server configured in the local database.
#'
#' Each dataset is defined by a row in a CSV configuration file.
#' The function validates the configuration, establishes database
#' connections when needed, and processes each dataset independently.
#' Transfer outcomes are returned as a data frame and appended to a
#' CSV log.
#'
#' Datasets can be configured with one of two refresh policies:
#' \describe{
#'   \item{static}{
#'     Preserves an existing destination table and skips the transfer.
#'     If the destination table does not exist, the dataset is transferred.
#'   }
#'   \item{check}{
#'     Compares source and destination table metadata. If the compared
#'     metadata match, the transfer is skipped. Otherwise, the destination
#'     table is replaced with a copy of the source data and table and
#'     column comments.
#'   }
#' }
#'
#' Each individual destination table replacement is performed within
#' a database transaction. If a replacement fails, its transaction
#' is rolled back. Errors are recorded against the affected dataset,
#' allowing processing of subsequent datasets to continue.
#'
#' The function does not create or configure the FDW server itself.
#' The specified server must already exist in the local database and
#' have access to the intended central database and source schemas.
#'
#' @param config_file Character scalar. Path to an existing CSV
#'   configuration file. The file must contain the following columns:
#'   \describe{
#'     \item{Dataset_Common_Name}{
#'       Identifier or common name of the dataset.
#'     }
#'     \item{Source_Schema}{
#'       Schema containing the source table in the central database.
#'     }
#'     \item{Source_Table}{
#'       Name of the source table in the central database.
#'     }
#'     \item{Full_Source_Name}{
#'       Fully qualified source table name in \code{schema.table}
#'       format. Must match \code{Source_Schema} and
#'       \code{Source_Table}.
#'     }
#'     \item{Table_Type}{
#'       Configured dataset table type. Required configuration field.
#'     }
#'     \item{Paired_Table}{
#'       Configured paired table information. Required configuration field.
#'     }
#'     \item{Notes}{
#'       Additional configuration notes. Required configuration field.
#'     }
#'     \item{Refresh}{
#'       Refresh policy. Must be either \code{static} or \code{check}.
#'     }
#'     \item{Destination_Schema}{
#'       Schema in the local database where the destination table
#'       will be stored.
#'     }
#'     \item{Destination_Table}{
#'       Name of the destination table in the local database.
#'     }
#'   }
#'
#' @param log_file Character scalar. Path to the CSV transfer log.
#'   The parent directory is created if necessary. Existing logs are
#'   appended to; a header is written when the file does not exist.
#'   The caller must ensure that the path is writable and that an
#'   existing log has a compatible structure.
#'
#' @param local_keyring Character scalar. Keyring identifier used by
#'   \code{get_pg_conn_list()} to retrieve local database connection
#'   settings when \code{local_con} is not supplied. Defaults to
#'   \code{"localpsql"}.
#'
#' @param central_keyring Character scalar. Keyring identifier used by
#'   \code{get_pg_conn_list()} to retrieve central database connection
#'   settings when \code{central_con} is not supplied. Defaults to
#'   \code{"provpsql"}.
#'
#' @param fdw_server Character scalar. Name of an existing foreign data
#'   wrapper server in the local database. The server must map to the
#'   intended central database. Defaults to \code{"central_db"}.
#'
#' @param load_schema Character scalar. Schema in the local database
#'   used to stage foreign tables imported through the FDW server.
#'   The schema is created if it does not exist. Defaults to
#'   \code{"load"}.
#'
#' @param local_con Optional open DBI connection to the local database.
#'   If supplied, the connection must be valid and is not disconnected
#'   by this function. If \code{NULL}, a connection is created using
#'   \code{local_keyring} and disconnected when processing finishes.
#'
#' @param central_con Optional open DBI connection to the central
#'   database. If supplied, the connection must be valid and is not
#'   disconnected by this function. If \code{NULL}, a connection is
#'   created using \code{central_keyring} and disconnected when
#'   processing finishes.
#'
#' @return A data frame with one row per configured dataset and the
#'   following columns:
#'   \describe{
#'     \item{timestamp}{
#'       Time at which processing of the dataset began.
#'     }
#'     \item{dataset}{
#'       Configured dataset identifier from
#'       \code{Dataset_Common_Name}.
#'     }
#'     \item{source}{
#'       Fully qualified source table name from
#'       \code{Full_Source_Name}.
#'     }
#'     \item{destination_schema}{
#'       Destination schema in the local database.
#'     }
#'     \item{destination_table}{
#'       Destination table name in the local database.
#'     }
#'     \item{refresh}{
#'       Configured refresh policy.
#'     }
#'     \item{status}{
#'       Transfer outcome:
#'       \code{SUCCESS},
#'       \code{SKIPPED_STATIC_EXISTS},
#'       \code{SKIPPED_METADATA_MATCH},
#'       or an error message prefixed with \code{ERROR:}.
#'     }
#'   }
#'
#' @details
#' The function requires the DBI package, a PostgreSQL-compatible DBI
#' driver, the \code{get_pg_conn_list()} connection helper, and an
#' appropriately configured FDW server.
#'
#' The source table is accessed through a foreign table in the local
#' staging schema. Existing foreign tables are reused after their
#' server and source mapping have been verified. The function does not
#' automatically refresh existing foreign table definitions when source
#' columns change.
#'
#' For datasets that are transferred, destination tables are
#' reconstructed from the source table's column definitions and data
#' using PostgreSQL table creation and insertion statements. Table and
#' column comments are copied.
#'
#' Other table properties, such as indexes, constraints, triggers,
#' grants, identity settings, and custom table storage options, are
#' not explicitly preserved.
#'
#' The \code{check} policy compares the source and destination table
#' comments and selected column metadata, including column names,
#' ordinal positions, data types, and column comments. It does not
#' compare table data, column nullability, or column defaults.
#' Changes to data alone therefore do not trigger a refresh.
#'
#' The intended refresh workflow assumes that changes to dataset
#' contents are accompanied by changes to the metadata being compared.
#' The function does not independently verify this assumption.
#'
#' A failed dataset transfer is recorded in the returned results and
#' log; it does not necessarily cause the overall function to fail.
#' Configuration, connection, or logging failures can still terminate
#' the overall operation.
#'
#' @seealso
#' \code{DBI::dbConnect()},
#' \code{DBI::dbWithTransaction()},
#' \code{utils::read.csv()}
#'
#' @examples
#' \dontrun{
#' results <- pg_transfer(
#'   config_file = "config/datasets.csv",
#'   log_file = "logs/transfer_log.csv"
#' )
#'
#' # Inspect unsuccessful transfers
#' subset(results, grepl("^ERROR:", status))
#' }
#'
#' \dontrun{
#' transfer_log <- pg_transfer(
#' config_file = config_file,
#' log_file = log_file,
#' local_keyring = "localpsql",
#' central_keyring = "provpsql",
#' fdw_server = "central_db",
#' load_schema = "public"
#' )
#'
#' print(transfer_log)
#' }
#'
#' \dontrun{
#' # Locate the example configuration file
#' example_config <- system.file(
#'   "extdata",
#'   "pg_transfer_example.csv",
#'   package = "dadmtools"
#' )
#'
#' # Copy it to your working directory
#' file.copy(
#'   example_config,
#'   "pg_transfer_example.csv"
#' )
#'
#' # Run the transfer using your customized configuration
#' results <- pg_transfer(
#'   config_file = "pg_transfer_example.csv",
#'   log_file = "transfer_log.csv"
#' )
#' }
#' @export

pg_transfer <- function(
    config_file,
    log_file,
    local_keyring = "localpsql",
    central_keyring = "provpsql",
    fdw_server = "central_db",
    load_schema = "load",
    local_con = NULL,
    central_con = NULL
) {

  # ----------------------------------------------------------
  # VALIDATE INPUTS
  # ----------------------------------------------------------

  validate_path <- function(x, name) {
    if (
      length(x) != 1L ||
      is.na(x) ||
      !nzchar(trimws(x))
    ) {
      stop(
        name, " must be a non-empty path.",
        call. = FALSE
      )
    }
  }

  validate_name <- function(x, name) {
    if (
      length(x) != 1L ||
      is.na(x) ||
      !nzchar(trimws(x))
    ) {
      stop(
        name, " must be a non-empty name.",
        call. = FALSE
      )
    }
  }

  validate_path(config_file, "config_file")
  validate_path(log_file, "log_file")
  validate_name(fdw_server, "fdw_server")
  validate_name(load_schema, "load_schema")

  if (!file.exists(config_file)) {
    stop(
      "Configuration file does not exist: ",
      config_file,
      call. = FALSE
    )
  }

  if (dir.exists(log_file)) {
    stop(
      "log_file points to a directory: ",
      log_file,
      call. = FALSE
    )
  }

  # Track ownership so caller-supplied connections are never
  # disconnected by this function.
  owns_local <- is.null(local_con)
  owns_central <- is.null(central_con)

  on.exit({

    if (owns_central && !is.null(central_con)) {
      try(
        DBI::dbDisconnect(central_con),
        silent = TRUE
      )
    }

    if (owns_local && !is.null(local_con)) {
      try(
        DBI::dbDisconnect(local_con),
        silent = TRUE
      )
    }

  }, add = TRUE)

  # ----------------------------------------------------------
  # CONNECT TO DATABASES
  # ----------------------------------------------------------

  if (owns_local) {

    local_cfg <- get_pg_conn_list(
      keyring_name = local_keyring
    )

    local_con <- .pg_transfer_connect(local_cfg)
  }

  if (owns_central) {

    central_cfg <- get_pg_conn_list(
      keyring_name = central_keyring
    )

    central_con <- .pg_transfer_connect(central_cfg)
  }

  if (!DBI::dbIsValid(local_con)) {
    stop(
      "The local database connection is not valid.",
      call. = FALSE
    )
  }

  if (!DBI::dbIsValid(central_con)) {
    stop(
      "The central database connection is not valid.",
      call. = FALSE
    )
  }

  local_db <- DBI::dbGetQuery(
    local_con,
    "SELECT current_database() AS database_name"
  )$database_name[[1]]

  central_db <- DBI::dbGetQuery(
    central_con,
    "SELECT current_database() AS database_name"
  )$database_name[[1]]

  message("Local database: ", local_db)
  message("Central database: ", central_db)

  # ----------------------------------------------------------
  # RUN TRANSFERS
  # ----------------------------------------------------------

  .pg_transfer_run(
    config_file = config_file,
    log_file = log_file,
    local_con = local_con,
    central_con = central_con,
    fdw_server = fdw_server,
    load_schema = load_schema
  )
}


# ============================================================
# CONNECTION HELPER
# ============================================================

.pg_transfer_connect <- function(cfg) {

  required <- c(
    "driver",
    "host",
    "port",
    "dbname",
    "user",
    "password"
  )

  missing <- required[
    !vapply(
      required,
      function(x) !is.null(cfg[[x]]) &&
        length(cfg[[x]]) > 0L,
      logical(1)
    )
  ]

  if (length(missing) > 0L) {
    stop(
      "Missing database connection settings: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }

  DBI::dbConnect(
    drv = cfg$driver,
    host = cfg$host,
    port = cfg$port,
    dbname = cfg$dbname,
    user = cfg$user,
    password = cfg$password
  )
}


# ============================================================
# SQL HELPERS
# ============================================================

.pg_transfer_qi <- function(con, x) {
  as.character(DBI::dbQuoteIdentifier(con, x))
}

.pg_transfer_qs <- function(con, x) {
  as.character(DBI::dbQuoteString(con, x))
}


.pg_transfer_relation_kind <- function(
    con,
    schema,
    table
) {

  result <- DBI::dbGetQuery(
    con,
    "
    SELECT c.relkind
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname = $1
      AND c.relname = $2
    ",
    params = list(schema, table)
  )

  if (nrow(result) == 0L) {
    return(NA_character_)
  }

  result$relkind[[1]]
}


.pg_transfer_schema_exists <- function(con, schema) {

  result <- DBI::dbGetQuery(
    con,
    "
    SELECT EXISTS (
      SELECT 1
      FROM pg_namespace
      WHERE nspname = $1
    ) AS present
    ",
    params = list(schema)
  )

  isTRUE(result$present[[1]])
}


.pg_transfer_ensure_schema <- function(con, schema) {

  DBI::dbExecute(
    con,
    paste0(
      "CREATE SCHEMA IF NOT EXISTS ",
      .pg_transfer_qi(con, schema)
    )
  )

  invisible(TRUE)
}


# ============================================================
# PARSE SOURCE LOCATION
# ============================================================


.pg_transfer_parse_source_name <- function(
    full_name,
    source_schema,
    source_table
) {

  validate_part <- function(x, name) {
    if (
      length(x) != 1L ||
      is.na(x) ||
      !nzchar(trimws(x))
    ) {
      stop(
        name, " must be a non-empty value.",
        call. = FALSE
      )
    }

    trimws(as.character(x))
  }

  full_name <- validate_part(
    full_name,
    "Full_Source_Name"
  )

  source_schema <- validate_part(
    source_schema,
    "Source_Schema"
  )

  source_table <- validate_part(
    source_table,
    "Source_Table"
  )

  parts <- trimws(
    strsplit(full_name, ".", fixed = TRUE)[[1]]
  )

  if (
    length(parts) != 2L ||
    any(!nzchar(parts))
  ) {
    stop(
      "Expected Full_Source_Name in schema.table format: ",
      full_name,
      call. = FALSE
    )
  }

  if (
    !identical(parts[[1]], source_schema) ||
    !identical(parts[[2]], source_table)
  ) {
    stop(
      "Full_Source_Name does not match Source_Schema ",
      "and Source_Table. Full_Source_Name = ",
      full_name,
      "; Source_Schema = ", source_schema,
      "; Source_Table = ", source_table,
      call. = FALSE
    )
  }

  list(
    schema = source_schema,
    table = source_table
  )
}



# ============================================================
# TABLE AND COLUMN METADATA
# ============================================================

.pg_transfer_get_table_comment <- function(
    con,
    schema,
    table
) {

  result <- DBI::dbGetQuery(
    con,
    "
    SELECT obj_description(c.oid, 'pg_class') AS comment
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    WHERE n.nspname = $1
      AND c.relname = $2
      AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
    ",
    params = list(schema, table)
  )

  if (nrow(result) == 0L) {
    stop(
      "Table not found: ",
      schema, ".", table,
      call. = FALSE
    )
  }

  result$comment[[1]]
}


.pg_transfer_get_column_metadata <- function(
    con,
    schema,
    table
) {

  DBI::dbGetQuery(
    con,
    "
    SELECT
      a.attname AS column_name,
      a.attnum AS ordinal_position,
      format_type(a.atttypid, a.atttypmod) AS data_type,
      a.attnotnull AS not_null,
      pg_get_expr(d.adbin, d.adrelid) AS column_default,
      col_description(c.oid, a.attnum) AS comment
    FROM pg_class c
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    JOIN pg_attribute a
      ON a.attrelid = c.oid
    LEFT JOIN pg_attrdef d
      ON d.adrelid = c.oid
     AND d.adnum = a.attnum
    WHERE n.nspname = $1
      AND c.relname = $2
      AND a.attnum > 0
      AND NOT a.attisdropped
    ORDER BY a.attnum
    ",
    params = list(schema, table)
  )
}


.pg_transfer_get_metadata <- function(
    con,
    schema,
    table
) {

  list(
    table_comment = .pg_transfer_get_table_comment(
      con, schema, table
    ),
    columns = .pg_transfer_get_column_metadata(
      con, schema, table
    )
  )
}


# ============================================================
# COMPARE METADATA
# ============================================================

.pg_transfer_same_metadata <- function(
    source,
    target
) {

  if (!identical(
    source$table_comment,
    target$table_comment
  )) {
    return(FALSE)
  }

  fields <- c(
    "column_name",
    "ordinal_position",
    "data_type",
    "comment"
  )

  a <- source$columns[, fields, drop = FALSE]
  b <- target$columns[, fields, drop = FALSE]

  rownames(a) <- NULL
  rownames(b) <- NULL

  isTRUE(all.equal(
    a,
    b,
    check.attributes = FALSE
  ))
}


# ============================================================
# ENSURE CORRECT FOREIGN TABLE
# ============================================================

.pg_transfer_ensure_foreign_table <- function(
    con,
    server,
    remote_schema,
    table,
    staging_schema
) {

  kind <- .pg_transfer_relation_kind(
    con,
    staging_schema,
    table
  )

  # Import the foreign table only if it is missing.
  if (is.na(kind)) {

    sql <- paste0(
      "IMPORT FOREIGN SCHEMA ",
      .pg_transfer_qi(con, remote_schema),
      " LIMIT TO (",
      .pg_transfer_qi(con, table),
      ") FROM SERVER ",
      .pg_transfer_qi(con, server),
      " INTO ",
      .pg_transfer_qi(con, staging_schema)
    )

    DBI::dbExecute(con, sql)

    kind <- .pg_transfer_relation_kind(
      con,
      staging_schema,
      table
    )
  }

  if (is.na(kind) || kind != "f") {
    stop(
      staging_schema, ".", table,
      " exists but is not a foreign table.",
      call. = FALSE
    )
  }

  # Read options as rows instead of parsing PostgreSQL's
  # text-array representation manually.
  mapping <- DBI::dbGetQuery(
    con,
    "
    SELECT
      s.srvname AS server_name,
      o.option_name,
      o.option_value
    FROM pg_foreign_table ft
    JOIN pg_class c
      ON c.oid = ft.ftrelid
    JOIN pg_namespace n
      ON n.oid = c.relnamespace
    JOIN pg_foreign_server s
      ON s.oid = ft.ftserver
    CROSS JOIN LATERAL
      pg_options_to_table(ft.ftoptions) o
    WHERE n.nspname = $1
      AND c.relname = $2
    ",
    params = list(staging_schema, table)
  )

  if (nrow(mapping) == 0L) {

    # A foreign table can have no explicit table options.
    server_result <- DBI::dbGetQuery(
      con,
      "
      SELECT s.srvname AS server_name
      FROM pg_foreign_table ft
      JOIN pg_class c ON c.oid = ft.ftrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
      JOIN pg_foreign_server s ON s.oid = ft.ftserver
      WHERE n.nspname = $1
        AND c.relname = $2
      ",
      params = list(staging_schema, table)
    )

    if (nrow(server_result) != 1L) {
      stop(
        "Could not verify FDW mapping for ",
        table,
        call. = FALSE
      )
    }

    actual_server <- server_result$server_name[[1]]
    actual_schema <- NA_character_
    actual_table <- NA_character_

  } else {

    actual_server <- unique(mapping$server_name)[[1]]

    option_value <- function(key) {

      hit <- mapping$option_name == key

      if (!any(hit)) {
        return(NA_character_)
      }

      mapping$option_value[which(hit)[1]]
    }

    actual_schema <- option_value("schema_name")
    actual_table <- option_value("table_name")
  }

  if (
    !identical(actual_server, server) ||
    !identical(actual_schema, remote_schema) ||
    !identical(actual_table, table)
  ) {

    stop(
      "Foreign table mapping mismatch for ",
      staging_schema, ".", table,
      ". Expected ", server, " -> ",
      remote_schema, ".", table,
      "; got ", actual_server, " -> ",
      actual_schema, ".", actual_table,
      call. = FALSE
    )
  }

  invisible(TRUE)
}


# ============================================================
# APPLY TABLE AND COLUMN COMMENTS
# ============================================================

.pg_transfer_apply_comments <- function(
    con,
    schema,
    table,
    metadata
) {

  table_comment <- metadata$table_comment

  comment_sql <- if (
    is.null(table_comment) ||
    length(table_comment) == 0L ||
    is.na(table_comment)
  ) {
    "NULL"
  } else {
    .pg_transfer_qs(con, table_comment)
  }

  DBI::dbExecute(
    con,
    paste0(
      "COMMENT ON TABLE ",
      .pg_transfer_qi(con, schema), ".",
      .pg_transfer_qi(con, table),
      " IS ", comment_sql
    )
  )

  cols <- metadata$columns

  for (i in seq_len(nrow(cols))) {

    comment <- cols$comment[[i]]

    comment_sql <- if (
      is.null(comment) ||
      length(comment) == 0L ||
      is.na(comment)
    ) {
      "NULL"
    } else {
      .pg_transfer_qs(con, comment)
    }

    DBI::dbExecute(
      con,
      paste0(
        "COMMENT ON COLUMN ",
        .pg_transfer_qi(con, schema), ".",
        .pg_transfer_qi(con, table), ".",
        .pg_transfer_qi(
          con,
          cols$column_name[[i]]
        ),
        " IS ", comment_sql
      )
    )
  }

  invisible(TRUE)
}


# ============================================================
# TRANSFER ONE TABLE
# ============================================================


.pg_transfer_one <- function(
    row,
    local_con,
    central_con,
    fdw_server,
    load_schema
) {

  get_value <- function(column) {
    value <- row[[column]][[1]]

    if (
      is.null(value) ||
      length(value) == 0L ||
      is.na(value)
    ) {
      return(NA_character_)
    }

    trimws(as.character(value))
  }

  dataset <- get_value("Dataset_Common_Name")

  source_schema <- get_value("Source_Schema")
  source_table <- get_value("Source_Table")
  full_name <- get_value("Full_Source_Name")

  destination_schema <- get_value("Destination_Schema")
  destination_table <- get_value("Destination_Table")

  refresh <- tolower(get_value("Refresh"))

  # Validate source and destination identifiers.
  required_values <- c(
    Dataset_Common_Name = dataset,
    Source_Schema = source_schema,
    Source_Table = source_table,
    Full_Source_Name = full_name,
    Destination_Schema = destination_schema,
    Destination_Table = destination_table
  )

  invalid <- names(required_values)[
    is.na(required_values) |
      !nzchar(required_values)
  ]

  if (length(invalid) > 0L) {
    stop(
      "Missing or blank configuration values: ",
      paste(invalid, collapse = ", "),
      call. = FALSE
    )
  }

  if (
    is.na(refresh) ||
    !refresh %in% c("static", "check")
  ) {
    stop(
      "Unknown Refresh value: ",
      refresh,
      ". Expected 'static' or 'check'.",
      call. = FALSE
    )
  }

  source <- .pg_transfer_parse_source_name(
    full_name = full_name,
    source_schema = source_schema,
    source_table = source_table
  )

  source_schema <- source$schema
  source_table <- source$table

  target_kind <- .pg_transfer_relation_kind(
    local_con,
    destination_schema,
    destination_table
  )

  target_exists <- !is.na(target_kind)

  # Static datasets are never replaced.
  if (refresh == "static" && target_exists) {

    if (!target_kind %in% c("r", "p")) {
      stop(
        "Existing static destination is not a table: ",
        destination_schema, ".", destination_table,
        call. = FALSE
      )
    }

    return("SKIPPED_STATIC_EXISTS")
  }

  # Retrieve metadata from the central database.
  source_meta <- .pg_transfer_get_metadata(
    central_con,
    source_schema,
    source_table
  )

  # Check datasets are refreshed only if their metadata changes.
  if (refresh == "check" && target_exists) {

    if (!target_kind %in% c("r", "p")) {
      stop(
        "Existing check destination is not a table: ",
        destination_schema, ".", destination_table,
        call. = FALSE
      )
    }

    target_meta <- .pg_transfer_get_metadata(
      local_con,
      destination_schema,
      destination_table
    )

    if (.pg_transfer_same_metadata(
      source_meta,
      target_meta
    )) {
      return("SKIPPED_METADATA_MATCH")
    }
  }

  # Prepare the foreign-table staging schema.
  .pg_transfer_ensure_schema(
    local_con,
    load_schema
  )

  .pg_transfer_ensure_foreign_table(
    con = local_con,
    server = fdw_server,
    remote_schema = source_schema,
    table = source_table,
    staging_schema = load_schema
  )

  # Ensure the destination schema exists.
  .pg_transfer_ensure_schema(
    local_con,
    destination_schema
  )

  destination <- paste0(
    .pg_transfer_qi(local_con, destination_schema),
    ".",
    .pg_transfer_qi(local_con, destination_table)
  )

  staging <- paste0(
    .pg_transfer_qi(local_con, load_schema),
    ".",
    .pg_transfer_qi(local_con, source_table)
  )

  # The replacement is atomic. If a statement fails, the
  # previous destination table is restored by the rollback.
  DBI::dbWithTransaction(local_con, {

    if (target_exists) {

      DBI::dbExecute(
        local_con,
        paste0(
          "DROP TABLE ",
          destination
        )
      )
    }

    # Create the destination with source column types.
    DBI::dbExecute(
      local_con,
      paste0(
        "CREATE TABLE ",
        destination,
        " AS SELECT * FROM ",
        staging,
        " WITH NO DATA"
      )
    )

    # Copy data on the database server, not through R memory.
    DBI::dbExecute(
      local_con,
      paste0(
        "INSERT INTO ",
        destination,
        " SELECT * FROM ",
        staging
      )
    )

    # Restore source table and column comments.
    .pg_transfer_apply_comments(
      con = local_con,
      schema = destination_schema,
      table = destination_table,
      metadata = source_meta
    )
  })

  "SUCCESS"
}


# ============================================================
# RUN CONFIGURATION AND WRITE TRANSFER LOG
# ============================================================

.pg_transfer_run <- function(
    config_file,
    log_file,
    local_con,
    central_con,
    fdw_server,
    load_schema
) {

  # Use base R so readr is not required.
  config <- utils::read.csv(
    config_file,
    stringsAsFactors = FALSE,
    check.names = FALSE,
    na.strings = character(0),
    strip.white = FALSE
  )


  required <- c(
    "Dataset_Common_Name",
    "Source_Schema",
    "Source_Table",
    "Full_Source_Name",
    "Table_Type",
    "Paired_Table",
    "Notes",
    "Refresh",
    "Destination_Schema",
    "Destination_Table"
  )

  missing <- setdiff(required, names(config))

  if (length(missing) > 0L) {
    stop(
      "Missing required CSV columns: ",
      paste(missing, collapse = ", "),
      call. = FALSE
    )
  }


  if (nrow(config) == 0L) {
    stop(
      "The configuration CSV contains no datasets.",
      call. = FALSE
    )
  }

  # Ensure the staging schema exists before processing.
  .pg_transfer_ensure_schema(
    local_con,
    load_schema
  )

  results <- lapply(
    seq_len(nrow(config)),
    function(i) {

      row <- as.list(
        config[i, , drop = FALSE]
      )

      get_value <- function(column) {

        value <- row[[column]][[1]]

        if (
          is.null(value) ||
          length(value) == 0L ||
          is.na(value)
        ) {
          return(NA_character_)
        }

        as.character(value)
      }

      started <- Sys.time()

      message(
        "[", i, "/", nrow(config), "] ",
        get_value("Dataset_Common_Name"), " (",
        get_value("Full_Source_Name"), " -> ",
        get_value("Destination_Schema"), ".",
        get_value("Destination_Table"), ")"
      )

      outcome <- tryCatch(
        .pg_transfer_one(
          row = row,
          local_con = local_con,
          central_con = central_con,
          fdw_server = fdw_server,
          load_schema = load_schema
        ),
        error = function(e) {
          paste0(
            "ERROR: ",
            conditionMessage(e)
          )
        }
      )

      message("  Status: ", outcome)

      data.frame(
        timestamp = format(
          started,
          "%Y-%m-%d %H:%M:%S",
          tz = Sys.timezone()
        ),
        dataset = get_value("Dataset_Common_Name"),
        source = get_value("Full_Source_Name"),
        destination_schema = get_value("Destination_Schema"),
        destination_table = get_value("Destination_Table"),
        refresh = get_value("Refresh"),
        status = outcome,
        stringsAsFactors = FALSE
      )
    }
  )

  log <- do.call(rbind, results)

  rownames(log) <- NULL

  # Create the log directory if necessary.
  log_dir <- dirname(log_file)

  if (!dir.exists(log_dir)) {

    dir.create(
      log_dir,
      recursive = TRUE,
      showWarnings = FALSE
    )
  }

  if (!dir.exists(log_dir)) {
    stop(
      "Could not create log directory: ",
      log_dir,
      call. = FALSE
    )
  }

  # Write a header for new files; append to existing logs.
  file_exists <- file.exists(log_file)

  utils::write.table(
    log,
    file = log_file,
    sep = ",",
    row.names = FALSE,
    col.names = !file_exists,
    append = file_exists,
    qmethod = "double",
    na = "",
    fileEncoding = "UTF-8"
  )

  message(
    "Transfer complete. ",
    sum(log$status == "SUCCESS"),
    " successful; ",
    sum(grepl("^ERROR:", log$status)),
    " errors; ",
    sum(grepl("^SKIPPED_", log$status)),
    " skipped."
  )

  log
}
