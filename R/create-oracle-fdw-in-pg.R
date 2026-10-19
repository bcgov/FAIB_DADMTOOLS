#' Create Oracle Foreign Data Wrapper Table in Postgres
#'
#' Imports an Oracle table as a foreign table via oracle_fdw. After import, any
#' Oracle SDO_GEOMETRY column that oracle_fdw mapped to text (which happens when
#' the column was declared through the PUBLIC synonym rather than
#' MDSYS.SDO_GEOMETRY) is retyped to PostGIS geometry. Geometry column names are
#' looked up from Oracle's data dictionary, so they don't need to be called SHAPE.
#'
#' @param in_table oracle table to import (e.g. WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY )
#' @param ora_conn_param  named list of oracle connection parameters (see get_ora_conn_list() function for more details)
#' @param pg_conn_param named list of postgres connection parameters (see get_pg_conn_list() function for more details)
#' @param out_server_name name of FDW Server that will be created
#' @param out_schema name of schema where foreign table will be imported
#'
#' @return string of output foreign table (schema.in_table)
#'
#' @examples coming soon


create_oracle_fdw_in_pg <- function(in_table, ora_conn_param, pg_conn_param, out_server_name, out_schema) {

  ora_server <- ora_conn_param["server"][[1]]
  idir <- ora_conn_param["user"][[1]]
  orapass <- ora_conn_param["password"][[1]]

  FDWServerExists <- function(out_server_name, pg_conn_param){
    sql <- glue('SELECT * FROM information_schema.foreign_servers f WHERE foreign_server_name = \'{out_server_name}\'')
    r <- sql_to_df(sql, pg_conn_param)
    nrow(r) > 0
  }

  if (grepl("\\.", in_table)) {
    ora_tbl_no_schema <- strsplit(in_table, "\\.")[[1]][[2]]
    ora_schema <- toupper(strsplit(in_table, "\\.")[[1]][[1]])
  } else {
    ora_schema <- ''
    ora_tbl_no_schema <- in_table
  }

  dadmtools::run_sql_r(glue('DROP FOREIGN TABLE IF EXISTS {out_schema}.{ora_tbl_no_schema};'), pg_conn_param)
  dadmtools::run_sql_r(glue('CREATE SCHEMA IF NOT EXISTS {out_schema};'), pg_conn_param)

  if(!(FDWServerExists(out_server_name, pg_conn_param))) {
    dadmtools::run_sql_r(glue("CREATE SERVER {out_server_name} FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '{ora_server}' );"), pg_conn_param)
    dadmtools::run_sql_r(glue("CREATE USER MAPPING FOR CURRENT_USER SERVER {out_server_name} OPTIONS (user '{idir}', password '{orapass}');"), pg_conn_param)
  } else {
    ## update the USER MAPPING username & password for when your IDIR password changes
    dadmtools::run_sql_r(glue("ALTER USER MAPPING FOR CURRENT_USER SERVER {out_server_name} OPTIONS (SET password '{orapass}');"), pg_conn_param)
    dadmtools::run_sql_r(glue("ALTER USER MAPPING FOR CURRENT_USER SERVER {out_server_name} OPTIONS (SET user '{idir}');"), pg_conn_param)
  }

  print(glue('Creating PG FDW table: {out_schema}.{ora_tbl_no_schema} from Oracle table: {in_table}'))
  qry <- glue('IMPORT FOREIGN SCHEMA "{ora_schema}" LIMIT TO ({ora_tbl_no_schema}) FROM SERVER {out_server_name} INTO {out_schema};')
  print(qry)
  dadmtools::run_sql_r(qry, pg_conn_param)

  fix_text_geometry_cols(ora_schema, ora_tbl_no_schema, out_server_name, out_schema, pg_conn_param)

  return(glue("{out_schema}.{ora_tbl_no_schema}"))
}


#' Retype SDO_GEOMETRY columns that oracle_fdw imported as text
#'
#' IMPORT FOREIGN SCHEMA only maps a column to geometry when ALL_TAB_COLUMNS
#' reports DATA_TYPE_OWNER = 'MDSYS'. Columns declared through the PUBLIC
#' synonym report 'PUBLIC' and come through as text, even though they are the
#' same type. This looks up every SDO_GEOMETRY column in Oracle (any owner) and
#' retypes the matching foreign table column to geometry where needed.
#'
#' @keywords internal
fix_text_geometry_cols <- function(ora_schema, ora_tbl_no_schema, out_server_name, out_schema, pg_conn_param) {

  pg_tbl <- tolower(ora_tbl_no_schema)
  ora_tbl <- toupper(ora_tbl_no_schema)
  owner_clause <- if (ora_schema == '') "owner = USER" else glue("owner = ''{ora_schema}''")

  ## Temporary foreign table over Oracle's dictionary, scoped to this table's
  ## geometry columns. Quotes inside the OPTIONS string are doubled.
  lookup_tbl <- glue("{out_schema}.fdw_geom_cols_{pg_tbl}")
  lookup_tbl <- substr(lookup_tbl, 1, nchar(out_schema) + 1 + 63)  # PG identifier limit
  dadmtools::run_sql_r(glue("DROP FOREIGN TABLE IF EXISTS {lookup_tbl};"), pg_conn_param)
  on.exit(dadmtools::run_sql_r(glue("DROP FOREIGN TABLE IF EXISTS {lookup_tbl};"), pg_conn_param), add = TRUE)

  dadmtools::run_sql_r(glue(
    "CREATE FOREIGN TABLE {lookup_tbl} (column_name text, data_type text, data_type_owner text)
     SERVER {out_server_name}
     OPTIONS (table '(SELECT column_name, data_type, data_type_owner
                      FROM all_tab_columns
                      WHERE {owner_clause}
                        AND table_name = ''{ora_tbl}''
                        AND data_type IN (''SDO_GEOMETRY'', ''ST_GEOMETRY''))');"
  ), pg_conn_param)

  ora_geom <- sql_to_df(glue("SELECT column_name, data_type, data_type_owner FROM {lookup_tbl};"), pg_conn_param)
  if (nrow(ora_geom) == 0) return(invisible(NULL))

  ## How the imported columns are currently typed in Postgres
  pg_cols <- sql_to_df(glue(
    "SELECT column_name, data_type, udt_name
     FROM information_schema.columns
     WHERE table_schema = '{out_schema}' AND table_name = '{pg_tbl}';"
  ), pg_conn_param)

  for (i in seq_len(nrow(ora_geom))) {
    ora_col <- ora_geom$column_name[i]
    ## oracle_fdw's default case = 'smart': all-uppercase names are lowercased, others kept as is
    pg_col <- if (ora_col == toupper(ora_col)) tolower(ora_col) else ora_col

    if (ora_geom$data_type[i] == 'ST_GEOMETRY') {
      warning(glue("{ora_tbl}.{ora_col} is Esri ST_GEOMETRY, which oracle_fdw cannot convert. ",
                   "Use a query-based foreign table with sde.st_asbinary() instead."))
      next
    }

    current <- pg_cols$udt_name[pg_cols$column_name == pg_col]
    if (length(current) == 0) {
      warning(glue("Geometry column {ora_col} not found in {out_schema}.{pg_tbl}; skipping."))
      next
    }
    if (current == 'geometry') next

    print(glue("Retyping {out_schema}.{pg_tbl}.{pg_col} from {current} to geometry ",
               "(Oracle type owner: {ora_geom$data_type_owner[i]})"))
    dadmtools::run_sql_r(glue(
      'ALTER FOREIGN TABLE {out_schema}.{pg_tbl} ALTER COLUMN "{pg_col}" TYPE geometry;'
    ), pg_conn_param)
  }

  invisible(NULL)
}
