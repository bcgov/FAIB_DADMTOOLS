#' Updates Foreign Key Data Source Table in PG
#'
#' @param data_src_tbl Data source table name
#' @param src_type data source type (one of 'gdb', 'raster','oracle','geopackage')
#' @param src_path source path (e.g. "w:/test/tes.gdb" or 'bcgw')
#' @param src_lyr data source  (e.g. 'veg_comp_poly' or  'whse_forest_vegetation.f_own')
#' @param pk primary of data source e.g. objectid
#' @param query where statement used to filter data source
#' @param inc 1 or 0,  indicates if data source is in PG
#' @param flds_to_keep string of fields kept from data input (e.g. "feature_id,proj_age_1,live_stand_volume_125")
#' @param notes coming soon
#' @param dst_tbl coming soon
#' @param dst_schema coming soon
#' @param pg_conn_param coming soon
#' @return no return
#' @export
#'
#' @examples coming soon



update_pg_metadata_tbl <- function(data_src_tbl,
                                   src_type,
                                   src_path,
                                   src_lyr,
                                   pk,
                                   query,
                                   inc,
                                   flds_to_keep,
                                   notes,
                                   dst_tbl,
                                   dst_schema,
                                   pg_conn_param)
{
  print(glue("Inserting record into {data_src_tbl} for {dst_schema}.{dst_tbl}"))
  if(is_blank(flds_to_keep)) {
    flds_to_keep <- ""
  }

  if(is_blank(notes)) {
    notes <- ""
  }

  data_source_schema <- strsplit(data_src_tbl, "\\.")[[1]][[1]]
  data_source_tbl <- strsplit(data_src_tbl, "\\.")[[1]][[2]]

  connz <- dbConnect(pg_conn_param["driver"][[1]],
                    host     = pg_conn_param["host"][[1]],
                    user     = pg_conn_param["user"][[1]],
                    dbname   = pg_conn_param["dbname"][[1]],
                    password = pg_conn_param["password"][[1]],
                    port     = pg_conn_param["port"][[1]])

  query        <- dbQuoteString(connz, query)
  notes        <- dbQuoteString(connz, notes)
  src_type     <- dbQuoteString(connz, src_type)
  src_path     <- dbQuoteString(connz, src_path)
  src_lyr      <- dbQuoteString(connz, src_lyr)
  pk           <- dbQuoteString(connz, pk)
  dst_schema   <- dbQuoteString(connz, dst_schema)
  dst_tbl      <- dbQuoteString(connz, dst_tbl)
  flds_to_keep <- dbQuoteString(connz, flds_to_keep)
  on.exit(RPostgres::dbDisconnect(connz))



  create_sql <- glue::glue("CREATE TABLE IF NOT EXISTS {data_src_tbl} (
                      src_type varchar,
                      src_path varchar,
                      src_lyr varchar,
                      primary_key varchar,
                      dst_schema varchar,
                      dst_tbl varchar,
                      query varchar,
                      flds_to_keep varchar,
                      notes varchar,
                      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                      CONSTRAINT data_sources_unique UNIQUE (dst_schema, dst_tbl)
                    );")
  dadmtools::run_sql_r(create_sql, pg_conn_param)

  insert_sql <- glue::glue("INSERT INTO {data_src_tbl}
                    (
                     src_type,
                     src_path,
                     src_lyr,
                     primary_key,
                     dst_schema,
                     dst_tbl,
                     query,
                     flds_to_keep,
                     notes
                    )
                     VALUES
                    (
                    {src_type},
                    {src_path},
                    {src_lyr},
                    {pk},
                    {dst_schema},
                    {dst_tbl},
                    {query},
                    {flds_to_keep},
                    {notes}
                    )
                    ON CONFLICT (dst_schema, dst_tbl) DO UPDATE
                    SET
                    src_type = EXCLUDED.src_type,
                    src_path = EXCLUDED.src_path,
                    src_lyr = EXCLUDED.src_lyr,
                    primary_key = EXCLUDED.primary_key,
                    dst_schema = EXCLUDED.dst_schema,
                    query = EXCLUDED.query,
                    flds_to_keep = EXCLUDED.flds_to_keep,
                    notes = EXCLUDED.notes,
                    created_at = now();")
  print(glue("Inserting record into {data_src_tbl} for {dst_schema}.{dst_tbl}"))

  tryCatch({
    # Code that might raise the error
    # For example:
    result <- dadmtools::run_sql_r(insert_sql, pg_conn_param)
    result
  }, error = function(e) {
    if (grepl("does not exist", e$message)) {
      # in the event of an error where a column does not exist, rename the current table and make a new one and insert into it
      # Handling specific error

      today_date <- format(Sys.Date(), "%Y_%m_%d")
      drop_qry <- glue("DROP TABLE IF EXISTS {data_source_schema}.{data_source_tbl}_deprecated_on_{today_date}")
      dadmtools::run_sql_r(drop_qry, pg_conn_param)
      rename_qry <- glue("ALTER TABLE {data_src_tbl} RENAME TO {data_source_tbl}_deprecated_on_{today_date}")
      dadmtools::run_sql_r(rename_qry, pg_conn_param)
      print(glue("WARNING: Missing column in {data_src_tbl}, renaming current data sources table to: {data_source_schema}.{data_source_tbl}_deprecated_on_{today_date} and creating new table: {data_src_tbl}"))
      comment_qry <- glue("COMMENT ON TABLE {data_source_schema}.{data_source_tbl}_deprecated_on_{today_date} IS 'Table was deprecated and renamed on {today_date} by dadmtools R package. See preferred table: {data_src_tbl}'")
      dadmtools::run_sql_r(comment_qry, pg_conn_param)
      check_for_constraint_qry <- glue("SELECT con.conname = 'data_sources_unique' as con_check
       FROM pg_catalog.pg_constraint con
            INNER JOIN pg_catalog.pg_class rel
                       ON rel.oid = con.conrelid
            INNER JOIN pg_catalog.pg_namespace nsp
                       ON nsp.oid = connamespace
       WHERE nsp.nspname = '{data_source_schema}'
             AND rel.relname = '{data_source_tbl}_deprecated_on_{today_date}';")
      check_df <- dadmtools::sql_to_df(check_for_constraint_qry, pg_conn_param)
      if (nrow(check_df) > 0) {
        drop_constraint <- glue("ALTER TABLE {data_source_schema}.{data_source_tbl}_deprecated_on_{today_date} DROP CONSTRAINT data_sources_unique")
        dadmtools::run_sql_r(drop_constraint, pg_conn_param)
      }
      dadmtools::run_sql_r(create_sql, pg_conn_param)
      dadmtools::run_sql_r(insert_sql, pg_conn_param)
    } else {
      # Handle other errors
      print(glue("ERROR: Unknown error ran into when attempting to insert into {data_src_tbl}. Error: {e$message}"))
      return()
    }
  }
  )
}
