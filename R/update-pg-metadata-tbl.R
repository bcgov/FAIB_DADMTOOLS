#' Updates Foreign Key Data Source Table in PG
#'
#' @param data_src_tbl Data source table name
#' @param src_type data source type (one of 'gdb', 'raster','oracle','geopackage')
#' @param src_path source path (e.g. "w:/test/tes.gdb" or 'bcgw')
#' @param src_lyr data source  (e.g. 'veg_comp_poly' or  'whse_forest_vegetation.f_own')
#' @param pk primary of data source e.g. objectid
#' @param suffix suffix used in resultant columns that were kept form the data source (e.g. "vri2021")
#' @param query where statement used to filter data source
#' @param inc 1 or 0,  indicates if data source is in PG
#' @param rslt_ind indicates the pk is in the  pg resultant or not
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
                                   suffix,
                                   query,
                                   inc,
                                   rslt_ind,
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
  suffix       <- dbQuoteString(connz, suffix)
  dst_schema   <- dbQuoteString(connz, dst_schema)
  dst_tbl      <- dbQuoteString(connz, dst_tbl)
  rslt_ind     <- dbQuoteString(connz, rslt_ind)
  flds_to_keep <- dbQuoteString(connz, flds_to_keep)
  on.exit(RPostgres::dbDisconnect(connz))



  create_sql <- glue::glue("CREATE TABLE IF NOT EXISTS {data_src_tbl} (
                      srctype varchar,
                      srcpath varchar,
                      srclyr varchar,
                      primarykey varchar,
                      suffix varchar,
                      schema varchar,
                      tblname varchar,
                      src_query varchar,
                      rslt_ind integer,
                      fields2keep varchar,
                      notes varchar,
                      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                      CONSTRAINT data_sources_unique UNIQUE (schema, tblname)
                    );")
  dadmtools::run_sql_r(create_sql, pg_conn_param)

  insert_sql <- glue::glue("INSERT INTO {data_src_tbl}
                    (
                     srctype,
                     srcpath,
                     srclyr,
                     primarykey,
                     suffix,
                     schema,
                     tblname,
                     src_query,
                     rslt_ind,
                     fields2keep,
                     notes
                    )
                     VALUES
                    (
                    {src_type},
                    {src_path},
                    {src_lyr},
                    {pk},
                    {suffix},
                    {dst_schema},
                    {dst_tbl},
                    {query},
                    {rslt_ind},
                    {flds_to_keep},
                    {notes}
                    )
                    ON CONFLICT (schema, tblname) DO UPDATE
                    SET
                    srctype = EXCLUDED.srctype,
                    srcpath = EXCLUDED.srcpath,
                    srclyr = EXCLUDED.srclyr,
                    primarykey = EXCLUDED.primarykey,
                    suffix = EXCLUDED.suffix,
                    src_query = EXCLUDED.src_query,
                    rslt_ind = EXCLUDED.rslt_ind,
                    fields2keep = EXCLUDED.fields2keep,
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
