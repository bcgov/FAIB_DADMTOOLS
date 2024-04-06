#' Update the foreign key field table in PG
#' @param dst_tbl non-spatial table to add to foreign key filed table (must have a foreign key column in the foreign key lookup table)
#' @param dst_schema non-spatial table schema to add to foreign key filed table (must have a foreign key column in the foreign key lookup table)
#' @param gr_skey_tbl current gr_skey table
#' @param suffix suffix added to the foreign key column
#' @param pg_conn_param Named list with the following connection parameters Driver,host,user,dbname,password,port,schema
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon

update_gr_skey_tbl_flds <- function(dst_tbl,
                                    dst_schema, 
                                    gr_skey_tbl, 
                                    suffix, 
                                    pg_conn_param)
{
  if (grepl("\\.", gr_skey_tbl)) {
    fk_schema <- strsplit(gr_skey_tbl, "\\.")[[1]][[1]]
    gr_skey_tbl <- strsplit(gr_skey_tbl, "\\.")[[1]][[2]]
  } else {
    fk_schema <- 'public'
  }

  sql <- glue("CREATE TABLE IF NOT EXISTS {fk_schema}.{gr_skey_tbl}_flds (
                fldname varchar(150) PRIMARY KEY,
                srcTable varchar(200));")
  run_sql_r(sql, pg_conn_param)

  res_cols <- sql_to_df(glue("SELECT
                                column_name
                              FROM
                                information_schema.columns
                              WHERE
                                table_schema = '{fk_schema}'
                              AND
                                table_name = '{gr_skey_tbl}'
                              AND
                                column_name like '%_{suffix}';"), pg_conn_param)

  field_cols <- sql_to_df(glue("SELECT
                                  fldname
                                FROM {fk_schema}.{gr_skey_tbl}_flds;"), pg_conn_param)

  for (i in 1:nrow(res_cols)) {
    val <- res_cols$column_name[[i]]
    field_cols <- sql_to_df(glue("SELECT
                                    fldname
                                  FROM
                                    {fk_schema}.{gr_skey_tbl}_flds
                                  WHERE
                                    fldname = '{val}';"), pg_conn_param)

    query <- glue("INSERT INTO {fk_schema}.{gr_skey_tbl}_flds (fldname, srcTable) VALUES ('{val}','{dst_schema}.{dst_tbl}')
                  ON CONFLICT (fldname)
                  DO UPDATE
                  SET srcTable = EXCLUDED.srcTable;")
    print(query)
    run_sql_r(query, pg_conn_param)
  }
}
