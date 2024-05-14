#' Update the gr_skey table with primary keys from input table
#' @param dst_tbl input pg table to be incorporated into gr_skey table
#' @param dst_schema input pg table schema to be incorporated into gr_skey table
#' @param pk column to incorporated into gr_skey table
#' @param suffix suffix to be appended to the pk for the column name in the gr_skey table
#' @param gr_skey_tbl current gr_skey table
#' @param pg_conn_param Named list with the following connection parameters Driver,host,user,dbname,password,port,schema
#' @param join_field defaults to 'gr_skey')
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon


add_pk_to_gr_skey_tbl  <- function(dst_tbl, 
                                  dst_schema,
                                  pk, 
                                  suffix, 
                                  gr_skey_tbl, 
                                  pg_conn_param, 
                                  join_field = 'gr_skey')
{
  print("Performing rslt_ind foreign key lookup table update...")
  if (grepl("\\.", gr_skey_tbl)) {
    fk_schema <- strsplit(gr_skey_tbl, "\\.")[[1]][[1]]
    gr_skey_tbl <- strsplit(gr_skey_tbl, "\\.")[[1]][[2]]
  } else {
   gr_skey_tbl <- gr_skey_tbl
   fk_schema <- 'public'
  }
  ## retrieve any columns in the foreign key lookup table that have the current suffix and then remove them
  ## this is to be able to overwrite any relevant fields
  pk_out <- glue("{pk}_{suffix}")
  res_cols <- sql_to_df(glue("SELECT
                                    column_name
                                  FROM
                                    information_schema.columns
                                  WHERE
                                    table_schema = '{fk_schema}'
                                  AND
                                    table_name = '{gr_skey_tbl}'
                                  AND
                                    column_name LIKE '%_{suffix}';"), pg_conn_param)
  if(is.data.frame(res_cols) && nrow(res_cols)> 0) {
    for (i in 1:length(res_cols$column_name)){
      val <- res_cols$column_name[i]
      print(glue("Dropping column: {val} from table: {fk_schema}.{gr_skey_tbl}"))
      run_sql_r(glue("ALTER TABLE {fk_schema}.{gr_skey_tbl} DROP COLUMN {val};"), pg_conn_param)
    }
  }

  run_sql_r(glue("DROP TABLE IF EXISTS {fk_schema}.{gr_skey_tbl}_{join_field};"), pg_conn_param)
  run_sql_r(glue("CREATE TABLE {fk_schema}.{gr_skey_tbl}_{join_field} AS
                        SELECT
                          a.*,
                          b.{pk} as {pk_out}
                        FROM
                          {fk_schema}.{gr_skey_tbl} a
                        LEFT JOIN {dst_schema}.{dst_tbl} b USING ({join_field});"), pg_conn_param)
  run_sql_r(glue("DROP TABLE IF EXISTS {fk_schema}.{gr_skey_tbl};"), pg_conn_param)
  run_sql_r(glue("ALTER TABLE {fk_schema}.{gr_skey_tbl}_{join_field} RENAME TO {gr_skey_tbl};"), pg_conn_param)
  run_sql_r(glue("ALTER TABLE {fk_schema}.{gr_skey_tbl} ADD PRIMARY KEY (gr_skey);"), pg_conn_param)
  print("Foreign key lookup table updated.")
}
