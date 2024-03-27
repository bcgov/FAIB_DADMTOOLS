#' Update the foreign key lookup table in PG
#' @param dst_tbl input pg table to be incorporated into foreign key lookup table
#' @param dst_schema input pg table schema to be incorporated into foreign key lookup table
#' @param pk column to incorporated into foreign key lookup table
#' @param suffix suffix added to the foreign key column
#' @param fk_tbl current foreign key lookup table
#' @param pg_conn_param Named list with the following connection parameters Driver,host,user,dbname,password,port,schema
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon


updateFKlookupPG  <- function(dst_tbl, 
                              dst_schema,
                              pk, 
                              suffix, 
                              fk_tbl, 
                              pg_conn_param, 
                              join_field = 'gr_skey')
{
  print("Performing rslt_ind foreign key lookup table update...")
  if (grepl("\\.", fk_tbl)) {
    fk_schema <- strsplit(fk_tbl, "\\.")[[1]][[1]]
    fk_tbl <- strsplit(fk_tbl, "\\.")[[1]][[2]]
  } else {
   fk_tbl <- fk_tbl
   fk_schema <- 'public'
  }
  ## retrieve any columns in the foreign key lookup table that have the current suffix and then remove them
  ## this is to be able to overwrite any relevant fields
  pk_out <- glue("{pk}_{suffix}")
  res_cols <- getTableQueryPG(glue("SELECT
                                    column_name
                                  FROM
                                    information_schema.columns
                                  WHERE
                                    table_schema = '{fk_schema}'
                                  AND
                                    table_name = '{fk_tbl}'
                                  AND
                                    column_name LIKE '%_{suffix}';"), pg_conn_param)
  if(is.data.frame(res_cols) && nrow(res_cols)> 0) {
    for (i in 1:length(res_cols$column_name)){
      val <- res_cols$column_name[i]
      print(glue("Dropping column: {val} from table: {fk_schema}.{fk_tbl}"))
      sendSQLstatement(glue("ALTER TABLE {fk_schema}.{fk_tbl} DROP COLUMN {val};"), pg_conn_param)
    }
  }

  sendSQLstatement(glue("DROP TABLE IF EXISTS {fk_schema}.{fk_tbl}_{join_field};"), pg_conn_param)
  sendSQLstatement(glue("CREATE TABLE {fk_schema}.{fk_tbl}_{join_field} AS
                        SELECT
                          a.*,
                          b.{pk} as {pk_out}
                        FROM
                          {fk_schema}.{fk_tbl} a
                        LEFT JOIN {dst_schema}.{dst_tbl} b USING ({join_field});"), pg_conn_param)
  sendSQLstatement(glue("DROP TABLE IF EXISTS {fk_schema}.{fk_tbl};"), pg_conn_param)
  sendSQLstatement(glue("ALTER TABLE {fk_schema}.{fk_tbl}_{join_field} RENAME TO {fk_tbl};"), pg_conn_param)
  sendSQLstatement(glue("ALTER TABLE {fk_schema}.{fk_tbl} ADD PRIMARY KEY (gr_skey);"), pg_conn_param)
  print("Foreign key lookup table updated.")
}
