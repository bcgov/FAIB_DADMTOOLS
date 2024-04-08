#' Create a PG spatial table from Foreign data wrapper table
#'
#' @param ora_tbl coming soon
#' @param out_tbl coming soon
#' @param pk coming soon
#' @param out_schema coming soon
#' @param ora_conn_param coming soon
#' @param fdw_schema coming soon
#' @return None
#' @export
#'
#' @examples coming soon


fdw_to_geom_tbl <- function(ora_tbl, out_tbl, pk, out_schema, ora_conn_param, fdw_schema = 'load'){

  if (grepl("\\.", ora_tbl)) {
    ora_tbl_no_schema <- strsplit(ora_tbl, "\\.")[[1]][[2]]
  } else {
    ora_tbl_no_schema <- fdwTblName
  }
  ora_tbl_no_schema <- paste(ora_tbl_no_schema, "_sp")
  geom_name <- faib_dadm_tools::get_pg_geom_name(fdw_schema, ora_tbl_no_schema, ora_conn_param)
  out_name_schema <- glue::glue("{out_schema}.{out_tbl}")
  sql <-  glue::glue_sql("SELECT 'SELECT ' || array_to_string(ARRAY(SELECT 'o' || '.' || c.column_name
                                            FROM information_schema.columns As c
                                            WHERE table_name = '{`ora_tbl_no_schema`}'  and
                                            table_schema = '{`fdw_schema`}'
                                            AND  c.column_name IN('{`geom_name`}','{`pk`}')  ), ',')
                     || ' FROM load.{`ora_tbl_no_schema`} As o' As sqlstmt")
  sqlstmt <- (faib_dadm_tools::sql_to_df(sql, ora_conn_param))$sqlstmt
  sql <-  glue::glue_sql("DROP TABLE IF EXISTS {`out_schema`}.{`out_tbl`};")
  faib_dadm_tools::run_sql_r(sql, ora_conn_param)
  sql <-  glue::glue_sql("CREATE TABLE {`out_schema`}.{`out_tbl`} AS {`sqlstmt`};")
  faib_dadm_tools::run_sql_r(sql, ora_conn_param)
  print('Creating index for non spatial table')
  sql <- glue::glue_sql("CREATE INDEX {`out_tbl`}_ogc_inx ON {`out_name_schema`}({`pk`});")
  faib_dadm_tools::run_sql_r(sql, ora_conn_param)
}
