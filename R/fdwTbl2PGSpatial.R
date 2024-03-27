#' Create a PG spatial table from Foreign data wrapper table
#'
#' @param ora_tbl coming soon
#' @param out_tbl coming soon
#' @param pk coming soon
#' @param schema coming soon
#' @param ora_conn_param coming soon
#' @return None
#' @export
#'
#' @examples coming soon


fdwTbl2PGSpatial <- function(ora_tbl, out_tbl, pk, out_schema, ora_conn_param, fdw_schema = 'load'){

  if (grepl("\\.", ora_tbl)) {
    ora_tbl_no_schema <- unlist(strsplit(ora_tbl, split = "[.]"))[-1]
  }else {ora_tbl_no_schema <- fdwTblName}
  ora_tbl_no_schema <- paste(ora_tbl_no_schema, "_sp")
  geom_name <- faibDataManagement::getGeomNamePG(fdw_schema, ora_tbl_no_schema, ora_conn_param)
  out_name_schema <- glue::glue("{out_schema}.{out_tbl}")
  sql <-  glue::glue_sql("SELECT 'SELECT ' || array_to_string(ARRAY(SELECT 'o' || '.' || c.column_name
                                            FROM information_schema.columns As c
                                            WHERE table_name = '{`ora_tbl_no_schema`}'  and
                                            table_schema = '{`fdw_schema`}'
                                            AND  c.column_name IN('{`geom_name`}','{`pk`}')  ), ',')
                     || ' FROM load.{`ora_tbl_no_schema`} As o' As sqlstmt")
  sqlstmt <- (faibDataManagement::getTableQueryPG(sql, ora_conn_param))$sqlstmt
  sql <-  glue::glue_sql("drop table if exists {`out_schema`}.{`out_tbl`};")
  faibDataManagement::sendSQLstatement(sql, ora_conn_param)
  sql <-  glue::glue_sql("create table {`out_schema`}.{`out_tbl`} as {`sqlstmt`};")
  faibDataManagement::sendSQLstatement(sql, ora_conn_param)
  print('Creating index for non spatial table')
  sql <- glue::glue_sql("create index {`out_tbl`}_ogc_inx on {`out_name_schema`}({`pk`});")
  faibDataManagement::sendSQLstatement(sql, ora_conn_param)
}
