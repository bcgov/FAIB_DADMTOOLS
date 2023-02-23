#' Create a PG spatial table from Foreign data wrapper table
#'
#' @param oratable coming soon
#' @param outTblName coming soon
#' @param pk coming soon
#' @param schema coming soon
#' @param connList coming soon
#' @return None
#' @export
#'
#' @examples coming soon


fdwTbl2PGSpatial <- function(oratable,outTblName,pk,outSchema,connList,fdwSchema = 'load'){

  if (grepl("\\.", oratable)) {
    oraTblNameNoSchema <- unlist(strsplit(oratable, split = "[.]"))[-1]
  }else {oraTblNameNoSchema <- fdwTblName}
  oraTblNameNoSchema <- paste(oraTblNameNoSchema,"_sp")
  geomName <- faibDataManagement::getGeomNamePG(fdwSchema,oraTblNameNoSchema,connList)
  outNameSchema <- glue::glue("{outSchema}.{outTblName}")
  sql <-  glue::glue_sql("SELECT 'SELECT ' || array_to_string(ARRAY(SELECT 'o' || '.' || c.column_name
                                            FROM information_schema.columns As c
                                            WHERE table_name = '{`oraTblNameNoSchema`}'  and
                                            table_schema = '{`fdwSchema`}'
                                            AND  c.column_name IN('{`geomName`}','{`pk`}')  ), ',')
                     || ' FROM load.{`oraTblNameNoSchema`} As o' As sqlstmt")
  sqlstmt <- (faibDataManagement::getTableQueryPG(sql,connList))$sqlstmt
  sql <-  glue::glue_sql("drop table if exists {`outSchema`}.{`outTblName`};")
  faibDataManagement::sendSQLstatement(sql,connList)
  sql <-  glue::glue_sql("create table {`outSchema`}.{`outTblName`} as {`sqlstmt`};")
  faibDataManagement::sendSQLstatement(sql,connList)
  print('Creatin index for non spatial table')
  sql <- glue::glue_sql("create index {`outTblName`}_ogc_inx on {`outNameSchema`}({`pk`});")
  faibDataManagement::sendSQLstatement(sql,connList)
}
