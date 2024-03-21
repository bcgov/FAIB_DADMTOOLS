#' Update the foreign key field table in PG
#' @param nsTbl non-spatial table to add to foreign key filed table (must have a foreign key column in the foreign key lookup table)
#' @param fkTBlName  foreign key lookup table name
#' @param suffix suffix added to the foreign key column
#' @param connList Named list with the following connection parameters Driver,host,user,dbname,password,port,schema
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon

updateFKfldTablePG <- function(nsTbl, fkTBlName, suffix, connList){
  if (grepl("\\.", fkTBlName)) {
    fkTBlNameNoSchema <- strsplit(fkTBlName, "\\.")[[1]][[2]]
    schema <- strsplit(fkTBlName, "\\.")[[1]][[1]]
  } else {
    schema <- 'public'
    fkTBlNameNoSchema <- fkTBlName
  }

  sql <- glue("CREATE TABLE IF NOT EXISTS {fkTBlName}_flds (
                fldname varchar(150) PRIMARY KEY,
                srcTable varchar(200));")
  sendSQLstatement(sql,connList)

  resCols <- getTableQueryPG(glue("SELECT
                                    column_name
                                  FROM
                                    information_schema.columns
                                  WHERE
                                    table_schema = '{schema}'
                                  AND
                                    table_name = '{fkTBlNameNoSchema}'
                                  AND
                                    column_name like '%_{suffix}';"),connList)

  fldCols <- getTableQueryPG(glue("SELECT
                                    fldname
                                  FROM {fkTBlName}_flds;"),connList)


  for (i in 1:nrow(resCols)) {
    val <- resCols$column_name[[i]]
    fldCols <- getTableQueryPG(glue("SELECT
                                      fldname
                                    FROM
                                      {fkTBlName}_flds
                                    WHERE
                                      fldname = '{val}';"), connList)

    query <- glue("INSERT INTO {fkTBlName}_flds (fldname, srcTable) VALUES ('{val}','{nsTbl}')
                  ON CONFLICT (fldname)
                  DO UPDATE
                  SET srcTable = EXCLUDED.srcTable;")
    print(query)
    sendSQLstatement(query, connList)
  }
}
