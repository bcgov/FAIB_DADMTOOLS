#' Update the foreign key lookup table in PG
#' @param inLayer input pg table to be incorporated into foreign key lookup table
#' @param pk column to incorporated into foreign key lookup table
#' @param suffix suffix added to the foreign key column
#' @param fkTBlName current foreign key lookup table
#' @param connList Named list with the following connection parameters Driver,host,user,dbname,password,port,schema
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon


updateFKlookupPG  <- function(inLayer, pk, suffix, fkTBlName, connList, joinFld = 'gr_skey'){
  print("Performing rslt_ind foreign key lookup table update...")
  if (grepl("\\.", fkTBlName)) {
    fkTBlNameNoSchema <- strsplit(fkTBlName, "\\.")[[1]][[2]]
    schema <- strsplit(fkTBlName, "\\.")[[1]][[1]]
  } else {
    schema <- 'public'
   fkTBlNameNoSchema <- fkTBlName
  }
  ## retrieve any columns in the foreign key lookup table that have the current suffix and then remove them
  ## this is to be able to overwrite any relevant fields
  pkOut <- glue("{pk}_{suffix}")
  resCols <- getTableQueryPG(glue("SELECT
                                    column_name
                                  FROM
                                    information_schema.columns
                                  WHERE
                                    table_schema = '{schema}'
                                  AND
                                    table_name = '{fkTBlNameNoSchema}'
                                  AND
                                    column_name LIKE '%_{suffix}';"), connList)
  if(is.data.frame(resCols) && nrow(resCols)> 0) {
    for (i in 1:length(resCols$column_name)){
      val <- resCols$column_name[i]
      print(glue("Dropping column: {val} from table: {fkTBlName}"))
      sendSQLstatement(glue("ALTER TABLE {fkTBlName} DROP COLUMN {val};"),connList)
    }
  }

  sendSQLstatement(glue("DROP TABLE IF EXISTS {fkTBlName}_{joinFld};"),connList)
  sendSQLstatement(glue("CREATE TABLE {fkTBlName}_{joinFld} AS
                        SELECT
                          a.*,
                          b.{pk} as {pkOut}
                        FROM
                          {fkTBlName} a
                        LEFT JOIN {inLayer} b USING ({joinFld});"),connList)
  sendSQLstatement(glue("DROP TABLE IF EXISTS {fkTBlName};"),connList)
  sendSQLstatement(glue("ALTER TABLE {fkTBlName}_{joinFld} RENAME TO {fkTBlNameNoSchema};"),connList)
  sendSQLstatement(glue("ALTER TABLE {fkTBlName} ADD PRIMARY KEY (gr_skey);"),connList)
  print("Foreign key lookup table updated.")
}
