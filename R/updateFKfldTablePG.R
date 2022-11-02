#' Update the foreign key field table in PG
#' @param nsTbl non-spatial table to add to foreign key filed table (must have a foreign key column in the foreing key lookup table)
#' @param fkTBlName  foreign key lookup table name
#' @param suffix suffix added to the foreign key column
#' @param connList Named list with the following connection parameters Driver,host,user,dbname,password,port,schema
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon

updateFKfldTablePG <- function(nsTbl,fkTBlName,suffix,connList){
  if (grepl("\\.", fkTBlName)) {
    fkTBlNameNoSchema <- unlist(strsplit(fkTBlName, split = "[.]"))[-1]
    schema <- unlist(strsplit(fkTBlName, split = "[.]"))[1]

  }
  else{schema <- 'public'
  fkTBlNameNoSchema <- fkTBlName}


  sql <- paste0("CREATE TABLE IF NOT EXISTS ", fkTBlName,"_flds (fldname varchar(100),srcTable varchar(100));")
  sendSQLstatement(sql,connList)

  resCols <- getTableQueryPG(paste0(" select column_name
                              from information_schema.columns
                              where table_schema = '",schema,"' and table_name = '",fkTBlNameNoSchema,"' and column_name like '%_", suffix,"';"),connList)

  fldCols <- getTableQueryPG(paste0("select fldname
                              from ", fkTBlName,"_flds;"),connList)


  for (i in 1:nrow(resCols))
  { print(i)
    val <- resCols$column_name[[i]]
    fldCols <- getTableQueryPG(paste0("select fldname
                              from ", fkTBlName,"_flds where fldname = '", val, "';"),connList)

    if(is.data.frame(fldCols) && nrow(resCols)> 0){
      print(paste0("DELETE FROM ", fkTBlName,"_flds where fldname = '", val, "';"))
      sendSQLstatement(paste0("DELETE FROM ", fkTBlName,"_flds where fldname = '", val, "';"),connList)
    }
    print(paste0("INSERT INTO ", fkTBlName,"_flds(fldname, srcTable) VALUES ('",val,"','",nsTbl,"');"))
    sendSQLstatement(paste0("INSERT INTO ", fkTBlName,"_flds(fldname, srcTable) VALUES ('",val,"','",nsTbl,"');"),connList)
  }
}
