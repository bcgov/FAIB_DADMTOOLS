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


updateFKlookupPG  <- function(inLayer,pk,suffix,fkTBlName,connList,joinFld='gr_skey'){
  if (grepl("\\.", fkTBlName)) {
    fkTBlNameNoSchema <- unlist(strsplit(fkTBlName, split = "[.]"))[-1]
    schema <- unlist(strsplit(fkTBlName, split = "[.]"))[1]

  }
  else{schema <- 'public'
  fkTBlNameNoSchema <- fkTBlName }

  pkOut <- paste0(pk,"_", suffix)
  resCols <- getTableQueryPG(paste0(" select column_name
                              from information_schema.columns
                              where table_schema = '",schema,"' and  table_name = '", fkTBlNameNoSchema,"' and column_name like '%_", suffix,"';"),connList)

  if(is.data.frame(resCols) && nrow(resCols)> 0){
    for (i in 1:length(resCols$column_name)){
      val <- resCols$column_name[i]
      print(val)
      sendSQLstatement(paste0("alter table ", fkTBlName," drop column ", val,";"),connList)
    }
  }

  sendSQLstatement(glue("drop table if exists {fkTBlName}_{joinFld};"),connList)
  sendSQLstatement(glue("create table {fkTBlName}_{joinFld} as
                   select a.*,b.{pk} as {pkOut}
                   from {fkTBlName} a
                   left outer join  {inLayer} b on a.{joinFld} = b.{joinFld};"),connList)
  sendSQLstatement(glue("drop table if exists {fkTBlName}_old cascade;"),connList)
  sendSQLstatement(glue("ALTER TABLE {fkTBlName} RENAME TO {fkTBlNameNoSchema}_old;"),connList)
  sendSQLstatement(glue("ALTER TABLE {fkTBlName}_{joinFld} RENAME TO {fkTBlNameNoSchema};"),connList)
  sendSQLstatement(glue("ALTER TABLE {fkTBlName} ADD PRIMARY KEY ({joinFld});"),connList)
}
