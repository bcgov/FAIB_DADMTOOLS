#' Create a PG non spatial table from Foreign data wrapper table
#'
#' @param foreignTable coming soon
#' @param outTblName coming soon
#' @param pk coming soon
#' @param schema coming soon
#' @param connList coming soon
#' @param attr2keep optional
#'
#' @return None
#' @export
#'
#' @examples coming soon


fdwTbl2PGnoSpatial <- function(foreignTable,outTblName,pk,outSchema,connList,fdwSchema = 'load', attr2keep=NULL, where = ''){

  if (grepl("\\.", foreignTable)) {


    oraTblNameNoSchema <- unlist(strsplit(foreignTable, split = "[.]"))[-1]
  }else {oraTblNameNoSchema <- fdwTblName}

  geomName <- faibDataManagement::getGeomNamePG(fdwSchema,oraTblNameNoSchema,connList)
  outNameSchema <- paste0(outSchema, ".", outTblName)
  if(where == ''){print("no where clause")}else{
    where <- gsub("\'","\'\'", where)
    where <- glue("where {where}")
  }

    print(where)
  if(is.null(attr2keep) || attr2keep == '' || is.na(attr2keep)){
    print('keep all attributes')
    sqlstmt <- (faibDataManagement::getTableQueryPG(paste0("SELECT 'SELECT ' || array_to_string(ARRAY(SELECT 'o' || '.' || c.column_name
                                            FROM information_schema.columns As c
                                            WHERE table_name = '", oraTblNameNoSchema,"'  and
                                            table_schema = 'load'
                                            AND  c.column_name NOT IN('", geomName,"')
  ), ',') || ' FROM load.",oraTblNameNoSchema," As o ",where, " ' As sqlstmt"),connList))$sqlstmt
  }else{
    #Add apostrophe to field names to keep,remove commas at end
    attr2keep <- paste0("'", attr2keep)
    attr2keep <- gsub(",", "','", attr2keep)
    if (endsWith(attr2keep, ',')) {attr2keep <- substr(attr2keep,1,nchar(attr2keep)-1)}
    attr2keep <- paste0(attr2keep,"'")
    print(attr2keep)
    sqlstmt <- (faibDataManagement::getTableQueryPG(paste0("SELECT 'SELECT ' || array_to_string(ARRAY(SELECT 'o' || '.' || c.column_name
                                            FROM information_schema.columns As c
                                            WHERE table_name = '", oraTblNameNoSchema,"'  and
                                            table_schema = 'load'
                                            AND  c.column_name NOT IN('", geomName,"') and c.column_name in (",attr2keep,")
  ), ',') || ' FROM load.",oraTblNameNoSchema, " As o ",where, " ' As sqlstmt"),connList))$sqlstmt
  }

  faibDataManagement::sendSQLstatement(  paste0('drop table if exists ', outSchema,'.',outTblName,  ';'),connList)
  faibDataManagement::sendSQLstatement(  paste0('create table ', outSchema, '.',outTblName, ' as ', sqlstmt,';'),connList)

  print('Creatin index for non spatial table')
  faibDataManagement::sendSQLstatement(paste0("create index ", outTblName,"_ogc_inx",  " on ", outNameSchema, "(", pk,");"),connList)
  }
