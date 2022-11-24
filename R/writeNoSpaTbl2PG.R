#' Write non spatial tables to PG using gdal on the machine.  Creates index on primary key.  Use for large files that would take too long to import into R
#'
#' @param src path of input dataset e.g. c:/test.gdb
#' @param outTblName Name of output PG table
#' @param dbName Name of PG database
#' @param lyr layer of input dataser e.g. roadsFC
#' @param pk primary of table e.g. objectid
#' @param select fields to keep in table e.g. "objectid,feature_id,segmentName"
#' @param where_clause sql where clause e.g. segment = 'highway2'
#' @param schema default database schema
#'
#' @return no return
#' @export
#'
#' @examples coming soon

writeNoSpaTbl2PG <- function(src,outTblName,connList,lyr=NULL,pk=NULL,select=NULL,where=NULL,schema=NULL){


  dbname <- connList["dbname"][[1]]
  user <- connList["user"][[1]]
  dest <- glue::glue("PG:dbname={single_quote(dbname)} user={single_quote(user)}")

  if(is.null(lyr)){lyr <- NULL}
  else if(is.na(lyr) || lyr == ''){lyr=NULL}

  if(is.null(pk)){ pk <- NULL}
  else if(is.na(pk) || pk == ''){pk=NULL}

  if(is.null(select) ||is.na(select) || select == '' ){print('keep all attributes')
    select <- ''}else{
      select <- gsub(",shape", "", select)
      select <- gsub(",geometry", "", select)
     if (endsWith(select, ',')) {select <-  substr(select,1,nchar(select)-1)}
 ############ else{select <-  sprintf('"%s"', select)}
      }

  print(select)

  if(is.null(schema) || is.na(schema) || schema == '') {
    outName <- outTblName
    outName2 <- outTblName}
  else{
    outName <- paste0(schema,".",outTblName)
    outName2 <- glue("{schema}.{outTblName}")
    }

  if( is.null(where) ||is.na(where) || where == ''){where <- ''}
  # else{
  #   )}
  #
  print('Creating non-spatial table')
  print(outName)

  if( is.null(schema) ||is.na(schema) || schema == ''){schema <- "SCHEMA=public"}else{schema <- glue("SCHEMA={schema}")}

  print(src)
  print(dest)
  print(lyr)
  print(c('-overwrite',
          '-nlt','NONE',
          '-gt', '200000',
          '-where', where,
          '-select', select,
          '-nln', outTblName,
          '-lco', schema,
          '-lco', 'OVERWRITE=YES'))
  sf::gdal_utils("vectortranslate",src,dest,options = c('-overwrite',
                                                        '-nlt','NONE',
                                                        '-gt', '200000',
                                                        '-where', where,
                                                        '-select', select,
                                                        '-nln', outTblName,
                                                        '-lco', schema,
                                                        '-lco', 'OVERWRITE=YES'),lyr)

  # print(system2('ogr2ogr',args=c('-nlt NONE',
  #                                '-overwrite',
  #                                '-gt 200000',
  #                                where,
  #                                select,
  #                                outName,
  #                                '--config PG_USE_COPY YES',
  #                                '-progress',
  #                                paste0('-f PostgreSQL PG:dbname=',connList["dbname"][[1]]),
  #                                src,
  #                                lyr), stderr = TRUE))
  #Create index on pk
  if( !is.null(pk)){
    print('Creatin index for non spatial table')
    outTblNameNoPer <-  gsub("\\.", "_", outTblName)
    faibDataManagement::sendSQLstatement(paste0("drop index if exists ", outTblNameNoPer,"_grskey_inx;"),connList)
    print('dropped index')
    faibDataManagement::sendSQLstatement(paste0("create index ", outTblNameNoPer,"_grskey_inx",  " on ", outName2, "(", pk,");"),connList)}}
