#' Creates FROM clause string from the standard FAIB_DATA_MANAGEMETN input csv.  Only uses inputs where inc = 1
#' @param inPgTbl Input master gr_Key PG Table Name (containing foreign keys)
#' @param schema PG database schema
#' @param inDF input data frame in same format as input csv
#' @param onlyIncRows include of rows wwhere onc field is = 1
#' @param filterSuffixVect vector of suffixes to be included in string
#' @return String
#' @export
#'
#' @examples sqljoins_asstring(masterTable,schema='postgres',inCSV='D:/inputs/inputCsv.csv')


sqljoins_asstring <- function(inPgTbl,schema,inDF,onlyIncRows=FALSE,filterSuffixVect = NULL){
  joinlist <- list()
  for (row in 1:nrow(inDF)) {
    inputVect <- c()
    inc <- gsub("[[:space:]]",'',tolower(inDF[row, "inc"])) ##  1 = include(i.e. will not skip) 0 = not included (i.e. will skip)
    rslt_ind <- gsub("[[:space:]]",'',tolower(inDF[row, "rslt_ind"])) ##1 = include(i.e. will add data to provincial resultant) 0 = not included (i.e. will not add data to provincial resultant)
    srctype <- gsub("[[:space:]]",'',tolower(inDF[row, "srctype"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    srcpath <- gsub("[[:space:]]",'',tolower(inDF[row, "srcpath"]))## path to input data. Note use bcgw for whse
    srclyr <- gsub("[[:space:]]",'',tolower(inDF[row, "srclyr"])) ## input layer name
    pk <- gsub("[[:space:]]",'',tolower(inDF[row, "primarykey"])) ## primary key field that will be added to resultant table
    suffix <- gsub("[[:space:]]",'',tolower(inDF[row, "suffix"])) ## suffix to be used in the resultant table
    nsTblm <- gsub("[[:space:]]",'',tolower(inDF[row, "tblname"])) ## name of output non spatial table
    query <- tolower(inDF[row, "src_query"])  ##where clause used to filter input dataset
    flds2keep <- gsub("[[:space:]]",'',tolower(inDF[row, "fields2keep"])) ## fields to keep in non spatial table
    inputVect <- append(inputVect,suffix)
    if(rslt_ind == 1  &&  srctype != 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,' ', suffix, ' on a.', pk,'_',suffix,' = ',suffix,'.',pk)}
    else if(rslt_ind == 0  &&  srctype != 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,'_gr_skey ', suffix, '_gr_skey on a.gr_skey = ',suffix,'_gr_skey.gr_skey',
                     ' left outer join ', schema, '.', nsTblm,' ', suffix, ' on ', suffix, '_gr_skey.',pk,' = ',suffix,'.',pk)}
    else  if(rslt_ind == 1  &&  srctype == 'raster'){ join <- ' '}
    else  if(rslt_ind == 0  &&  srctype == 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,'_gr_skey ', suffix, '_gr_skey on a.gr_skey = ',suffix,'_gr_skey.gr_skey')}

    inputVect <- append(inputVect,join)
    inputVect <- append(inputVect,inc)
    joinlist[[length(joinlist)+1]] <- inputVect
  }

  finalString <- paste0(schema,'.',inPgTbl, ' a ')
  if(onlyIncRows){
    for(vec in joinlist){
      if(vec[3] == 1){
        if(is.null(filterSuffixVect) | vec[1] %in% filterSuffixVect )
        {finalString <- paste(finalString, vec[2])}
      }}
  }else {for(vec in joinlist){
    if(is.null(filterSuffixVect) | vec[1] %in% filterSuffixVect )
    {
      finalString <- paste(finalString, vec[2])}
  }}
  return(shQuote(finalString))
}
