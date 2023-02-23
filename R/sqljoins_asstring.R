#' Creates a string  that can be used in a sql from clause.  Uses a standard input csv to create join
#' @param inPgTbl Iput PG Table Name
#' @param schema PG database schema
#' @param inCSV input csv filename, note: this file must be formatted in a specific manner
#' @return String
#' @export
#'
#' @examples coming soon


sqljoins_asstring <- function(inPgTbl,schema,inCSV){
  joinlist <- list()
  for (row in 1:nrow(inCSV)) {
    inputVect <- c()
    inc <- gsub("[[:space:]]",'',tolower(inFile[row, "inc"])) ##  1 = include(i.e. will not skip) 0 = not included (i.e. will skip)
    rslt_ind <- gsub("[[:space:]]",'',tolower(inFile[row, "rslt_ind"])) ##1 = include(i.e. will add data to provincial resultant) 0 = not included (i.e. will not add data to provincial resultant)
    srctype <- gsub("[[:space:]]",'',tolower(inFile[row, "srctype"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    srcpath <- gsub("[[:space:]]",'',tolower(inFile[row, "srcpath"]))## path to input data. Note use bcgw for whse
    srclyr <- gsub("[[:space:]]",'',tolower(inFile[row, "srclyr"])) ## input layer name
    pk <- gsub("[[:space:]]",'',tolower(inFile[row, "primarykey"])) ## primary key field that will be added to resultant table
    suffix <- gsub("[[:space:]]",'',tolower(inFile[row, "suffix"])) ## suffix to be used in the resultant table
    nsTblm <- gsub("[[:space:]]",'',tolower(inFile[row, "tblname"])) ## name of output non spatial table
    query <- tolower(inFile[row, "src_query"])  ##where clause used to filter input dataset
    flds2keep <- gsub("[[:space:]]",'',tolower(inFile[row, "fields2keep"])) ## fields to keep in non spatial table
    inputVect <- append(inputVect,suffix)
    if(rslt_ind == 1  &&  srctype != 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,' ', suffix, ' on a.', pk,'_',suffix,' = ',suffix,'.',pk)}
    else if(rslt_ind == 0  &&  srctype != 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,'_ogc_fid ', suffix, '_ogc_fid on a.ogc_fid = ',suffix,'_ogc_fid.ogc_fid',
                     ' left outer join ', schema, '.', nsTblm,' ', suffix, ' on ', suffix, '_ogc_fid.',pk,' = ',suffix,'.',pk)}
    else  if(rslt_ind == 1  &&  srctype == 'raster'){ join <- ' '}
    else  if(rslt_ind == 0  &&  srctype == 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,'_ogc_fid ', suffix, '_ogc_fid on a.ogc_fid = ',suffix,'_ogc_fid.ogc_fid')}

    inputVect <- append(inputVect,join)
    joinlist[[length(joinlist)+1]] <- inputVect
  }

  finalString <- paste0(schema,'.',inPgTbl, ' a ')
  if(is.null(incSuffixVec)){
    for(vec in joinlist){finalString <- paste(finalString,vec[2])}}
  else {for(vec in joinlist){
    if(vec[1] %in% incSuffixVec){finalString <- paste(finalString,vec[2])}}}
  return(shQuote(finalString))
}
