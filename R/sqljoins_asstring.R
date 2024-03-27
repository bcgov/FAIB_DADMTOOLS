#' Creates FROM clause string from the standard FAIB_DATA_MANAGEMETN input csv.  Only uses inputs where inc = 1
#' @param in_pg_tbl Input master gr_Key PG Table Name (containing foreign keys)
#' @param schema PG database schema
#' @param in_df input data frame in same format as input csv
#' @param only_inc_rows include of rows wwhere onc field is = 1
#' @param filter_suffix_vect vector of suffixes to be included in string
#' @return String
#' @export
#'
#' @examples sqljoins_asstring(masterTable,schema='postgres',inCSV='D:/inputs/inputCsv.csv')


sqljoins_asstring <- function(in_pg_tbl, schema, in_df, only_inc_rows = FALSE, filter_suffix_vect = NULL){
  join_list <- list()
  for (row in 1:nrow(in_df)) {
    input_vect <- c()
    inc <- gsub("[[:space:]]",'',tolower(in_df[row, "inc"])) ##  1 = include(i.e. will not skip) 0 = not included (i.e. will skip)
    rslt_ind <- gsub("[[:space:]]",'',tolower(in_df[row, "rslt_ind"])) ##1 = include(i.e. will add data to provincial resultant) 0 = not included (i.e. will not add data to provincial resultant)
    srctype <- gsub("[[:space:]]",'',tolower(in_df[row, "srctype"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    srcpath <- gsub("[[:space:]]",'',tolower(in_df[row, "srcpath"]))## path to input data. Note use bcgw for whse
    srclyr <- gsub("[[:space:]]",'',tolower(in_df[row, "srclyr"])) ## input layer name
    pk <- gsub("[[:space:]]",'',tolower(in_df[row, "primarykey"])) ## primary key field that will be added to resultant table
    suffix <- gsub("[[:space:]]",'',tolower(in_df[row, "suffix"])) ## suffix to be used in the resultant table
    nsTblm <- gsub("[[:space:]]",'',tolower(in_df[row, "tblname"])) ## name of output non spatial table
    query <- tolower(in_df[row, "src_query"])  ##where clause used to filter input dataset
    flds2keep <- gsub("[[:space:]]",'',tolower(in_df[row, "fields2keep"])) ## fields to keep in non spatial table
    input_vect <- append(input_vect, suffix)
    if(rslt_ind == 1  &&  srctype != 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,' ', suffix, ' on a.', pk,'_',suffix,' = ',suffix,'.',pk)}
    else if(rslt_ind == 0  &&  srctype != 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,'_gr_skey ', suffix, '_gr_skey on a.gr_skey = ',suffix,'_gr_skey.gr_skey',
                     ' left outer join ', schema, '.', nsTblm,' ', suffix, ' on ', suffix, '_gr_skey.',pk,' = ',suffix,'.',pk)}
    else  if(rslt_ind == 1  &&  srctype == 'raster'){ join <- ' '}
    else  if(rslt_ind == 0  &&  srctype == 'raster'){
      join <- paste0('left outer join ', schema, '.', nsTblm,'_gr_skey ', suffix, '_gr_skey on a.gr_skey = ',suffix,'_gr_skey.gr_skey')}

    input_vect <- append(input_vect,join)
    input_vect <- append(input_vect,inc)
    join_list[[length(join_list)+1]] <- input_vect
  }

  final_string <- paste0(schema,'.',in_pg_tbl, ' a ')
  if(only_inc_rows){
    for(vec in join_list){
      if(vec[3] == 1){
        if(is.null(filter_suffix_vect) | vec[1] %in% filter_suffix_vect )
        {final_string <- paste(final_string, vec[2])}
      }}
  }else {for(vec in join_list){
    if(is.null(filter_suffix_vect) | vec[1] %in% filter_suffix_vect )
    {
      final_string <- paste(final_string, vec[2])}
  }}
  return(shQuote(final_string))
}
