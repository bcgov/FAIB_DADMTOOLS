#' Updates Foreign Key Data Source Table in PG
#'
#' @param outTableName Data source table name
#' @param srctype data source type (one of 'gdb', 'raster','oracle','geopackage')
#' @param srcpath source path (e.g. "w:/test/tes.gdb" or 'bcgw')
#' @param srclyr data source  (e.g. 'veg_comp_poly' or  'whse_forest_vegetation.f_own')
#' @param pk primary of data source e.g. objectid
#' @param suffix suffix used in resultant columns that were kept form the data source (e.g. "vri2021")
#' @param nsTblm name of the non spatial data of data source stored in PG e.g. f_own
#' @param query where statement used to filter data source
#' @param inc 1 or 0,  indicates if data source is in PG
#' @param rslt_ind indicates the pk is in the  pg resultant or not
#' @param fields2keep string of fields kept from data input (e.g. "feature_id,proj_age_1,live_stand_volume_125")
#'
#' @return no return
#' @export
#'
#' @examples coming soon



updateFKsrcTblpg <- function(outTableName,srctype,srcpath,srclyr,pk,suffix,nsTblm,query,inc,rslt_ind,fields2keep,connList ){

  print(c(srctype,srcpath,srclyr,pk,suffix,nsTblm,query,inc,rslt_ind,fields2keep))
  # valueList <- lappy(valueList,function(x){if(is.null(x) || is.na(x)){x = ''}else{x}})



  query <- gsub("'","''",query)
  print(query)

  sql <- glue::glue("CREATE TABLE IF NOT EXISTS {outTableName} (srctype varchar, srcpath varchar ,srclyr varchar,primarykey varchar,suffix varchar,tblname varchar,src_query varchar,inc integer,rslt_ind integer,fields2keep varchar);")
  faibDataManagement::sendSQLstatement(sql,connList)
  print(sql)
  sql <- glue::glue("DELETE FROM {outTableName} WHERE suffix = {single_quote(suffix)};")
  faibDataManagement::sendSQLstatement(sql,connList)
  print(sql)
  sql <- glue::glue("INSERT INTO {outTableName}(srctype,srcpath,srclyr,primarykey,suffix,
                            tblname,src_query,inc,rslt_ind,fields2keep)
                           VALUES ({single_quote(srctype)},
                                {single_quote(srcpath)},
                                {single_quote(srclyr)},
                                {single_quote(pk)},
                                {single_quote(suffix)},
                                {single_quote(nsTblm)},
                                {single_quote(query)},
                                {single_quote(inc)},
                                {single_quote(rslt_ind)},
                               {single_quote(fields2keep)});")
  print(sql)
  faibDataManagement::sendSQLstatement(sql,connList)
   print(sql)


   }
