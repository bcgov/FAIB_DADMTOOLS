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



updateFKsrcTblpg <- function(outTableName, srctype, srcpath, srclyr, pk, suffix, nsTblm, query, inc, rslt_ind, fields2keep, schema, connList) {

  if(is_blank(fields2keep)) {
    fields2keep <- ""
  }

  query <- gsub("'","\'",query)

  outTableNameNoSchema <- strsplit(outTableName, "\\.")[[1]][[2]]

  sql <- glue::glue("CREATE TABLE IF NOT EXISTS {outTableName} (
                      srctype varchar,
                      srcpath varchar,
                      srclyr varchar,
                      primarykey varchar,
                      suffix varchar,
                      schema varchar,
                      tblname varchar,
                      src_query varchar,
                      rslt_ind integer,
                      fields2keep varchar,
                      created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                      CONSTRAINT {outTableNameNoSchema}_unique UNIQUE (schema, tblname)
                    );")
  faibDataManagement::sendSQLstatement(sql, connList)

  sql <- glue::glue("INSERT INTO {outTableName}
                    (
                     srctype,
                     srcpath,
                     srclyr,
                     primarykey,
                     suffix,
                     schema,
                     tblname,
                     src_query,
                     rslt_ind,
                     fields2keep
                    )
                     VALUES
                    (
                    {single_quote(srctype)},
                    {single_quote(srcpath)},
                    {single_quote(srclyr)},
                    {single_quote(pk)},
                    {single_quote(suffix)},
                    {single_quote(schema)},
                    {single_quote(nsTblm)},
                    {single_quote(query)},
                    {single_quote(rslt_ind)},
                    {single_quote(fields2keep)}
                    )
                    ON CONFLICT (schema, tblname) DO UPDATE
                    SET
                    srctype = EXCLUDED.srctype,
                    srcpath = EXCLUDED.srcpath,
                    srclyr = EXCLUDED.srclyr,
                    primarykey = EXCLUDED.primarykey,
                    suffix = EXCLUDED.suffix,
                    src_query = EXCLUDED.src_query,
                    rslt_ind = EXCLUDED.rslt_ind,
                    fields2keep = EXCLUDED.fields2keep,
                    created_at = now();")
  print(glue("Inserting record into {outTableName} for {schema}.{nsTblm}"))
  faibDataManagement::sendSQLstatement(sql, connList)
}
