#' Updates Foreign Key Data Source Table in PG
#'
#' @param dst_tbl Data source table name
#' @param src_type data source type (one of 'gdb', 'raster','oracle','geopackage')
#' @param srcpath source path (e.g. "w:/test/tes.gdb" or 'bcgw')
#' @param srclyr data source  (e.g. 'veg_comp_poly' or  'whse_forest_vegetation.f_own')
#' @param pk primary of data source e.g. objectid
#' @param suffix suffix used in resultant columns that were kept form the data source (e.g. "vri2021")
#' @param nsTblm name of the non spatial data of data source stored in PG e.g. f_own
#' @param query where statement used to filter data source
#' @param inc 1 or 0,  indicates if data source is in PG
#' @param rslt_ind indicates the pk is in the  pg resultant or not
#' @param flds_to_keep string of fields kept from data input (e.g. "feature_id,proj_age_1,live_stand_volume_125")
#' @param dst_schema
#' @param pg_conn_param
#' @return no return
#' @export
#'
#' @examples coming soon



updateFKsrcTblpg <- function(data_src_tbl, 
                            src_type, 
                            src_path, 
                            src_lyr, 
                            pk, 
                            suffix, 
                            query, 
                            inc, 
                            rslt_ind, 
                            flds_to_keep, 
                            dst_tbl,
                            dst_schema, 
                            pg_conn_param) 
{

  if(is_blank(flds_to_keep)) {
    flds_to_keep <- ""
  }

  query <- gsub("'","\'", query)

  sql <- glue::glue("CREATE TABLE IF NOT EXISTS {data_src_tbl} (
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
                      CONSTRAINT data_sources_unique UNIQUE (schema, tblname)
                    );")
  faibDataManagement::sendSQLstatement(sql, pg_conn_param)

  sql <- glue::glue("INSERT INTO {data_src_tbl}
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
                    {single_quote(src_type)},
                    {single_quote(src_path)},
                    {single_quote(src_lyr)},
                    {single_quote(pk)},
                    {single_quote(suffix)},
                    {single_quote(dst_schema)},
                    {single_quote(dst_tbl)},
                    {single_quote(query)},
                    {single_quote(rslt_ind)},
                    {single_quote(flds_to_keep)}
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
  print(glue("Inserting record into {data_src_tbl} for {dst_schema}.{dst_tbl}"))
  faibDataManagement::sendSQLstatement(sql, pg_conn_param)
}
