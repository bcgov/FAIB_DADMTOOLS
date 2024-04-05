#' Creates FROM clause string from the standard faib_dadm_tools input df
#' @param base_join_tbl Input pg table, used as first table to be joined, format: schema.table_name, Eg. whse.all_bc_gr_skey
#' @param schema PG database schema used for all tables in in_df
#' @param in_df input dataframe of config_parameters.csv
#' @param only_inc_rows include of rows wwhere onc field is = 1
#' @param filter_suffix_vect vector of suffixes to be included in string
#' @param metadata_tbl Boolean to say whether in_df is based on metadata pg (TRUE) or config_parameters.csv (TRUE). If metadata_tbl = TRUE, schema and pk are taken from data_sources table entries rather than from argument.
#' @return String
#' @export
#'
#' @examples str_sql_joins(base_join_tbl = "whse.all_bc_gr_skey", schema= "whse", pk = "pgid", in_df = read.csv("config_parameters.csv"))
#' @examples str_sql_joins(base_join_tbl = "whse.all_bc_gr_skey", schema= "whse", pk = "pgid", in_df = read.csv("config_parameters.csv"), filter_suffix_vect=c("sev_ras", "tsa"))
#' @examples str_sql_joins(base_join_tbl = "whse.all_bc_gr_skey", schema= "whse", in_df = sql_to_df("select * FROM whse.data_sources", get_pg_conn_list()), metadata_tbl = TRUE)



str_sql_joins <- function(base_join_tbl = "whse.all_bc_gr_skey", 
                          schema = NULL, 
                          in_df, 
                          pk = NULL, 
                          only_inc_rows = TRUE, 
                          filter_suffix_vect = NULL, 
                          metadata_tbl = FALSE
                          ) {
  if ((is.null(pk)) && (!(metadata_tbl))) {
    print("ERROR: Must provide pk when data_sources IS FALSE")
    return()
  }
  join_list <- list()
  for (row in 1:nrow(in_df)) {

    ## if in_df is the metadata table, assume inc to be 1
    if (metadata_tbl) {
      inc = 1
    } else {
      inc <- gsub("[[:space:]]","",tolower(in_df[row, "inc"])) ##  1 = include(i.e. will not skip) 0 = not included (i.e. will skip)
    }
    rslt_ind  <- gsub("[[:space:]]","",tolower(in_df[row, "rslt_ind"])) ##1 = include(i.e. will add data to provincial resultant) 0 = not included (i.e. will not add data to provincial resultant)
    srctype   <- gsub("[[:space:]]","",tolower(in_df[row, "srctype"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    srcpath   <- gsub("[[:space:]]","",tolower(in_df[row, "srcpath"]))## path to input data. Note use bcgw for whse
    suffix    <- gsub("[[:space:]]","",tolower(in_df[row, "suffix"])) ## suffix to be used in the resultant table
    in_tbl    <- gsub("[[:space:]]","",tolower(in_df[row, "tblname"])) ## name of output non spatial table

    ## if user has selected to only include rows where inc = 1, pass to next row if inc = 0
    if (only_inc_rows) {
      if (inc == 0) {
        next
      }
    }
    ## error out if no suffix provided as needed in join aliases
    if (is_blank(suffix)) {
      print(glue("ERROR: no suffix provided where srcpath = {srcpath}, exiting script."))
      return()
    }

    ## if filter_suffix_vect provided, pass to next row if suffix not in list
    if (!is.null(filter_suffix_vect)){
      if (!(suffix %in% filter_suffix_vect)) {
        next
      }
    }

    ## if in_df is the metadata table, retrieve pk & schema from metadata_tbl
    if (metadata_tbl) {
      schema <- gsub("[[:space:]]","",tolower(in_df[row, "schema"]))
      pk     <- gsub("[[:space:]]","",tolower(in_df[row, "primarykey"]))
    }

    if(rslt_ind == 1  &&  srctype != "raster") {
      join <- glue("LEFT JOIN {schema}.{in_tbl} {suffix} ON a.{pk}_{suffix} = {suffix}.{pk}")
    } else if(rslt_ind == 0  &&  srctype != "raster") {
      join <- glue("LEFT JOIN {schema}.{in_tbl}_gr_skey {suffix}_gr_skey ON a.gr_skey = {suffix}_gr_skey.gr_skey
      LEFT JOIN {schema}.{in_tbl} {suffix} ON {suffix}_gr_skey.{pk} = {suffix}.{pk}")
    } else if(rslt_ind == 1  &&  srctype == "raster") {
      join <- " "
    } else if(rslt_ind == 0  &&  srctype == "raster") {
      join <- glue("LEFT JOIN {schema}.{in_tbl}_gr_skey {suffix}_gr_skey ON a.gr_skey = {suffix}_gr_skey.gr_skey")
    }

    join_list <- append(join_list, join)
  }
  base_string <- glue("{base_join_tbl} a")
  join_list <- append(base_string, join_list)
  return(cat(paste(join_list, collapse="\n")))
}
