#' Update the resultant table with fields from previously imported datasets
#' @param in_csv File path to batch add fields csv, Defaults to "batch_add_fields_to_resultant.csv"
#' @param pg_conn_param named list of postgres connection parameters (see get_pg_conn_list() function for more details)
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon


batch_add_fields_to_resultant  <- function(
    in_csv           = "batch_add_fields_to_resultant.csv",
    pg_conn_param     = dadmtools::get_pg_conn_list()
)


{

  start_time <- Sys.time()
  print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
  in_file <- read.csv(in_csv)
  in_file$row_num <- seq_len(nrow(in_file))
  in_file <- in_file[in_file$include == 1,]
  for (row in 1:nrow(in_file)) {
    print(paste('row:', in_file$row_num[row], ' of batch add fields csv'))
    overwrite_rslt      <- as.logical(gsub("[[:space:]]","",toupper(in_file[row, "overwrite_resultant_table"]))) ##1 = include (i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
    overwrite_flds      <- as.logical(gsub("[[:space:]]","",toupper(in_file[row, "overwrite_fields"]))) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    incl_prefix         <- as.logical(gsub("[[:space:]]","",toupper(in_file[row, "include_prefix"]))) ## name of output non spatial table
    new_rslt            <- gsub("[[:space:]]","",tolower(in_file[row, "new_resultant_name"])) ## path to input data. Note use bcgw for whse
    gr_skey_tbl         <- gsub("[[:space:]]","",tolower(in_file[row, "gr_skey_table"])) ## input layer name
    att_tbl             <- gsub("[[:space:]]","",tolower(in_file[row, "attribute_table"])) ## suffix to be used in the resultant table
    rslt_tbl            <- gsub("[[:space:]]","",tolower(in_file[row, "current_resultant_table"])) ## name of output non spatial table
    incl_flds           <- gsub("[[:space:]]","",tolower(in_file[row, "included_fields"])) ## name of output non spatial table
    upd_flds            <- gsub("[[:space:]]","",tolower(in_file[row, "update_field_names"])) ## name of output non spatial table
    prefix              <- gsub("[[:space:]]","",tolower(in_file[row, "prefix"])) ## name of output non spatial table
    key_rslt            <- gsub("[[:space:]]","",tolower(in_file[row, "key_resultant_tbl"])) ## name of output non spatial table
    key_grskey          <- gsub("[[:space:]]","",tolower(in_file[row, "key_grskey_tbl"])) ## name of output non spatial table
    key_att             <- gsub("[[:space:]]","",tolower(in_file[row, "key_join_tbl"])) ## name of output non spatial table
    notes               <- in_file[row, "notes"]  ##where clause used to filter input dataset
    ## checks

    check_blanks <-   c(overwrite_rslt, overwrite_flds, incl_prefix ,new_rslt, gr_skey_tbl, rslt_tbl, incl_flds, key_rslt, key_grskey)

    for(blank in check_blanks){
      print(blank)
      if (any(c(dadmtools::is_blank(blank)))){
        print("ERROR: Argument not provided, one of: overwrite_resultant_table,overwrite_fields,include_prefix,new_resultant_name,gr_skey_table,
                attribute_table,current_resultant_table,included_fields,update_field_names,prefix,key_resultant_tbl,key_grskey_tbl,key_join_tbl")
        stop()
      } }

    if(dadmtools::is_blank(prefix)) {
      prefix <- NULL
    }
    
    if(dadmtools::is_blank(upd_flds)) {
      upd_flds <- NULL
    } else {
      upd_flds <- unlist(strsplit(upd_flds, ","))
    }
    
    if(dadmtools::is_blank(notes)) {
      notes <- NULL
    }
    
    if(dadmtools::is_blank(att_tbl)) {
      att_tbl <- NULL
    }
    
    if(dadmtools::is_blank(key_att)) { 
      key_att <- NULL
    }


    if(is.null(prefix) && incl_prefix) { 
      print("ERROR: Prefix not provided while include_prefix is TRUE")
      stop()
    }


    if(is.null(key_att) && !is.null(att_tbl)) {
      print("ERROR: key_join_tbl not provided while attribute_table is populated")
      stop()
    }


    incl_flds <- unlist(strsplit(incl_flds, ","))


    for (i in c(overwrite_rslt,
                overwrite_flds,
                incl_prefix,
                new_rslt,
                gr_skey_tbl,
                att_tbl,
                rslt_tbl,
                incl_flds,
                upd_flds,
                prefix,
                key_rslt,
                key_grskey,
                key_att,
                notes )) {
      print(i)
    }

    print(att_tbl)
    print(key_att)

    if(length(incl_flds) == length(upd_flds)) {
      print(TRUE)
    }

    dadmtools::add_fields_to_resultant( resultant_table_output_name = new_rslt,
                                        join_gr_skey_table          = gr_skey_tbl ,
                                        fields_to_include           = incl_flds,
                                        join_attribute_table        = att_tbl,
                                        overwrite_fields            = overwrite_flds,
                                        resultant_table             = rslt_tbl,
                                        pg_conn_param               = pg_conn_param,
                                        key_resultant_tbl           = key_rslt,
                                        key_grskey_tbl              = key_grskey,
                                        key_join_tbl                = key_att,
                                        updated_field_names         = upd_flds,
                                        include_prefix              = incl_prefix,
                                        prefix                      = prefix,
                                        overwrite_resultant_table   = overwrite_rslt)
  }
  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  print(glue("Script started at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
  print(glue("Script duration: {duration} minutes\n"))
}
