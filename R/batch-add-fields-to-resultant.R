#' Once layers have been imported using batch_import_to_pg_gr_skey or import_to_pg_gr_skey, build a flat, denormalized table, also known as a resultant table using this batch function. The function requires an existing table (e.g. sandbox.all_bc_gr_skey) with joining key (e.g. gr_skey) to build the resultant table off of. Requires populating a configuration input csv file (e.g. see example on git repository called batch_add_fields_to_resultant.csv and description in the README.)
#' @param in_csv File path to input csv, defaults to "batch_add_fields_to_resultant.csv". See example csv and README for instructions in git repository: https://github.com/bcgov/FAIB_DADMTOOLS/tree/main
#' @param pg_conn_param named list of postgres connection parameters (i.e. get_pg_conn_list())
#'
#' @return nothing is returned
#'
#' @examples
#' Download and edit example input batch_add_fields_to_resultant.csv from https://github.com/bcgov/FAIB_DADMTOOLS/tree/main. Example of function run:
#' batch_add_fields_to_resultant(
#'    in_csv           = "batch_add_fields_to_resultant.csv",
#'    pg_conn_param     = dadmtools::get_pg_conn_list()
#')


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
    print(glue('On row: {row}/{nrow(in_file)} of file: {basename(in_csv)} where include = 1'))
    overwrite_resultant_table <- as.logical(gsub("[[:space:]]","",toupper(in_file[row, "overwrite_resultant_table"]))) ##1 = include (i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
    overwrite_fields          <- as.logical(gsub("[[:space:]]","",toupper(in_file[row, "overwrite_fields"])))
    include_prefix            <- as.logical(gsub("[[:space:]]","",toupper(in_file[row, "include_prefix"])))
    new_resultant_name        <- gsub("[[:space:]]","",tolower(in_file[row, "new_resultant_name"]))
    gr_skey_table             <- gsub("[[:space:]]","",tolower(in_file[row, "gr_skey_table"]))
    attribute_table           <- gsub("[[:space:]]","",tolower(in_file[row, "attribute_table"]))
    current_resultant_table   <- gsub("[[:space:]]","",tolower(in_file[row, "current_resultant_table"]))
    included_fields           <- gsub("[[:space:]]","",tolower(in_file[row, "included_fields"]))
    update_field_names        <- gsub("[[:space:]]","",tolower(in_file[row, "update_field_names"]))
    prefix                    <- gsub("[[:space:]]","",tolower(in_file[row, "prefix"]))
    key_resultant_tbl         <- gsub("[[:space:]]","",tolower(in_file[row, "key_resultant_tbl"]))
    key_grskey_tbl            <- gsub("[[:space:]]","",tolower(in_file[row, "key_grskey_tbl"]))
    key_join_tbl              <- gsub("[[:space:]]","",tolower(in_file[row, "key_join_tbl"]))
    notes                     <- in_file[row, "notes"]

    ## checks
    check_blanks <- list(
      overwrite_resultant_table = overwrite_resultant_table,
      overwrite_fields = overwrite_fields,
      include_prefix = include_prefix,
      new_resultant_name = new_resultant_name,
      gr_skey_table = gr_skey_table,
      current_resultant_table = current_resultant_table,
      included_fields = included_fields,
      key_resultant_tbl = key_resultant_tbl,
      key_grskey_tbl = key_grskey_tbl
    )

    for (var_name in names(check_blanks)) {
      var_value <- check_blanks[[var_name]]
      if (any(c(dadmtools::is_blank(var_value)))){
        print(glue("ERROR: Required argument missing: {var_name}"))
        stop()
      }
    }


    if(dadmtools::is_blank(prefix)) {
      prefix <- NULL
    }

    if(dadmtools::is_blank(update_field_names)) {
      update_field_names <- NULL
    } else {
      update_field_names <- unlist(strsplit(update_field_names, ","))
    }

    if(dadmtools::is_blank(notes)) {
      notes <- NULL
    }

    if(dadmtools::is_blank(attribute_table)) {
      attribute_table <- NULL
    }

    if(dadmtools::is_blank(key_join_tbl)) {
      key_join_tbl <- NULL
    }


    if(is.null(prefix) && include_prefix) {
      print("ERROR: Prefix not provided while include_prefix is TRUE")
      stop()
    }


    if(is.null(key_join_tbl) && !is.null(attribute_table)) {
      print("ERROR: key_join_tbl not provided while attribute_table is populated")
      stop()
    }


    included_fields <- unlist(strsplit(included_fields, ","))


    dadmtools:::add_fields_to_resultant( new_resultant_name        = new_resultant_name,
                                        gr_skey_table             = gr_skey_table ,
                                        included_fields           = included_fields,
                                        attribute_table           = attribute_table,
                                        overwrite_fields          = overwrite_fields,
                                        current_resultant_table   = current_resultant_table,
                                        pg_conn_param             = pg_conn_param,
                                        key_resultant_tbl         = key_resultant_tbl,
                                        key_grskey_tbl            = key_grskey_tbl,
                                        key_join_tbl              = key_join_tbl,
                                        update_field_names        = update_field_names,
                                        include_prefix            = include_prefix,
                                        prefix                    = prefix,
                                        overwrite_resultant_table = overwrite_resultant_table)
  }
  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  print(glue("Script started at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
  print(glue("Script duration: {duration} minutes\n"))
}
