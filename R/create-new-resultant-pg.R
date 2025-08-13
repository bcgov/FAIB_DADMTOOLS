#' Creates a resultant table (i.e. a flat, denormalized table) using this function. The function requires populating a configuration input csv file with the desired inputs tables and fields to include in the newly created resultant. See example on git repository root directory create_new_resultant_inputs.csv and description in the README.)
#' @param in_csv File path to input csv, defaults to "create_new_resultant_inputs.csv". See example csv and README for instructions in git repository: https://github.com/bcgov/FAIB_DADMTOOLS/tree/main.
#' @param resultant_name The name of the output resultant table and schema (ie. 'public.tsa25_resultant')
#' @param key_field_resultant_table  Optional argument. Global id field used to join to the input gr_skey tbale.  Defaults to is gr_skey
#' @param pg_conn_param Named list of postgres connection parameters (i.e. get_pg_conn_list())
#'
#' @return resultant_name
#' @export
#'
#' @examples
#' ## Download and edit example input create_new_resultant_inputs.csv from https://github.com/bcgov/FAIB_DADMTOOLS/tree/main. Example of function run:
#' create_new_resultant_pg(
#'    in_csv                    = "create_new_resultant_inputs.csv",
#'    resultant_name            = 'public.tsr25_resultant',
#'    key_field_resultant_table = 'gr_skey',
#'    pg_conn_param             = dadmtools::get_pg_conn_list()
#')


create_new_resultant_pg  <- function(
    in_csv                    = "create_new_resultant_inputs.csv",
    resultant_name,
    key_field_resultant_table = 'gr_skey',
    pg_conn_param             = dadmtools::get_pg_conn_list()
)

{

  #### get resultant_name without schema

  if (grepl("\\.", resultant_name)) {
    resultant_table_schema <- strsplit(resultant_name, "\\.")[[1]][[1]]
    resultant_table_no_schema <- strsplit(resultant_name, "\\.")[[1]][[2]]
  } else {
    ## provided without schema, default to public schema
    resultant_table_no_schema <- resultant_name
    resultant_table_schema <- 'public'
  }

  #### Check if resultant name already exists
  resultant_blank_pg <- !dadmtools::is_blank(dadmtools::sql_to_df(
  glue("SELECT table_name FROM information_schema.tables
        WHERE table_schema = '{resultant_table_schema}'
        AND table_name = '{resultant_table_no_schema}'"), pg_conn_param)$table_name)

  if(resultant_blank_pg){
    print(glue("Resultant table name: {resultant_name} already exists, please provide a table name that does not exist."))
    stop()
  }

###################
  today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
  gr_skey_table_accumulator <- c()
  gr_skey_table_join_string_accumulator <- c()
  attribute_table_accumulator <- c()
  attribute_table_join_string_accumulator <- c()
  final_field_alias_accumulator <- c()
  final_full_select_field_name_accumulator <- c()
  included_fields_accumulator <- c()
  table_alias_counter <- 1
  on_joins = ""


  start_time <- Sys.time()
  print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
  in_file <- read.csv(in_csv)
  in_file$row_num <- seq_len(nrow(in_file))
  in_file <- in_file[in_file$include == 1,]

  for (row in 1:nrow(in_file)) {
    print(glue('On row: {row}/{nrow(in_file)} of file: {basename(in_csv)} where include = 1'))
    gr_skey_table             <- gsub("[[:space:]]","",tolower(in_file[row, "input_gr_skey_table"]))
    attribute_table           <- gsub("[[:space:]]","",tolower(in_file[row, "input_attribute_table"]))
    included_fields           <- gsub("[[:space:]]","",tolower(in_file[row, "input_fields_to_include"]))
    update_field_names        <- gsub("[[:space:]]","",tolower(in_file[row, "output_field_names"]))
    prefix                    <- gsub("[[:space:]]","",tolower(in_file[row, "prefix"]))
    key_grskey_tbl            <- gsub("[[:space:]]","",tolower(in_file[row, "key_field_grskey_table"]))
    key_attribute_tbl         <- gsub("[[:space:]]","",tolower(in_file[row, "key_field_attribute_table"]))
    notes                     <- in_file[row, "notes"]



    #### get gr_skey_table without schema
    if (grepl("\\.", gr_skey_table)) {
      gr_skey_table_schema <- strsplit(gr_skey_table, "\\.")[[1]][[1]]
      gr_skey_table_no_schema <- strsplit(gr_skey_table, "\\.")[[1]][[2]]
    } else {
      ## provided without schema, default to public schema
      gr_skey_table_no_schema <- gr_skey_table
      gr_skey_table_schema <- 'public'
    }

    #### get attribute_table without schema
    if(!dadmtools::is_blank(attribute_table)){
      if (grepl("\\.", attribute_table)) {
        attribute_table_schema <- strsplit(attribute_table, "\\.")[[1]][[1]]
        attribute_table_no_schema <- strsplit(attribute_table, "\\.")[[1]][[2]]
      } else {
        ## provided without schema, default to public schema
        attribute_table_no_schema <- attribute_table
        attribute_table_schema <- 'public'
      }
    } else {
      attribute_table_schema <- gr_skey_table_schema
      attribute_table_no_schema <- gr_skey_table_no_schema
    }



    #### Get vector list of all fields (minus keys) if all input fields are selected indicated by a *
    if (included_fields == "*") {


      qry <- glue("SELECT column_name
          FROM information_schema.columns
          WHERE table_name = '{attribute_table_no_schema}'  and
          table_schema = '{attribute_table_schema}';")

      all_attribute_fields <- dadmtools::sql_to_df(qry, pg_conn_param )$column_name
      included_fields <- setdiff(all_attribute_fields, c(key_field_resultant_table, key_attribute_tbl, key_grskey_tbl))
      update_field_names <- included_fields
    }


    ######################### checks#########################################
    check_blanks <- list(
      gr_skey_table = gr_skey_table,
      resultant_name = resultant_name,
      included_fields = included_fields,
      key_grskey_tbl = key_grskey_tbl,
      key_field_resultant_table = key_field_resultant_table
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



    if(dadmtools::is_blank(notes)) {
      notes <- NULL
    }

    if(dadmtools::is_blank(attribute_table)) {
      attribute_table <- NULL
    }




    if(is.null(key_attribute_tbl) && !is.null(attribute_table)) {
      print("ERROR: key_attribute_tbl not provided while attribute_table is populated")
      stop()
    }

    if(dadmtools::is_blank(update_field_names)) {
      update_field_names <- NULL
    } else {
      update_field_names <- unlist(strsplit(update_field_names, ","))
    }

    included_fields <- unlist(strsplit(included_fields, ","))


    #### Ensure update_field_names and included_fields are the same length
    if (!is.null(update_field_names) ) {
      if (length(update_field_names) != length(included_fields)) {
        print(glue("ERROR: number of fields provided in update_field_names does not match included_fields"))
        stop()
      }
    }


    #### retrieve all fields from attribute_table if exists, otherwise from gr_skey_table
    if (!is.null(attribute_table)) {
      join_table_fields_query <- paste0("SELECT column_name FROM information_schema.columns WHERE table_name = '", attribute_table_no_schema, "' and table_schema = '",attribute_table_schema,"';")
      join_table_fields_all <- dadmtools::sql_to_df(join_table_fields_query,pg_conn_param)$column_name
    } else {
      join_table_fields_query <- paste0("SELECT column_name FROM information_schema.columns WHERE table_name = '", gr_skey_table_no_schema, "' and table_schema = '",gr_skey_table_schema,"';")
      join_table_fields_all <- dadmtools::sql_to_df(join_table_fields_query,pg_conn_param)$column_name
    }


    #### ensure that included fields are within join_table_fields_all (i.e. fields from attribute_table if exists, otherwise from gr_skey_table)
    if (!(all(sapply(included_fields, function(x) (x %in%  join_table_fields_all))))) {
      included_fields_str <- paste(included_fields, collapse = ", ")
      if (!is.null(attribute_table)) {
        print(glue("WARNING: input_fields_to_include: {included_fields_str} are not all in attribute_table: {attribute_table}"))
      } else {
        print(glue("WARNING: input_fields_to_include: {included_fields_str} are not all in gr_skey_table: {gr_skey_table}"))
      }
    }



    ## ensure update_field_names is a valid vector
    if (!is.null(update_field_names)) {
      validate_vector <- function(vec) {
        # Check if vector is not character
        if (!is.character(vec)) {
          stop(glue("ERROR: The update_field_names: {update_field_names} must be a character vector."))
        }

        # Check if any value starts with a number or a special character
        if (any(grepl("^[0-9]", vec) | grepl("^[^a-zA-Z0-9]", vec))) {
          stop(glue("ERROR: update_field_names: {update_field_names} must not start with a number or a special character."))
        }
      }
      validate_vector(update_field_names)
    }




###ACCUMULATE JOIN CLAUSE#########################################
###############################################

    table_alias_gr_skey <- paste0("tblg", as.character(table_alias_counter) )
    if(is_blank(attribute_table) ){
      table_alias_att <- paste0("tblg", as.character(table_alias_counter) )
    }else{
    table_alias_att <- paste0("tbl", as.character(table_alias_counter) )}

    gr_skey_table_join <- glue("left join {gr_skey_table_schema}.{gr_skey_table_no_schema} {table_alias_gr_skey} on a.{key_field_resultant_table} = {table_alias_gr_skey}.{key_grskey_tbl}")


    if(is_blank(attribute_table) ){
      attribute_table_join <- " "}else{
    attribute_table_join <- glue("left join {attribute_table_schema}.{attribute_table_no_schema} {table_alias_att} on {table_alias_att}.{key_attribute_tbl} = {table_alias_gr_skey}.{key_attribute_tbl}")}


    gr_skey_table_join_string_accumulator <- c(gr_skey_table_join_string_accumulator, gr_skey_table_join)
    attribute_table_join_string_accumulator <- c(attribute_table_join_string_accumulator, attribute_table_join)


##################################################################
##ACCUMULATE FIELDS##############################

    #### remove resultant keys (ie. gr_skey) from included_fields or update_field_names if in the final field names
    if (is.null(update_field_names)){
      indices_to_remove <- which(included_fields == key_field_resultant_table)
      if (length(indices_to_remove > 0)){
        included_fields <- included_fields[-indices_to_remove]
      }
    } else {
      indices_to_remove <- which(update_field_names == key_field_resultant_table)
      #### remove key_field_resultant_table (ie. gr_skey) from included_fields & update_field_names
      if (length(indices_to_remove > 0)) {
        included_fields <- included_fields[-indices_to_remove]
      }
      if (length(indices_to_remove > 0)) {
        update_field_names <- update_field_names[-indices_to_remove]
      }
    }


  #### Create field name accumulator
    if (dadmtools::is_blank(prefix)) {

      if(dadmtools::is_blank(update_field_names)){
        field_aliases <- included_fields}else{
          field_aliases <- update_field_names}
    }else{

      if(dadmtools::is_blank(update_field_names)){
        field_aliases <- paste0(prefix,'_',included_fields)}else{
          field_aliases <- paste0(prefix,'_',update_field_names)
        }
    }

    ### add field_aliases into field accumulator and check they don't already exist from a previous dataset.  Function stops if field_aliases already exist
    for(fld  in field_aliases) {
        if(fld %in% final_field_alias_accumulator){
          print(paste0("Field name:", fld, " already exists in dataset:", names(final_field_alias_accumulator)[fld]))
          stop()
          }}

    ##Add attribute table as a name to be referenced in above error message
    names(field_aliases) <- rep(glue("{attribute_table_schema}.{attribute_table_no_schema}"), length(field_aliases))

    final_full_select_field_name <- paste0(table_alias_att,".", included_fields, ' as ', field_aliases )
    names(final_full_select_field_name) <- rep(glue("{gr_skey_table_schema}.{gr_skey_table_no_schema}"), length(final_full_select_field_name))

    final_field_alias_accumulator <- c(final_field_alias_accumulator, field_aliases)
    final_full_select_field_name_accumulator <- c(final_full_select_field_name_accumulator, final_full_select_field_name)

    included_fields_accumulator <- c(included_fields_accumulator, included_fields)

  #################################################
    attribute_table_accumulator <- c(attribute_table_accumulator,glue("{attribute_table_schema}.{attribute_table_no_schema}"))
    gr_skey_table_accumulator <- c(gr_skey_table_accumulator,gr_skey_table )
    table_alias_counter <- table_alias_counter + 1

}

#####Create a temp resultant table with gr-skey from all the tables
  resultant_name_temp <- glue("{resultant_name}_dadmtools_temp")
  ## if tmp table already exists (from a failed previous run, drop & recreate it)
  qry <- glue("DROP TABLE IF EXISTS {resultant_name_temp};")
  dadmtools::run_sql_r(qry, pg_conn_param)
  qry <- glue("CREATE TABLE {resultant_name_temp} AS  ",
              paste(paste0("(SELECT gr_skey FROM ", gr_skey_table_accumulator, ")"), collapse = " UNION"))
  dadmtools::run_sql_r(qry,pg_conn_param)


#####Create final table using the final_full_select_field_name_accumulator,attribute_table_join_string_accumulator, and
#####gr_skey_table_join_string_accumulator
final_fields <-  paste(unname(final_full_select_field_name_accumulator), collapse = ',')
gr_skey_joins <- paste(unname(gr_skey_table_join_string_accumulator), collapse = ' ')
attribute_joins <- paste(unname(attribute_table_join_string_accumulator), collapse = ' ')

#Create final table
qry <- glue("CREATE TABLE {resultant_name} as
            Select a.{key_field_resultant_table} ,{final_fields} from {resultant_name_temp} a {gr_skey_joins} {attribute_joins};")
print(qry)
dadmtools::run_sql_r(qry, pg_conn_param)

#Create index
run_sql_r(glue("ALTER TABLE {resultant_name} ADD PRIMARY KEY ({key_field_resultant_table});"), pg_conn_param)


#add metadata
attribute_tables <- paste(attribute_table_accumulator, collapse = ",")
dst_tbl_comment <- glue("COMMENT ON TABLE {resultant_name} IS 'Table last updated by the dadmtools R package at {today_date}.
                                              TABLE added fields from the following tables {attribute_tables};'")
print(dst_tbl_comment)
dadmtools::run_sql_r(dst_tbl_comment, pg_conn_param)
dadmtools::run_sql_r(glue("ANALYZE {resultant_name};"), pg_conn_param)

#create metadata table
qry <- glue("DROP TABLE IF EXISTS {resultant_name}_data_sources;")
run_sql_r(qry, pg_conn_param)

qry <- glue("CREATE TABLE IF NOT EXISTS  {resultant_name}_data_sources (
                field_name varchar(150),
                src_field_name varchar(150),
                src_attribute_table_name varchar(150),
                src_gr_skey_table varchar(150)) ;")

run_sql_r(qry, pg_conn_param)


for (i in seq_along( included_fields_accumulator   )) {
  field_names_val <- final_field_alias_accumulator[i]
  src_field_names_val <- included_fields_accumulator[i]
  src_attribute_table_name <- names(final_field_alias_accumulator)[i]
  src_grskey_table_name <- names(final_full_select_field_name_accumulator)[i]

  query <-  glue("INSERT INTO {resultant_name}_data_sources
              (field_name, src_field_name, src_attribute_table_name, src_gr_skey_table)
              VALUES ('{as.character(field_names_val)}',
                      '{as.character(src_field_names_val)}', '{src_attribute_table_name}',
                      '{src_grskey_table_name}');")
  dadmtools::run_sql_r(query, pg_conn_param)

}
dadmtools::run_sql_r(glue("ANALYZE {resultant_name}_data_sources;"), pg_conn_param)


#drop temp table
qry <- glue("DROP TABLE IF EXISTS {resultant_name_temp}; ")
dadmtools::run_sql_r(qry, pg_conn_param)




  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  print(glue("Script started at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
  print(glue("Script duration: {duration} minutes\n"))
  return(resultant_name)
}
