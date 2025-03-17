#' Update the resultant table with fields from previously imported datasets.
#' @param resultant_table_output_name user selected name of the output resultant table. Note: Include schema in the name.
#' @param join_gr_skey_table the name of the table containing the key (e.g. gr_skey) to join to resultant key.
#' @param fields_to_include vector of fields to include from joining tables.
#' @param join_attribute_table the name of the attribute table containing the key (e.g. pgid) to join to gr_skey table. Attributes from this table will be in the final resultant table. If the table name is left blank, gr_skey table attributes will be used in the final resultant table.
#' @param overwrite_fields DEFAULT:FALSE. TRUE = New Fields will overwrite any fields that already exists in the current Resultant table. FALSE = Existing Resultant table fields will not be overwritten.
#' @param resultant_table name of the existing resultant table.
#' @param pg_conn_param named list of postgres connection parameters (see get_pg_conn_list() function for more details)
#' @param key_resultant_tbl DEFAULT:'gr_skey'. join key in resultant table (e.g. gr_skey)
#' @param key_grskey_tbl DEFAULT:'gr_skey'. join key in gr_key table (e.g. gr_skey)
#' @param key_join_tbl DEFAULT:'pgid'. join key in attribute table (e.g. pgid)
#' @param updated_field_names OPTIONAL vector of updated fields names to include int final resultant table.
#' @param include_prefix DEFAULT:FALSE. TRUE = a prefix will be added to new field names. FALSE = no prefix will be added to field names.
#' @param prefix the prefix to be added to the beginning of new field names
#' @param overwrite_resultant_table DEFAULT:FALSE. TRUE = any table with the resultant_table_output_name will be overwritten. FALSE = Resultant tbale will not be overwritten.
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon


add_fields_to_resultant  <- function(
    resultant_table_output_name,
    join_gr_skey_table,
    fields_to_include,
    join_attribute_table = NULL,
    overwrite_fields = FALSE,
    resultant_table = 'resultant',
    pg_conn_param = dadmtools::get_pg_conn_list(),
    key_resultant_tbl = 'gr_skey',
    key_grskey_tbl = 'gr_skey',
    key_join_tbl = 'pgid',
    updated_field_names = NULL,
    include_prefix = FALSE,
    prefix = NULL,
    overwrite_resultant_table = FALSE)


{

  today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
  ####GET TABLE NAMES WITHOUT SCHEMA
  if (grepl("\\.", resultant_table)) {
    resultant_schema <- strsplit(resultant_table, "\\.")[[1]][[1]]
    resultant_table_no_schema <- strsplit(resultant_table, "\\.")[[1]][[2]]
  } else {
    resultant_table_no_schema <- resultant_table
    resultant_schema <- 'public'
  }

  if (grepl("\\.", join_gr_skey_table)) {
    join_gr_skey_table_schema <- strsplit(join_gr_skey_table, "\\.")[[1]][[1]]
    join_gr_skey_table_no_schema <- strsplit(join_gr_skey_table, "\\.")[[1]][[2]]
  } else {
    join_gr_skey_table_no_schema <- join_gr_skey_table
    join_gr_skey_table_schema <- 'public'
  }

  if(!is.null(join_attribute_table)){
    if (grepl("\\.", join_attribute_table)) {
      join_attribute_table_schema <- strsplit(join_attribute_table, "\\.")[[1]][[1]]
      join_attribute_table_no_schema <- strsplit(join_attribute_table, "\\.")[[1]][[2]]
    } else {
      join_attribute_table_no_schema <- join_attribute_table
      join_attribute_table_schema <- 'public'
    }
  } else {
    join_attribute_table_schema <- NULL
    join_attribute_table_no_schema <- NULL
  }


  if (grepl("\\.", resultant_table_output_name)) {
    resultant_output_schema <- strsplit(resultant_table_output_name, "\\.")[[1]][[1]]
    resultant_table_output_no_schema <- strsplit(resultant_table_output_name, "\\.")[[1]][[2]]
  } else {
    resultant_table_output_no_schema <- resultant_table_output_name
    resultant_output_schema <- 'public'
  }

  print(paste('Updated length is:',length(updated_field_names)))
  print(paste('fields_to_include length is:',length(fields_to_include)))
  
  ####VERIFY FIELDS TO BE ADDED TO RESULTANT
  if (!is.null(updated_field_names) ) {
    if (length(updated_field_names) == length(fields_to_include)) {
      print("field lengths match")
    } else {
      print(glue("ERROR: updated_field_names vector length does not match fields_to_include vector length"))
      stop()
    }
  }

  if(!is.null(join_attribute_table)) {
    join_table_fields_query <- paste0("SELECT column_name FROM information_schema.columns WHERE table_name = '", join_attribute_table_no_schema, "' and table_schema = '",join_attribute_table_schema,"';")
    join_table_fields_all <- dadmtools::sql_to_df(join_table_fields_query,pg_conn_param)$column_name
  } else {
      join_table_fields_query <- paste0("SELECT column_name FROM information_schema.columns WHERE table_name = '", join_gr_skey_table_no_schema, "' and table_schema = '",join_gr_skey_table_schema,"';")
      join_table_fields_all <- dadmtools::sql_to_df(join_table_fields_query,pg_conn_param)$column_name
  }

  if (all(sapply(fields_to_include, function(x) (x %in%  join_table_fields_all)  ))) {
    print("fields_to_include look good")
  } else {
    print("fields_to_include fields are not all in join table ")
  }

  if (!is.null(updated_field_names)) {
    validate_vector <- function(vec) {
      # Check if vector is not character
      if (!is.character(vec)) {
        stop("ERROR: The updated field names must be a character vector.")
      }

      # Check if any value starts with a number or a special character
      if (any(grepl("^[0-9]", vec) | grepl("^[^a-zA-Z0-9]", vec))) {
        stop("ERROR: updated field names must not start with a number or a special character.")
      }

      print("Vector is valid!")
    }

    validate_vector(updated_field_names)
  }

  ###### Get the column names of the resultant table
  resultant_cols_query <- paste0("SELECT column_name FROM information_schema.columns WHERE table_name = '", resultant_table_no_schema, "' and  table_schema = '",resultant_schema, "';")
  resultant_cols <- dadmtools::sql_to_df(resultant_cols_query,pg_conn_param)$column_name


  #remove resultant keys from field vectors if in the final field names
  if(!include_prefix){
    if(is.null(updated_field_names)){
      indices_to_remove <- which(fields_to_include == key_resultant_tbl)
      if (length(indices_to_remove > 0)){
      fields_to_include <- fields_to_include[-indices_to_remove]}
    } else {
      indices_to_remove <- which(updated_field_names == key_resultant_tbl)
      if (length(indices_to_remove > 0)) {
        fields_to_include <- fields_to_include[-indices_to_remove]
      }
      if (length(indices_to_remove > 0)) {
        updated_field_names <- updated_field_names[-indices_to_remove]
      }
    }
  }


  ###### Determine fields to add or update
  if (include_prefix) {
    fields_to_add <- paste0(prefix,'_',fields_to_include)[!(paste0(prefix,'_',fields_to_include) %in% resultant_cols)]
    fields_to_add_new_names <- paste0(prefix,'_',updated_field_names)[!(paste0(prefix,'_',updated_field_names) %in% resultant_cols)]
  } else {
    fields_to_add <- fields_to_include[!(fields_to_include %in% resultant_cols)]
    fields_to_add_new_names <- updated_field_names[!(updated_field_names %in% resultant_cols)]
  }

  if (include_prefix) {
    fields_to_update <- paste0(prefix,'_',fields_to_include)[(paste0(prefix,'_',fields_to_include) %in% resultant_cols)]
    fields_to_update_new_names <- paste0(prefix,'_',updated_field_names)[(paste0(prefix,'_',updated_field_names) %in% resultant_cols)]}else{
    fields_to_update <- fields_to_include[(fields_to_include %in% resultant_cols)]
    fields_to_update_new_names <- updated_field_names[(updated_field_names %in% resultant_cols)]
  }

  ######Set field names for select cause
  if(overwrite_fields) {
    #remove new fields from resultant fields vector
    if(is.null(updated_field_names)){
      resultant_cols <- setdiff(resultant_cols, fields_to_update)
    } else {
      resultant_cols <- setdiff(resultant_cols, fields_to_update_new_names)
    }

    final_resultant_fields <- paste0('rslt.', resultant_cols)

    #create vector of new fields to add
    if(is.null(updated_field_names)) {
      if(include_prefix) {
        final_join_vect <- paste0(prefix,'_',fields_to_include)
        final_join_fields <- paste0('att.',fields_to_include, ' as ',prefix,'_',fields_to_include)}else{
        final_join_vect <- paste0(fields_to_include)
        final_join_fields <- paste0('att.',fields_to_include)
      }
    } else {
      if(include_prefix) {
        final_join_vect <- paste0(prefix,'_',updated_field_names)
        final_join_fields <- paste0('att.',fields_to_include, ' as ',prefix,'_',updated_field_names)
      } else {
        final_join_vect <- paste0( updated_field_names)
        final_join_fields <- paste0('att.',fields_to_include, ' as ', updated_field_names)
      } 
    }

  } else {
    final_resultant_fields <- paste0('rslt.', resultant_cols)

    if(is.null(updated_field_names)){
      if(include_prefix){
        final_join_vect <- paste0(prefix,'_',fields_to_include)
        final_join_fields <- paste0('att.', fields_to_include, ' as ',prefix, '_', fields_to_include)
      } else {
        final_join_vect <- paste0(fields_to_add)
        final_join_fields <- paste0('att.', fields_to_add)
      }
    } else {
      if(include_prefix){
        final_join_vect <- paste0(prefix,'_',updated_field_names)
        final_join_fields <- paste0('att.',fields_to_include, ' as ',prefix,'_',updated_field_names)
      } else {
        final_join_vect <- paste0(fields_to_add_new_names)
        final_join_fields <- paste0('att.',fields_to_add_new_names)
      }

    }
  }

  final_join_fields <- paste0(final_join_fields,collapse = ",")
  final_resultant_fields <- paste0(final_resultant_fields,collapse = ",")



  if (overwrite_resultant_table) {
    run_sql_r(glue("DROP TABLE IF EXISTS {resultant_table_output_name}_4398349023_temp;"), pg_conn_param)
    if(!is.null(join_attribute_table)) {
      run_sql_r(glue("CREATE TABLE {resultant_table_output_name}_4398349023_temp AS
                        SELECT
                          {final_resultant_fields},{final_join_fields}
                        FROM
                        {resultant_table} rslt
                        LEFT JOIN {join_gr_skey_table} g on rslt.{key_resultant_tbl} = g.{key_grskey_tbl}
                        LEFT JOIN {join_attribute_table} att on g.{key_join_tbl} = att.{key_join_tbl};"), pg_conn_param)}else{

                          run_sql_r(glue("CREATE TABLE {resultant_table_output_name}_4398349023_temp AS
                        SELECT
                          {final_resultant_fields},{final_join_fields}
                        FROM
                        {resultant_table} rslt
                        LEFT JOIN {join_gr_skey_table} att on rslt.{key_resultant_tbl} = att.{key_grskey_tbl};"), pg_conn_param)
    }

    run_sql_r(glue("DROP TABLE IF EXISTS {resultant_table_output_name};"), pg_conn_param)
    run_sql_r(glue("ALTER TABLE {resultant_table_output_name}_4398349023_temp RENAME TO {resultant_table_output_no_schema};"), pg_conn_param)
    run_sql_r(glue("ALTER TABLE {resultant_table_output_name} ADD PRIMARY KEY ({key_resultant_tbl});"), pg_conn_param)
    dst_tbl_comment <- glue("COMMENT ON TABLE {resultant_table_output_name} IS 'Table last updated by the dadmtools R package at {today_date}.
                                              TABLE last added fields from {join_gr_skey_table};'")
    dadmtools::run_sql_r(dst_tbl_comment, pg_conn_param)
    dadmtools::run_sql_r(glue("ANALYZE {resultant_table_output_name};"), pg_conn_param)
    print("Resultant table created")

  } else {
    output_tbl_exists <- (dadmtools::sql_to_df(glue("SELECT EXISTS (
                    SELECT *
                    FROM
                      information_schema.tables
                    WHERE
                      table_schema = '{resultant_output_schema}'
                    AND
                      table_name = '{resultant_table_output_no_schema}');"), conn))$exists

    if(output_tbl_exists) {
      print(glue("ERROR: Output resultant table already exists"))
      stop()  
    } else {
      if(!is.null(join_attribute_table)) {
        run_sql_r(glue("CREATE TABLE {resultant_table_output_name} AS
                        SELECT
                          {final_resultant_fields},{final_join_fields}
                        FROM
                        {resultant_table} rslt
                        LEFT JOIN {join_gr_skey_table} g on rslt.{key_resultant_tbl} = g.{key_grskey_tbl}
                        LEFT JOIN {join_attribute_table} att on g.{key_join_tbl} = att.{key_join_tbl};"), pg_conn_param)
      } else {
        run_sql_r(glue("CREATE TABLE {resultant_table_output_name} AS
        SELECT
          {final_resultant_fields},{final_join_fields}
        FROM
        {resultant_table} rslt
        LEFT JOIN {join_gr_skey_table} att on rslt.{key_resultant_tbl} = att.{key_grskey_tbl};"), pg_conn_param)
      }

      run_sql_r(glue("ALTER TABLE {resultant_table_output_name} ADD PRIMARY KEY ({key_resultant_tbl});"), pg_conn_param)

      dst_tbl_comment <- glue("COMMENT ON TABLE {resultant_table_output_name} IS 'Table last updated by the dadmtools R package at {today_date}.
                          TABLE last added fields from {join_gr_skey_table};;'")
      dadmtools::run_sql_r(dst_tbl_comment, pg_conn_param)
      dadmtools::run_sql_r(glue("ANALYZE {resultant_table_output_name};"), pg_conn_param)
      print("Resultant table created")
    }
  }



  dadmtools::update_resultant_field_tbl(
                          field_names = final_join_vect,
                          fields_to_include,
                          resultant_table_no_schema,
                          resultant_schema,
                          join_attribute_table_no_schema,
                          join_attribute_table_schema,
                          join_gr_skey_table_no_schema,
                          join_gr_skey_table_schema,
                          resultant_table_output_no_schema,
                          resultant_output_schema ,
                          pg_conn_param)
}
