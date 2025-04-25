#' Build or update a flat, denormalized table, also known as a resultant table, with fields from previously imported datasets.
#' @param new_resultant_name (required): The user-defined name for the output resultant table, including the schema. E.g. sandbox.tsa_resultant
#' @param gr_skey_table (required): The name of the table containing the key (e.g., gr_skey) used to join with the resultant key. E.g. sandbox.adm_nr_districts_sp_gr_skey
#' @param included_fields (required): A vector of fields to include from joining tables (e.g. district_name, org_unit)
#' @param attribute_table (optional): The name of the attribute table containing the key (e.g., pgid) used to join with `gr_skey_table`. Attributes from this table will be included in the final resultant table. If `attribute_table` is not provided, the attributes specified in `included_fields` will be selected from `gr_skey_table` for the final resultant table.
#' @param overwrite_fields (required): A logical value (TRUE or FALSE) indicating whether to overwrite existing fields with new field names specified in `update_field_names`. Default : FALSE. TRUE = New Fields will overwrite any fields that already exists in the current Resultant table. FALSE = Existing Resultant table fields will not be overwritten.
#' @param current_resultant_table (required): Name of the existing resultant table (e.g. sandbox.all_bc_gr_skey)
#' @param pg_conn_param (required): named list of postgres connection parameters (e.g. get_pg_conn_list())
#' @param key_resultant_tbl (required): Default: 'gr_skey'. The join key in resultant table (e.g. gr_skey)
#' @param key_grskey_tbl (required): Default:'gr_skey'. The join key in 'gr_skey_table' (e.g. gr_skey)
#' @param key_join_tbl (optional): Default:'pgid'. The join key in 'attribute_table' (e.g. pgid). Only used if `attribute_table` is provided.
#' @param update_field_names (optional):  A vector of new field names to use in the final resultant table, replacing those specified in 'included_fields'. The number of field names in this vector must match the number in 'included_fields'. (e.g. admin_district_name, admin_org_unit)
#' @param include_prefix (required): A logical value (TRUE or FALSE) indicating whether to add prefix to resultant field names. If TRUE, `prefix` is required.
#' @param prefix (optional): A prefix to prepend to field names in the resultant table. By default, it updates 'included_fields', but if 'update_field_names' is provided, the prefix will be applied to those instead.
#' @param overwrite_resultant_table (required): A logical value (TRUE or FALSE) indicating whether to overwrite the 'new_resultant_name'
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon


add_fields_to_resultant  <- function(
    new_resultant_name,
    gr_skey_table,
    included_fields,
    attribute_table = NULL,
    overwrite_fields = FALSE,
    current_resultant_table = 'resultant',
    pg_conn_param = dadmtools::get_pg_conn_list(),
    key_resultant_tbl = 'gr_skey',
    key_grskey_tbl = 'gr_skey',
    key_join_tbl = 'pgid',
    update_field_names = NULL,
    include_prefix = FALSE,
    prefix = NULL,
    overwrite_resultant_table = FALSE)


{

  today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
  #### get current_resultant_table without schema
  if (grepl("\\.", current_resultant_table)) {
    resultant_schema <- strsplit(current_resultant_table, "\\.")[[1]][[1]]
    resultant_table_no_schema <- strsplit(current_resultant_table, "\\.")[[1]][[2]]
  } else {
    ## provided without schema, default to public schema
    resultant_table_no_schema <- current_resultant_table
    resultant_schema <- 'public'
  }

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
  if(!is.null(attribute_table)){
    if (grepl("\\.", attribute_table)) {
      attribute_table_schema <- strsplit(attribute_table, "\\.")[[1]][[1]]
      attribute_table_no_schema <- strsplit(attribute_table, "\\.")[[1]][[2]]
    } else {
      ## provided without schema, default to public schema
      attribute_table_no_schema <- attribute_table
      attribute_table_schema <- 'public'
    }
  } else {
    attribute_table_schema <- NULL
    attribute_table_no_schema <- NULL
  }

  #### get new_resultant_name without schema
  if (grepl("\\.", new_resultant_name)) {
    resultant_output_schema <- strsplit(new_resultant_name, "\\.")[[1]][[1]]
    resultant_table_output_no_schema <- strsplit(new_resultant_name, "\\.")[[1]][[2]]
  } else {
    ## provided without schema, default to public schema
    resultant_table_output_no_schema <- new_resultant_name
    resultant_output_schema <- 'public'
  }


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
      print(glue("WARNING: included_fields: {included_fields_str} are not all in attribute_table: {attribute_table}"))
    } else {
      print(glue("WARNING: included_fields: {included_fields_str} are not all in gr_skey_table: {gr_skey_table}"))
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

  #### Get the column names of the current_resultant_table
  resultant_cols_query <- paste0("SELECT column_name FROM information_schema.columns WHERE table_name = '", resultant_table_no_schema, "' and  table_schema = '", resultant_schema, "';")
  current_resultant_cols <- dadmtools::sql_to_df(resultant_cols_query,pg_conn_param)$column_name


  #### remove resultant keys (ie. gr_skey) from included_fields or update_field_names if in the final field names
  if (is.null(update_field_names)){
    indices_to_remove <- which(included_fields == key_resultant_tbl)
    if (length(indices_to_remove > 0)){
      included_fields <- included_fields[-indices_to_remove]
    }
  } else {
    indices_to_remove <- which(update_field_names == key_resultant_tbl)
    #### remove key_resultant_tbl (ie. gr_skey) from included_fields & update_field_names
    if (length(indices_to_remove > 0)) {
      included_fields <- included_fields[-indices_to_remove]
    }
    if (length(indices_to_remove > 0)) {
      update_field_names <- update_field_names[-indices_to_remove]
    }
  }

  #### Determine fields to add
  #### remove any columns existing in current_resultant_cols from included_fields
  if (include_prefix) {
    fields_to_add <- paste0(prefix,'_', included_fields)[!(paste0(prefix, '_', included_fields) %in% current_resultant_cols)]
    fields_to_add_new_names <- paste0(prefix,'_',update_field_names)[!(paste0(prefix,'_',update_field_names) %in% current_resultant_cols)]
  } else {
    fields_to_add <- included_fields[!(included_fields %in% current_resultant_cols)]
    fields_to_add_new_names <- update_field_names[!(update_field_names %in% current_resultant_cols)]
  }

  #### Determine fields to update
  if (include_prefix) {
    fields_to_update <- paste0(prefix,'_',included_fields)[(paste0(prefix,'_',included_fields) %in% current_resultant_cols)]
    fields_to_update_new_names <- paste0(prefix,'_',update_field_names)[(paste0(prefix,'_',update_field_names) %in% current_resultant_cols)]
  } else {
    fields_to_update <- included_fields[(included_fields %in% current_resultant_cols)]
    fields_to_update_new_names <- update_field_names[(update_field_names %in% current_resultant_cols)]
  }

  #### Set field names for select case
  if (overwrite_fields) {
    if(is.null(update_field_names)){
      current_resultant_cols <- setdiff(current_resultant_cols, fields_to_update)
    } else {
      current_resultant_cols <- setdiff(current_resultant_cols, fields_to_update_new_names)
    }

    final_resultant_fields <- paste0('rslt.', current_resultant_cols)

    #### create vector of new fields to add
    if(is.null(update_field_names)) {
      if(include_prefix) {
        final_join_vect <- paste0(prefix,'_', included_fields)
        final_join_fields <- paste0('att.', included_fields, ' as ', prefix, '_', included_fields)
      } else {

        final_join_vect <- paste0(included_fields)
        final_join_fields <- paste0('att.',included_fields)
      }
    } else {
      if(include_prefix) {
        final_join_vect <- paste0(prefix,'_', update_field_names)
        final_join_fields <- paste0('att.', included_fields, ' as ', prefix, '_', update_field_names)
      } else {
        final_join_vect <- paste0( update_field_names)
        final_join_fields <- paste0('att.', included_fields, ' as ', update_field_names)
      }
    }

  } else {
    final_resultant_fields <- paste0('rslt.', current_resultant_cols)

    if (is.null(update_field_names)) {
      if (include_prefix) {
        final_join_vect <- paste0(prefix,'_',included_fields)
        final_join_fields <- paste0('att.', included_fields, ' as ', prefix, '_', included_fields)
      } else {
        if (length(fields_to_add) > 0) {
          final_join_vect <- paste0(fields_to_add)
          final_join_fields <- paste0('att.', fields_to_add)
        } else {
          final_join_vect <- NULL
          final_join_fields <- NULL
        }
      }
    } else {
      if(include_prefix){
        final_join_vect <- paste0(prefix,'_',update_field_names)
        final_join_fields <- paste0('att.',included_fields, ' as ', prefix, '_', update_field_names)
      } else {
        final_join_vect <- paste0(fields_to_add_new_names)
        final_join_fields <- paste0('att.', fields_to_add_new_names)
      }
    }
  }

  final_all_fields <- c(final_resultant_fields, final_join_fields)
  final_all_fields <- paste0(final_all_fields,collapse = ",")
  # final_join_fields <- paste0(final_join_fields,collapse = ",")
  # final_resultant_fields <- paste0(final_resultant_fields, collapse = ",")

  if (overwrite_resultant_table) {
    run_sql_r(glue("DROP TABLE IF EXISTS {new_resultant_name}_4398349023_temp;"), pg_conn_param)
    if(!is.null(attribute_table)) {
      run_sql_r(glue("CREATE TABLE {new_resultant_name}_4398349023_temp AS
        SELECT
          {final_all_fields}
        FROM
        {current_resultant_table} rslt
        LEFT JOIN {gr_skey_table} g on rslt.{key_resultant_tbl} = g.{key_grskey_tbl}
        LEFT JOIN {attribute_table} att on g.{key_join_tbl} = att.{key_join_tbl};"), pg_conn_param)
    } else {
      run_sql_r(glue("CREATE TABLE {new_resultant_name}_4398349023_temp AS
        SELECT
          {final_all_fields}
        FROM
        {current_resultant_table} rslt
        LEFT JOIN {gr_skey_table} att on rslt.{key_resultant_tbl} = att.{key_grskey_tbl};"), pg_conn_param)
    }

    run_sql_r(glue("DROP TABLE IF EXISTS {new_resultant_name};"), pg_conn_param)
    run_sql_r(glue("ALTER TABLE {new_resultant_name}_4398349023_temp RENAME TO {resultant_table_output_no_schema};"), pg_conn_param)
    run_sql_r(glue("ALTER TABLE {new_resultant_name} ADD PRIMARY KEY ({key_resultant_tbl});"), pg_conn_param)
    dst_tbl_comment <- glue("COMMENT ON TABLE {new_resultant_name} IS 'Table last updated by the dadmtools R package at {today_date}.
                                              TABLE last added fields from {gr_skey_table};'")
    dadmtools::run_sql_r(dst_tbl_comment, pg_conn_param)
    dadmtools::run_sql_r(glue("ANALYZE {new_resultant_name};"), pg_conn_param)

  } else {
    output_tbl_exists <- (dadmtools::sql_to_df(glue("SELECT EXISTS (
                    SELECT *
                    FROM
                      information_schema.tables
                    WHERE
                      table_schema = '{resultant_output_schema}'
                    AND
                      table_name = '{resultant_table_output_no_schema}');"), pg_conn_param))$exists


    if(output_tbl_exists) {
      print(glue("ERROR: Output resultant table already exists"))
      stop()
    } else {
      if(!is.null(attribute_table)) {
        run_sql_r(glue("CREATE TABLE {new_resultant_name} AS
                        SELECT
                          {final_all_fields}
                        FROM
                        {current_resultant_table} rslt
                        LEFT JOIN {gr_skey_table} g on rslt.{key_resultant_tbl} = g.{key_grskey_tbl}
                        LEFT JOIN {attribute_table} att on g.{key_join_tbl} = att.{key_join_tbl};"), pg_conn_param)
      } else {
        run_sql_r(glue("CREATE TABLE {new_resultant_name} AS
        SELECT
          {final_all_fields}
        FROM
        {current_resultant_table} rslt
        LEFT JOIN {gr_skey_table} att on rslt.{key_resultant_tbl} = att.{key_grskey_tbl};"), pg_conn_param)
      }

      run_sql_r(glue("ALTER TABLE {new_resultant_name} ADD PRIMARY KEY ({key_resultant_tbl});"), pg_conn_param)

      dst_tbl_comment <- glue("COMMENT ON TABLE {new_resultant_name} IS 'Table last updated by the dadmtools R package at {today_date}.
                          TABLE last added fields from {gr_skey_table};;'")
      dadmtools::run_sql_r(dst_tbl_comment, pg_conn_param)
      dadmtools::run_sql_r(glue("ANALYZE {new_resultant_name};"), pg_conn_param)
    }
  }

  dadmtools:::update_resultant_field_tbl(
    field_names = final_join_vect,
    included_fields,
    resultant_table_no_schema,
    resultant_schema,
    attribute_table_no_schema,
    attribute_table_schema,
    gr_skey_table_no_schema,
    gr_skey_table_schema,
    resultant_table_output_no_schema,
    resultant_output_schema ,
    pg_conn_param)
}
