#' Update the resultant field table in PG
#' @param field_names vector of field names added to the resultant
#' @param src_field_names vector of the original field names of source table
#' @param resultant_table_name name of resultant table
#' @param resultant_table_schema name of schema of resultant table
#' @param src_attribute_table name of source attribute table
#' @param src_gr_skey_table name of source gr_skey_table
#' @param src_gr_skey_schema name of source schema of gr_skey_table
#' @param out_name name of out resultant table
#' @param out_schema name of out resultant schema
#' @param pg_conn_param params
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon

## consider renaming to update_resultant_field_metadata_tbl
update_resultant_field_tbl <- function(
                                    field_names = NULL,
                                    src_field_names = NULL,
                                    resultant_table_name = NULL,
                                    resultant_table_schema = NULL,
                                    src_attribute_table_name = NULL,
                                    src_attribute_table_schema = NULL,
                                    src_gr_skey_table = NULL,
                                    src_gr_skey_schema = NULL,
                                    out_name = resultant_table_name,
                                    out_schema = resultant_table_schema,
                                    pg_conn_param)

{

  sql <- glue("CREATE TABLE IF NOT EXISTS {out_schema}.{out_name}_flds (
                field_name varchar(150),
                src_field_name varchar(150),
                src_attribute_table_name varchar(150),
                src_attribute_table_schema varchar(150),
                src_gr_skey_table varchar(150),
                src_gr_skey_schema varchar(150)) ;")

  run_sql_r(sql, pg_conn_param)


  query <- glue("SELECT column_name FROM information_schema.columns WHERE table_name = '{out_name}' and table_schema = '{out_schema}';")
  resultant_field_names <- dadmtools::sql_to_df(query,pg_conn_param)$column_name


  query <- glue("SELECT field_name FROM {out_schema}.{out_name}_flds;")
  metadata_tbl_fields <- dadmtools::sql_to_df(query,pg_conn_param)$field_name

  drop_columns <- c(setdiff(metadata_tbl_fields,resultant_field_names), intersect(metadata_tbl_fields,field_names))
  print(drop_columns)




  for (fld in drop_columns) {
    drop_sql <- glue("DELETE FROM {out_schema}.{out_name}_flds WHERE field_name = '{fld}';")
    dadmtools::run_sql_r(drop_sql, pg_conn_param )
    print("Column {fld} dropped successfully.")
  }

  print(length(field_names))
  print(field_names[1])
  for (i in seq_along(field_names)) {
    field_names_val <- field_names[i]
    src_field_names_val <- src_field_names[i]
    if(is.null(resultant_table_schema)){resultant_table_schema <- ''}
    if(is.null(resultant_table_name)){resultant_table_name <- ''}
    if(is.null(src_attribute_table_name)){src_attribute_table_name <- ''}
    if(is.null(src_attribute_table_schema)){src_attribute_table_schema <- ''}
    if(is.null(src_gr_skey_schema)){src_gr_skey_schema <- ''}
    if(is.null(src_gr_skey_table)){src_gr_skey_table <- ''}

    query <-  glue("INSERT INTO {out_schema}.{out_name}_flds
              (field_name, src_field_name,
               src_attribute_table_name, src_attribute_table_schema, src_gr_skey_table, src_gr_skey_schema)
              VALUES ('{as.character(field_names_val)}',
                      '{as.character(src_field_names_val)}', '{src_attribute_table_name}', '{src_attribute_table_schema}',
                      '{src_gr_skey_table}', '{src_gr_skey_schema}');")

    print(query)
    dadmtools::run_sql_r(query, pg_conn_param)

  }

  query <- glue("SELECT column_name FROM information_schema.columns WHERE table_name = '{out_name}' and table_schema = '{out_schema}';")
  resultant_field_names <- dadmtools::sql_to_df(query,pg_conn_param)$column_name


  query <- glue("SELECT field_name FROM {out_schema}.{out_name}_flds;")
  metadata_tbl_fields <- dadmtools::sql_to_df(query,pg_conn_param)$field_name

  add_resultant_columns <- setdiff(resultant_field_names,metadata_tbl_fields)
  print(add_resultant_columns)

  if (length(add_resultant_columns) > 0){
  for (i in (1:length(add_resultant_columns))) {

    query <- glue("INSERT INTO {out_schema}.{out_name}_flds
                  (field_name,src_field_name,src_attribute_table_name,src_attribute_table_schema,src_gr_skey_table,src_gr_skey_schema )
                  VALUES ('{add_resultant_columns[i]}','{add_resultant_columns[i]}',NULL,NULL,NULL, NULL )
  ;")
    print(query)
    dadmtools::run_sql_r(query, pg_conn_param)
  }}


}
