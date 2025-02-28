#' Creates new dataset gr_skey table with gr_skey key overlaps removed by either grouping values into arrays or keeping only rows with the max or min value in select field
#'
#' @param gr_skey_table Schema and table name of the pre-existing gr_skey table
#' @param attribute_table the name of the attribute table containing the key (e.g. pgid) to join to gr_skey table. Attributes from this table will be in the final resultant table. If the table name is left blank, gr_skey table attributes will be used in the final resultant table.
#' @param array_ind TRUE of FALSE, if True then output table will have fields grouped into arrays (by gr_skey)
#' @param array_fields Vector of field names to be goruped as array
#' @param remove_by_rank_ind True or False.  If True only gr_skey rows that have the largest/Smallest rank_field values will be kept
#' @param rank_field the field used to rank as either largest or smallest when grouped by gr_skey
#' @param rank_type ASC or DESC, when ASC then the rows with smallest rank field values will be kept. When DESC then rows with largest rank field values will be kept.
#' @param pg_conn_param Defaults to get_pg_conn_list()

#'
#' @return no return
#' @export
#'
#' @examples coming soon

remove_overlaps_gr_skey_tbl <- function(gr_skey_table,
                                       attribute_table,
                                       array_ind     = TRUE,
                                       array_fields = NULL,
                                       remove_by_rank_ind   = FALSE,
                                       rank_field       = NULL,
                                       rank_type    = "ASC",
                                       pg_conn_param  = dadmtools::get_pg_conn_list())
{



  #browser()

  gr_skey_table <- tolower(gr_skey_table)
  attribute_table <- tolower(attribute_table)
  if(!is.null(array_fields)){array_fields <- paste0(tolower(attribute_table))}
  if(!is.null(rank_field)){rank_field <- tolower(rank_field)}


  field_exists <- function(table_name, field_name, schema) {
    query <- sprintf(
      "SELECT column_name
     FROM information_schema.columns
     WHERE table_schema = '%s'
     AND table_name = '%s'",
      schema, table_name
    )
    field_name <- tolower(field_name)
    result <- dadmtools::sql_to_df(query,pg_conn_param)

    # Check if field_name exists in the result
    return(field_name %in% result$column_name)
  }


  if(!is.null(attribute_table)){
    if (grepl("\\.", attribute_table)) {
      attribute_table_schema <- strsplit(attribute_table, "\\.")[[1]][[1]]
      attribute_table_no_schema <- strsplit(attribute_table, "\\.")[[1]][[2]]
    } else {
      attribute_table_no_schema <- attribute_table
      attribute_table_schema <- 'public'
    }}




  ####VERIFIY FIELDS are legitimate
  if ((array_ind && remove_by_rank_ind) || (!array_ind && !remove_by_rank_ind)  ) {
      print(glue("ERROR: array_ind and remove_by_rank_ind cannot both be TRUE or cannot both be FALSE"))
      stop()
  }

  if(array_ind){
    if (is.null(array_fields) || dadmtools::is_blank(array_fields)  ) {
      print(glue("ERROR: Please provide a array_fields vector"))
      stop()
    } else{
      for(fld in array_fields){
      if (!field_exists(attribute_table_no_schema,fld,attribute_table_schema)){
        print(glue("ERROR: array_field {fld} not in attribute table"))
        stop() }}

            modified_array_fields <- paste0("array_agg(", array_fields, ") as ", array_fields, "_array")
            modified_array_fields <- paste0(modified_array_fields,collapse = ',')
            print ( modified_array_fields)

          dadmtools::run_sql_r(glue("DROP TABLE IF EXISTS {gr_skey_table}_duplicates_as_array;"), pg_conn_param)
          dadmtools::run_sql_r(glue("Create TABLE {gr_skey_table}_duplicates_as_array as

                                    select grskey.gr_skey, {modified_array_fields}
                                    from {gr_skey_table} grskey join {attribute_table} att on
                                    grskey.pgid = att.pgid
                                    group by grskey.gr_skey

                                    ;"), pg_conn_param)}
    print(glue('Adding gr_skey as primary key to {gr_skey_table}_duplicates_as_array'))
    dadmtools::run_sql_r(glue("ALTER TABLE {gr_skey_table}_duplicates_as_array ADD PRIMARY KEY (gr_skey);"), pg_conn_param)


    }else if (remove_by_rank_ind){

  if (is.null(rank_field) || dadmtools::is_blank(rank_field) || is.null(rank_type) || dadmtools::is_blank(rank_type) || (!rank_type %in% c('ASC','DESC'))   ) {
    print(glue("ERROR: Please provide a rank_field and rank_type "))
    stop()
  } else{
    if (!field_exists(attribute_table_no_schema,rank_field,attribute_table_schema)){
      print(glue("ERROR: rank_field not in attribute table"))
      stop() }else{


        dadmtools::run_sql_r(glue("DROP TABLE IF EXISTS {gr_skey_table}_no_duplicates_{rank_type}_{rank_field};"), pg_conn_param)
        dadmtools::run_sql_r(glue("Create TABLE  {gr_skey_table}_no_duplicates_{rank_type}_{rank_field} as

                                  SELECT a.*
                                  FROM (SELECT grskey.gr_skey, att.*,
                                  ROW_NUMBER() OVER (PARTITION BY grskey.gr_skey ORDER BY {rank_field} {rank_type}) ranked_order
                                  FROM  {gr_skey_table} grskey join {attribute_table} att on
                                    grskey.pgid = att.pgid) a
                                  WHERE a.ranked_order = 1
                                    ;"), pg_conn_param)}


    print(glue('Adding gr_skey as primary key to {gr_skey_table}_no_duplicates_{rank_type}_{rank_field}'))
    dadmtools::run_sql_r(glue("ALTER TABLE {gr_skey_table}_no_duplicates_{rank_type}_{rank_field} ADD PRIMARY KEY (gr_skey);"), pg_conn_param)



    }





  }}




