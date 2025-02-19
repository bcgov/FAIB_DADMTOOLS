#' Update FAIB hectares database from input csv of datasets
#'
#' @param in_csv File path to data sources csv, Defaults to "config_parameters.csv"
#' @param pg_conn_param Keyring object of Postgres credentials, defaults to dadmtools::get_pg_conn_list()
#' @param ora_conn_param Keyring object of Oracle credentials, defaults to dadmtools::get_ora_conn_list()
#' @param crop_extent list of c(ymin, ymax, xmin, xmax) in EPSG:3005, defaults to c(273287.5,1870587.5,367787.5,1735787.5)
#' @param gr_skey_tbl Schema and table name of the pre-existing gr_skey table. Argument to be used with suffix and rslt_ind within in_csv. Defaults to "whse.all_bc_gr_skey"
#' @param raster_schema If import_rast_to_pg = TRUE, schema of imported raster, defaults to "raster"
#' @param template_tif The file path to the gr_skey geotiff to be used as a template raster, fefaults to "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
#' @param mask_tif The file path to the geotiff to be used as a mask, defaults to "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif"
#' @param data_src_tbl Schema and table name of the metadata table in postgres that updates with any newly imported layer. Defaults to "whse.data_sources"
#' @param out_tif_path Directory where output tif if exported and where vector is temporally stored prior to import
#' @param import_rast_to_pg If TRUE, raster is imported into database in raster_schema. Defaults to FALSE
#'
#'
#' @return no return
#' @export
#'
#' @examples coming soon

remove_overlaps_gr_skey_tbl <- function(gr_skey_table,
                                       attribute_table,
                                       array_ind     = TRUE,
                                       array_fields = NULL,
                                       remove_by_sort_ind   = FALSE,
                                       sort_field       = NULL,
                                       rank_type    = "ASC",
                                       pg_conn_param  = dadmtools::get_pg_conn_list())
{

  #browser()
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
  if ((array_ind && remove_by_sort_ind) || (!array_ind && !remove_by_sort_ind)  ) {
      print(glue("ERROR: array_ind and remove_by_sort_ind cannot both be TRUE or cannot both be FALSE"))
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


    }else if (remove_by_sort_ind){

  if (is.null(sort_field) || dadmtools::is_blank(sort_field) || is.null(rank_type) || dadmtools::is_blank(rank_type) || (!rank_type %in% c('ASC','DESC'))   ) {
    print(glue("ERROR: Please provide a sort_field and rank_type "))
    stop()
  } else{
    if (!field_exists(attribute_table_no_schema,sort_field,attribute_table_schema)){
      print(glue("ERROR: sort_field not in attribute table"))
      stop() }else{


        dadmtools::run_sql_r(glue("DROP TABLE IF EXISTS {gr_skey_table}_no_duplicates_{rank_type}_{sort_field};"), pg_conn_param)
        dadmtools::run_sql_r(glue("Create TABLE  {gr_skey_table}_no_duplicates_{rank_type}_{sort_field} as

                                  SELECT a.*
                                  FROM (SELECT grskey.gr_skey, att.*,
                                  ROW_NUMBER() OVER (PARTITION BY grskey.gr_skey ORDER BY {sort_field} {rank_type}) ranked_order
                                  FROM  {gr_skey_table} grskey join {attribute_table} att on
                                    grskey.pgid = att.pgid) a
                                  WHERE a.ranked_order = 1
                                    ;"), pg_conn_param)}


    print(glue('Adding gr_skey as primary key to {gr_skey_table}_no_duplicates_{rank_type}_{sort_field}'))
    dadmtools::run_sql_r(glue("ALTER TABLE {gr_skey_table}_no_duplicates_{rank_type}_{sort_field} ADD PRIMARY KEY (gr_skey);"), pg_conn_param)



    }





  }}




