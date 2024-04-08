#' Update FAIB hectares database from input csv of datasets
#'
#' @param in_csv File path to data sources csv, Defaults to "config_parameters.csv"
#' @param pg_conn_param Keyring object of Postgres credentials, defaults to faib_dadm_tools::get_pg_conn_list()
#' @param ora_conn_param Keyring object of Oracle credentials, defaults to faib_dadm_tools::get_ora_conn_list()
#' @param crop_extent list of c(ymin, ymax, xmin, xmax) in EPSG:3005, defaults to c(273287.5,1870587.5,367787.5,1735787.5)
#' @param gr_skey_tbl Schema and table name of the pre-existing gr_skey table. Argument to be used with suffix and rslt_ind within in_csv. Defaults to "whse.all_bc_gr_skey"
#' @param dst_schema Destination import schema of non spatial table & related gr_skey table, defaults to "whse"
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

batch_import_to_pg_gr_skey <- function(in_csv           = "config_parameters.csv",
                                      pg_conn_param     = faib_dadm_tools::get_pg_conn_list(),
                                      ora_conn_param    = faib_dadm_tools::get_ora_conn_list(),
                                      crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5),
                                      gr_skey_tbl       = "whse.all_bc_gr_skey",
                                      dst_schema        = "whse",
                                      raster_schema     = "raster",
                                      template_tif      = "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif",
                                      mask_tif          = "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif",
                                      data_src_tbl      = "whse.data_sources",
                                      out_tif_path,
                                      import_rast_to_pg = FALSE
                                      )
{
  start_time <- Sys.time()
  print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
  in_file <- read.csv(in_csv)
  in_file <- in_file[in_file$inc == 1,]
  for (row in 1:nrow(in_file)) {
    rslt_ind      <- gsub("[[:space:]]","",tolower(in_file[row, "rslt_ind"])) ##1 = include (i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
    src_type      <- gsub("[[:space:]]","",tolower(in_file[row, "srctype"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    src_path      <- gsub("[[:space:]]","",tolower(in_file[row, "srcpath"])) ## path to input data. Note use bcgw for whse
    src_lyr       <- gsub("[[:space:]]","",tolower(in_file[row, "srclyr"])) ## input layer name
    suffix        <- gsub("[[:space:]]","",tolower(in_file[row, "suffix"])) ## suffix to be used in the resultant table
    dst_tbl       <- gsub("[[:space:]]","",tolower(in_file[row, "tblname"])) ## name of output non spatial table
    query         <- in_file[row, "src_query"]  ##where clause used to filter input dataset
    notes         <- in_file[row, "notes"]  ##where clause used to filter input dataset
    flds_to_keep  <- gsub("[[:space:]]","",tolower(in_file[row, "fields2keep"])) ## fields to keep in non spatial table
    ## checks
    if (any(c(is_blank(src_type), is_blank(src_path), is_blank(src_lyr), is_blank(dst_tbl), is_blank(out_tif_path)))){
      print("ERROR: Argument not provided, one of src_type, src_path, src_lyr, dst_tbl, out_tif_path was left blank. Exiting script.")
      return()
    }

    if (!(src_type %in% c("gdb", "oracle", "geopackage", "gpkg", "raster", "shp", "shapefile"))) {
      print(glue("ERROR: Invalid src_type: {src_type}. Hint, provide one of: gdb, oracle, geopackage, gpkg, raster, shapefile or shp. Exiting script."))
      return()
    }

    faib_dadm_tools::import_to_pg_gr_skey(rslt_ind          = rslt_ind,
                                          src_type          = src_type,
                                          src_path          = src_path,
                                          src_lyr           = src_lyr,
                                          suffix            = suffix,
                                          dst_tbl           = dst_tbl,
                                          query             = query,
                                          flds_to_keep      = flds_to_keep,
                                          notes             = notes,
                                          pg_conn_param     = pg_conn_param,
                                          ora_conn_param    = ora_conn_param,
                                          crop_extent       = crop_extent,
                                          gr_skey_tbl       = gr_skey_tbl,
                                          dst_schema        = dst_schema,
                                          raster_schema     = raster_schema,
                                          template_tif      = template_tif,
                                          mask_tif          = mask_tif,
                                          data_src_tbl      = data_src_tbl,
                                          out_tif_path      = out_tif_path,
                                          import_rast_to_pg = import_rast_to_pg)
  }
  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  print(glue("Script started at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
  print(glue("Script duration: {duration} minutes\n"))
}
