#' Update FAIB hectares database from input csv of datasets
#'
#' @param inCSV File path to data sources csv, see inputsDatasets2load2PG.csv for example
#' @param connList Keyring object of Postgres credentials
#' @param oraConnList Keyring object of Oracle credentials
#' @param cropExtent list of c(ymin, ymax, xmin, xmax) in EPSG:3005
#' @param gr_skey_tbl Tablename of the pre-existing foreign key lookup gr_skey table. Argument to be used with suffix and rslt_ind within inCSV. Schema is assumed to be wrkSchema
#' @param wrkSchema Schema where gr_skey, non spatial table will be imported as well as the schema of gr_skey_tbl
#' @param rasSchema If importrast2pg=TRUE, schema of imported raster
#' @param grskeyTIF The file path to the gr_skey geotiff to be used as a template raster
#' @param maskTif The file path to the geotiff to be used as a mask
#' @param dataSourceTblName coming soon
#' @param outTifpath Directory where output tif if exported and where vector is temporally stored prior to import
#' @param importrast2pg If TRUE, raster is imported into database in rasSchema
#'
#'
#' @return no return
#' @export
#'
#' @examples coming soon

add_batch_2_pg_grskey_grid <- function(inCSV            = 'D:\\Projects\\provDataProject\\tools\\prov_data_resultant3.csv',
                                      connList          = faibDataManagement::get_pg_conn_list(),
                                      oraConnList       = faibDataManagement::get_ora_conn_list(),
                                      cropExtent        = c(273287.5,1870587.5,367787.5,1735787.5),
                                      gr_skey_tbl       = 'all_bc_gr_skey',
                                      wrkSchema         = 'whse',
                                      rasSchema         = 'raster',
                                      grskeyTIF         = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
                                      maskTif           = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                                      dataSourceTblName = 'data_sources',
                                      outTifpath        = 'D:\\Projects\\provDataProject',
                                      importrast2pg     = FALSE
                                      )
{
  start_time <- Sys.time()
  print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
  inFile <- read.csv(inCSV)
  inFile <- inFile[inFile$inc == 1,]
  for (row in 1:nrow(inFile)) {
    rslt_ind  <- gsub("[[:space:]]",'',tolower(inFile[row, "rslt_ind"])) ##1 = include(i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
    srctype   <- gsub("[[:space:]]",'',tolower(inFile[row, "srctype"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    srcpath   <- gsub("[[:space:]]",'',tolower(inFile[row, "srcpath"]))## path to input data. Note use bcgw for whse
    srclyr    <- gsub("[[:space:]]",'',tolower(inFile[row, "srclyr"])) ## input layer name
    suffix    <- gsub("[[:space:]]",'',tolower(inFile[row, "suffix"])) ## suffix to be used in the resultant table
    nsTblm    <- gsub("[[:space:]]",'',tolower(inFile[row, "tblname"])) ## name of output non spatial table
    query     <- inFile[row, "src_query"]  ##where clause used to filter input dataset
    flds2keep <- gsub("[[:space:]]",'',tolower(inFile[row, "fields2keep"])) ## fields to keep in non spatial table


    faibDataManagement::add_data_2_pg_grskey_grid(rslt_ind          = rslt_ind,
                                                  srctype           = srctype,
                                                  srcpath           = srcpath,
                                                  srclyr            = srclyr,
                                                  suffix            = suffix,
                                                  nsTblm            = nsTblm,
                                                  query             = query,
                                                  flds2keep         = flds2keep,
                                                  connList          = connList,
                                                  oraConnList       = oraConnList,
                                                  cropExtent        = cropExtent,
                                                  gr_skey_tbl       = gr_skey_tbl,
                                                  wrkSchema         = wrkSchema,
                                                  rasSchema         = rasSchema,
                                                  grskeyTIF         = grskeyTIF,
                                                  maskTif           = maskTif,
                                                  dataSourceTblName = dataSourceTblName,
                                                  outTifpath        = outTifpath,
                                                  importrast2pg     = importrast2pg)
  }
  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  print(glue("Script duration: {duration} minutes\n"))
}
