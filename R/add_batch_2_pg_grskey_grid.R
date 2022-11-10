#' Update FAIB hectares database from input csv of datasets
#'
#' @param inCSV coming soon
#' @param connList coming soon
#' @param cropExtent coming soon
#' @param gr_skey_tbl coming soon
#' @param wrkSchema coming soon
#' @param rasSchema coming soon
#' @param templateRaster coming soon
#' @param dataSourceTblName coming soon
#' @param setwd coming soon
#'
#'
#' @return no return
#' @export
#'
#' @examples coming soon

add_batch_2_pg_grskey_grid <- function(inCSV = 'D:\\Projects\\provDataProject\\tools\\prov_data_resultant3.csv',
                                connList = faibDataManagement::get_pg_conn_list(),
                                oraConnList = faibDataManagement::get_ora_conn_list(),
                              cropExtent = c(273287.5,1870587.5,367787.5,1735787.5),
                                gr_skey_tbl = 'all_bc_res_gr_skey',
                                wrkSchema = 'whse',
                               rasSchema = 'raster',
                              grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
                              maskTif='S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Lands_and_Islandsincluded.tif',
                              dataSourceTblName = 'data_sources',
                              setwd='D:/Projects/provDataProject',
                              outTifpath = 'D:\\Projects\\provDataProject',
                            importrast2pg = FALSE){
  inFile <- read.csv(inCSV)
  inFile <- inFile[inFile$inc == 1,]
  for (row in 1:nrow(inFile)) {
    rslt_ind <- gsub("[[:space:]]",'',tolower(inFile[row, "rslt_ind"])) ##1 = include(i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
    srctype <- gsub("[[:space:]]",'',tolower(inFile[row, "srctype"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    srcpath <- gsub("[[:space:]]",'',tolower(inFile[row, "srcpath"]))## path to input data. Note use bcgw for whse
    srclyr <- gsub("[[:space:]]",'',tolower(inFile[row, "srclyr"])) ## input layer name
    pk <- gsub("[[:space:]]",'',tolower(inFile[row, "primarykey"])) ## primary key field that will be added to resultant table
    suffix <- gsub("[[:space:]]",'',tolower(inFile[row, "suffix"])) ## suffix to be used in the resultant table
    nsTblm <- gsub("[[:space:]]",'',tolower(inFile[row, "tblname"])) ## name of output non spatial table
    query <- tolower(inFile[row, "src_query"])  ##where clause used to filter input dataset
    flds2keep <- gsub("[[:space:]]",'',tolower(inFile[row, "fields2keep"])) ## fields to keep in non spatial table


    faibDataManagement::add_data_2_pg_grskey_grid(rslt_ind,
                                                  srctype,
                                                  srcpath,
                                                  srclyr,
                                                  pk,
                                                  suffix,
                                                  nsTblm,
                                                  query,
                                                  flds2keep,
                                                  connList = connList,
                                                  oraConnList = oraConnList,
                                                  cropExtent = cropExtent,
                                                  gr_skey_tbl = gr_skey_tbl,
                                                  wrkSchema = wrkSchema,
                                                  rasSchema = rasSchema,
                                                  grskeyTIF = grskeyTIF,
                                                  maskTif=maskTif,
                                                  dataSourceTblName = dataSourceTblName,
                                                  setwd= setwd,
                                                  outTifpath = outTifpath,
                                                  importrast2pg = importrast2pg)
  }



}
