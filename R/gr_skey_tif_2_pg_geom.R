#' Import Grskey TIF 2 pg table
#'
#' @param grskeyTIF coming soon
#' @param maskTif coming soon
#' @param cropExtent coming soon
#' @param outCropTifName coming soon
#' @param connList coming soon
#'
#' @return coming soon
#' @export
#'
#' @examples coming soon


gr_skey_tif_2_pg_geom <- function(
    grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
    maskTif = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Lands_and_Islandsincluded.tif',
    cropExtent = c(273287.5,1870587.5,367787.5,1735787.5),
    outCropTifName = 'D:\\Projects\\provDataProject\\gr_skey_cropped.tif',
    connList = faibDataManagement::get_pg_conn_list()
    ){

  #Remove ocean from gr_skey raster, write a new tif
  terraExt <- terra::ext(cropExtent[1], cropExtent[2], cropExtent[3], cropExtent[4])
  grskeyRast <- terra::rast(grskeyTIF)
  landRast <- terra::rast(maskTif)

  rastList <- list(grskeyRast,landRast)
  cropList <- lapply(rastList,function(x){
    crs(x) <-  "epsg:3005"
    terra::crop(x,terraExt,datatype='INT4S')})

  grskeyRast <- cropList[[1]]
  landRast <- cropList[[2]]

  grskeyRast[landRast <= 0] <- NA
  writeRaster(grskeyRast, outCropTifName, datatype='INT4U',overwrite=TRUE)

  #GR_SKEY_RASTER to PG
  cmd<- paste0('raster2pgsql -s 3005 -d -C -r -P -I -M -t 100x100 ',outCropTifName, ' raster.grskey_bc_land | psql')
  shell(cmd)
  #Convert gr_skey raster to point table
  faibDataManagement::pg_rast_2_centroid('raster.grskey_bc_land','whse.all_bc_gr_skey','gr_skey',connList)
  #Create a non spatial table for every gr_skey raster pixel.  Table has a filed for gr_skey and ogc_fid
  sendSQLstatement("drop table if exists whse.all_bc_res",connList)
  sendSQLstatement("create table whse.all_bc_res as select gr_skey from whse.all_bc_gr_skey ;",connList)
  sendSQLstatement("ALTER TABLE whse.all_bc_res ADD CONSTRAINT all_bc_res_pkey PRIMARY KEY (gr_skey);",connList)
  sendSQLstatement("drop index if exists all_bc_res_gr_skey_inx;",connList)
  print('Creating Index')
  sendSQLstatement(paste0("create index all_bc_res_gr_skey_inx on whse.all_bc_res(gr_skey);"),connList)
}






