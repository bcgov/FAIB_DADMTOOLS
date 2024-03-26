#' Convert tif to grskey table
#'
#' @param inTIF File path to input geotiff
#' @param grskeyTIF File path to template geotiff with gr_skey values
#' @param maskTif File path to geotiff to be used as mask (values that ar N/a in mask will be nulled )
#' @param cropExtent Raster crop extent, list of c(ymin, ymax, xmin, xmax) in EPSG:3005
#' @param valueColName Name of output column
#'
#' @return coming soon
#' @export
#'
#' @examples coming soon


tif2grskeytbl <- function(inTIF = 'D:\\Projects\\provDataProject\\fadm_tfl_all_sp.tif',
                          grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
                          maskTif='S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                          cropExtent = c(273287.5,1870587.5,367787.5,1735787.5),
                          valueColName = 'val')
{

  terraExt <- terra::ext(cropExtent[1], cropExtent[2], cropExtent[3], cropExtent[4])
  tifRast <- terra::rast(inTIF)
  grskeyRast <- terra::rast(grskeyTIF)
  landRast <- terra::rast(maskTif)

  rastList <- list(grskeyRast, landRast)
  cropList <- lapply(rastList,function(x){
                crs(x) <-  "epsg:3005"
                terra::crop(x, terraExt, datatype='INT4S')
                }
              )
  raster_datatype <- datatype(tifRast)
  tifRast <- terra::extend(tifRast, terraExt, datatype = raster_datatype)
  tifRast <- terra::crop(tifRast, terraExt, datatype = raster_datatype)
  grskeyRast <- cropList[[1]]
  landRast <- cropList[[2]]

  landRast[landRast <= 0] <- NA
  tifRast <- terra::mask(tifRast, landRast, datatype = raster_datatype)

  ## if the raster datatype is integer,
  if (grepl("INT", raster_datatype)) {
    tifValues <- as.integer(tifRast[])
  } else {
    tifValues <- as.numeric(tifRast[])
  }
  grskeyValues <- as.integer(grskeyRast[])

  df <- data.frame(grskeyValues, tifValues)
  colnames(df) <- c('gr_skey', 'value')
  df <- df[!is.na(df$value),]
  colnames(df) <- c('gr_skey', valueColName)
  return(df)
}
