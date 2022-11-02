#' Use gdal to crop Raster to specified extent
#'
#' @param inTif path and name to TIF
#' @param cropExtent vector with extents in the following order  <xmin>,<ymax>,<ymin>,<xmax>
#' @param outTif output tif path and name
#' @param res resolution default is 100
#'
#' @return no return
#' @export
#'
#' @examples gdalCropRaster('myTif.tif',extVect,'myTifCropped.tif')

gdalCropRaster <- function(inTif, cropExtent,outTif, res=NULL){
  extent_str <- paste("-te", cropExtent[1], cropExtent[3], cropExtent[2], cropExtent[4] )
  res <- "-tr 100 100"
  print(extent_str)
  overwrite <- "-overwrite"
  system2('gdalwarp', args= c(extent_str,inTif,outTif, res,overwrite), stderr = TRUE)
}
