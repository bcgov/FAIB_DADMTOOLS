#' Rasterize spatial data using Terra
#'
#' @param inSrc path of gdb or geopackage
#' @param field field to be rasterized
#' @param template path of template raster
#' @param cropExtent OPTIONAL - extent of output raster <xmin>,<ymax>,<ymin>,<xmax> default c(273287.5,1870587.5,367787.5,1735787.5)
#' @param inlyr OPTIONAL - input data layer e.g. 'veg_comp_poly'
#' @param outTifpath path of out TIF defualt D:/Projects/provDataProject
#' @param outTifname  name of out TIF
#' @param datatype  default Int64
#'
#' @return output tif name
#' @export
#'
#' @examples coming soon

rasterizeTerra <- function(
    inSrc,
    field,
    template,
    cropExtent = NULL,
    inlyr=NULL,
    outTifpath = NULL,
    outTifname = NULL,
    datatype ='INT4S'){

  dest <- file.path(outTifpath,outTifname)

  if( !is.null(inlyr)){
  inVect <- terra::vect(x = inSrc,layer = inlyr)
  }else{
    inVect <- terra::vect(x = inSrc)

  }

  templateRast <- terra::rast(template)
  rastBnd <- terra::rasterize(inVect, templateRast, field = field ,datatype=datatype)
  if( !is.null(cropExtent)){
    rastBnd <-  terra::crop(rastBnd,cropExtent,datatype=datatype)}

  terra::writeRaster(rastBnd,dest,datatype=datatype,overwrite = TRUE)
  return(dest)
}









