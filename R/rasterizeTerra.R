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
    datatype ='INT4S',
    nodata = 0){

  dest <- file.path(outTifpath, outTifname)

  # Check if the file exists
  if (file.exists(dest)) {
    # If exists, delete the file
    file.remove(dest)
  }

  if( !is.null(inlyr)) {
  inVect <- terra::vect(x = inSrc, layer = inlyr)
  } else {
    inVect <- terra::vect(x = inSrc)

  }


  templateRast <- terra::rast(template)
  print(glue("Rasterizing field: {field} using datatype: {datatype}"))
  rastBnd <- terra::rasterize(inVect, templateRast, field = field, wopt = list(datatype = datatype))
  if( !is.null(cropExtent)) {
    rastBnd <-  terra::crop(rastBnd, cropExtent, datatype = datatype)
    }
  print(glue("Writing raster: {dest} using datatype: {datatype}"))
  terra::writeRaster(rastBnd, dest, datatype = datatype, overwrite = TRUE, NAflag=nodata)
  print('Raster created successfully.')
  return(dest)
}









