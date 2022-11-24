#' Get extent from vector dataset that snaps to template raster
#'
#' @param dsn coming soon
#' @param layer coming soon
#' @param templateRaster coming soon
#'
#' @return extent vector (i.e. c(xmin,xmax,ymin,ymax))
#' @export
#'
#' @examples coming soon


getGrskeyExtent <- function(
    dsn,
    layer,
    templateRaster = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif'){
  templateRaster<-  rast(templateRaster)
  inVect <- terra::vect(sf::st_read(dsn,layer))
  test <- terra::crop(templateRaster,inBnd,datatype='INT4S')
  cropExtent <- ext(terra::crop(grskeyTIF,inBnd,datatype='INT4S'))
  return(c(cropExtent[1],cropExtent[2],cropExtent[3],cropExtent[4] ))
  }
