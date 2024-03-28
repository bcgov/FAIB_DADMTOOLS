#' Get extent from vector dataset that snaps to template raster
#'
#' @param dsn coming soon
#' @param layer coming soon
#' @param template_tif coming soon
#'
#' @return extent vector (i.e. c(xmin,xmax,ymin,ymax))
#' @export
#'
#' @examples coming soon


getGrskeyExtent <- function(
    dsn,
    layer,
    template_tif = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif')
{
  template_rast <- rast(template_tif)
  in_vect <- terra::vect(sf::st_read(dsn, layer))
  crop_extent <- ext(terra::crop(template_rast, in_vect, datatype='INT4S'))
  return(c(crop_extent[1], crop_extent[2], crop_extent[3], crop_extent[4]))
  }
