#' Creates a raster mask from a vector input
#'
#' @param in_src path of gdb or geopackage
#' @param field field to be rasterized
#' @param template path of template raster
#' @param crop_extent OPTIONAL - extent of output raster c(<xmin>,<ymax>,<ymin>,<xmax>) default c(273287.5,1870587.5,367787.5,1735787.5)
#' @param in_lyr OPTIONAL - input data layer e.g. 'veg_comp_poly'
#' @param out_tif_path path of out TIF
#' @param out_tif_name  name of out TIF
#' @param datatype  defaults to Int64
#'
#' @return output tif name
#' @export
#'
#' @examples coming soon

create_mask_tif <- function(
    in_src,
    field,
    template,
    crop_extent = c(273287.5,1870587.5,367787.5,1735787.5),
    in_lyr = NULL,
    out_tif_path = NULL,
    out_tif_name = NULL,
    datatype = 'INT4S'){


  dest <- file.path(out_tif_path, out_tif_name)

  if( !is.null(in_lyr)){
    in_vect <- terra::vect(x = in_src,layer = in_lyr)
  }else{
    in_vect <- terra::vect(x = in_src)

  }

  template_rast <- terra::rast(template)
  rast_band <- terra::rasterize(in_vect, template_rast, field = field, datatype = datatype)
  if( !is.null(crop_extent)){
    rast_band <-  terra::crop(rast_band, crop_extent, datatype = datatype)}

  rast_band[!is.na(rast_band)]  <- 1

  terra::writeRaster(rast_band, dest, datatype = datatype, overwrite = TRUE)
  terra::tmpFiles(remove=TRUE)
  return(dest)
}
