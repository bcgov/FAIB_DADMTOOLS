#' Rasterize spatial data using Terra
#'
#' @param src_sf Simple feature object
#' @param field field to be rasterized
#' @param template_tif path of template raster
#' @param crop_extent OPTIONAL - extent of output raster <xmin>,<ymax>,<ymin>,<xmax> default c(273287.5,1870587.5,367787.5,1735787.5)
#' @param src_lyr OPTIONAL - input data layer e.g. 'veg_comp_poly'
#' @param out_tif_path path of out TIF defualt D:/Projects/provDataProject
#' @param out_tif_name  name of out TIF
#' @param datatype  default Int64
#' @param nodata No data value, defaults to 0
#'
#' @return output tif name
#' @export
#'
#' @examples coming soon

rasterize_terra <- function(src_sf,
                          field,
                          template_tif,
                          crop_extent = NULL,
                          src_lyr = NULL,
                          out_tif_path = NULL,
                          out_tif_name = NULL,
                          datatype ='INT4S',
                          nodata = 0)
{

  dest_tif <- file.path(out_tif_path, out_tif_name)

  # Check if the file exists
  if (file.exists(dest_tif)) {
    # If exists, delete the file
    file.remove(dest_tif)
  }

  if( !is.null(src_lyr)) {
    in_vect <- terra::vect(x = src_sf, layer = src_lyr)
  } else {
    in_vect <- terra::vect(x = src_sf)

  }


  template_rast <- terra::rast(template_tif)
  print(glue("Rasterizing field: {field} using datatype: {datatype}"))
  rast_band <- terra::rasterize(in_vect, template_rast, field = field, wopt = list(datatype = datatype))
  if( !is.null(crop_extent)) {
    rast_band <-  terra::crop(rast_band, crop_extent, datatype = datatype)
    }
  print(glue("Writing raster: {dest_tif} using datatype: {datatype}"))
  terra::writeRaster(rast_band, dest_tif, datatype = datatype, overwrite = TRUE, NAflag = nodata)

  terra::tmpFiles(remove=TRUE)
  print('Raster created successfully.')
  return(dest_tif)
}









