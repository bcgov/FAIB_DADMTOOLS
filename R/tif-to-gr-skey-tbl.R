#' Convert tif to grskey table
#'
#' @param src_tif_filename File path to input geotiff
#' @param template_tif File path to template geotiff with gr_skey values
#' @param mask_tif File path to geotiff to be used as mask (values that ar N/a in mask will be nulled )
#' @param crop_extent Raster crop extent, list of c(ymin, ymax, xmin, xmax) in EPSG:3005
#' @param val_field_name Name of output column
#'
#' @return coming soon
#' @export
#'
#' @examples coming soon


tif_to_gr_skey_tbl <- function(src_tif_filename = 'D:\\Projects\\provDataProject\\fadm_tfl_all_sp.tif',
                               template_tif     = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
                               mask_tif         = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                               crop_extent      = c(273287.5,1870587.5,367787.5,1735787.5),
                               val_field_name   = 'val')
{
  tif_rast <- terra::rast(src_tif_filename)
  if (res(tif_rast)[1] != 100 && res(tif_rast)[2] != 100) {
    print(glue('ERROR: Exiting script. Provided raster must have a resolution of 100 x 100 and snapped to gr_skey grid. Provided raster resolution of source file {src_tif_filename} is: {res(tif_rast)[1]} x {res(tif_rast)[2]}.'))
    return(NULL)
  }

  terra_extent <- terra::ext(crop_extent[1], crop_extent[2], crop_extent[3], crop_extent[4])
  template_rast <- terra::rast(template_tif)
  mask_rast <- terra::rast(mask_tif)

  rast_list <- list(template_rast, mask_rast)
  crop_list <- lapply(rast_list, function(x){
                crs(x) <-  "epsg:3005"
                terra::extend(x, terra_extent, datatype='INT4S')
                terra::crop(x, terra_extent, datatype='INT4S')
                }
              )

  template_rast <- crop_list[[1]]
  mask_rast <- crop_list[[2]]
  raster_datatype <- datatype(tif_rast)
  tif_rast <- terra::extend(tif_rast, terra_extent, datatype = raster_datatype)
  tif_rast <- terra::crop(tif_rast, terra_extent, datatype = raster_datatype)


  mask_rast[mask_rast <= 0] <- NA
  tif_rast <- terra::mask(tif_rast, mask_rast, datatype = raster_datatype)

  ## if the raster datatype is integer,
  if (grepl("INT", raster_datatype)) {
    tif_values <- as.integer(tif_rast[])
  } else {
    tif_values <- as.numeric(tif_rast[])
  }
  grskeyValues <- as.integer(template_rast[])

  df <- data.frame(grskeyValues, tif_values)
  colnames(df) <- c('gr_skey', 'value')
  df <- df[!is.na(df$value),]
  colnames(df) <- c('gr_skey', val_field_name)
  return(df)
}
