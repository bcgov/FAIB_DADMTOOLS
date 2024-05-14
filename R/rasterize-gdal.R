#' Rasterize spatial data using Gdal on setup on local machine
#'
#' @param in_lyr input data layer e.g. 'veg_comp_poly'
#' @param field field to be rasterized
#' @param out_tif_path path of out TIF defualt D:/Projects/provDataProject
#' @param out_tif_name  name of out TIF
#' @param src_path path pf gdb or geopackage
#' @param pg_conn_param Named list with the following pg connection parameters Driver,host,user,dbname,password,port,schema
#' @param crop_extent vector with extents in the following order  <xmin>,<ymax>,<ymin>,<xmax> default c(273287.5,1870587.5,367787.5,1735787.5)
#' @param nodata  no data value of out tif default is 0
#' @param where  default NULL
#' @param datatype  default Int64
#'
#' @return output tif name
#' @export
#'
#' @examples comming soon

rasterize_gdal <- function(
    in_lyr,
    field,
    out_tif_path,
    out_tif_name,
    src_path = NULL,
    pg_conn_param = NULL,
    crop_extent = c(273287.5,1870587.5,367787.5,1735787.5),
    nodata = 0,
    where = NULL,
    datatype = 'UInt32')
{
    if(is.null(src_path) && is.null(pg_conn_param)){
      stop("Must have one src_path or pg_conn_param populated")
    } else if(!is.null(src_path) && !is.null(pg_conn_param)){
      stop("Must have only one src_path or pg_conn_param populated")
    }

  if(!is.null(src_path)){
      src <- src_path
    }

    if(!is.null(pg_conn_param)) {
      dbname <- pg_conn_param["dbname"][[1]]
      user <- pg_conn_param["user"][[1]]
      src <- glue::glue("\"PG:dbname={single_quote(dbname)} user={single_quote(user)}\"")
    }
  dst_ras_filename <- file.path(out_tif_path, out_tif_name)


  if(is_blank(where)) {
    where <- ''
  } else {
    where <- glue("WHERE {where}")
  }


  value <- glue("-a {field}")
  comp <- '-co COMPRESS=LZW'
  datatype <- glue("-ot {datatype}")

  proj <- "-a_srs EPSG:3005"
  extent_string <- paste("-te", crop_extent[1], crop_extent[3], crop_extent[2], crop_extent[4] )
  cell_size <- "-tr 100 100"
  if (is_blank(in_lyr)){
    in_lyr <- ''
  }


  sql <- paste0('-dialect sqlite -sql ', glue::double_quote(glue("SELECT ROW_NUMBER() OVER () AS {field}, * FROM {in_lyr} {where}")))
  nodata <- glue('-a_nodata ', nodata)
  print(glue("Writing raster: {dst_ras_filename} using datatype: {datatype}"))
  print(paste('gdal_rasterize', datatype, comp, value, proj, extent_string, cell_size, src, dst_ras_filename, sql, nodata))
  print(system2('gdal_rasterize',args = c(datatype, comp, value, proj, extent_string, cell_size, src, dst_ras_filename, sql, nodata), stderr = TRUE))

  print('Raster created successfully.')
  return(dst_ras_filename)

}
