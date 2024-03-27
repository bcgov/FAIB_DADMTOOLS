#' Rasterize spatial data using SF.  Accepts postgis, gdb and geopackage
#'
#' @param in_lyr input data layer e.g. 'veg_comp_poly'
#' @param field field to be rasterized
#' @param out_tif_dir path of out TIF defualt D:/Projects/provDataProject
#' @param out_tif_name  name of out TIF
#' @param in_src path pf gdb or geopackage
#' @param pg_conn_param Named list with the following pg connection parameters Driver,host,user,dbname,password,port,schema
#' @param vec_extent vector with extents in the following order  <xmin>,<ymax>,<ymin>,<xmax> default c(273287.5,1870587.5,367787.5,1735787.5)
#' @param nodata  no data value of out tif defualt is 0
#' @return output tif name
#' @export
#'
#' @examples coming soon
#'
#'

rasterizeSF <- function(
                        in_lyr,
                        field,
                        out_tif_dir = 'D:\\Projects\\provDataProject',
                        out_tif_name,
                        in_src = NULL,
                        pg_conn_param = NULL,
                        vec_extent = c(273287.5,1870587.5,367787.5,1735787.5),
                        nodata = 0,
                        where = NULL,
                        datatype = 'Int32L'){

  if(is.null(in_src) && is.null(pg_conn_param)){stop("Must have one in_src or pg_conn_param populated")}
  if(!is.null(in_src) && !is.null(pg_conn_param)){stop("Must have only one in_src or pg_conn_param populated")}
  if(!is.null(in_src)){ src <- in_src}
  if(!is.null(pg_conn_param)){
    dbname <- pg_conn_param["dbname"][[1]]
    user <- pg_conn_param["user"][[1]]
    src <- glue::glue("PG:dbname={single_quote(dbname)} user={single_quote(user)}")
  }
  dest <- file.path(out_tif_dir, out_tif_name)
  if(!is.null(where)){where <- where}else{
    where <- ''}
  print(where)
  #Rasterize input Vector
  sf::gdal_utils("rasterize",src,dest,options = c('-tr','100',' 100',
                     '-te',vec_extent[1], vec_extent[3], vec_extent[2], vec_extent[4],
                     '-ot', datatype,
                     '-l', in_lyr,
                     '-a', field,
                     '-a_srs','EPSG:3005',
                     '-a_nodata', nodata,
                     '-co','COMPRESS=LZW',
                     '-where', where),
                 '-overwrite')
  return(dest)
}
