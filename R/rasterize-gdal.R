#' Rasterize spatial data using Gdal on setup on local machine
#'
#' @param in_lyr input data layer e.g. 'veg_comp_poly'
#' @param field field to be rasterized
#' @param out_tif_path path of out TIF defualt D:/Projects/provDataProject
#' @param out_tif_name  name of out TIF
#' @param src_path path pf gdb or geopackage
#' @param pg_conn_param Named list with the following pg connection parameters Driver,host,user,dbname,password,port,schema
#' @param crop_extent vector with extents in the following order  <xmin>,<xmax>,<ymin>,<ymax> default c(273287.5,1870587.5,367787.5,1735787.5)
#' @param nodata  no data value of out tif default is 0
#' @param where  default NULL
#' @param overlap_where  default NULL
#' @param datatype  default UInt32
#' @param touches OPTIONAL - if TRUE, every cell touched by a line or polygon is
#'   burned in (gdal_rasterize -at). If FALSE (default), a polygon only gets a
#'   cell when it covers the cell's center point. Has no effect on point data.
#' @param largest_area OPTIONAL - if TRUE, each cell gets the value of whatever
#'   covers the largest area of it, where the part of the cell not covered by
#'   any polygon counts as nodata and competes like a polygon. GDAL has no
#'   equivalent, so the selected features are exported with ogr2ogr (using the
#'   same SQL, so the field numbering is unchanged) and rasterized with
#'   rasterize_r(). See rasterize_r() for the full rule. Polygons only. Cannot
#'   be combined with touches.
#'
#' @return output tif name
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
    overlap_where = NULL,
    datatype = 'UInt32',
    touches = FALSE,
    largest_area = FALSE)
{
  # Escape apostrophes that sit inside a word (letter/digit on both sides),
  # e.g. 'Coeur d'Alene' -> 'Coeur d''Alene'. Quotes that open or close a
  # value are left alone, and already-escaped '' is not changed.
  escape_apostrophes <- function(x) {
    gsub("(?<=[\\p{L}\\p{N}])'(?=[\\p{L}\\p{N}])", "''", enc2utf8(x), perl = TRUE)
  }

  if (isTRUE(touches) && isTRUE(largest_area)) {
    stop("Choose either touches = TRUE or largest_area = TRUE, not both.")
  }

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
    host <- pg_conn_param["host"][[1]]
    password <- pg_conn_param["password"][[1]]
    port <- pg_conn_param["port"][[1]]
    src <- glue::glue("\"PG:dbname={single_quote(dbname)} user={single_quote(user)} host={single_quote(host)} port={single_quote(port)} password={single_quote(password)}\"")
  }
  dst_ras_filename <- file.path(out_tif_path, out_tif_name)


  if(is_blank(where)) {
    where <- ''
  } else {
    where <- glue("WHERE {escape_apostrophes(where)}")
  }

  if(is_blank(overlap_where)) {
    overlap_where <- ''
  } else {
    overlap_where <- glue("WHERE {escape_apostrophes(overlap_where)}")
  }


  value <- glue("-a {field}")
  comp <- '-co COMPRESS=LZW'
  ot <- glue("-ot {datatype}")
  all_touched <- if (isTRUE(touches)) '-at' else character(0)

  proj <- "-a_srs EPSG:3005"
  extent_string <- paste("-te", crop_extent[1], crop_extent[3], crop_extent[2], crop_extent[4] )
  cell_size <- "-tr 100 100"
  if (is_blank(in_lyr)){
    in_lyr <- ''
  }


  ################## Old bug###
  #########################sql <- paste0('-dialect sqlite -sql ', glue::double_quote(glue("With t1 as (SELECT ROW_NUMBER() OVER () AS {field}, * FROM {in_lyr}) select * from t1 {where}")))
  ################## Old bug###
  ################## Old bug###
  ################## Old bug###
  ################## Old bug###sql <- paste0('-dialect sqlite -sql ', glue::double_quote(glue("SELECT ROW_NUMBER() OVER () AS {field}, *  from {in_lyr} {where}")))
  #    sql <- paste0('-dialect sqlite -sql ', glue::double_quote(glue("SELECT * FROM (SELECT ROW_NUMBER() OVER () AS {field},* FROM {in_lyr} {where} ) AS numbered {overlap_where} ")))
  # print(sql)

  sql <- paste0('-dialect sqlite -sql ', glue::double_quote(glue("With t1 as (SELECT ROW_NUMBER() OVER () AS {field},* FROM {in_lyr}), t2 as (select * from t1 {where}) select * from t2 {overlap_where} ")))
  print(sql)


  if (isTRUE(largest_area)) {
    # Export the selected features with the same SQL, so {field} is numbered
    # exactly as gdal_rasterize would number it, then rasterize with rasterize_r()
    tmp_gpkg <- tempfile(fileext = ".gpkg")
    on.exit(unlink(tmp_gpkg), add = TRUE)
    ogr_args <- c('-f GPKG', '-t_srs EPSG:3005', '-nlt PROMOTE_TO_MULTI', '-nln features',
                  shQuote(tmp_gpkg), src, sql)
    print(glue("Exporting features for largest-area rasterization: {tmp_gpkg}"))
    print(paste(c('ogr2ogr', ogr_args), collapse = ' '))
    print(system2('ogr2ogr', args = ogr_args, stderr = TRUE))
    if (!file.exists(tmp_gpkg)) stop("ogr2ogr did not create the temporary GeoPackage; see the messages above.")

    in_sf <- sf::st_read(tmp_gpkg, layer = "features", quiet = TRUE)
    if (nrow(in_sf) == 0) stop("No features selected for rasterization.")

    # Same grid gdal_rasterize would use: crop_extent at 100 m in BC Albers
    template <- terra::rast(terra::ext(crop_extent[1], crop_extent[2], crop_extent[3], crop_extent[4]),
                            resolution = 100, crs = "EPSG:3005")
    terra_types <- c(Byte = 'INT1U', UInt16 = 'INT2U', Int16 = 'INT2S', UInt32 = 'INT4U',
                     Int32 = 'INT4S', Float32 = 'FLT4S', Float64 = 'FLT8S')
    terra_datatype <- if (datatype %in% names(terra_types)) terra_types[[datatype]] else datatype

    rasterize_r(src_sf       = in_sf,
                field        = field,
                template_tif = template,
                out_tif_path = out_tif_path,
                out_tif_name = out_tif_name,
                datatype     = terra_datatype,
                nodata       = nodata,
                largest_area = TRUE)
  } else {
    nodata_arg <- glue('-a_nodata ', nodata)
    args <- c(ot, comp, value, all_touched, proj, extent_string, cell_size, src, dst_ras_filename, sql, nodata_arg)
    print(glue("Writing raster: {dst_ras_filename} using datatype: {datatype} (touches = {touches})"))
    print(paste(c('gdal_rasterize', args), collapse = ' '))
    print(system2('gdal_rasterize', args = args, stderr = TRUE))
  }

  print('Raster created successfully.')
  print(glue("src = {src}") )
  return(dst_ras_filename)

}
