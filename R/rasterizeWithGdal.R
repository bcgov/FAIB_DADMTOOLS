#' Rasterize spatial data using Gdal on setup on local machine
#'
#' @param inlyr input data layer e.g. 'veg_comp_poly'
#' @param field field to be rasterized
#' @param outTifpath path of out TIF defualt D:/Projects/provDataProject
#' @param outTifname  name of out TIF
#' @param inSrc path pf gdb or geopackage
#' @param pgConnList Named list with the following pg connection parameters Driver,host,user,dbname,password,port,schema
#' @param vecExtent vector with extents in the following order  <xmin>,<ymax>,<ymin>,<xmax> default c(273287.5,1870587.5,367787.5,1735787.5)
#' @param nodata  no data value of out tif default is 0
#' @param where  default NULL
#' @param datatype  default Int64
#'
#' @return output tif name
#' @export
#'
#' @examples comming soon

rasterizeWithGdal <- function(
    inlyr,
    field,
    outTifpath = 'D:\\Projects\\provDataProject',
    outTifname,
    inSrc = NULL,
    pgConnList = NULL,
    vecExtent = c(273287.5,1870587.5,367787.5,1735787.5),
    nodata = 0,
    where=NULL,
    datatype='UInt32')
{
    if(is.null(inSrc) && is.null(pgConnList)){
      stop("Must have one inSrc or pgConnList populated")
    } else if(!is.null(inSrc) && !is.null(pgConnList)){
      stop("Must have only one inSrc or pgConnList populated")
    }

  if(!is.null(inSrc)){
      src <- inSrc
    }

    if(!is.null(pgConnList)){
      dbname <- pgConnList["dbname"][[1]]
      user <- pgConnList["user"][[1]]
      src <- glue::glue("\"PG:dbname={single_quote(dbname)} user={single_quote(user)}\"")
    }
  dest <- file.path(outTifpath, outTifname)


  if(is_blank(where)) {
    where <- ''
  } else {
    where <- glue("WHERE {where}")
  }


  value <- paste("-a", field)
  comp <- '-co COMPRESS=LZW'
  datatype <- glue("-ot ", datatype)

  proj <- "-a_srs EPSG:3005"
  extent_str <- paste("-te", vecExtent[1], vecExtent[3], vecExtent[2], vecExtent[4] )
  cellSize <- "-tr 100 100"
  if (is_blank(inlyr)){
    inlyr <- ''
  }
  print(inlyr)
  spc <- ' '

  sql <- paste0('-dialect sqlite -sql ', glue::double_quote(glue("SELECT ROW_NUMBER() OVER () AS fid, * FROM {inlyr} {where}")))
  nodata <- glue('-a_nodata ', nodata)
  print(glue("Writing raster: {dest} using datatype: {datatype}"))
  print(paste('gdal_rasterize',datatype, comp,value,proj,extent_str,cellSize,src,spc,dest,sql,nodata))
  print(system2('gdal_rasterize',args=c(datatype, comp,value,proj,extent_str,cellSize,src,spc,dest,sql,nodata), stderr = TRUE))
  print('Raster created successfully.')
  return(dest)}
