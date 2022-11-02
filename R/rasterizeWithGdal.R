#' Rasterize spatial data using Gdal on setup on local machine
#'
#' @param pk field name of whose values will be used in the raster (must be a numric type field e.g. integers, float etc)
#' @param src path of input dataset (e.g D:/data/data.gdb)
#' @param lyr input data layer e.g. 'veg_comp_poly'
#' @param cropExtent vector with extents in the following order  <xmin>,<ymax>,<ymin>,<xmax>
#' @param where_clause sql where use to filter data
#' @param outName optional output name of tif default is the lyr parameter concatenated with the pk parameter
#'
#' @return output tif name
#' @export
#'
#' @examples comming soon

rasterizeWithGdal <- function(pk, src, lyr, extent, where_clause=NULL, outName=NULL){
  value <- paste("-a", pk)
  tifName <- paste0( lyr, pk, ".tif")
  if( !is.null(outName)){
    tifName <- paste0( outName, ".tif")
  }
  comp <- '-co COMPRESS=LZW'
  datatype <- "-ot UInt32"

  proj <- "-a_srs EPSG:3005"
  extent_str <- paste("-te", extent[1], extent[3], extent[2], extent[4] )
  cellSize <- "-tr 100 100"
  inLyr <- sprintf('-l %s', lyr)
  where <- ''
  spc <- ' '
  if( !is.null(where_clause)){
    where <- where <- sprintf('-where "%s"', where_clause)
  }
  print(where)
  print(paste('gdal_rasterize',datatype, comp,value,proj,extent_str,cellSize,inLyr,src,spc,tifName,where,'-a_nodata -99'))
  print(system2('gdal_rasterize',args=c(datatype,comp,value,proj,extent_str,cellSize,inLyr,src,spc,tifName,where,'-a_nodata -99'), stderr = TRUE))
  return(tifName)}
