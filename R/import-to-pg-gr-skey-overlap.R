#' Import data with spatial overlaps into Postgres as gr_skey table
#'
#' @param inSrc input source
#' @param inLyr input layer
#' @param groupFld coming soon
#' @param outPGTblName output pg table name
#' @param schema Defaults to 'whse'
#' @param grskeyTIF Defaults to 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif'
#' @param maskTif Defaults to 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif'
#' @param connList Defaults to get_pg_conn_list()
#' @param terraExt Defaults to terra::ext(c(273287.5,1870587.5,367787.5,1735787.5))
#'
#'
#' @return no return
#' @export
#'
#' @examples coming soon


import_to_pg_gr_skey_overlap <- function(
  inSrc,
  inLyr,
  groupFld,
  outPGTblName,
  schema = 'whse',
  grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
  maskTif = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif' ,
  connList = get_pg_conn_list(),
  terraExt = terra::ext(c(273287.5,1870587.5,367787.5,1735787.5))
  )

{
  pgTblNameGlue <- glue('{schema}.{outPGTblName}_overlaps_gr_skey')
  pgTblNameNoSpaGlue <- glue('{schema}.{outPGTblName}')
  pgTblNameNoSpa <- RPostgres::Id(schema = schema, table = outPGTblName)
  pgTblName <- RPostgres::Id(schema = schema, table = glue('{outPGTblName}_overlaps_gr_skey'))

  ##Drop existing tables
  run_sql_r(glue('drop table if exists {pgTblNameGlue}'), connList = connList)
  run_sql_r(glue('drop table if exists {pgTblNameNoSpaGlue}'), connList = connList)

  ###Crop gr_skey and land and islands raster and mask out ocean
  grskeyRast <- terra::rast(grskeyTIF)
  landRast <- terra::rast(maskTif)
  rastList <- list(grskeyRast,landRast)
  cropList <- lapply(rastList,function(x){
  crs(x) <-  "epsg:3005"
  terra::crop(x,terraExt,datatype='INT4S')})
  grskeyRast <- cropList[[1]]
  landRast <- cropList[[2]]
  landRast[landRast <= 0] <- NA
  grskeyRast <- terra::mask(grskeyRast,landRast,datatype='INT4S')

  ###Create dataframe from gr_skey raster
  grskeyValues <- as.integer(grskeyRast[])
  df <- data.frame(grskeyValues)

  #import  boundaries into sf and filter on needed rows. Create a unique id called fid.
  bnd <- st_read(inSrc, inLyr, stringsAsFactors=FALSE)
  bnd$fid <- seq.int(nrow(bnd))
  bnd <- st_cast(bnd,"MULTIPOLYGON" )
  groupIDs <-  unique(bnd[[groupFld]])
  print(groupIDs)

  #loop thorugh bnd dataset:
  ## 1. rasterize each row on fid.  2.  add raster values to gr_skey dataframe 3.  append dataframe to table in postgres
  names <- c('gr_skey','fids')
  for (groupid in groupIDs) {
    print(groupid)
    toRas <- vect(bnd[bnd[[groupFld]] == groupid,])
    tifRast <- rasterize(toRas, grskeyRast, field = "fid",datatype='INT4S')
    tifRast <- as.integer(tifRast[])
    df$fids <- tifRast
    colnames(df) <- names
    df1 <- df[!is.na(df$fid),]
    df1 <- df1[!is.na(df1$gr_skey),]
    faib_dadm_tools::df_to_pg(pgTblName,df1,connList = connList, overwrite=F,append=T)
  }


  ##Group by gr_skey, creating an array for each bnd fid
  bndNoSpa <- st_drop_geometry(bnd)
  faib_dadm_tools::df_to_pg(pgTblNameNoSpa,bndNoSpa,connList = connList, overwrite=T,append=F)
  faib_dadm_tools::run_sql_r(paste0('create index ', outPGTblName, '_indx on ', pgTblNameGlue,'(gr_skey)'),connList = connList)

  }
