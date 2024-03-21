#' Import Grskey TIF 2 pg table
#'
#' @param grskeyTIF coming soon
#' @param maskTif coming soon
#' @param cropExtent coming soon
#' @param outCropTifName coming soon
#' @param connList coming soon
#' @param pgtblname coming soon
#'
#' @return coming soon
#' @export
#'
#' @examples coming soon


gr_skey_tif_2_pg_geom <- function(
    grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
    maskTif = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
    cropExtent = c(273287.5,1870587.5,367787.5,1735787.5),
    outCropTifName = 'D:\\Projects\\provDataProject\\gr_skey_cropped.tif',
    connList = faibDataManagement::get_pg_conn_list(),
    pgtblname = "whse.all_bc_gr_skey"
    ) {

  #Remove ocean from gr_skey raster, write a new tif
  terraExt <- terra::ext(cropExtent[1], cropExtent[2], cropExtent[3], cropExtent[4])
  grskeyRast <- terra::rast(grskeyTIF)
  landRast <- terra::rast(maskTif)

  rastList <- list(grskeyRast,landRast)
  cropList <- lapply(rastList,function(x){
    terra::crs(x) <-  "epsg:3005"
    terra::crop(x,terraExt,datatype='INT4S')})

  grskeyRast <- cropList[[1]]
  landRast <- cropList[[2]]

  landRast[landRast <= 0] <- NA
  grskeyRast <- terra::mask(grskeyRast,landRast,datatype='INT4S')
  writeRaster(grskeyRast, outCropTifName, datatype='INT4S',overwrite=TRUE)

  #GR_SKEY_RASTER to PG
  host <- connList["host"][[1]]
  user <- connList["user"][[1]]
  dbname <- connList["dbname"][[1]]
  password <- connList["password"][[1]]
  port <- connList["port"][[1]]
  sendSQLstatement("DROP TABLE IF EXISTS raster.grskey_bc_land;", connList)
  cmd<- glue('raster2pgsql -s 3005 -d -C -r -P -I -M -t 100x100 {outCropTifName} raster.grskey_bc_land | psql postgresql://{user}:{password}@{host}:{port}/{dbname}')
  shell(cmd)
  #Convert gr_skey raster to point table
  qry <- glue("DROP TABLE IF EXISTS {pgtblname};")
  qry1 <- 'SET client_min_messages TO WARNING;'
  qry2 <- glue("CREATE TABLE {pgtblname} AS
                  WITH tbl1 AS  (
                    SELECT
                      RAST
                    FROM
                      raster.grskey_bc_land
                  ), tbl2 as (
                    SELECT
                      ST_Tile(RAST, 1,1) AS RAST
                    FROM
                      tbl1
                  )
                  SELECT
                    public.st_pixelascentroid(rast,1,1)::geometry(Point,3005) AS geom,
                    (public.ST_SummaryStats(rast)).sum AS gr_skey
                  FROM
                    tbl2
                  WHERE
                    (public.ST_SummaryStats(rast)).sum is not null;")
  sendSQLstatement(qry,connList)
  conn<-DBI::dbConnect(connList["driver"][[1]],
                  host = connList["host"][[1]],
                  user = connList["user"][[1]],
                  dbname = connList["dbname"][[1]],
                  password = connList["password"][[1]],
                  port = connList["port"][[1]])
  RPostgres::dbExecute(conn, statement = qry1)
  RPostgres::dbExecute(conn, statement = qry2)
  RPostgres::dbDisconnect(conn)
  tblname <- strsplit(pgtblname, "\\.")[[1]][[2]]
  sendSQLstatement(glue("ALTER TABLE {pgtblname} ADD PRIMARY KEY (gr_skey);"),connList)
  sendSQLstatement(glue("DROP INDEX IF EXISTS {tblname}_gr_skey_idx;"),connList)
  sendSQLstatement(glue("CREATE INDEX {tblname}_gr_skey_idx ON {pgtblname} USING GIST(geom);"),connList)
  sendSQLstatement(glue("ANALYZE {pgtblname};"),connList)
}





