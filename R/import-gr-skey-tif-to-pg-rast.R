#' Import Grskey TIF 2 pg table
#'
#' @param template_tif The file path to the gr_skey template geotiff, defaults to  "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
#' @param mask_tif The file path to the geotiff to be used as a mask, defaults to "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif"
#' @param crop_extent Raster crop extent, list of c(ymin, ymax, xmin, xmax) in EPSG:3005, defaults to c(273287.5,1870587.5,367787.5,1735787.5)
#' @param out_crop_tif_name filename of the written output tif
#' @param pg_conn_param Keyring object of Postgres credentials, defaults to dadmtools::get_pg_conn_list()
#' @param dst_tbl Destination table of the imported gr_skey_tbl table (format: schema_name.table_name), defaults to "whse.all_bc_gr_skey"
#'
#' @return coming soon
#' @export
#'
#' @examples coming soon


import_gr_skey_tif_to_pg_rast <- function(
    out_crop_tif_name,
    template_tif = "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif",
    mask_tif = "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif",
    crop_extent = c(273287.5,1870587.5,367787.5,1735787.5),
    pg_conn_param = dadmtools::get_pg_conn_list(),
    dst_tbl = "whse.all_bc_gr_skey"
    ) {

  #Remove ocean from gr_skey raster, write a new tif
  terra_extent <- terra::ext(crop_extent[1], crop_extent[2], crop_extent[3], crop_extent[4])
  gr_skey_rast <- terra::rast(template_tif)
  make_rast <- terra::rast(mask_tif)

  rast_list <- list(gr_skey_rast, make_rast)
  crop_list <- lapply(rast_list, function(x){
    terra::crs(x) <-  "epsg:3005"
    terra::crop(x, terra_extent, datatype = "INT4S")
    }
  )

  gr_skey_rast <- crop_list[[1]]
  make_rast <- crop_list[[2]]

  make_rast[make_rast <= 0] <- NA
  gr_skey_rast <- terra::mask(gr_skey_rast, make_rast, datatype = "INT4S")
  writeRaster(gr_skey_rast, out_crop_tif_name, datatype = "INT4S", overwrite = TRUE)

  #GR_SKEY_RASTER to PG
  host <- pg_conn_param["host"][[1]]
  user <- pg_conn_param["user"][[1]]
  dbname <- pg_conn_param["dbname"][[1]]
  password <- pg_conn_param["password"][[1]]
  port <- pg_conn_param["port"][[1]]
  run_sql_r("DROP TABLE IF EXISTS raster.grskey_bc_land;", pg_conn_param)
  cmd<- glue("raster2pgsql -s 3005 -d -C -r -P -I -M -t 100x100 {out_crop_tif_name} raster.grskey_bc_land | psql postgresql://{user}:{password}@{host}:{port}/{dbname}")
  shell(cmd)
  #Convert gr_skey raster to point table
  qry <- glue("DROP TABLE IF EXISTS {dst_tbl};")
  qry1 <- "SET client_min_messages TO WARNING;"
  qry2 <- glue("CREATE TABLE {dst_tbl} AS
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
                    (public.ST_SummaryStats(rast)).sum::integer AS gr_skey
                  FROM
                    tbl2
                  WHERE
                    (public.ST_SummaryStats(rast)).sum is not null;")
  run_sql_r(qry, pg_conn_param)
  conn<-DBI::dbConnect(pg_conn_param["driver"][[1]],
                  host = pg_conn_param["host"][[1]],
                  user = pg_conn_param["user"][[1]],
                  dbname = pg_conn_param["dbname"][[1]],
                  password = pg_conn_param["password"][[1]],
                  port = pg_conn_param["port"][[1]])
  RPostgres::dbExecute(conn, statement = qry1)
  RPostgres::dbExecute(conn, statement = qry2)
  RPostgres::dbDisconnect(conn)
  tblname <- strsplit(dst_tbl, "\\.")[[1]][[2]]
  run_sql_r(glue("ALTER TABLE {dst_tbl} ADD PRIMARY KEY (gr_skey);"), pg_conn_param)
  run_sql_r(glue("DROP INDEX IF EXISTS {tblname}_gr_skey_idx;"), pg_conn_param)
  run_sql_r(glue("CREATE INDEX {tblname}_gr_skey_idx ON {dst_tbl} USING GIST(geom);"), pg_conn_param)
  run_sql_r(glue("ANALYZE {dst_tbl};"), pg_conn_param)
  print(glue("Finished importing {dst_tbl}"))
}





