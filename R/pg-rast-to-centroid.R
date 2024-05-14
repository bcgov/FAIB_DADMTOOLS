#' Converts postgres raster to postgres points table
#'
#' @param in_ras coming soon
#' @param out_lyr_name coming soon
#' @param out_field coming soon
#' @param pg_conn_param Keyring object of Postgres credentials
#'
#' @return coming soon
#' @export
#'
#' @examples coming soon

#function for converting postgres raster to points
pg_rast_to_centroid <- function(in_ras,
                               out_lyr_name,
                               out_field,
                               pg_conn_param = dadmtools::get_pg_conn_list()){
  indexName <-  paste0(gsub("\\.", "_", "out_lyr_name"), '_indx_geom')
  qry <- "DROP FUNCTION IF EXISTS FAIB_CENTROID_TILED_RASTER;"
  qry2 <-"CREATE OR REPLACE FUNCTION FAIB_CENTROID_TILED_RASTER(outTbl VARCHAR,indexName VARCHAR,srcRast VARCHAR,outFld VARCHAR DEFAULT 'val') RETURNS VARCHAR
AS $$
DECLARE
	qry VARCHAR;
	rast VARCHAR;
	qry2 VARCHAR;
BEGIN
	rast = 'rast';
	EXECUTE 'DROP TABLE IF EXISTS ' || outTbl || ';';

	qry = 'CREATE TABLE ' || outTbl || ' AS
with tbl1 as  (SELECT RAST FROM ' || srcRast || '),
tbl2 as (SELECT ST_Tile(RAST, 1,1) AS RAST FROM tbl1)
SELECT public.st_pixelascentroid(rast,1,1) AS geom,
		(public.ST_SummaryStats(rast)).sum AS ' || outFld || '
from tbl2 where (public.ST_SummaryStats(rast)).sum is not null;';

	RAISE NOTICE '%', qry;
	EXECUTE qry;
	EXECUTE 'ALTER TABLE '  || outTbl || ' DROP CONSTRAINT IF EXISTS all_bc_gr_skey_pkey;';
	EXECUTE 'ALTER TABLE '  || outTbl || ' ADD CONSTRAINT all_bc_gr_skey_pkey PRIMARY KEY (gr_skey);';
	--Create an index on the output raster
	EXECUTE 'DROP INDEX IF EXISTS ' || outTbl ||'ind_geom;';
	EXECUTE 'CREATE INDEX ' || indexName || ' ON ' || outTbl || ' USING GIST(geom);';
	RETURN outTbl;
END;
$$ LANGUAGE plpgsql;"
  dadmtools::run_sql_r(qry, pg_conn_param)
  dadmtools::run_sql_r(qry2, pg_conn_param)
  qry3 <- glue("SELECT FAIB_CENTROID_TILED_RASTER('{out_lyr_name}','{indexName}', '{in_ras}','{out_field}')")
  print(qry3)
  dadmtools::run_sql_r(qry3, pg_conn_param)

}
