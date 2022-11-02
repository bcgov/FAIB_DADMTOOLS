#' Converts postgres raster to  posgres points table
#'
#' @param inRas coming soon
#' @param outLyrName coming soon
#' @param outField coming soon
#'
#' @return coming soon
#' @export
#'
#' @examples coming soon

#function for converting postgres raster to points
pg_rast_2_centroid <- function(inRas,
                               outLyrName,
                               outField,
                               connList = faibDataManagement::get_pg_conn_list()){
  indexName <-  paste0(gsub("\\.", "_", "outLyrName"), '_indx_geom')
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
	EXECUTE 'ALTER TABLE '  || outTbl || ' ADD CONSTRAINT all_bc_gr_skey_pkey PRIMARY KEY (ogc_fid);';
	--Create an index on the output raster
	EXECUTE 'DROP INDEX IF EXISTS ' || outTbl ||'ind_geom;';
	EXECUTE 'CREATE INDEX ' || indexName || ' ON ' || outTbl || ' USING GIST(geom);';
	RETURN outTbl;
END;
$$ LANGUAGE plpgsql;"
  faibDataManagement::sendSQLstatement(qry,connList)
  faibDataManagement::sendSQLstatement(qry2,,connList)
  qry3 <- paste0("SELECT FAIB_CENTROID_TILED_RASTER('", outLyrName,"','",indexName,"', '",inRas,"','",outField,"')")
  print(qry3)
  faibDataManagement::sendSQLstatement(qry3,connList)

}
