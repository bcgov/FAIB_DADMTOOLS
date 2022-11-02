#' converts a PG raster into a non spatial pg table
#' @param outLyrName name of output non-spatial table
#' @param inRas  input PG Raster
#' @param outField name of the value field in output table
#' @param templateRas template pg raster with the same the rsolution znd extent of the input raster
#'
#' @return nothing is returned
#' @export
#'
#' @examples coming soon


pgRas2rows <- function(outLyrName,inRas,outField,templateRas,connList){
  indexName <-  paste0(gsub("\\.", "_", outLyrName), '_indx_geom')
  print(indexName)
  qry1 <- "DROP FUNCTION IF EXISTS FAIB_TILED_RASTER_TO_ROWS;"
  qry2 <- "CREATE OR REPLACE FUNCTION FAIB_TILED_RASTER_TO_ROWS(outTbl VARCHAR,indexName VARCHAR, srcRast VARCHAR, templateRast VARCHAR, outFld VARCHAR DEFAULT 'val') RETURNS VARCHAR
AS $$
DECLARE
	qry VARCHAR;
	rast VARCHAR;
	qry2 VARCHAR;
BEGIN
	rast = 'rast';
	EXECUTE 'DROP TABLE IF EXISTS ' || outTbl || ';';

	qry = 'CREATE TABLE ' || outTbl || ' AS
with tbl1 as  ( select a.rid as rid, b.rast as rast from  ' || templateRast || ' a, ' || srcRast || ' b
      			WHERE ST_UpperLeftX(a.rast) = ST_UpperLeftX(b.rast) AND ST_UpperLeftY(a.rast) = ST_UpperLeftY(b.rast)
		  		order by a.rid ),
tbl2 as (SELECT rid,ST_Tile(RAST, 1,1) AS RAST FROM tbl1),
tbl3 as (select *, ROW_NUMBER() OVER () AS rid_tile from tbl2),
tbl4 as (select *, ROW_NUMBER() OVER (PARTITION BY rid ORDER BY rid_tile ) AS tile_id from tbl3),
tbl5 as (SELECT RID * 10000 + tile_id as ogc_fid,
		(ST_SummaryStats(rast)).sum AS ' || outFld || '
		from tbl4)
select ogc_fid, ' || outFld || ' from tbl5 where ' || outFld || ' is not null;';


	RAISE NOTICE '%', qry;
	EXECUTE qry;
	EXECUTE 'ALTER TABLE '  || outTbl || ' DROP CONSTRAINT IF EXISTS ' || indexName || '_pkey;';
	EXECUTE 'ALTER TABLE '  || outTbl || ' ADD CONSTRAINT ' || indexName || '_pkey PRIMARY KEY (ogc_fid);';
	--Create an index on the output raster
	EXECUTE 'DROP INDEX IF EXISTS ' || indexName || ';';
	EXECUTE 'CREATE INDEX ' || indexName || ' ON ' || outTbl || '(OGC_FID);';
	RETURN outTbl;
END;
$$ LANGUAGE plpgsql;"
  faibDataManagement::sendSQLstatement(qry1,connList)
  faibDataManagement::sendSQLstatement(qry2,connList)
  qry3 <- paste0("SELECT FAIB_TILED_RASTER_TO_ROWS('", outLyrName,"','",indexName,"', '",inRas,"', '",templateRas,"','",outField,"')")
  print(qry3)
  faibDataManagement::sendSQLstatement(qry3,connList)

}
