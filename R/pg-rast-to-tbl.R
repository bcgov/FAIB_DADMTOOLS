#' converts a PG raster into a non spatial pg table
#' @param out_lyr_name name of output non-spatial table
#' @param in_ras  input PG Raster
#' @param out_field name of the value field in output table
#' @param template_ras template pg raster with the same the resolution and extent of the input raster
#' @param pg_conn_param Keyring object of Postgres credentials
#' @return nothing is returned
#' @export
#'
#' @examples coming soon


pg_rast_to_tbl <- function(out_lyr_name, in_ras, out_field, template_ras, pg_conn_param){
  index_name <-  paste0(gsub("\\.", "_", out_lyr_name), '_indx_geom')
  print(index_name)
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
  faib_dadm_tools::run_sql_r(qry1, pg_conn_param)
  faib_dadm_tools::run_sql_r(qry2, pg_conn_param)
  qry3 <- glue("SELECT FAIB_TILED_RASTER_TO_ROWS('{out_lyr_name}','{index_name}', '{in_ras}', '{template_ras}', '{out_field}')")
  print(qry3)
  faib_dadm_tools::run_sql_r(qry3, pg_conn_param)

}
