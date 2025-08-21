#' Create a shapefile from fields in a Postgres Table
#' @param pg_conn_param named (optional): list - connection parameters (default is returned by dadmtools::get_pg_conn_list)
#' @param fields_to_shape (required): string - field names to convert to shapefile - if more than one,must be separated by a comma
#' @param pg_att_table (required): string - name of Postgres schema.table with field to convert. Optionally can have geometry field
#' @param pg_attskey_table (optional): string - name of Postgres schema.table with key to geom and attribute tables (e.g. gr_skey or ogc_fid),   required if geometry not in attribute table
#' @param pg_skgeom_table (optional): string - name of Postgres schema.table with geometry, if geometry not in attribute table
#' @param geom_field (required): string - name of geometry field in pg_table_with_geom
#' @param key_skgeo_tbl (optional): string - name of join field to join pg_skey_table and pg_geom_table (e.g. gr_skey), required if joining 3 tables.
#' @param key_attskey_tbl (optional): string - name of join field to join pg_att_table and pg_skey_table (e.g. pgid), required if joining 2 or 3 tables.
#' @param out_shapefile_path (required): string - path to output shapefile directory
#' @param dst_shapefile_name (required): string - name of output shapefile saved to out_shapefile_path
#' @param query (optional): string SQL where clause used to filter input dataset, e.g. "thlb_fact > 0", "own is not null". Default NULL (no query applied).

pg_to_shapefile <- function(pg_conn_param = dadmtools::get_pg_conn_list(),
                            fields_to_shape,
                            pg_att_table,
                            pg_attskey_table = NULL,
                            pg_skgeom_table = NULL,
                            geom_field,
                            key_skgeo_tbl = NULL,
                            key_attskey_tbl = NULL,
                            out_shapefile_path,
                            dst_shapefile_name,
                            query)
{
  fld_count <- sum(gregexpr(',', fields_to_shape)[[1]] >0)
  group_by <- " group by 1"
  if(fld_count > 0){
    nums <- 2
    while(fld_count > 0){
      group_by_nums <- glue(", ", as.character(nums))
      nums <- nums + 1
      fld_count <- fld_count -1
    }
    group_by <- glue(" group by 1", group_by_nums)
  }

  shape_name <- gsub("\\.shp", "", dst_shapefile_name)
  temp_tblname <- glue('temp_', shape_name, format(Sys.time(), "%Y%m%d%H%M%S")) %>% tolower()

  where_clause <- ""
  if ( (is.null(query)) || (nchar(query) == 0)) {
    where_clause <- glue(group_by, ");")
  } else {
    where_clause <- glue(" where ", query, " ", group_by, ");")
  }

  # find att_sk table
  as_sk <- 'as a '
  if (!(is.null(pg_attskey_table) || (nchar(pg_attskey_table) == 0))){
    as_sk <- glue(as_sk, 'left join ',
                  pg_attskey_table,
                  ' as sk ',
                  'on (a.',key_attskey_tbl, '=sk.', key_attskey_tbl, ') ')
  }

  # find geom table
  g <- 'a.'
  as_b <- ''
  if (!(is.null(pg_skgeom_table) || (nchar(pg_skgeom_table) == 0))){
    g <- "b."
    if (is.null(pg_attskey_table) || (nchar(pg_attskey_table) == 0)){
      as_b <- glue("left join ", pg_skgeom_table,
                   " as b on (b.", key_skgeo_tbl, "=a.",key_skgeo_tbl, ") ")
    } else {
      as_b <- glue("left join ", pg_skgeom_table,
                   " as b on (b.", key_skgeo_tbl, "=sk.",key_skgeo_tbl, ") ")
    }}


  temp_tbl_sql <- glue("create table ", temp_tblname,
                       " as (select ", fields_to_shape, ", ",
                       "ST_Union(",g, geom_field, ") as singlegeom ",
                       "from ", pg_att_table, " ", as_sk, as_b,
                       where_clause)

  run_sql_r(temp_tbl_sql, pg_conn_param)

  # makes shapefile sub-directory for file being created
  if ( !(dir.exists(paste0(out_shapefile_path, "/", shape_name)))){
    dir.create(paste0(out_shapefile_path, "/", shape_name))
  }

  # set current directory to shapefile destination directory
  setwd(paste0(out_shapefile_path, "/", shape_name))

  hostname <- pg_conn_param["host"][[1]]
  username <- pg_conn_param["user"][[1]]
  pswrd <- pg_conn_param["password"][[1]]
  port <- pg_conn_param["port"][[1]]
  dbname <- pg_conn_param["dbname"][[1]]

  path2shapefile <- paste0(out_shapefile_path, "/", shape_name, "/")
  dst_shapefile_name_v0 <- glue(shape_name, '_v0', ".shp")
  dst_shapefile_name_v1 <- glue(shape_name, '_v1', ".shp")

  pgsql2shp_cmd <- glue("pgsql2shp",
                        " -u ", username,
                        " -h ", hostname,
                        " -P ", pswrd,
                        " -p ", port,
                        " -g ", "singlegeom",
                        " -f ", path2shapefile, dst_shapefile_name_v0, " ",
                        dbname, " ", temp_tblname)

  system(pgsql2shp_cmd, intern=FALSE,
         show.output.on.console = TRUE,
         ignore.stderr = FALSE)

  drop_tbl <- glue("drop table if exists ", temp_tblname, ";")
  run_sql_r(drop_tbl, pg_conn_param)

  # project new shapefile
  proj_cmd <- glue("ogr2ogr -t_srs EPSG:3005 ", dst_shapefile_name_v1,
                   " ", dst_shapefile_name_v0)

  system(proj_cmd, intern=FALSE,
         show.output.on.console = TRUE,
         ignore.stderr = FALSE)

  # delete all rows where area is 0
  area_cmd <- glue("ogr2ogr ", dst_shapefile_name, " ", dst_shapefile_name_v1,
                   " -dialect SQLite -sql ", '"SELECT * FROM ', shape_name, "_v1",
                   ' where ST_Area(geometry) > 0"')

  system(area_cmd, intern=FALSE,
         show.output.on.console = TRUE,
         ignore.stderr = FALSE)

  # Delete temporary shapefiles
  shape_v0 <- list.files(pattern=paste0("\\", shape_name, "_v0."))
  file.remove(shape_v0)
  shape_v1 <- list.files(pattern=paste0("\\", shape_name, "_v1."))
  file.remove(shape_v1)

}
