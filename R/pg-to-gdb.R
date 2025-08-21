#' Convert a Postgres table to GDB
#'@param pg_conn_param named (optional): list - connection parameters. Default is  dadmtools::get_pg_conn_list().
#' @param fields_to_gdb (optional): string - comma separated list of fields to export. Default is all fields
#' @param pg_att_table (required): string - name of Postgres table with fields to export to GDB
#' @param pg_attskey_table (optional): string - name of Postgres table with join field to `pg_att_table` and table with geometry.
#' @param pg_skgeom_table (optional): string - name of Postgres table with geometry and join field to `pg_att_table` or `pg_attskey_table`.
#' @param geom_field (required): string - name of geometry field
#' @param dst_ha_field (optional): string - name of hectares field created in destination GDB. Default is `fid_ha`.
#' @param key_skgeo_tbl (optional): string - name of key field (e.g. gr_skey) to join geometry table (`pg_skgeom_table`) with either `pg_att_table` or `pg_attskey_table`.
#' @param key_attskey_tbl (optional): string - name of key field (e.g. pgid) to join attribute table (`pg_att_table`) with join table (`pg_attskey_table`).
#' @param query (optional): string - SQL query to filter results (e.g. thlb_fact > 0). SQL statement placed after `where`. Default is all rows returned.
#' @param dst_gdb_name (required): string - name of destination GDB being created.
#' @param out_gdb_path (required): string - path to directory where GDB will be saved to.
#=================================================

pg_to_gdb <- function(pg_conn_param = dadmtools::get_pg_conn_list(),
                      fields_to_gdb = NULL,
                      pg_att_table,
                      pg_attskey_table = NULL,
                      pg_skgeom_table = NULL,
                      geom_field,
                      dst_ha_field = "fid_ha",
                      key_skgeo_tbl = "gr_skey",
                      key_attskey_tbl = "pgid",
                      query = NULL,
                      dst_gdb_name,
                      out_gdb_path)
{

  start_time <- Sys.time()
  print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))

  # set current directory to GDB destination directory
  setwd(out_gdb_path)

  # set connection strings ready for export
  driver <- pg_conn_param["driver"][[1]]
  hostname <- pg_conn_param["host"][[1]]
  username <- pg_conn_param["user"][[1]]
  pswrd <- pg_conn_param["password"][[1]]
  port <- pg_conn_param["port"][[1]]
  dbname <- pg_conn_param["dbname"][[1]]

  # get geometry type from geometry table
  if(!(is.null(pg_skgeom_table) || (nchar(pg_skgeom_table)) == 0)){
    sql_r <- glue("select distinct(ST_GeometryType(",
                  geom_field, ")) as geom_type ",
                  "from ", pg_skgeom_table)
    # or attribute table
  } else {
    sql_r <- glue("select distinct(ST_GeometryType(", geom_field, ")) ",
                  "as geom_type ",
                  "from ", pg_att_table)
  }
  # get all geometry types (e.g. Polygon and Multipolygon - this shouldn't happen!)
  geom_type_rows <- sql_to_df(sql_r, pg_conn_param)
  # just in case - warn user more than one geometry type
  if (nrow(geom_type_rows) > 1){
    print("Only first geometry type is being converted.")
  }
  # Get geometry type for conversion to GDB
  geo_type <- sql_to_df(sql_r, pg_conn_param)[[1]][1]

  # update fields_to_gdb if NULL (all fields)
  if((is.null(fields_to_gdb)) || (nchar(fields_to_gdb)) == 0){
    #select all fields except geometry
    # remove schema
    schema_tbl <- strsplit(pg_att_table, "\\.")[[1]]
    tbl <- pg_att_table
    if (length(schema_tbl) > 1){tbl=schema_tbl[2]}
    sql_r <- glue("SELECT column_name ",
                  "FROM information_schema.columns ",
                  "WHERE table_name = '", tbl, "' ",
                  "AND column_name != '", geom_field, "';")

    column_names <- dadmtools::sql_to_df(sql_r, pg_conn_param)
    # get field count of all fields to convert for group by
    fld_count <- sum(gregexpr(',', column_names)[[1]] >0)
    if(fld_count > 0){
      nums <- 2
      fields_to_gdb <- column_names[[1]][1]
      while(fld_count > 0){
        fields_to_gdb <- glue(fields_to_gdb, ", ", column_names[[1]][nums])
        nums <- nums + 1
        fld_count <- fld_count -1
      } # end while
    } # end if fld_count > 0
  } # end if fields_to_gdb is null

  fields_to_gdb_orig <- fields_to_gdb

  # create the PG temp table to convert to GDB
  gdb_name <- gsub("\\.gdb", "", dst_gdb_name)
  temp_tblname <- glue('temp_', gdb_name,
                       format(Sys.time(), "%Y%m%d%H%M%S")) %>%
    tolower()

  # Create SQL joins - depends on whether `pg_attskey_table` is used
  as_sk <- 'as a '
  if (!(is.null(pg_attskey_table) || (nchar(pg_attskey_table) == 0))){
    as_sk <- glue(as_sk, 'left join ',
                  pg_attskey_table,
                  ' as sk ',
                  'on (a.',key_attskey_tbl, '=sk.', key_attskey_tbl, ') ')
    # add correct table alias to `key_attskey_tbl`
    fields_to_gdb <- gsub(key_attskey_tbl, glue("a.", key_attskey_tbl),
                          fields_to_gdb)
  } # end if

  # find table that has geometry for SQL joins
  g <- 'a.'
  as_b <- ''
  if (!(is.null(pg_skgeom_table) || (nchar(pg_skgeom_table) == 0))){
    g <- "b."
    # add table alias as prefix to field key_skgeo_tbl
    fields_to_gdb <- gsub(key_skgeo_tbl,
                          glue(g, key_skgeo_tbl), fields_to_gdb)
    if (is.null(pg_attskey_table) || (nchar(pg_attskey_table) == 0)){
      as_b <- glue("left join ", pg_skgeom_table,
                   " as b on (b.", key_skgeo_tbl, "=a.",key_skgeo_tbl, ") ")
    } else {
      as_b <- glue("left join ", pg_skgeom_table,
                   " as b on (b.", key_skgeo_tbl, "=sk.",key_skgeo_tbl, ") ")
    } # end if pg_attskey_table
  } # end if  pg_skgeom_table ( pg_attribute has geometry)

  # only union polygons, adding new hectares field
  # points are converted without unioning
  if(geo_type == "ST_Polygon"){
    st_union <- glue("ST_Union(", g, geom_field, ") as geometry, ",
                     "ST_Area(ST_Union(", g, geom_field, "))/10000.0 as ",
                     dst_ha_field, " ")
    fld_count <- sum(gregexpr(',', fields_to_gdb)[[1]] >0)
    # build group by statement
    group_by <- " group by "
    if(fld_count > 0){
      nums <- 2
      group_by_nums <- "1"
      while(fld_count > 0){
        group_by_nums <- glue(group_by_nums, ", ", as.character(nums))
        nums <- nums + 1
        fld_count <- fld_count -1
      }
      group_by <- glue(group_by, group_by_nums)
    } else { # only one field for export
      group_by <- " group by 1"
      # nums is updated - for future geometry grouping
      nums <- 2
    }

  }else{ # point geometry - not unioned, not grouped
    st_union <- glue(g, geom_field, " as geometry ")
    group_by <- ""
  }

  # building where clause
  where_clause <- ""
  # exclude all rows without geometry
  if ( (is.null(query)) || (nchar(query) == 0)) {
    where_clause <- glue(" where ", geom_field, " is not null ", group_by, ");")
  } else { # add user query - and group by statement created above
    where_clause <- glue(" where ", query, " and ", geom_field,
                         " is not null ", group_by, ");")
  }

  # buid SQL statement to create temporary Postgres table
  temp_tbl_sql <- glue("create table ", temp_tblname,
                       " as (select ", fields_to_gdb,
                       ", ", st_union,
                       "from ", pg_att_table, " ",
                       as_sk, as_b, where_clause)

  # run SQL statement to reate temporary Postgres table
  run_sql_r(temp_tbl_sql, pg_conn_param)

  # add a unique id for each feature - required to convert to GDB
  # build fid in case the id exists in PostGres attributes being converted
  num <- 1
  fid <- "fid"
  while (fid %in% fields_to_gdb_orig){
    fid <- glue(fid, as.character(num))
    num <- num+1
  }
  sql_r <- glue("alter table ", temp_tblname,
                " add column ", fid,
                " serial primary key;")

  run_sql_r(sql_r, pg_conn_param)

  # make GDB pat if it doesn't exist
  if ( !(dir.exists(out_gdb_path))){
    dir.create(out_gdb_path)
  }


  con <- RPostgres::dbConnect(drv = driver,
                              host=hostname,
                              user = username,
                              port = port,
                              password=pswrd,
                              dbname=dbname)
  on.exit(RPostgres::dbDisconnect(con))

  # check resulting geometry type in temp table
  # can result in multipolygons and polygons - GDBs only accept one geometry type
  sql_r <- glue("select distinct(ST_GeometryType(geometry)) as geom_type ",
                "from ", temp_tblname)

  geom_type_rows <- sql_to_df(sql_r, pg_conn_param)

  # if more than one type - convert all to multipolygons
  if (nrow(geom_type_rows) > 1) {
    temp_tblname_orig <- temp_tblname
    temp_tblname <- glue(temp_tblname_orig, "_geo")
    sql_r <- glue("create table ", temp_tblname, " as (",
                  "SELECT ", fields_to_gdb_orig,
                  ", ST_Multi(ST_CollectionExtract(geometry)) AS geometry, ",
                  "ST_Area(ST_Multi(ST_CollectionExtract(geometry)))/10000.0 ",
                  "as ", dst_ha_field,
                  " FROM ", temp_tblname_orig,
                  " where geometry is not null ",
                  group_by, ",", nums, ");")

    run_sql_r(sql_r, pg_conn_param)
    sql_drop <- glue("DROP TABLE IF EXISTS ", temp_tblname_orig)
    run_sql_r(sql_drop, pg_conn_param)

    # get geometry type of updated table
    sql_r <- glue("select geometry from ", temp_tblname)
    sf_obj <- sf::st_read(con, query=sql_r)
    geo_type <- as.character(sf::st_geometry_type(sf_obj)[[1]][1])

  }
  # convert ST geometry types for GDB conversion
  if (geo_type == 'ST_Point'){geo_type <- 'Point'}
  if (geo_type == 'ST_Polygon'){geo_type <- 'Polygon'}

  # Build ogr2ogr
  ogr2ogr_cmd <- glue('ogr2ogr -f "FileGDB" ',
                      dst_gdb_name,
                      ' PG:',
                      '"host=', hostname,
                      ' port= ', port,
                      ' user=', username,
                      ' dbname=', dbname,
                      ' password=', pswrd,
                      '" ', temp_tblname,
                      ' -nln ', gdb_name,
                      ' -nlt ', geo_type,
                      ' -lco GEOMETRY_NAME=geometry ',
                      ' -lco FID=', fid)

  # Make the ogr2ogr system call to create GDB
  system(ogr2ogr_cmd, intern=FALSE,
         show.output.on.console = TRUE,
         ignore.stderr = FALSE)

  # clean up - delete temporary Postgres table
  sql_drop <- glue("DROP TABLE IF EXISTS ", temp_tblname)
  run_sql_r(sql_drop, pg_conn_param)

  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  print(glue("Script ended at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
  print(glue("Script duration: {duration} minutes\n"))

  print(glue("CHECK results this is a generalized function for typical exports."))
  print(glue("GDB ", dst_gdb_name, " created in directory ", out_gdb_path))
}
