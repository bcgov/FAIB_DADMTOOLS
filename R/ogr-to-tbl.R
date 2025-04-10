#' Write non spatial tables to PG using gdal on the machine.  Creates index on primary key.  Use for large files that would take too long to import into R
#'
#' @param src path of input dataset e.g. c:/test.gdb
#' @param dst_tbl Name of output PG table
#' @param pg_conn_param Keyring object of Postgres credentials
#' @param lyr layer of input dataser e.g. roadsFC
#' @param pk primary of table e.g. objectid
#' @param select fields to keep in table e.g. "objectid,feature_id,segmentName"
#' @param where sql where clause e.g. segment = 'highway2'
#' @param dst_schema default database schema
#' @param tbl_comment Set to TRUE if GDAL is installed on machine e.g. TRUE
#'
#' @return no return
#'
#' @examples coming soon

ogr_to_tbl <- function(src,
                       dst_tbl,
                       pg_conn_param,
                       lyr = NULL,
                       pk = NULL,
                       select = NULL,
                       where = NULL,
                       dst_schema = NULL,
                       tbl_comment = NULL
                             )
{

  dbname <- pg_conn_param["dbname"][[1]]
  user <- pg_conn_param["user"][[1]]

  # ## Tidy up lyr input
  if(is_blank(lyr)) {
    lyr <- NULL
  }

  # ## Tidy up pk input
  if(is_blank(pk)) {
    pk <- NULL
  }


  ## tidy up the fields to keep
  if(is_blank(select)) {
    # ## Extract the fields from layer
    data = read_sf(src, query = glue('SELECT * FROM {lyr} LIMIT 1'))
    field_names = tolower(names(data))
    rm(data)
    field_names <- field_names[!(field_names %in% c("shape", "geometry", "geom", "_ogr_geometry_", "geometry_l", "geometry_length", pk))]
    select <- paste(field_names, collapse = ",")
  } else {
    ##remove trailing comma if exists
    if (endsWith(select, ',')) {
      select <-  substr(select, 1, nchar(select) - 1)
    }
    ## convert select into list
    field_names <- strsplit(select, ",")[[1]]
    ## remove geometry, length related field (unnecessary) and pk fields if exist
    field_names <- field_names[!(field_names %in% c("shape", "geometry", "geom", "_ogr_geometry_", "geometry_l", "geometry_length", pk))]
    ## convert back to string
    select <- paste(field_names, collapse = ",")
  }


  if(is_blank(dst_schema)) {
    dst_tbl <- dst_tbl
  }

  if(is_blank(where)) {
    where <- ''
  } else {
    where <- glue("WHERE {where}")
  }


  if(is_blank(dst_schema)) {
    dst_schema <- "public"
  }

  if (endsWith(src, '.shp')) {
    precision <- "-lco precision=NO"
  } else {
    precision <- ''
  }

  sql <- glue::double_quote(glue("SELECT {select}, ROW_NUMBER() OVER () AS {pk} FROM {lyr} {where}"))
  print(paste('ogr2ogr',
              '-nlt NONE',
              '-overwrite',
              '-gt 200000',
              '-dialect sqlite',
              paste('-sql',sql),
              paste('-nln',dst_tbl),
              paste0('-lco SCHEMA=', dst_schema),
              paste('-lco', 'OVERWRITE=YES'),
              '--config PG_USE_COPY YES',
              precision,
              paste0('-f PostgreSQL PG:dbname=',dbname),
              src))

  print((system2('ogr2ogr',args=c('-nlt NONE',
                                  '-overwrite',
                                  '-gt 200000',
                                  '-dialect sqlite',
                                  paste('-sql',sql),
                                  paste('-nln',dst_tbl),
                                  paste0('-lco SCHEMA=', dst_schema),
                                  paste('-lco', 'OVERWRITE=YES'),
                                  '--config PG_USE_COPY YES',
                                  precision,
                                  paste0('-f PostgreSQL PG:dbname=', dbname),
                                  src), stderr = TRUE)))
  print('Import complete, moving to post processing')

  # ## add harded coded fid primary key sequenctial integer
  query <- glue("SELECT concat('ALTER TABLE {dst_schema}.{dst_tbl} DROP CONSTRAINT ', constraint_name) AS my_query
                FROM
                  information_schema.table_constraints
                WHERE
                  table_schema = '{dst_schema}'
                AND
                  table_name = '{dst_tbl}'
                AND
                  constraint_type = 'PRIMARY KEY';")
  sqlstmt <- (dadmtools::sql_to_df(query, pg_conn_param))$my_query
  dadmtools::run_sql_r(sqlstmt, pg_conn_param)
  dadmtools::run_sql_r(glue("ALTER TABLE {dst_schema}.{dst_tbl} ADD PRIMARY KEY ({pk});"), pg_conn_param)
  dadmtools::run_sql_r(tbl_comment, pg_conn_param)
  dadmtools::run_sql_r(glue("ANALYZE {dst_schema}.{dst_tbl};"), pg_conn_param)

  #Convert Real field types to Numeric
  query <- glue("
  SELECT table_name, column_name
  FROM information_schema.columns
  WHERE table_schema = '{dst_schema}' and table_name = '{dst_tbl}' AND data_type = 'real';")

  real_columns <-  dadmtools::sql_to_df(query,pg_conn_param)
  print(real_columns)

  if (nrow(real_columns) > 0) {
  # Step 3: Loop over columns and convert them to NUMERIC
  for (i in 1:nrow(real_columns)) {
    column_name <- real_columns$column_name[i]

    # Create the ALTER TABLE query
    alter_query <- glue("ALTER TABLE {dst_schema}.{dst_tbl} ALTER COLUMN {column_name} SET DATA TYPE NUMERIC;")

    dadmtools::run_sql_r(alter_query,pg_conn_param)}}





  }
