#' Write non spatial tables to PG using gdal on the machine.  Creates index on primary key.  Use for large files that would take too long to import into R
#'
#' @param src path of input dataset e.g. c:/test.gdb
#' @param outTblName Name of output PG table
#' @param dbName Name of PG database
#' @param lyr layer of input dataser e.g. roadsFC
#' @param pk primary of table e.g. objectid
#' @param select fields to keep in table e.g. "objectid,feature_id,segmentName"
#' @param where_clause sql where clause e.g. segment = 'highway2'
#' @param outSchema default database schema
#' @param useGdal Set to TRUE if GDAL is installed on machine e.g. TRUE
#'
#' @return no return
#' @export
#'
#' @examples coming soon

writeNoSpaTbl2PG <- function(src, outTblName, connList, lyr = NULL, pk = NULL, select = NULL, where = NULL, outSchema = NULL, table_comment = NULL) {

  dbname <- connList["dbname"][[1]]
  user <- connList["user"][[1]]

  # ## Tidy up lyr input
  if(is_blank(lyr)) {
    lyr <- NULL
  }

  # ## Tidy up pk input
  if(is_blank(pk)) {
    pk <- NULL
  }

  # ## Tidy up select
  if(is_blank(select)) {
    select <- ''
  } else {
      select <- gsub(",shape", "", select)
      select <- gsub(",geometry", "", select)
      select <- gsub(", shape", "", select)
      select <- gsub(", geometry", "", select)
      if (endsWith(select, ',')) {
        select <-  substr(select, 1, nchar(select) - 1)
      }
  }


  if(is_blank(outSchema)) {
    outTblName <- outTblName
  }

  if(is_blank(where)) {
    where <- ''
  } else {
    where <- glue("WHERE {where}")
  }


  if(is_blank(outSchema)) {
    outSchema <- "public"
  }

  if (endsWith(src, '.shp')) {
    precision <- "-lco precision=NO"
  } else {
    precision <- ''
  }

  sql <- glue::double_quote(glue("SELECT {select}, ROW_NUMBER() OVER () AS fid FROM {lyr} {where}"))
  print(paste('ogr2ogr',
              '-nlt NONE',
              '-overwrite',
              '-gt 200000',
              '-dialect sqlite',
              paste('-sql',sql),
              paste('-nln',outTblName),
              paste0('-lco SCHEMA=', outSchema),
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
                                  paste('-nln',outTblName),
                                  paste0('-lco SCHEMA=', outSchema),
                                  paste('-lco', 'OVERWRITE=YES'),
                                  '--config PG_USE_COPY YES',
                                  precision,
                                  paste0('-f PostgreSQL PG:dbname=',dbname),
                                  src), stderr = TRUE)))

  # ## add harded coded fid primary key sequenctial integer
  faibDataManagement::sendSQLstatement(glue("ALTER TABLE {outSchema}.{outTblName} DROP CONSTRAINT {outTblName}_pkey;"), connList)
  faibDataManagement::sendSQLstatement(glue("ALTER TABLE {outSchema}.{outTblName} ADD PRIMARY KEY (fid);"), connList)
  faibDataManagement::sendSQLstatement(table_comment, connList)
  faibDataManagement::sendSQLstatement(glue("ANALYZE {outSchema}.{outTblName};"),connList)
  }
