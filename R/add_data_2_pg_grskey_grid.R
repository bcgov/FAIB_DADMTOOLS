#' Update FAIB hectares database from input dataset
#'
#' @param rslt_ind coming soon
#' @param srctype coming soon
#' @param srcpath coming soon
#' @param srclyr coming soon
#' @param pk coming soon
#' @param suffix coming soon
#' @param nsTblm coming soon
#' @param query coming soon
#' @param flds2keep coming soon
#' @param connList coming soon
#' @param cropExtent coming soon
#' @param gr_skey_tbl coming soon
#' @param wrkSchema coming soon
#' @param rasSchema coming soon
#' @param fdwSchema coming soon
#' @param grskeyTIF coming soon
#' @param maskTif coming soon
#' @param dataSourceTblName coming soon
#' @param setwd coming soon
#'
#'
#' @return no return
#' @export
#'
#' @examples coming soon


add_data_2_pg_grskey_grid <- function(rslt_ind,
                                      srctype,
                                      srcpath,
                                      srclyr,
                                      pk,
                                      suffix,
                                      nsTblm,
                                      query,
                                      flds2keep,
                                      connList = faibDataManagement::get_pg_conn_list(),
                                      oraConnList = faibDataManagement::get_ora_conn_list(),
                                      cropExtent = c(273287.5,1870587.5,367787.5,1735787.5),
                                      gr_skey_tbl = 'all_bc_res_gr_skey',
                                      wrkSchema = 'whse',
                                      rasSchema = 'raster',
                                      fdwSchema = 'load',
                                      grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
                                      maskTif='S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Lands_and_Islandsincluded.tif',
                                      dataSourceTblName = 'data_sources',
                                      setwd='D:/Projects/provDataProject',
                                      outTifpath = 'D:\\Projects\\provDataProject',
                                      importrast2pg = FALSE
)

{
  #Get inputs from input file
  rslt_ind  <- gsub("[[:space:]]",'',tolower(rslt_ind)) ## 1 = include(i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
  srctype   <- gsub("[[:space:]]",'',tolower(srctype)) ## format of data source i.e. gdb,oracle, postgres, geopackage, raster
  srcpath   <- gsub("[[:space:]]",'',tolower(srcpath))## path to input data. Note use bcgw for whse
  srclyr    <- gsub("[[:space:]]",'',tolower(srclyr)) ## input layer name
  pk        <- gsub("[[:space:]]",'',tolower(pk)) ## primary key field that will be added to resultant table
  suffix    <- gsub("[[:space:]]",'',tolower(suffix)) ## suffix to be used in the resultant table
  nsTblm    <- gsub("[[:space:]]",'',tolower(nsTblm)) ## name of output non spatial table
  query     <- query  ## where clause used to filter input dataset
  flds2keep <- gsub("[[:space:]]",'',tolower(flds2keep)) ## fields to keep in non spatial table
  dataSourceTblName <- glue::glue("{wrkSchema}.{dataSourceTblName}")

  dst_schema_name <- wrkSchema ## destination schema name
  dst_table_name  <- nsTblm ## destination table name
  dst_gr_skey_table_name <- glue("{nsTblm}_gr_skey") ## destination table gr skey name

  today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
  dest_table_comment <- glue("COMMENT ON TABLE {dst_schema_name}.{dst_table_name} IS 'Table created by the FAIB_DATA_MANAGEMENT R package at {today_date}.
                                            TABLE relates to {dst_schema_name}.{dst_gr_skey_table_name}
                                            Data source details:
                                            Source Type: {srctype}
                                            Source Path: {srcpath}
                                            Source Layer: {srclyr}
                                            Source Primary key: {pk}
                                            Source where query: {query}';")
  dest_gr_skey_table_comment <- glue("COMMENT ON TABLE {dst_schema_name}.{dst_gr_skey_table_name} IS 'Table created by the FAIB_DATA_MANAGEMENT R package at {today_date}.
                                            TABLE relates to {dst_schema_name}.{dst_table_name}
                                            Data source details:
                                            Source Type: {srctype}
                                            Source Path: {srcpath}
                                            Source Layer: {srclyr}
                                            Source Primary key: {pk}
                                            Source where query: {query}';")
  ##convert whitespace to null when where clause is null
  if (query == '' || is.null(query) || is.na(query)) {
    # print("null is here")
    where_clause <- NULL
    query <- ''

  } else {
    where_clause <- query
  }


  if(tolower(srctype) != 'raster') {

    if(tolower(srctype) == 'oracle'){
      oraServer <- oraConnList["server"][[1]]
      idir      <- oraConnList["user"][[1]]
      orapass   <- oraConnList["password"][[1]]
      outServerName <- 'oradb'

      src_schema_name <- strsplit(srclyr, "\\.")[[1]][[1]]
      src_table_name  <- strsplit(srclyr, "\\.")[[1]][[2]]

      fdw_schema_name <- fdwSchema
      fdw_table_name  <- strsplit(srclyr, "\\.")[[1]][[2]]
      ## Create a FDW table in PG
      fklyr <- createOracleFDWpg(srclyr, oraConnList, connList, outServerName, fdw_schema_name)

      connz <- dbConnect(connList["driver"][[1]],
                       host     = connList["host"][[1]],
                       user     = connList["user"][[1]],
                       dbname   = connList["dbname"][[1]],
                       password = connList["password"][[1]],
                       port     = connList["port"][[1]])
      on.exit(RPostgres::dbDisconnect(connz))

      # =============================================================================
      # Create Non Spatial Table with attributes from FDW
      # =============================================================================
      print(glue("Creating non-spatial PG table: {dst_schema_name}.{dst_table_name} from FDW table: {fdw_schema_name}.{fdw_table_name} to PG"))
      faibDataManagement::fdwTbl2PGnoSpatial(
        foreignTable  = src_table_name,
        outTblName    = dst_table_name,
        pk            = pk,
        outSchema     = dst_schema_name,
        connList      = connList,
        attr2keep     = flds2keep,
        where         = query,
        table_comment = dest_table_comment
      )
      print("Table created successfully.")
      # =============================================================================
      # Create GR SKEY Spatial Table
      # =============================================================================

      ## Create a SQL query including the spatial field and primary key field of a Foreign Data Wrapper table
      qry <- getFDWtblSpSQL(dst_table_name  = dst_table_name,
                            dst_schema_name = dst_schema_name,
                            oratable        = srclyr,
                            pk              = pk,
                            connList        = connList,
                            fdwSchema       = fdw_schema_name,
                            where           = query
      )

      castList <- c("MULTIPOLYGON","MULTIPOINT","MULTILINE")
      print(glue('Converting SQL query which joins FDW table: {fdw_schema_name}.{fdw_table_name} and {dst_schema_name}.{dst_table_name} and WHERE query into R Simple Feature Collection'))
      for (i in castList) {
        #ERROR HANDLING
        possibleError <- tryCatch(
          inSF <- st_cast(st_read(connz, query = qry, crs = 3005),i ),
          error=function(e) e
        )
        if(inherits(possibleError, "error")) next else { break }
      }
      ##################### Rasterize using TERRA #########
      outTifName <- glue("{nsTblm}.tif")
      inRas <- rasterizeTerra(
        inSrc      = inSF,
        field      = "fid",
        template   = grskeyTIF,
        cropExtent = cropExtent,
        outTifpath = outTifpath,
        outTifname = outTifName,
        datatype   = 'INT4U',
        nodata     = 0
      )

      inSrcTemp <- NULL
      pgConnTemp <- connList
    } else {
      # =============================================================================
      # Create Non Spatial Table with attributes from FDW
      # =============================================================================
      print(glue("Creating non-spatial PG table: {dst_schema_name}.{dst_table_name} from FDW table: {fdw_schema_name}.{fdw_table_name} to PG"))
      faibDataManagement::fdwTbl2PGnoSpatial(
        foreignTable  = src_table_name,
        outTblName    = dst_table_name,
        pk            = pk,
        outSchema     = dst_schema_name,
        connList      = connList,
        attr2keep     = flds2keep,
        where         = query,
        table_comment = dest_table_comment
      )
      print("Table created successfully.")


      # =============================================================================
      # Create Non Spatial Table with attributes from FDW
      # ============================================================================
      fklyr <- srclyr
      print(glue("Write Non-spatial table: {fklyr} to PG"))
      faibDataManagement::writeNoSpaTbl2PG(
                                            srcpath,
                                            nsTblm,
                                            connList,
                                            pk     = pk,
                                            schema = wrkSchema,
                                            lyr    = srclyr,
                                            where  = query,
                                            select = flds2keep
      )
      inSrcTemp <- srcpath
      pgConnTemp <- NULL
      print(glue("Wrote Non-spatial table: {fklyr} to PG"))

      #Create tif from input
      outTifName <- glue("{nsTblm}.tif")
      print(glue("Writing raster: {outTifName}"))
      inRas <- rasterizeWithGdal(
                                  fklyr,
                                  pk,
                                  outTifpath = outTifpath,
                                  outTifname = outTifName,
                                  inSrc      = inSrcTemp,
                                  pgConnList = pgConnTemp,
                                  vecExtent  = cropExtent,
                                  nodata     = 0,
                                  where      = where_claus
      )
      print(glue("Raster created successfully."))
    }
  }

  if(tolower(srctype) == 'raster') {
    outTifName <- glue("{nsTblm}.tif")
    inRas <- srcpath
  }

  inRasbase <- basename(inRas)
  ## Tif to PG Raster
  if(importrast2pg) {
    pgRasName <- paste0(rasSchema, '.ras_', substr(inRasbase, 1, nchar(inRasbase)-4))
    cmd <- glue('raster2pgsql -s 3005 -d -C -r -P -I -M -t 100x100 {inRas} {pgRasName} | psql -d prov_data')
    print(cmd)
    shell(cmd)
    print('Imported tif to PG')
  }

  # #Convert postgres raster to Non spatial table with gr_skey
  joinTbl2 <- RPostgres::Id(schema = dst_schema_name, table = dst_gr_skey_table_name)
  print(glue('Creating PG table: {dst_schema_name}.{dst_gr_skey_table_name} from values in tif and gr_skey'))
  faibDataManagement::sendSQLstatement(glue("DROP TABLE IF EXISTS {dst_schema_name}.{dst_gr_skey_table_name};"), connList)
  faibDataManagement::df2PG(
                            joinTbl2,
                            faibDataManagement::tif2grskeytbl(
                                                              inRas,
                                                              cropExtent   = cropExtent,
                                                              grskeyTIF    = grskeyTIF,
                                                              maskTif      = maskTif,
                                                              valueColName = "fid"
                            ),
                            connList
  )
  print(glue('Created PG table: {dst_schema_name}.{dst_gr_skey_table_name} from values in tif and gr_skey'))

  ## Adding primary key to table
  print(glue('Adding gr_skey as primary key to {dst_schema_name}.{dst_gr_skey_table_name}'))
  faibDataManagement::sendSQLstatement(glue("ALTER TABLE {dst_schema_name}.{dst_gr_skey_table_name} ADD PRIMARY KEY (gr_skey);"), connList)
  ## Adding in foreign key
  print(glue('Adding foreign key constraint to {dst_schema_name}.{dst_gr_skey_table_name} referencing {dst_schema_name}.{dst_table_name} using (fid);'))
  faibDataManagement::sendSQLstatement(glue("ALTER TABLE {dst_schema_name}.{dst_gr_skey_table_name} ADD CONSTRAINT {dst_gr_skey_table_name}_fkey FOREIGN KEY (fid) REFERENCES {dst_schema_name}.{dst_table_name} (fid);"), connList)
  ## Add index on fid
  faibDataManagement::sendSQLstatement(glue("DROP INDEX IF EXISTS {dst_gr_skey_table_name}_fid_idx;"), connList)
  faibDataManagement::sendSQLstatement(glue("CREATE INDEX {dst_gr_skey_table_name}_fid_idx ON {dst_schema_name}.{dst_gr_skey_table_name} USING btree (fid)"), connList)
  ## Add comment on table
  faibDataManagement::sendSQLstatement(dest_gr_skey_table_comment, connList)
  ## Update the query planner with the latest changes
  faibDataManagement::sendSQLstatement(glue("ANALYZE {dst_schema_name}.{dst_gr_skey_table_name};"),connList)

  if(rslt_ind == 1) {
    gr_skey_tbl <- glue("{dst_schema_name}.{dst_gr_skey_table_name}")
    faibDataManagement::updateFKlookupPG(
                                          joinTbl,
                                          pk,
                                          suffix,
                                          gr_skey_tbl,
                                          connList
    )
    print("Created new foreign key lookup table")

    #Update Metadata tables
    faibDataManagement::updateFKfldTablePG(
                                          nsTblm,
                                          gr_skey_tbl,
                                          suffix,
                                          connList
    )

    srcpath <- gsub("[[:space:]]",'',srcpath)
    print('srcpath - 1')

    faibDataManagement::updateFKsrcTblpg(
                                          dataSourceTblName,
                                          srctype,
                                          srcpath,
                                          srclyr,
                                          pk,
                                          suffix,
                                          nsTblm,
                                          query,
                                          1,
                                          rslt_ind,
                                          flds2keep,
                                          connList
    )

    print("Updated data sources table")
    faibDataManagement::sendSQLstatement(paste0("drop table if exists ", joinTbl, ";"), connList)
    faibDataManagement::sendSQLstatement(paste0("drop table if exists ", gr_skey_tbl, "_old;"), connList)
    print("Deleted excess tables")
    faibDataManagement::sendSQLstatement(glue("ANALYZE {gr_skey_tbl};"), connList)
  }
}

