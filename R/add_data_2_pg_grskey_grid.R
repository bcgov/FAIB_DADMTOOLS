#' Update FAIB hectares database from input dataset
#'
#'
#'
#' @param rslt_ind Used in combination with gr_skey_tbl and suffix arguments. 1 = include (i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
#' @param srctype Format of data source, options: gdb, oracle, postgres, geopackage, raster, shp
#' @param srcpath Path to input data. Note use bcgw for whse oracle layers
#' @param srclyr Input layer name
#' @param suffix Use in combination with rslt_ind and gr_skey_tbl. Suffix to be added to the primary key field name in the gr_skey table
#' @param nsTblm Name of output non spatial table
#' @param query Where clause used to filter input dataset, Eg. fire_year = 2023 and BURN_SEVERITY_RATING in ('High','Medium')
#' @param flds2keep Fields to keep in non spatial table, Eg. fid,tsa_number,thlb_fact,tsr_report_year,abt_name
#' @param connList Keyring object of Postgres credentials
#' @param cropExtent Raster crop extent, list of c(ymin, ymax, xmin, xmax) in EPSG:3005
#' @param gr_skey_tbl Pre-existing table that contains gr_skey field
#' @param wrkSchema Schema to insert the new gr_skey and non spatial table
#' @param rasSchema If importrast2pg set to TRUE, the import schema for the raster
#' @param fdwSchema If srctype=oracle, the schema to load the foreign data wrapper table
#' @param grskeyTIF The file path to the gr_skey geotiff
#' @param maskTif The file path to the geotiff to be used as a mask
#' @param dataSourceTblName coming soon
#' @param outTifpath Directory where output tif if exported and where vector is temporally stored prior to import
#' @param importrast2pg If TRUE, raster is imported into database in rasSchema
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
                                      maskTif='S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                                      dataSourceTblName = 'data_sources',
                                      outTifpath = 'D:\\Projects\\provDataProject',
                                      importrast2pg = FALSE
)

{
  print(paste(rep("*", 80), collapse=""))
  print(glue("Importing {srcpath} | layername={srclyr}..."))
  print(paste(rep("*", 80), collapse=""))
  ## Get inputs from input file
  rslt_ind  <- gsub("[[:space:]]",'',tolower(rslt_ind)) ## 1 = include (i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
  srctype   <- gsub("[[:space:]]",'',tolower(srctype)) ## format of data source i.e. gdb, oracle, postgres, geopackage, raster, shp
  srcpath   <- gsub("[[:space:]]",'',tolower(srcpath))## path to input data. Note use bcgw for whse
  srclyr    <- gsub("[[:space:]]",'',tolower(srclyr)) ## input layer name
  suffix    <- gsub("[[:space:]]",'',tolower(suffix)) ## suffix to be used in the resultant table
  nsTblm    <- gsub("[[:space:]]",'',tolower(nsTblm)) ## name of output non spatial table
  query     <- query  ## where clause used to filter input dataset
  flds2keep <- gsub("[[:space:]]",'',tolower(flds2keep)) ## fields to keep in non spatial table
  dataSourceTblName <- glue::glue("{wrkSchema}.{dataSourceTblName}")

  dst_schema_name <- wrkSchema ## destination schema name
  dst_table_name  <- nsTblm ## destination table name
  dst_gr_skey_table_name <- glue("{nsTblm}_gr_skey") ## destination table gr skey name
  pk_id = "faib_fid"
  no_data_value = 0
  today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
  dest_table_comment <- glue("COMMENT ON TABLE {dst_schema_name}.{dst_table_name} IS 'Table created by the FAIB_DATA_MANAGEMENT R package at {today_date}.
                                            TABLE relates to {dst_schema_name}.{dst_gr_skey_table_name}
                                            Data source details:
                                            Source type: {srctype}
                                            Source path: {srcpath}
                                            Source layer: {srclyr}
                                            Source where query: {query}';")
  dest_gr_skey_table_comment <- glue("COMMENT ON TABLE {dst_schema_name}.{dst_gr_skey_table_name} IS 'Table created by the FAIB_DATA_MANAGEMENT R package at {today_date}.
                                            TABLE relates to {dst_schema_name}.{dst_table_name}
                                            Data source details:
                                            Source type: {srctype}
                                            Source path: {srcpath}
                                            Source layer: {srclyr}
                                            Source where query: {query}';")
  ## convert whitespace to null when where clause is null
  if (is_blank(query)) {
    where_claus <- NULL
    query <- ''
  } else {
    where_claus <- query
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
      print(glue("Creating non-spatial PG table: {dst_schema_name}.{dst_table_name} from FDW table: {fdw_schema_name}.{fdw_table_name}"))
      faibDataManagement::fdwTbl2PGnoSpatial(
        foreignTable  = src_table_name,
        outTblName    = dst_table_name,
        pk            = pk_id,
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
                            pk              = pk_id,
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
        if(inherits(possibleError, "error")) next else {
          break
        }
      }
      ## Rasterize using TERRA
      outTifName <- glue("{nsTblm}.tif")
      inRas <- rasterizeTerra(
        inSrc      = inSF,
        field      = pk_id,
        template   = grskeyTIF,
        cropExtent = cropExtent,
        outTifpath = outTifpath,
        outTifname = outTifName,
        datatype   = 'INT4U',
        nodata     = no_data_value
      )

      inSrcTemp <- NULL
      pgConnTemp <- connList
    } else {
      # =============================================================================
      # Copy layer to local directory.
      # This is done as the created ID field is created on the fly first for ogr2ogr
      # and second for the gdal_rasterize command. There is a small chance that the
      # file will be modified between the first & second command which will impact
      # change the order of the on the fly ID field creation. To prevent this, the
      # file is copied to a outTifpath directory.
      # =============================================================================
      print(glue("Copying {srcpath} to {outTifpath}"))
      if (srctype %in% c("shapefile", "shp")) {
        files <- list.files(path = dirname(srcpath), pattern = paste0("^", tools::file_path_sans_ext(basename(srcpath)), "\\..*"), ignore.case = TRUE)
        for (file in files) {
          file.copy(file.path(dirname(srcpath), file), file.path(outTifpath, file), overwrite = TRUE)
        }
      } else {
        file.copy(from = srcpath, to = outTifpath, overwrite = TRUE, recursive = TRUE, copy.mode = TRUE)
      }

      old_src_path <- srcpath
      srcpath <- file.path(outTifpath, basename(srcpath))

      # =============================================================================
      # Create Non Spatial Table with attributes from FDW
      # =============================================================================
      print(glue("Creating non-spatial PG table: {dst_schema_name}.{dst_table_name} from source: {srcpath}|layername={srclyr}"))
      faibDataManagement::writeNoSpaTbl2PG(
                                            src           = srcpath,
                                            outTblName    = dst_table_name,
                                            connList      = connList,
                                            pk            = pk_id,
                                            outSchema     = dst_schema_name,
                                            lyr           = srclyr,
                                            where         = query,
                                            select        = flds2keep,
                                            table_comment = dest_table_comment
      )

      print("Table created successfully.")
      #Create tif from input
      outTifName <- glue("{nsTblm}.tif")
      inRas <- rasterizeWithGdal(
                                  inlyr      = srclyr,
                                  field      = pk_id,
                                  outTifpath = outTifpath,
                                  outTifname = outTifName,
                                  inSrc      = srcpath,
                                  pgConnList = NULL,
                                  vecExtent  = cropExtent,
                                  nodata     = no_data_value,
                                  where      = where_claus
      )
      # =============================================================================
      # Remove locally created layer from outTifpath
      # =============================================================================
      print(glue("Removing {basename(srcpath)} from {outTifpath}"))

      if (srctype %in% c("shapefile", "shp")) {
        files <- list.files(path = dirname(srcpath), pattern = paste0("^", tools::file_path_sans_ext(basename(srcpath)), "\\..*"), ignore.case = TRUE)
        for (file in files) {
          file.remove(file.path(outTifpath, file))
        }
      } else if (srctype == 'gdb') {
        ## file.remove consistently ran into permission issues when ran on a gdb
        ## switched to gdalmanage
        print(system2('gdalmanage',args=c("delete", srcpath), stderr = TRUE))
      } else {
        file.remove(srcpath, recursive = TRUE)
      }

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
    cmd <- glue('raster2pgsql -s 3005 -d -C -r -P -I -M -N {no_data_value} -t 100x100 {inRas} {pgRasName} | psql -d prov_data')
    print(cmd)
    shell(cmd)
    print('Imported tif to PG')
  }

  # #Convert postgres raster to Non spatial table with gr_skey
  joinTbl2 <- RPostgres::Id(schema = dst_schema_name, table = dst_gr_skey_table_name)
  print(glue('Creating PG table: {dst_schema_name}.{dst_gr_skey_table_name} from values in tif and gr_skey'))
  faibDataManagement::sendSQLstatement(glue("DROP TABLE IF EXISTS {dst_schema_name}.{dst_gr_skey_table_name};"), connList)
  if (tolower(srctype) == 'raster') {
    band_field_name <- 'val'
  } else {
    band_field_name <- pk_id
  }
  faibDataManagement::df2PG(
                            joinTbl2,
                            faibDataManagement::tif2grskeytbl(
                                                              inRas,
                                                              cropExtent   = cropExtent,
                                                              grskeyTIF    = grskeyTIF,
                                                              maskTif      = maskTif,
                                                              valueColName = band_field_name
                            ),
                            connList
  )
  print(glue('Created PG table: {dst_schema_name}.{dst_gr_skey_table_name} from values in tif and gr_skey'))

  ## Adding primary key to table
  print(glue('Adding gr_skey as primary key to {dst_schema_name}.{dst_gr_skey_table_name}'))
  faibDataManagement::sendSQLstatement(glue("ALTER TABLE {dst_schema_name}.{dst_gr_skey_table_name} ADD PRIMARY KEY (gr_skey);"), connList)

  if (tolower(srctype) != 'raster') {
    ## Adding in foreign key
    print(glue('Adding foreign key constraint to {dst_schema_name}.{dst_gr_skey_table_name} referencing {dst_schema_name}.{dst_table_name} using ({pk_id});'))
    faibDataManagement::sendSQLstatement(glue("ALTER TABLE {dst_schema_name}.{dst_gr_skey_table_name} ADD CONSTRAINT {dst_gr_skey_table_name}_fkey FOREIGN KEY ({pk_id}) REFERENCES {dst_schema_name}.{dst_table_name} ({pk_id});"), connList)
    ## Add index on fid
    faibDataManagement::sendSQLstatement(glue("DROP INDEX IF EXISTS {dst_gr_skey_table_name}_{pk_id}_idx;"), connList)
    faibDataManagement::sendSQLstatement(glue("CREATE INDEX {dst_gr_skey_table_name}_{pk_id}_idx ON {dst_schema_name}.{dst_gr_skey_table_name} USING btree ({pk_id})"), connList)

  }
  ## Add comment on table
  faibDataManagement::sendSQLstatement(dest_gr_skey_table_comment, connList)
  ## Update the query planner with the latest changes
  faibDataManagement::sendSQLstatement(glue("ANALYZE {dst_schema_name}.{dst_gr_skey_table_name};"),connList)

  if(rslt_ind == 1) {
    faibDataManagement::updateFKlookupPG(
                                          inLayer     = glue("{dst_schema_name}.{dst_gr_skey_table_name}"),
                                          pk          = pk_id,
                                          suffix      = suffix,
                                          fkTBlName   = gr_skey_tbl,
                                          connList    = connList
    )



    #Update Metadata tables
    faibDataManagement::updateFKfldTablePG(
                                          glue("{dst_schema_name}.{dst_table_name}"),
                                          gr_skey_tbl,
                                          suffix,
                                          connList
    )

  }

  faibDataManagement::updateFKsrcTblpg(
    outTableName = dataSourceTblName,
    srctype      = srctype,
    srcpath      = srcpath,
    srclyr       = srclyr,
    pk           = pk_id,
    suffix       = suffix,
    nsTblm       = nsTblm,
    query        = query,
    rslt_ind     = rslt_ind,
    fields2keep  = flds2keep,
    schema       = wrkSchema,
    connList     = connList
  )
}

