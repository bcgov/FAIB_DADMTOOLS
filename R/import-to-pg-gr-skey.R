#' Update FAIB hectares database from input dataset
#'
#'
#'
#' @param rslt_ind Used in combination with gr_skey_tbl and suffix arguments. 1 = include (i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
#' @param src_type Format of data source, Options: gdb, oracle, postgres, geopackage, raster, shp
#' @param src_path Path to input data. Note use "bcgw" for whse oracle layers
#' @param src_lyr Input layer name
#' @param suffix Use in combination with rslt_ind and gr_skey_tbl. Suffix to be added to the primary key field name in the gr_skey table
#' @param dst_tbl Name of output non spatial table
#' @param query Where clause used to filter input dataset, Eg. fire_year = 2023 and BURN_SEVERITY_RATING in ('High','Medium')
#' @param flds_to_keep Fields to keep in non spatial table, Eg. fid,tsa_number,thlb_fact,tsr_report_year,abt_name
#' @param notes Notes field
#' @param out_tif_path Directory where output tif if exported and where vector is temporally stored prior to import
#' @param pg_conn_param Keyring object of Postgres credentials
#' @param ora_conn_param Keyring object of Oracle credentials
#' @param crop_extent Raster crop extent, list of c(ymin, ymax, xmin, xmax) in EPSG:3005
#' @param gr_skey_tbl Used in combination with Schema and table name of the pre-existing foreign key lookup gr_skey table. Argument to be used with suffix and rslt_ind within in_csv.
#' @param dst_schema Schema to insert the newly created gr_skey and dst_tbl (Eg. non spatial table)
#' @param raster_schema If import_rast_to_pg set to TRUE, the import schema for the raster. Defaults to "raster"
#' @param fdw_schema If src_type=oracle, the schema to load the foreign data wrapper table, Defaults to "load"
#' @param template_tif The file path to the gr_skey template geotiff
#' @param mask_tif The file path to the geotiff to be used as a mask
#' @param data_src_tbl Schema and table name of the metadata table in postgres that updates with any newly imported layer
#' @param import_rast_to_pg If TRUE, raster is imported into database in raster_schema
#'
#'
#' @return no return
#' @export
#'
#' @examples coming soon


import_to_pg_gr_skey <- function(rslt_ind,
                                src_type,
                                src_path,
                                src_lyr,
                                suffix,
                                dst_tbl,
                                query,
                                flds_to_keep,
                                notes,
                                out_tif_path,
                                pg_conn_param     = dadmtools::get_pg_conn_list(),
                                ora_conn_param    = dadmtools::get_ora_conn_list(),
                                crop_extent       = c(273287.5, 1870587.5, 367787.5, 1735787.5),
                                gr_skey_tbl       = 'whse.all_bc_gr_skey',
                                dst_schema        = 'whse',
                                raster_schema     = 'raster',
                                fdw_schema        = 'load',
                                template_tif      = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
                                mask_tif          = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                                data_src_tbl      = 'whse.data_sources',
                                import_rast_to_pg = FALSE
)

{
  cat("\n")
  cat(paste(rep("*", 80), collapse = ""), "\n")
  print(glue("Starting import of {src_path} | layername={src_lyr}..."))
  cat(paste(rep("*", 80), collapse = ""), "\n")
  cat("\n")
  ## Get inputs from input file
  rslt_ind     <- gsub("[[:space:]]",'',tolower(rslt_ind)) ## 1 = include (i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
  src_type     <- gsub("[[:space:]]",'',tolower(src_type)) ## format of data source i.e. gdb, oracle, postgres, geopackage, raster, shp
  src_path     <- gsub("[[:space:]]",'',tolower(src_path))## path to input data. Note use bcgw for whse
  src_lyr      <- gsub("[[:space:]]",'',tolower(src_lyr)) ## input layer name
  suffix       <- gsub("[[:space:]]",'',tolower(suffix)) ## suffix to be used in the resultant table
  dst_tbl      <- gsub("[[:space:]]",'',tolower(dst_tbl)) ## name of output non spatial table
  flds_to_keep <- gsub("[[:space:]]",'',tolower(flds_to_keep)) ## fields to keep in non spatial table
  ## checks
  if (any(c(is_blank(src_type), is_blank(src_path), is_blank(src_lyr), is_blank(dst_tbl), is_blank(out_tif_path)))){
    print("ERROR: Argument not provided, one of src_type, src_path, src_lyr, dst_tbl, out_tif_path was left blank. Exiting script.")
    return()
  }

  if (!(src_type %in% c("gdb", "oracle", "postgres", "geopackage", "raster", "shp", "shapefile"))) {
    print(glue("ERROR: Invalid src_type: {src_type}. Hint, provide one of: gdb, oracle, postgres, geopackage, raster or shp. Exiting script."))
    return()
  }

  if (!(rslt_ind %in% c(0, 1))) {
    print(glue("ERROR: Invalid rslt_ind: {rslt_ind}. Hint, provide either 0 or 1. Exiting script."))
    return()
  }

  dst_gr_skey_tbl <- glue("{dst_tbl}_gr_skey") ## destination table gr skey name
  pk_id = "pgid"
  no_data_value = 0
  today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
  query_escaped <- gsub("'", "''", query)
  dst_tbl_comment <- glue("COMMENT ON TABLE {dst_schema}.{dst_tbl} IS 'Table created by the dadmtools R package at {today_date}.
                                            TABLE relates to {dst_schema}.{dst_gr_skey_tbl}
                                            Data source details:
                                            Source type: {src_type}
                                            Source path: {src_path}
                                            Source layer: {src_lyr}
                                            Source where query: {query_escaped}';")
  dst_gr_skey_tbl_comment <- glue("COMMENT ON TABLE {dst_schema}.{dst_gr_skey_tbl} IS 'Table created by the dadmtools R package at {today_date}.
                                            TABLE relates to {dst_schema}.{dst_tbl}
                                            Data source details:
                                            Source type: {src_type}
                                            Source path: {src_path}
                                            Source layer: {src_lyr}
                                            Source where query: {query_escaped}';")

  ## convert whitespace to null when where clause is null
  if (is_blank(query)) {
    where_claus <- NULL
    query <- ''
  } else {
    where_claus <- query
  }

  if(tolower(src_type) != 'raster') {

    if(tolower(src_type) == 'oracle'){
      src_schema <- strsplit(src_lyr, "\\.")[[1]][[1]]
      src_table  <- strsplit(src_lyr, "\\.")[[1]][[2]]

      fdw_tbl  <- strsplit(src_lyr, "\\.")[[1]][[2]]
      ## Create a FDW table in PG
      fklyr <- create_pg_fdw(src_lyr, ora_conn_param, pg_conn_param, 'oradb', fdw_schema)

      connz <- dbConnect(pg_conn_param["driver"][[1]],
                       host     = pg_conn_param["host"][[1]],
                       user     = pg_conn_param["user"][[1]],
                       dbname   = pg_conn_param["dbname"][[1]],
                       password = pg_conn_param["password"][[1]],
                       port     = pg_conn_param["port"][[1]])
      on.exit(RPostgres::dbDisconnect(connz))
      # =============================================================================
      # Create Non Spatial Table with attributes from FDW
      # =============================================================================
      print(glue("Creating non-spatial PG table: {dst_schema}.{dst_tbl} from FDW table: {fdw_schema}.{fdw_tbl}"))
      dadmtools::fdw_to_tbl(
        src_tbl       = fdw_tbl,
        src_schema    = fdw_schema,
        dst_tbl       = dst_tbl,
        dst_schema    = dst_schema,
        pk            = pk_id,
        pg_conn_param = pg_conn_param,
        flds_to_keep  = flds_to_keep,
        where         = query,
        tbl_comment   = dst_tbl_comment
      )
      print("Table created successfully.")

      # =============================================================================
      # Create GR SKEY Spatial Table
      # =============================================================================

      ## Create a SQL query including the spatial field and primary key field of a Foreign Data Wrapper table
      qry <- get_sql_fdw_id_geom(dst_tbl      = dst_tbl,
                                dst_schema    = dst_schema,
                                ora_tbl       = src_lyr,
                                pk            = pk_id,
                                pg_conn_param = pg_conn_param,
                                fdw_schema    = fdw_schema,
                                where         = query
      )

      cast_list <- c("MULTIPOLYGON","MULTIPOINT","MULTILINE")
      print(glue('Converting SQL query which joins FDW table: {fdw_schema}.{fdw_tbl} and {dst_schema}.{dst_tbl} and WHERE query into R Simple Feature Collection'))
      for (i in cast_list) {
        #ERROR HANDLING
        possible_error <- tryCatch(
          in_sf <- st_cast(st_read(connz, query = qry, crs = 3005),i ),
          error=function(e) e
        )
        if(inherits(possible_error, "error")) next else {
          break
        }
      }

      ## Rasterize using TERRA
      dst_ras_filename <- rasterize_terra(
        src_sf       = in_sf,
        field        = pk_id,
        template_tif = template_tif,
        crop_extent  = crop_extent,
        out_tif_path = out_tif_path,
        out_tif_name = glue("{dst_tbl}.tif"),
        datatype     = 'INT4U',
        nodata       = no_data_value
      )
      old_src_path <- src_path
    } else {
      # =============================================================================
      # Copy layer to local directory.
      # This is done as the created ID field is created on the fly first for ogr2ogr
      # and second for the gdal_rasterize command. There is a small chance that the
      # file will be modified between the first & second command which will impact
      # change the order of the on the fly ID field creation. To prevent this, the
      # file is copied to a out_tif_path directory.
      # =============================================================================
      print(glue("Copying {src_path} to {out_tif_path}"))
      if (dirname(src_path) != dirname(out_tif_path)) {
        if (src_type %in% c("shapefile", "shp")) {
          files <- list.files(path = dirname(src_path), pattern = paste0("^", tools::file_path_sans_ext(basename(src_path)), "\\..*"), ignore.case = TRUE)
          for (file in files) {
            file.copy(file.path(dirname(src_path), file), file.path(out_tif_path, file), overwrite = TRUE)
          }
        } else {
          file.copy(from = src_path, to = out_tif_path, overwrite = TRUE, recursive = TRUE, copy.mode = TRUE)
        }
      }

      old_src_path <- src_path
      src_path <- file.path(out_tif_path, basename(src_path))

      # =============================================================================
      # Create Non Spatial Table with attributes from FDW
      # =============================================================================
      print(glue("Creating non-spatial PG table: {dst_schema}.{dst_tbl} from source: {src_path}|layername={src_lyr}"))
      dadmtools::ogr_to_tbl(
                                      src           = src_path,
                                      dst_tbl       = dst_tbl,
                                      pg_conn_param = pg_conn_param,
                                      lyr           = src_lyr,
                                      pk            = pk_id,
                                      select        = flds_to_keep,
                                      where         = query,
                                      dst_schema    = dst_schema,
                                      tbl_comment   = dst_tbl_comment
      )

      print("Table created successfully.")
      #Create tif from input
      dst_ras_filename <- rasterize_gdal(
                                  in_lyr        = src_lyr,
                                  field         = pk_id,
                                  out_tif_path  = out_tif_path,
                                  out_tif_name  = glue("{dst_tbl}.tif"),
                                  src_path      = src_path,
                                  pg_conn_param = NULL,
                                  crop_extent   = crop_extent,
                                  nodata        = no_data_value,
                                  where         = where_claus
      )
      # =============================================================================
      # Remove locally created layer from out_tif_path
      # =============================================================================
      print(glue("Removing {basename(src_path)} from {out_tif_path}"))

      if (src_type %in% c("shapefile", "shp")) {
        files <- list.files(path = dirname(src_path), pattern = paste0("^", tools::file_path_sans_ext(basename(src_path)), "\\..*"), ignore.case = TRUE)
        for (file in files) {
          file.remove(file.path(out_tif_path, file))
        }
      } else if (src_type == 'gdb') {
        ## file.remove consistently ran into permission issues when ran on a gdb
        ## switched to gdalmanage
        print(system2('gdalmanage',args=c("delete", src_path), stderr = TRUE))
      } else {
        file.remove(src_path, recursive = TRUE)
      }

    }
  }

  if(tolower(src_type) == 'raster') {
    if (dirname(src_path) != dirname(out_tif_path)) {
      print(glue("Copying {src_path} to {out_tif_path}"))
      file.copy(from = src_path, to = out_tif_path, overwrite = TRUE, recursive = TRUE, copy.mode = TRUE)
    }
    old_src_path <- src_path
    dst_ras_filename <- file.path(out_tif_path, basename(src_path))
  }


  ## Tif to PG Raster
  if(import_rast_to_pg) {
    raster_tbl <- glue("{raster_schema}.ras_{substr(basename(dst_ras_filename),1,nchar(basename(dst_ras_filename))-4)}")
    cmd <- glue('raster2pgsql -s 3005 -d -C -r -P -I -M -N {no_data_value} -t 100x100 {dst_ras_filename} {raster_tbl} | psql -d prov_data')
    print(cmd)
    shell(cmd)
    print('Imported tif to PG')
  }

  #Convert postgres raster to non-spatial table with gr_skey
  dst_gr_skey_tbl_pg_obj <- RPostgres::Id(schema = dst_schema, table = dst_gr_skey_tbl)
  print(glue('Creating PG table: {dst_schema}.{dst_gr_skey_tbl} from values in tif and gr_skey'))
  dadmtools::run_sql_r(glue("DROP TABLE IF EXISTS {dst_schema}.{dst_gr_skey_tbl};"), pg_conn_param)
  if (tolower(src_type) == 'raster') {
    band_field_name <- 'val'
  } else {
    band_field_name <- pk_id
  }
  dadmtools::df_to_pg(
                            pg_tbl = dst_gr_skey_tbl_pg_obj,
                            in_df  = dadmtools::tif_to_gr_skey_tbl(
                                                              src_tif_filename = dst_ras_filename,
                                                              crop_extent      = crop_extent,
                                                              template_tif     = template_tif,
                                                              mask_tif         = mask_tif,
                                                              val_field_name   = band_field_name
                            ),
                            pg_conn_param = pg_conn_param
  )
  print(glue('Created PG table: {dst_schema}.{dst_gr_skey_tbl} from values in tif and gr_skey'))
  if(tolower(src_type) == 'raster') {
    if (dirname(src_path) != dirname(out_tif_path)) {
      print(glue("Removing {basename(src_path)} from {out_tif_path}"))
      file.remove(dst_ras_filename)
    }
  }

  ## Adding primary key to table
  print(glue('Adding gr_skey as primary key to {dst_schema}.{dst_gr_skey_tbl}'))
  dadmtools::run_sql_r(glue("ALTER TABLE {dst_schema}.{dst_gr_skey_tbl} ADD PRIMARY KEY (gr_skey);"), pg_conn_param)

  if (tolower(src_type) != 'raster') {
    ## Adding in foreign key
    print(glue('Adding foreign key constraint to {dst_schema}.{dst_gr_skey_tbl} referencing {dst_schema}.{dst_tbl} using ({pk_id});'))
    dadmtools::run_sql_r(glue("ALTER TABLE {dst_schema}.{dst_gr_skey_tbl} ADD CONSTRAINT {dst_gr_skey_tbl}_fkey FOREIGN KEY ({pk_id}) REFERENCES {dst_schema}.{dst_tbl} ({pk_id});"), pg_conn_param)
    ## Add index on fid
    dadmtools::run_sql_r(glue("DROP INDEX IF EXISTS {dst_gr_skey_tbl}_{pk_id}_idx;"), pg_conn_param)
    dadmtools::run_sql_r(glue("CREATE INDEX {dst_gr_skey_tbl}_{pk_id}_idx ON {dst_schema}.{dst_gr_skey_tbl} USING btree ({pk_id})"), pg_conn_param)
  }
  ## Add comment on table
  dadmtools::run_sql_r(dst_gr_skey_tbl_comment, pg_conn_param)
  ## Update the query planner with the latest changes
  dadmtools::run_sql_r(glue("ANALYZE {dst_schema}.{dst_gr_skey_tbl};"), pg_conn_param)

  if(rslt_ind == 1) {
    if (!(is_blank(suffix))) {
      dadmtools::add_pk_to_gr_skey_tbl(dst_tbl      = dst_gr_skey_tbl,
                                            dst_schema    = dst_schema,
                                            pk            = pk_id,
                                            suffix        = suffix,
                                            gr_skey_tbl   = gr_skey_tbl,
                                            pg_conn_param = pg_conn_param
      )



      #Update Metadata tables
      dadmtools::update_gr_skey_tbl_flds(dst_tbl      = dst_tbl,
                                              dst_schema    = dst_schema,
                                              gr_skey_tbl   = gr_skey_tbl,
                                              suffix        = suffix,
                                              pg_conn_param = pg_conn_param
      )
    } else {
      print("No suffix provided, skipping performing rslt_ind foreign key lookup table update.")
    }

  }
  dadmtools::update_pg_metadata_tbl(
    data_src_tbl    = data_src_tbl,
    src_type        = src_type,
    src_path        = old_src_path,
    src_lyr         = src_lyr,
    pk              = pk_id,
    suffix          = suffix,
    query           = query,
    rslt_ind        = rslt_ind,
    flds_to_keep    = flds_to_keep,
    notes           = notes,
    dst_tbl         = dst_tbl,
    dst_schema      = dst_schema,
    pg_conn_param   = pg_conn_param
  )
}

