#' Imports spatial data into a gridded attribute format within PostgreSQL, structuring data according to the gr_skey grid system.
#'
#'Function supports both vector and raster inputs. For vector data (e.g., Shapefile, FGDB, GeoPackage), the function imports the attribute table into PostgreSQL and generates a corresponding raster attribute table, where each record represents a raster pixel (one hectare). The gr_skey field acts as a globally unique primary key, while pgid links the raster attributes to the vector attributes. Each record within the raster attribute table represents one pixel.
#'For raster data (e.g., geotiff), the raster is required to have BC Albers coordinate reference system, (Ie. EPSG: 3005), the same grid definition as the 'template_tif' and 'mask_tif' provided to the 'import_gr_skey_tif_to_pg_rast' function and only one band. For TSR, it is recommended to use the gr_skey grid. The function imports the raster as a single attribute table. The table includes 'gr_skey', unique global cell id and the input raster pixel value. Each record represent one pixel.
#'
#'
#'
#' @param src_type Format of data source. Raster option will only work when the raster matches the spatial resolution, alignment and projection (BC Albers) of the gr_skey grid (specified by 'template_tif' argument) imported using 'import_gr_skey_tif_to_pg_rast'. Options: gdb, oracle, raster, geopackage, gpkg, shapefile, shp
#' @param src_path Path to input data. Note: use 'bcgw' when srctype = 'oracle' for whse oracle layers. Use full path and filename when srctype = 'gdb' or 'raster' or 'shp' or 'geopackage'
#' @param src_lyr Input layer name. Note: provide oracle schema and layer name, e.g. 'WHSE_FOREST_VEGETATION.bec_biogeoclimatic_poly' when src_type = 'oracle'. Provide layername within the file geodatabase, e.g. 'tsa_boundaries_2020' when src_type = 'gdb'. Provide the shapefile name without extension, e.g. 'k3o_cfa' when src_type = 'shp' or 'shapefile'. Provide the layername within the geopackage, e.g. 'FireSeverity_Final' when 'src_type = 'gpkg' or 'geopackage'. Argument is not used in import when src_type = 'raster' - though it is imported into metadata table.
#' @param dst_tbl Name of imported non spatial table in PostgreSQL
#' @param query Optional argument to filter source layer. Where clause used to filter input dataset, e.g., fire_year = 2023 and BURN_SEVERITY_RATING in ('High','Medium').
#' @param flds_to_keep Optional argument. By default, all fields are retained. Use this argument to filter fields to keep in non spatial table, Eg. fid,tsa_number,thlb_fact,tsr_report_year,abt_name.
#' @param notes Optional argument. Notes field.
#' @param overlap_ind TRUE or FALSE.  If TRUE, it indicates that the input spatial layer has overlaps and imported <dst_tbl>_gr_skey table will duplicate gr_skey records where spatial overlaps occur.  If FALSE, spatial overlaps will be ignored (i.e only the higher pgid value will be kept when overlaps occur)
#' @param overlap_group_fields The field groupings that will be used to handle spatial overlaps. I.e. each unique combination of the specified fields will be rasterized separately.
#' @param out_tif_path Directory where output tif is exported and where vector is temporally stored prior to import
#' @param pg_conn_param Keyring object of Postgres credentials. Defaults to dadmtools::get_pg_conn_list()
#' @param ora_conn_param Keyring object of Oracle credentials. Defaults to dadmtools::get_ora_conn_list()
#' @param crop_extent Raster crop extent, list of c(xmin, xmax, ymin, ymax) in EPSG:3005. Defaults to c(273287.5, 1870587.5, 367787.5, 1735787.5)
#' @param dst_schema Schema to insert the newly created gr_skey and dst_tbl (Eg. non spatial table). Defaults to "whse"
#' @param raster_schema If import_rast_to_pg set to TRUE, the import schema for the raster. Defaults to "raster"
#' @param fdw_schema If src_type=oracle, the schema to load the foreign data wrapper table. Defaults to "load"
#' @param template_tif The file path to the gr_skey template geotiff. Defaults to 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif'
#' @param mask_tif The file path to the geotiff to be used as a mask. Defaults to 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif'
#' @param data_src_tbl Schema and table name of the metadata table in postgres that updates with any newly imported layer. Defaults to 'whse.datasources'
#' @param import_rast_to_pg If TRUE, raster is imported into database in raster_schema. Defaults to FALSE
#'
#'
#' @return no return
#' @export
#'
#' @examples
#' ## Example of importing the vector: whse_admin_boundaries.adm_nr_districts_sp
#' ## from BCGW into postgres database in gr_skey format:
#' library(dadmtools)
#' import_to_pg_gr_skey(
#'  src_type             = 'oracle',
#'  src_path             = 'bcgw',
#'  src_lyr              = 'whse_admin_boundaries.adm_nr_districts_sp',
#'  dst_tbl              = 'adm_nr_districts_sp',
#'  query                = '',
#'  flds_to_keep         = NA,
#'  notes                = '',
#'  overlap_ind          = FALSE,
#'  overlap_group_fields = '',
#'  out_tif_path         = 'C:\\projects\\output\\',
#'  pg_conn_param        = dadmtools::get_pg_conn_list(),
#'  ora_conn_param       = dadmtools::get_ora_conn_list(),
#'  crop_extent          = c(273287.5, 1870587.5, 367787.5, 1735787.5),
#'  dst_schema           = 'sandbox',
#'  raster_schema        = 'raster',
#'  fdw_schema           = 'load',
#'  template_tif         = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
#'  mask_tif             = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
#'  data_src_tbl         = 'sandbox.data_sources',
#'  import_rast_to_pg    = FALSE,
#'  grskey_schema        = NULL)
#'
#' ## look at a summary of the imported data
#' sql_to_df('SELECT adm.district_name, count(*) as ha FROM sandbox.adm_nr_districts_sp adm JOIN  sandbox.adm_nr_districts_sp_gr_skey adm_key ON adm.pgid = adm_key.pgid GROUP BY adm.district_name LIMIT 2', dadmtools::get_pg_conn_list())
#'#                             district_name      ha
#'1 100 Mile House Natural Resource District 1235763
#'2 Campbell River Natural Resource District 1473460
#'
#' ## Example of importing a vector with overlaps: whse_forest_vegetation.pest_infestation_poly
#' ## from BCGW into postgres database in gr_skey format. The following imports the
#' ## pest_infestation_poly with a filter:
#' ## WHERE PEST_SPECIES_CODE = 'IDW' AND CAPTURE_YEAR > 2019
#' ## overlap_group_fields = 'CAPTURE_YEAR'.
#' ## In other words, each unique capture year where species_code = 'IDW'
#' ## will be imported separately so overlaps in capture_year will be captured.
#'
#' library(dadmtools)
#' import_to_pg_gr_skey(
#'  src_type             = 'oracle',
#'  src_path             = 'bcgw',
#'  src_lyr              = 'whse_forest_vegetation.pest_infestation_poly',
#'  dst_tbl              = 'pest_infestation_poly',
#'  query                = "PEST_SPECIES_CODE = 'IDW' AND CAPTURE_YEAR > 2019",
#'  flds_to_keep         = 'PEST_SPECIES_CODE, PEST_SEVERITY_CODE, CAPTURE_YEAR',
#'  notes                = '',
#'  overlap_ind          = TRUE,
#'  overlap_group_fields = 'CAPTURE_YEAR',
#'  out_tif_path         = 'C:\\projects\\output\\',
#'  pg_conn_param        = dadmtools::get_pg_conn_list(),
#'  ora_conn_param       = dadmtools::get_ora_conn_list(),
#'  crop_extent          = c(273287.5, 1870587.5, 367787.5, 1735787.5),
#'  dst_schema           = 'sandbox',
#'  raster_schema        = 'raster',
#'  fdw_schema           = 'load',
#'  template_tif         = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
#'  mask_tif             = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
#'  data_src_tbl         = 'sandbox.data_sources',
#'  import_rast_to_pg    = FALSE,
#'  grskey_schema        = NULL)
#'
#' ## Example of querying the pest data within 100 Mile House Natural Resource District
#' ## for capture_year 2020-2024.
#'
#' query <- "SELECT
#' adm.district_name,
#' pest.capture_year,
#' pest.PEST_SPECIES_CODE,
#' pest.PEST_SEVERITY_CODE,
#' count(*) as ha
#' FROM
#' sandbox.pest_infestation_poly_gr_skey_overlap pest_key
#' JOIN sandbox.pest_infestation_poly pest on pest.pgid = pest_key.pgid
#' LEFT JOIN sandbox.adm_nr_districts_sp_gr_skey adm_key ON adm_key.gr_skey = pest_key.gr_skey
#' LEFT JOIN sandbox.adm_nr_districts_sp adm ON adm.pgid = adm_key.pgid
#' WHERE
#' 	adm.district_name = '100 Mile House Natural Resource District'
#' AND
#' 	pest.capture_year IN (2020, 2021, 2022, 2023, 2024)
#' GROUP BY
#' 	adm.district_name,
#' 	pest.capture_year,
#' 	pest.PEST_SPECIES_CODE,
#' 	pest.PEST_SEVERITY_CODE
#' ORDER BY
#' 	capture_year, count(*) DESC"
#'
#' sql_to_df(query, dadmtools::get_pg_conn_list())
#'
#'                              district_name capture_year pest_species_code pest_severity_code    ha
#'1  100 Mile House Natural Resource District         2020               IDW                  L     7
#'2  100 Mile House Natural Resource District         2021               IDW                  M 10443
#'3  100 Mile House Natural Resource District         2021               IDW                  L  2577
#'4  100 Mile House Natural Resource District         2022               IDW                  M 39705
#'5  100 Mile House Natural Resource District         2022               IDW                  L  4663
#'6  100 Mile House Natural Resource District         2022               IDW                  S    15
#'7  100 Mile House Natural Resource District         2023               IDW                  M 27305
#'8  100 Mile House Natural Resource District         2023               IDW                  L  7836
#'9  100 Mile House Natural Resource District         2023               IDW                  T  2036
#'10 100 Mile House Natural Resource District         2024               IDW                  L 89344
#'11 100 Mile House Natural Resource District         2024               IDW                  S 29418
#'12 100 Mile House Natural Resource District         2024               IDW                  M 23195
#'

import_to_pg_gr_skey <- function(
                                src_type,
                                src_path,
                                src_lyr,
                                dst_tbl,
                                query,
                                flds_to_keep,
                                notes,
                                overlap_ind,
                                overlap_group_fields,
                                out_tif_path,
                                pg_conn_param     = dadmtools::get_pg_conn_list(),
                                ora_conn_param    = dadmtools::get_ora_conn_list(),
                                crop_extent       = c(273287.5, 1870587.5, 367787.5, 1735787.5),
                                dst_schema        = 'whse',
                                raster_schema     = 'raster',
                                fdw_schema        = 'load',
                                template_tif      = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
                                mask_tif          = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
                                data_src_tbl      = 'whse.data_sources',
                                import_rast_to_pg = FALSE,
                                grskey_schema = NULL
)

{





  cat("\n")
  cat(paste(rep("*", 80), collapse = ""), "\n")
  print(glue("Starting import of {src_path} | layername={src_lyr}..."))
  cat(paste(rep("*", 80), collapse = ""), "\n")
  cat("\n")
  ## Get inputs from input file
  src_type     <- gsub("[[:space:]]",'',tolower(src_type)) ## format of data source i.e. gdb, oracle, postgres, geopackage, raster, shp
  src_path     <- gsub("[[:space:]]",'',tolower(src_path))## path to input data. Note use bcgw for whse
  src_lyr      <- gsub("[[:space:]]",'',tolower(src_lyr)) ## input layer name
  dst_tbl      <- gsub("[[:space:]]",'',tolower(dst_tbl)) ## name of output non spatial table
  dst_schema   <- gsub("[[:space:]]",'',tolower(dst_schema)) ## name of output non spatial table
  flds_to_keep <- gsub("[[:space:]]",'',tolower(flds_to_keep)) ## fields to keep in non spatial table
  overlap_group_fields <- gsub("[[:space:]]",'',tolower(overlap_group_fields)) ## field to group overlaps
  overlap_ind   <- as.logical(gsub("[[:space:]]","",toupper(overlap_ind))) # indicates whether overlaps will be group by group field (e.g. TRUE) or ignored (e.g.FALSE)
  ## checks
  if (any(c(is_blank(src_type), is_blank(src_path), is_blank(src_lyr), is_blank(dst_tbl), is_blank(out_tif_path),is_blank(overlap_ind)  ))){
    print("ERROR: Argument not provided, one of overlap_ind, src_type, src_path, src_lyr, dst_tbl, out_tif_path was left blank. Exiting script.")
    return()
  }

  if (!(src_type %in% c("gdb", "oracle", "postgres", "geopackage", "gpkg", "raster", "shp", "shapefile"))) {
    print(glue("ERROR: Invalid src_type: {src_type}. Hint, provide one of: gdb, oracle, postgres, geopackage, raster or shp. Exiting script."))
    return()
  }

  ## If user imported layers with overlap, print out the overlap field provided
  if (overlap_ind) {
    print(paste("Layer with overlap indicated by config_parameters.csv. Overlap field provided: ", overlap_group_fields))
  }

  if (overlap_ind && is_blank(overlap_group_fields)) {
    print(glue("ERROR: Argument not provided for overlap_group_fields while OVERLAP_IND is set to TRUE "))
    return()
  }

  ## if user did not specify grskey_schema, overwrite grskey_schema with dst_schema
  if(is.null(grskey_schema)){
    grskey_schema <- dst_schema
    dst_gr_skey_tbl <- glue("{dst_tbl}_gr_skey")
  } else {
    dst_gr_skey_tbl <- glue("{dst_tbl}")
  }

  ## if overlap, give gr_skey table  a different name
  if(overlap_ind){
    dst_gr_skey_tbl <- glue("{dst_gr_skey_tbl}_overlap")
  }

  ## ERROR CHECK: Review any gdb for Multisurface polygons as they are known geometry type that GDAL library will not import
  ## If a multisurface geometry type is found, kick out an error, continue on, and then spew out error again in error summary

  multisurface_error <- NULL
  if ((src_type %in% c("gdb"))) {
    ## check to ensure src_path exists
    if (!(file.exists(src_path))) {
      print(glue("ERROR: Source path does not exist: {src_path}, cannot import, moving on to next import layer. "))
      return()
    }
    contains_multisurface <- dadmtools::check_multisurface_gdb(src_path,src_lyr)
    if(contains_multisurface){
      multisurface_error <- glue("ERROR: Invalid GEOMETRY in {src_path} | layername = {src_lyr}. Layer was not imported. Please fix invalid geometry by fixing or removing arc, curves or other complex geometry. Hint exporting data to shapefile may help fix this issue.")
      print(multisurface_error)
      cat('Carrying on to next import...\n')
      return(multisurface_error)
    }
  }

  pk_id = "pgid"
  no_data_value = 0
  today_date <- format(Sys.time(), "%Y-%m-%d %I:%M:%S %p")
  connz <- dbConnect(pg_conn_param["driver"][[1]],
                    host     = pg_conn_param["host"][[1]],
                    user     = pg_conn_param["user"][[1]],
                    dbname   = pg_conn_param["dbname"][[1]],
                    password = pg_conn_param["password"][[1]],
                    port     = pg_conn_param["port"][[1]])

  if(query == '' || is.null(query) || is.na(query)) {
    query_escaped <- ''
  } else {
    query_escaped <- gsub("\'","\'\'", query)
  }

  if(tolower(src_type) != 'raster') {
    dst_tbl_comment <- glue("COMMENT ON TABLE {dst_schema}.{dst_tbl} IS 'Table created by the dadmtools R package at {today_date}.
                                              TABLE relates to {grskey_schema}.{dst_gr_skey_tbl}
                                              Data source details:
                                              Source type: {src_type}
                                              Source path: {src_path}
                                              Source layer: {src_lyr}
                                              Source where query: {query_escaped}';")
    dst_gr_skey_tbl_comment <- glue("COMMENT ON TABLE {grskey_schema}.{dst_gr_skey_tbl} IS 'Table created by the dadmtools R package at {today_date}.
                                              TABLE relates to {dst_schema}.{dst_tbl}
                                              Data source details:
                                              Source type: {src_type}
                                              Source path: {src_path}
                                              Source layer: {src_lyr}
                                              Source where query: {query_escaped}';")
  } else {
    dst_tbl_comment <- glue("COMMENT ON TABLE {dst_schema}.{dst_tbl} IS 'Table created by the dadmtools R package at {today_date}.
                                              Data source details:
                                              Source type: {src_type}
                                              Source path: {src_path}
                                              Source layer: {src_lyr}
                                              Source where query: {query_escaped}';")
    dst_gr_skey_tbl_comment <- glue("COMMENT ON TABLE {grskey_schema}.{dst_gr_skey_tbl} IS 'Table created by the dadmtools R package at {today_date}.
                                              Data source details:
                                              Source type: {src_type}
                                              Source path: {src_path}
                                              Source layer: {src_lyr}
                                              Source where query: {query_escaped}';")
  }

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
      fklyr <- create_oracle_fdw_in_pg(src_lyr, ora_conn_param, pg_conn_param, 'oradb', fdw_schema)

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
      dadmtools:::fdw_to_tbl(
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

      if(overlap_ind) {
        select_flds <- paste0(c(pk_id,paste0('non_spatial.',strsplit(overlap_group_fields, ",")[[1]])),collapse = ",")
      } else {
        select_flds <- pk_id
      }

      ## Create a SQL query including the spatial field and primary key field of a Foreign Data Wrapper table
      qry <- dadmtools:::get_sql_fdw_id_geom(dst_tbl      = dst_tbl,
                                dst_schema    = dst_schema,
                                ora_tbl       = src_lyr,
                                pk            = select_flds,
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

      if(!overlap_ind) {
        ## Rasterize using TERRA
        dst_ras_filename <- dadmtools::rasterize_terra(
          src_sf       = in_sf,
          field        = pk_id,
          template_tif = template_tif,
          crop_extent  = crop_extent,
          out_tif_path = out_tif_path,
          out_tif_name = glue("{dst_tbl}.tif"),
          datatype     = 'INT4U',
          nodata       = no_data_value
        )
      } else if (overlap_ind) {
        ##Loop through rasterization, convert into a gr_skey, and append into pg table
        #dst_gr_skey_tbl <- glue("{dst_gr_skey_tbl}_overlap")
        dst_gr_skey_tbl_pg_obj <- RPostgres::Id(schema = grskey_schema, table = dst_gr_skey_tbl )

        ###Drop existing table
        #####
        dadmtools::run_sql_r(glue("DROP TABLE IF EXISTS {grskey_schema}.{dst_gr_skey_tbl};"), pg_conn_param)

        ###Loop through each group then create a raster for each group and append the results onto the final pg table#######
        ####################
        sql <- glue("select {overlap_group_fields} from {dst_schema}.{dst_tbl} group by {overlap_group_fields}" )
        overlap_groups_df <-  dadmtools::sql_to_df(sql,pg_conn_param)
        fields <- unlist(strsplit(overlap_group_fields, ","))
        for (i in 1:nrow(overlap_groups_df)) {
          row <- overlap_groups_df[i, , drop = FALSE]  # Get the row as a dataframe
          print(row)
          filtered_df <- merge(in_sf, row, by = fields)

          ## Rasterize using TERRA
          dst_ras_filename <- dadmtools::rasterize_terra(
            src_sf       = filtered_df,
            field        = pk_id,
            template_tif = template_tif,
            crop_extent  = crop_extent,
            out_tif_path = out_tif_path,
            out_tif_name = glue("{dst_tbl}.tif"),
            datatype     = 'INT4U',
            nodata       = no_data_value)

          in_df <- dadmtools::tif_to_gr_skey_tbl(
            src_tif_filename = dst_ras_filename,
            crop_extent      = crop_extent,
            template_tif     = template_tif,
            mask_tif         = mask_tif,
            val_field_name   = pk_id
          )

          if (is.null(in_df)){
            print("ERROR: Could not coonvert Raster to GR_SKEY table")
            return()
          }

          dadmtools::df_to_pg(
            pg_tbl = dst_gr_skey_tbl_pg_obj,
            in_df,
            pg_conn_param = pg_conn_param,
            overwrite = FALSE,
            append = TRUE
          )

        }

        print(glue('Created PG table: {grskey_schema}.{dst_gr_skey_tbl} from values in tif and gr_skey'))

      }
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
      dadmtools:::ogr_to_tbl(
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

      if(!overlap_ind) {
        #Create tif from input
        dst_ras_filename <- dadmtools:::rasterize_gdal(
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

      } else if (overlap_ind) {

        ##Loop through rasterization, convert into a gr_skey, and append into pg table
        #dst_gr_skey_tbl <- glue("{dst_gr_skey_tbl}_overlap")
        dst_gr_skey_tbl_pg_obj <- RPostgres::Id(schema = grskey_schema, table = dst_gr_skey_tbl )

        ###Drop existing table
        #####
        dadmtools::run_sql_r(glue("DROP TABLE IF EXISTS {grskey_schema}.{dst_gr_skey_tbl};"), pg_conn_param)

        ###Loop through each group then create a raster for each group and append the results onto the final pg table#######
        ####################
        sql <- glue("select {overlap_group_fields} from {dst_schema}.{dst_tbl} group by {overlap_group_fields}" )
        overlap_groups_df <-  dadmtools::sql_to_df(sql,pg_conn_param)

        # Function to add quotes to character values
        add_quotes <- function(x) {
          if (is.character(x)) {
            return(ifelse(is.na(x), NA, paste0("'", x, "'")))
          } else {
            return(x)
          }
        }

        # Apply function to all columns
        overlap_groups_df[] <- lapply(overlap_groups_df, add_quotes)

        for (i in 1:nrow(overlap_groups_df)) {
          row <- overlap_groups_df[i, ,drop = FALSE]  # Get the row as a dataframe
          print(row)
          if (is.null(where_claus)) {
            where_clause_overlap <- NULL
          } else {
            where_clause_overlap <- glue("({where_claus})")
          }

          for (col in names(row)) {
            value <- row[1,col]
            if (is.null(value) | is.na(value)) {
              colquery = glue("{col} is Null")
            } else {
              colquery = glue("{col} = {value}")
            }
            if(is.null(where_clause_overlap)){where_clause_overlap <- colquery}else{where_clause_overlap <- glue("{where_clause_overlap} and {colquery}")}
          }
          print(where_clause_overlap)


          if(!dadmtools::is_blank(where_claus)){
            src_lyr_rasterize <- glue("{src_lyr} where {where_claus}" )
          } else {
            src_lyr_rasterize <- src_lyr
          }

          dst_ras_filename <- dadmtools:::rasterize_gdal(
            in_lyr        = src_lyr_rasterize,
            field         = pk_id,
            out_tif_path  = out_tif_path,
            out_tif_name  = glue("{dst_tbl}.tif"),
            src_path      = src_path,
            pg_conn_param = NULL,
            crop_extent   = crop_extent,
            nodata        = no_data_value,
            where         = where_clause_overlap
          )

          in_df <- dadmtools::tif_to_gr_skey_tbl(
            src_tif_filename = dst_ras_filename,
            crop_extent      = crop_extent,
            template_tif     = template_tif,
            mask_tif         = mask_tif,
            val_field_name   = pk_id
          )

          if (is.null(in_df)){
            print("ERROR: Could not convert Raster to GR_SKEY table")
            return()
          }

          dadmtools::df_to_pg(
            pg_tbl = dst_gr_skey_tbl_pg_obj,
            in_df,
            pg_conn_param = pg_conn_param,
            overwrite = FALSE,
            append = TRUE
          )

        }

        print(glue('Created PG table: {grskey_schema}.{dst_gr_skey_tbl} from values in tif and gr_skey'))
      }

      if (src_type %in% c("shapefile", "shp")) {
        dir_name <- dirname(src_path)
        base_name <- tools::file_path_sans_ext(basename(src_path))
        pattern <- paste0("^", base_name, "\\.(shp|shx|dbf|prj|sbn|sbx|xml|cpg|shp\\.xml)$")
        shapefiles <- list.files(path = dir_name, pattern = pattern, ignore.case = TRUE)
        for (file in shapefiles) {
          file.remove(file.path(out_tif_path, file))
        }
      } else if (src_type == 'gdb') {
        ## file.remove consistently ran into permission issues when ran on a gdb
        ## switched to gdalmanage
        print(system2('gdalmanage',args=c("delete", src_path), stderr = TRUE))
      }
      else {
        file.remove(src_path, recursive = TRUE)
      }

    }
  }

  if(tolower(src_type) == 'raster') {

    if (overlap_ind) {
      print("ERROR: overlap_ind must be set to FALSE for rasters")
            return()
    }


    if (dirname(src_path) != dirname(out_tif_path)) {
      print(glue("Copying {src_path} to {out_tif_path}"))
      file.copy(from = src_path, to = out_tif_path, overwrite = TRUE, recursive = TRUE, copy.mode = TRUE)
    }
    old_src_path <- src_path
    dst_ras_filename <- file.path(out_tif_path, basename(src_path))
  }

if(!overlap_ind){
  ## Tif to PG Raster
  if(import_rast_to_pg) {
    raster_tbl <- glue("{raster_schema}.ras_{substr(basename(dst_ras_filename),1,nchar(basename(dst_ras_filename))-4)}")
    cmd <- glue('raster2pgsql -s 3005 -d -C -r -P -I -M -N {no_data_value} -t 100x100 {dst_ras_filename} {raster_tbl} | psql -d {pg_conn_param["dbname"][[1]]}')
    print(cmd)
    shell(cmd)
    print('Imported tif to PG')
  }

  #Convert postgres raster to non-spatial table with gr_skey
  dst_gr_skey_tbl_pg_obj <- RPostgres::Id(schema = grskey_schema, table = dst_gr_skey_tbl)
  print(glue('Creating PG table: {grskey_schema}.{dst_gr_skey_tbl} from values in tif and gr_skey'))
  dadmtools::run_sql_r(glue("DROP TABLE IF EXISTS {grskey_schema}.{dst_gr_skey_tbl};"), pg_conn_param)

  if (tolower(src_type) == 'raster') {
    band_field_name <- 'val'
  } else {
    band_field_name <- pk_id
  }

  in_df <- dadmtools::tif_to_gr_skey_tbl(
    src_tif_filename = dst_ras_filename,
    crop_extent      = crop_extent,
    template_tif     = template_tif,
    mask_tif         = mask_tif,
    val_field_name   = band_field_name
  )
  ## if the above function returned NULL - then the input raster had a problem - exit script safely.
  if (is.null(in_df)){
    return()
  }

  dadmtools::df_to_pg(
                      pg_tbl = dst_gr_skey_tbl_pg_obj,
                      in_df,
                      pg_conn_param = pg_conn_param
  )
  print(glue('Created PG table: {grskey_schema}.{dst_gr_skey_tbl} from values in tif and gr_skey'))
  if(tolower(src_type) == 'raster') {
    if (dirname(src_path) != dirname(out_tif_path)) {
      print(glue("Removing {basename(src_path)} from {out_tif_path}"))
      file.remove(dst_ras_filename)
    }
  }
}

  ## Adding primary key to table
  if (!overlap_ind){
    print(glue('Adding gr_skey as primary key to {grskey_schema}.{dst_gr_skey_tbl}'))
    dadmtools::run_sql_r(glue("ALTER TABLE {grskey_schema}.{dst_gr_skey_tbl} ADD PRIMARY KEY (gr_skey);"), pg_conn_param)
  } else if (overlap_ind) {
    dadmtools::run_sql_r(glue("DROP INDEX IF EXISTS {dst_gr_skey_tbl}_gr_skey_idx;"), pg_conn_param)
    dadmtools::run_sql_r(glue("CREATE INDEX {dst_gr_skey_tbl}_gr_skey_idx ON {grskey_schema}.{dst_gr_skey_tbl} USING btree (gr_skey)"), pg_conn_param)
  }

  if (tolower(src_type) != 'raster') {
    ## Adding in foreign key
    print(glue('Adding foreign key constraint to {grskey_schema}.{dst_gr_skey_tbl} referencing {dst_schema}.{dst_tbl} using ({pk_id});'))
    dadmtools::run_sql_r(glue("ALTER TABLE {grskey_schema}.{dst_gr_skey_tbl} ADD CONSTRAINT {dst_gr_skey_tbl}_fkey FOREIGN KEY ({pk_id}) REFERENCES {dst_schema}.{dst_tbl} ({pk_id});"), pg_conn_param)
    ## Add index on pgid
    dadmtools::run_sql_r(glue("DROP INDEX IF EXISTS {dst_gr_skey_tbl}_{pk_id}_idx;"), pg_conn_param)
    dadmtools::run_sql_r(glue("CREATE INDEX {dst_gr_skey_tbl}_{pk_id}_idx ON {grskey_schema}.{dst_gr_skey_tbl} USING btree ({pk_id})"), pg_conn_param)
  }
  ## Add comment on table
  dadmtools::run_sql_r(dst_gr_skey_tbl_comment, pg_conn_param)
  ## Update the query planner with the latest changes
  dadmtools::run_sql_r(glue("ANALYZE {grskey_schema}.{dst_gr_skey_tbl};"), pg_conn_param)


  dadmtools:::update_pg_metadata_tbl(
    data_src_tbl    = data_src_tbl,
    src_type        = src_type,
    src_path        = old_src_path,
    src_lyr         = src_lyr,
    pk              = pk_id,
    query           = query,
    flds_to_keep    = flds_to_keep,
    notes           = notes,
    dst_tbl         = dst_tbl,
    dst_schema      = dst_schema,
    pg_conn_param   = pg_conn_param,
    overlap_ind = overlap_ind ,
    overlap_group_fields = overlap_group_fields
  )

  terra::tmpFiles(remove=TRUE)
}

