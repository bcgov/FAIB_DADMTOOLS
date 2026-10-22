
#' Import spatial data into PostgreSQL using the gr_skey grid system
#'
#' Imports vector or raster data into PostgreSQL and generates a corresponding
#' gr_skey attribute table. The function supports Oracle, file geodatabases,
#' Shapefiles, GeoPackages, and raster files.
#'
#' For vector data, the function imports the source attributes into a
#' non-spatial PostgreSQL table and rasterizes the spatial features against
#' the gr_skey grid. The resulting gr_skey table contains one record per
#' intersected grid cell, with gr_skey identifying the grid cell and pgid
#' linking the record to the corresponding source feature. When overlapping
#' features are retained, multiple records may exist for a single gr_skey.
#'
#' For raster data, the function creates a gr_skey attribute table containing
#' the grid cell identifier (gr_skey) and the corresponding raster pixel
#' value. Input rasters must use EPSG:3005, contain a single band, and match
#' the resolution and alignment of the gr_skey template grid.
#'
#' File-based vector and raster sources are validated before import to ensure
#' that their coordinate reference system can be identified as EPSG:3005
#' (BC Albers). Sources with an unknown EPSG code or a different EPSG code
#' are rejected. Oracle sources are assumed to use EPSG:3005 and bypass this
#' source CRS validation.
#'
#' @param src_type Source format. Supported values are gdb, oracle, raster,
#'   geopackage, gpkg, shapefile, shp, and postgres.
#' @param src_path Path to the input file or directory. For Oracle sources,
#'   use the connection alias (e.g., "bcgw"). For file-based sources,
#'   provide the full path to the source.
#' @param src_lyr Source layer name. For Oracle sources, provide the schema
#'   and table name (e.g., "WHSE_FOREST_VEGETATION.bec_biogeoclimatic_poly").
#'   For file geodatabases and GeoPackages, provide the layer name within
#'   the source. For Shapefiles, provide the filename without its extension.
#'   For raster sources, this argument is not used for reading the raster,
#'   although it is included in metadata.
#' @param dst_tbl Name of the destination PostgreSQL attribute table.
#' @param query Optional SQL WHERE-clause expression used to filter the
#'   source features.
#' @param flds_to_keep Optional comma-separated list of source attribute
#'   fields to retain. By default, all fields are retained.
#' @param notes Optional notes describing the imported dataset.
#' @param overlap_ind Logical value indicating whether overlapping vector
#'   features should be retained in the gr_skey table. If TRUE, multiple
#'   records may be created for the same grid cell. If FALSE, overlaps are
#'   resolved during rasterization. Must be FALSE for raster inputs.
#' @param overlap_group_fields Optional comma-separated field names used to
#'   group features when processing overlapping vector data. Required when
#'   overlap_ind = TRUE.
#' @param out_tif_path Directory used for temporary source files and generated
#'   raster outputs.
#' @param pg_conn_param PostgreSQL connection parameters. Defaults to
#'   dadmtools::get_pg_conn_list().
#' @param ora_conn_param Oracle connection parameters. Defaults to
#'   dadmtools::get_ora_conn_list().
#' @param crop_extent Raster crop extent in EPSG:3005, specified as
#'   c(xmin, xmax, ymin, ymax). Defaults to
#'   c(273287.5, 1870587.5, 367787.5, 1735787.5).
#' @param dst_schema PostgreSQL schema for the destination attribute table.
#'   Defaults to "whse".
#' @param raster_schema PostgreSQL schema used when importing raster data.
#'   Defaults to "raster".
#' @param fdw_schema PostgreSQL schema used for Oracle foreign data wrapper
#'   tables. Defaults to "load".
#' @param template_tif Path to the gr_skey template GeoTIFF defining the
#'   target grid. Defaults to the provincial one-hectare gr_skey template.
#' @param mask_tif Path to the GeoTIFF used to mask the gr_skey grid.
#'   Defaults to the provincial terrestrial boundary mask.
#' @param data_src_tbl Metadata table updated after a successful import.
#'   Defaults to "whse.data_sources".
#' @param import_rast_to_pg Logical value indicating whether to import the
#'   generated or input raster into PostgreSQL. Defaults to FALSE.
#' @param grskey_schema Optional PostgreSQL schema for the gr_skey table.
#'   Defaults to NULL, which uses dst_schema. When supplied, dst_tbl is used
#'   as the gr_skey table name; otherwise, the gr_skey table name is derived
#'   from dst_tbl with the suffix "_gr_skey".
#'
#' @return The function primarily operates through PostgreSQL tables and
#'   messages. It does not return a documented data object.
#'
#' @export

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

  # browser()

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

  # Check that file-based sources exist before attempting CRS validation.
  # Oracle sources use a connection alias (e.g. "bcgw"), not a file path.
  if (src_type %in% c("raster", "shp", "shapefile",
                      "gpkg", "geopackage", "gdb")) {

    if (!file.exists(src_path)) {
      msg <- glue::glue(
        "ERROR: Source path does not exist: {src_path}. ",
        "Cannot import layer {src_lyr}; skipping this import."
      )
      message(msg)
      return(msg)
    }
  }

  # Validate CRS only after the source path has been confirmed.
  dadmtools::check_source_crs(
    src_type = src_type,
    src_path = src_path,
    src_lyr = src_lyr,
    expected_epsg = 3005
  )

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
          where_clause_overlap <- NULL
          row <- overlap_groups_df[i, ,drop = FALSE]  # Get the row as a dataframe
          print(row)
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

          dst_ras_filename <- dadmtools:::rasterize_gdal(
            in_lyr        = src_lyr,
            field         = pk_id,
            out_tif_path  = out_tif_path,
            out_tif_name  = glue("{dst_tbl}.tif"),
            src_path      = src_path,
            pg_conn_param = NULL,
            crop_extent   = crop_extent,
            nodata        = no_data_value,
            where         = where_claus,
            overlap_where = where_clause_overlap,
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
