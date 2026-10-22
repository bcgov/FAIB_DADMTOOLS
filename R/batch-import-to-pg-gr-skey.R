#' Batch import spatial data into a gridded attribute format within PostgreSQL, structuring data according to the gr_skey grid system.
#'
#'Function supports both vector and raster inputs. For vector data (e.g., Shapefile, FGDB, GeoPackage), the function imports the attribute table into PostgreSQL and generates a corresponding raster attribute table, where each record represents a raster pixel (one hectare). The gr_skey field acts as a globally unique primary key, while pgid links the raster attributes to the vector attributes. Each record within the raster attribute table represents one pixel.
#'For raster data (e.g., geotiff), the raster is required to have BC Albers coordinate reference system, (Ie. EPSG: 3005), the same grid definition as the 'template_tif' and 'mask_tif' provided to the 'import_gr_skey_tif_to_pg_rast' function and only one band. For TSR, it is recommended to use the gr_skey grid. The function imports the raster as a single attribute table. The table includes 'gr_skey', unique global cell id and the input raster pixel value. Each record represent one pixel.
#'
#'
#' @param in_csv File path to input configuration csv, defaults to "config_parameters.csv". The rasterization method can be set per dataset with the optional csv columns 'rasterization_touches' and 'rasterization_largest_area' (TRUE/FALSE, also accepts 1/0, T/F, yes/no). A blank cell, or a csv without the column, uses the touches / largest_area argument of this function.
#' @param pg_conn_param Keyring object of Postgres credentials, defaults to dadmtools::get_pg_conn_list()
#' @param ora_conn_param Keyring object of Oracle credentials, defaults to dadmtools::get_ora_conn_list()
#' @param crop_extent list of c(ymin, ymax, xmin, xmax) in EPSG:3005, defaults to c(273287.5,1870587.5,367787.5,1735787.5)
#' @param gr_skey_tbl Schema and table name of the pre-existing gr_skey table imported using function: 'import_gr_skey_tif_to_pg_rast'. Defaults to "whse.all_bc_gr_skey"
#' @param raster_schema If import_rast_to_pg = TRUE, schema of imported raster, defaults to "raster"
#' @param template_tif The file path to the gr_skey geotiff to be used as a template raster, defaults to "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif"
#' @param mask_tif The file path to the geotiff to be used as a mask, defaults to "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif"
#' @param data_src_tbl Schema and table name of the metadata table in postgres that updates with any newly imported layer. Defaults to "whse.data_sources"
#' @param out_tif_path Directory where output tif is exported and where vector is temporally stored prior to import
#' @param import_rast_to_pg If TRUE, raster is imported into database in raster_schema. Defaults to FALSE
#' @param grskey_schema Schema for the gr_skey tables. Defaults to NULL, which uses each row's dst_schema
#' @param touches Default for rows whose 'rasterization_touches' value in in_csv is blank or missing. If TRUE, vector inputs burn in every cell touched by a line or polygon. If FALSE (default), a polygon only gets a cell when it covers the cell's center point.
#' @param largest_area Default for rows whose 'rasterization_largest_area' value in in_csv is blank or missing. If TRUE, each cell gets the polygon that covers the largest area of it, where the part of the cell not covered by any polygon counts as nodata and competes like a polygon (see rasterize_r for details). Defaults to FALSE. A row cannot have both touches and largest_area set to TRUE; such a row is skipped and reported as an error.
#'
#'
#' @return no return
#' @export
#'
#' @examples
#' ## After updating config_parameters.csv with source data and arguments, run the following:
#' batch_import_to_pg_gr_skey(
#'    in_csv            = 'C:\\<specify path>\\config_parameters.csv',
#'    out_tif_path      = 'C:\\<specify path>\\'
#' )

batch_import_to_pg_gr_skey <- function(in_csv           = "config_parameters.csv",
                                       out_tif_path,
                                       pg_conn_param     = dadmtools::get_pg_conn_list(),
                                       ora_conn_param    = dadmtools::get_ora_conn_list(),
                                       crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5),
                                       gr_skey_tbl       = "whse.all_bc_gr_skey",
                                       raster_schema     = "raster",
                                       template_tif      = "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif",
                                       mask_tif          = "S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif",
                                       data_src_tbl      = "whse.data_sources",
                                       import_rast_to_pg = FALSE,
                                       grskey_schema     = NULL,
                                       touches           = FALSE,
                                       largest_area      = FALSE
)
{
  start_time <- Sys.time()
  print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
  in_file <- read.csv(in_csv)
  in_file <- in_file[in_file$include == 1,]
  ms_errors <- list()

  ## Read a TRUE/FALSE csv value; blank, NA or a missing column falls back to default
  read_flag <- function(row, col, default) {
    if (!(col %in% names(in_file))) return(default)
    val <- toupper(trimws(as.character(in_file[row, col])))
    if (is_blank(val)) return(default)
    if (val %in% c("TRUE", "T", "1", "YES", "Y"))  return(TRUE)
    if (val %in% c("FALSE", "F", "0", "NO", "N")) return(FALSE)
    NA
  }
  for (row in 1:nrow(in_file)) {
    src_type      <- gsub("[[:space:]]","",tolower(in_file[row, "src_type"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    src_path      <- tolower(trimws(in_file[row, "src_path"])) ## path to input data. Note use bcgw for whse
    src_lyr       <- gsub("[[:space:]]","",tolower(in_file[row, "src_lyr"])) ## input layer name
    dst_schema    <- gsub("[[:space:]]","",tolower(in_file[row, "dst_schema"])) ## name of output non spatial table
    dst_tbl       <- gsub("[[:space:]]","",tolower(in_file[row, "dst_tbl"])) ## name of output non spatial table
    query         <- in_file[row, "query"]  ##where clause used to filter input dataset
    notes         <- in_file[row, "notes"]  ##where clause used to filter input dataset
    flds_to_keep  <- gsub("[[:space:]]","",tolower(in_file[row, "flds_to_keep"])) ## fields to keep in non spatial table
    overlap_ind   <- as.logical(gsub("[[:space:]]","",toupper(in_file[row, "overlap_ind"])))
    group_field   <- gsub("[[:space:]]","",tolower(in_file[row, "overlap_group_fields"])) ## group field to be used in the resultant table
    row_touches      <- read_flag(row, "rasterization_touches", touches) ## burn every cell touched (vector only)
    row_largest_area <- read_flag(row, "rasterization_largest_area", largest_area) ## largest-area rasterization (polygons only)

    ## checks
    if (any(c(is_blank(src_type), is_blank(src_path), is_blank(src_lyr), is_blank(dst_tbl), is_blank(out_tif_path)))){
      print("ERROR: Argument not provided, one of src_type, src_path, src_lyr, dst_tbl, out_tif_path was left blank. Exiting script.")
      return()
    }

    if (!(src_type %in% c("gdb", "oracle", "geopackage", "gpkg", "raster", "shp", "shapefile"))) {
      print(glue("ERROR: Invalid src_type: {src_type}. Hint, provide one of: gdb, oracle, geopackage, gpkg, raster, shapefile or shp. Exiting script."))
      return()
    }


    ## per-row rasterization checks: report and skip this dataset, carry on with the rest
    if (is.na(row_touches) || is.na(row_largest_area)) {
      ms_error <- glue("ERROR: {src_lyr} -> {dst_schema}.{dst_tbl}: rasterization_touches and rasterization_largest_area must be TRUE, FALSE or blank. Row skipped.")
      print(ms_error); ms_errors <- append(ms_errors, ms_error)
      next
    }
    if (row_touches && row_largest_area) {
      ms_error <- glue("ERROR: {src_lyr} -> {dst_schema}.{dst_tbl}: rasterization_touches and rasterization_largest_area cannot both be TRUE. Row skipped.")
      print(ms_error); ms_errors <- append(ms_errors, ms_error)
      next
    }
    if (src_type != "raster") {
      method <- if (row_largest_area) "largest area" else if (row_touches) "touches (all cells touched)" else "cell center"
      print(glue("Rasterization method for {dst_schema}.{dst_tbl}: {method}"))
    }

    ms_error <- dadmtools::import_to_pg_gr_skey(
      src_type             = src_type,
      src_path             = src_path,
      src_lyr              = src_lyr,
      dst_tbl              = dst_tbl,
      query                = query,
      flds_to_keep         = flds_to_keep,
      notes                = notes,
      overlap_ind          = overlap_ind,
      overlap_group_fields = group_field,
      pg_conn_param        = pg_conn_param,
      ora_conn_param       = ora_conn_param,
      crop_extent          = crop_extent,
      dst_schema           = dst_schema,
      raster_schema        = raster_schema,
      template_tif         = template_tif,
      mask_tif             = mask_tif,
      data_src_tbl         = data_src_tbl,
      out_tif_path         = out_tif_path,
      import_rast_to_pg    = import_rast_to_pg,
      grskey_schema        = grskey_schema,
      touches              = row_touches,
      largest_area         = row_largest_area
    )
    if (!is.null(ms_error) && length(ms_error) > 0) {
      if (startsWith(ms_error[1], "ERROR:")) {
        ms_errors <- append(ms_errors, ms_error)  # append to list
      }
    }
  }
  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  cat('\n********************************************************************************\n')
  cat(glue("Script finished. Duration: {round(duration, 1)} min(s), script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}, ended at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}\n"))
  cat('\n********************************************************************************\n')
  if (length(ms_errors) > 0) {
    if (length(ms_errors) == 1) {
      cat(glue("\033[31m{length(ms_errors)} ERROR found:\033[0m\n\n\n"))
    } else {
      cat(glue("\033[31m{length(ms_errors)} ERROR's found:\033[0m\n\n\n"))
    }
  }
  for (error in ms_errors) {
    cat(glue("\033[31m{error}\033[0m\n\n\n"))
  }
}
