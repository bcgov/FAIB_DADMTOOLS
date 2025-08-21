#' Batch imports Postgres table fields (data types: integer | double | text) to TIFF.
#' Converts to an integer TIF. Double data types (decimals) are multiplied by the parameter d_mulitiplier, rounded and the result converted to an integer.
#' Text is converted to a text:sequence pair and saved to a CAT file, and the sequence is converted to the TIF.
#'
#' @param in_csv File path to input configuration csv, defaults to "batch_pg_to_seles_tif.csv"
#' @param pg_conn_param Keyring object of Postgres credentials, defaults to dadmtools::get_pg_conn_list()
#' @param template_tif (required): string - file path to template tif (e.g. skey.tif)
#' @param out_tif_path (required): string - path to output rasters directory
#' @param out_cat_path (required): string - path to output cat files directory. Only used for text field types. Default NULL.
#' @param gdb (required): string - path to GDB that contains the unit boundary feature class (e.g. bnd) - used to create extent
#' @param bnd (required): string - boundary feature class name

batch_pg_to_seles_tif <- function(in_csv = "batch_pg_to_seles_tif.csv",
                                  pg_conn_param = get_pg_conn_list(),
                                  template_tif,
                                  out_tif_path,
                                  out_cat_path,
                                  gdb,
                                  bnd)
{
  start_time <- Sys.time()
  print(glue("Script started at {format(start_time, '%Y-%m-%d %I:%M:%S %p')}"))
  in_file <- read.csv(in_csv)
  in_file <- in_file[in_file$include == 1,]
  for (row in 1:nrow(in_file)) {
    pg_att_table <- gsub("[[:space:]]","",tolower(in_file[row, "pg_att_table"])) ## attribute table
    pg_attskey_table <- gsub("[[:space:]]","",tolower(in_file[row, "pg_attskey_table"])) ## attribute skey table
    pg_skgeom_table <- gsub("[[:space:]]","",tolower(in_file[row, "pg_skgeom_table"])) ## geometry table
    geom_field <- gsub("[[:space:]]","",tolower(in_file[row, "geom_field"])) ## geometry field name
    field_to_tif <- gsub("[[:space:]]","",tolower(in_file[row, "field_to_tif"])) ## name of field to convert to tif
    key_skgeo_tbl <- gsub("[[:space:]]","",tolower(in_file[row, "key_skgeo_tbl"]))  ##key field between skey table and geometry table
    key_attskey_tbl <- gsub("[[:space:]]","",tolower(in_file[row, "key_attskey_tbl"]))  ##key field between skey table and attribute table
    dst_tif_name <- gsub("[[:space:]]","",tolower(in_file[row, "dst_tif_name"]))  ##Name of TIF being created
    d_multiplier <- in_file[row, "d_multiplier"]  ##Multiplier used if data type is decimal
    query <- in_file[row, "query"]  ##where clause used to filter input dataset
    notes <- in_file[row, "notes"]  ##user notes field


    ## checks
    if (any(c(is_blank(pg_att_table), is_blank(geom_field), is_blank(field_to_tif), is_blank(dst_tif_name), is_blank(out_tif_path)))){
      print("ERROR: Argument not provided, one of pg_att_table, geom_field, field_to_tif, dst_tif_name, out_tif_path was left blank. Exiting script.")
      return()
    }


    pg_to_seles_tif(pg_conn_param = pg_conn_param,
                    field_to_tif = field_to_tif,
                    pg_att_table = pg_att_table,
                    pg_attskey_table = pg_attskey_table,
                    pg_skgeom_table = pg_skgeom_table,
                    geom_field = geom_field,
                    key_skgeo_tbl = key_skgeo_tbl,
                    key_attskey_tbl = key_attskey_tbl,
                    template_tif = template_tif,
                    out_tif_path = out_tif_path,
                    dst_tif_name = dst_tif_name,
                    out_cat_path = out_cat_path,
                    d_multiplier = d_multiplier,
                    query = query,
                    gdb = gdb,
                    bnd = bnd)

  }
  end_time <- Sys.time()
  duration <- difftime(end_time, start_time, units = "mins")
  print(glue("Script started at {format(end_time, '%Y-%m-%d %I:%M:%S %p')}"))
  print(glue("Script duration: {duration} minutes\n"))
}
