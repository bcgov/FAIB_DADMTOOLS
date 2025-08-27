#' Rasterize Field in Postgres Table (dataypes: integer, decimal, text)
#' Seles only uses integer rasters.
#' If the field is an integer, a TIF file is created with the integer value
#' If the field is a text field, a CAT file will be created, a
#' value - integer pairing. The TIF file will be created from the integer.
#' The CAT file is used in Seles to display the legend.
#' If the filed file is a decimal, it will be multiplied by the parameter d_multiplier, then converted
#' to an integer. A TIF file will be created from the integer.
#'
#' @param pg_conn_param named (required): list - connection parameters (returned by dadmtools::get_pg_conn_list)
#' @param field_to_tif (required): string - field name in pg_table_with_geom to convert to raster
#' @param pg_att_table (required): string - name of Postgres schema.table with field to convert. Optionally can have geometry field
#' @param pg_attskey_table (optional): string - name of Postgres schema.table with key to geom and attribute tables (e.g. gr_skey or ogc_fid),   required if geometry not in attribute table
#' @param pg_skgeom_table (optional): string - name of Postgres schema.table with geometry, if geometry not in attribute table
#' @param geom_field (required): string - name of geometry field in pg_table_with_geom
#' @param key_skgeo_tbl (optional): string - name of join field to join pg_skey_table and pg_geom_table (e.g. gr_skey), required if joining 3 tables.
#' @param key_attskey_tbl (optional): string - name of join field to join pg_att_table and pg_skey_table (e.g. pgid), required if joining 2 or 3 tables.
#' @param template_tif (required): string - file path to template tif (e.g. skey.tif)
#' @param out_tif_path (required): string - path to output rasters directory
#' @param dst_tif_name (required): string - name of output tif saved to out_tif_path
#' @param out_cat_path (optional): string - path to output cat files directory. Only used for text field types. Default NULL.
#' @param d_mulitiplier (optional): integer multiplier to use for decimal datatypes. Only used for decimal data types. Default 1000
#' @param query (optional): string SQL where clause used to filter input dataset, e.g. "thlb_fact > 0", "own is not null". Default NULL (no query applied).
pg_to_seles_tif <- function(pg_conn_param,
                            field_to_tif,
                            pg_att_table,
                            pg_attskey_table = NULL,
                            pg_skgeom_table = NULL,
                            geom_field,
                            key_skgeo_tbl = NULL,
                            key_attskey_tbl = NULL,
                            template_tif,
                            out_tif_path,
                            dst_tif_name,
                            out_cat_path = NULL,
                            d_multiplier = 1000,
                            query = NULL,
                            gdb,
                            bnd)
{

  data_type_handled <- glue("'smallint', ", "'integer', ", "'boolean', ",
                            "'double precision', ",
                            "'character varying', ", "'text'")

  driver <- pg_conn_param["driver"][[1]]
  host <- pg_conn_param["host"][[1]]
  user <- pg_conn_param["user"][[1]]
  password <- pg_conn_param["password"][[1]]
  port <- pg_conn_param["port"][[1]]
  dbname <- pg_conn_param["dbname"][[1]]

  con <- RPostgres::dbConnect(driver,
                              host=host,
                              user = user,
                              port = port,
                              password=password,
                              dbname=dbname)
  on.exit(RPostgres::dbDisconnect(con))

  # check GDB and bnd feature classes exist
  cmd <- glue("ogrinfo --quiet -so -geom=NO {gdb} {bnd}")
  ret <- system(command = cmd)

  if (ret[1] == 0){
    print('success')
  }else{
    print(glue("Feature class '{bnd}' not found in GDB '{gdb}'"))
  }
  tb_schema <- gsub("\\..*","",pg_att_table)
  tb_table <- gsub(".*\\.", "", pg_att_table)
  sql_tb <- glue("SELECT data_type
                       FROM information_schema.columns
                       WHERE table_schema = '{tb_schema}'
                        AND table_name = '{tb_table}'
                      and column_name = '{field_to_tif}'")
  tb_type_df <- dadmtools::sql_to_df(sql_tb, pg_conn_param)

  if (length(tb_type_df$data_type) == 0){
    return_msg <- glue("No table rows returned.
                        Check if the database {pg_conn_param$dbname}
                        has the table {pg_att_table}
                        with the field {field_to_tif}.")
  } else if (!(tb_type_df$data_type %in% c("smallint", "character varying",
                                           "integer", "boolean", "double precision",
                                           "text")))
  {
    return_msg <- glue("Unhandled dataype {tb_type_df$data_type}.
                       Convert field '{field_to_tif}' to one of the
                       following PostgreSQL data types:
                       {data_type_handled}.")
  } else if (!(ret[1] == 0))
  {
    return_msg <- glue("Feature class '{bnd}' not found in GDB '{gdb}'")
  } else {
    where_clause <- ""
    if ( (is.null(query)) || (nchar(query) == 0)) {
      where_clause <- ";"
    } else {
      where_clause <- glue(" where ", query, ";")
    }

    # find att_sk table
    as_sk <- 'as a '
    if (!(is.null(pg_attskey_table) || (nchar(pg_attskey_table) == 0))){
      as_sk <- glue(as_sk, 'left join ',
                    pg_attskey_table,
                    ' as sk ',
                    'on (a.',key_attskey_tbl, '=sk.', key_attskey_tbl, ') ')
    }

    # find geom table
    g <- 'a.'
    as_b <- ''

    if (!(is.null(pg_skgeom_table) || (nchar(pg_skgeom_table) == 0))){
      g <- "b."
      if (is.null(pg_attskey_table) || (nchar(pg_attskey_table) == 0)){
        as_b <- glue("left join ", pg_skgeom_table,
                     " as b on (b.", key_skgeo_tbl, "=a.",key_skgeo_tbl, ") ")
      } else {
        as_b <- glue("left join ", pg_skgeom_table,
                     " as b on (b.", key_skgeo_tbl, "=sk.",key_skgeo_tbl, ") ")
      }
    }

    build_sql <- glue("select a.", field_to_tif, ", ", g,
                      geom_field, " as geometry ",
                      "from ", pg_att_table, " ", as_sk, as_b,
                      where_clause)
    # get sf object
    src_sf_1 <- sf::st_read(con, query=build_sql)
    src_sf <- src_sf_1[!st_is_empty(src_sf_1), ]
    src_sf_convert <- NULL
    field_convert <- field_to_tif
    return_msg <- "starting..."

    if (tb_type_df$data_type %in% c("character varying", "text")){
      src_sf_cat <- as.data.frame(unique(src_sf[[1]]))
      src_sf_cat$val <- src_sf_cat[[1]]
      seq_count <- length(src_sf_cat$val)
      src_sf_cat$cat <- seq(1,seq_count)
      src_sf_cat_merge <- subset(src_sf_cat, select=c(2,3))
      src_sf_cat$cat_val <- paste0(" ", src_sf_cat$cat, ":",src_sf_cat$val)
      src_sf_save <- subset(src_sf_cat, select=c(4))
      cat_name <- gsub("\\.tif", "", dst_tif_name)
      cat_filepath <- glue(out_cat_path, "/",cat_name)
      write.table(src_sf_save, file = cat_filepath,
                  sep = "\t", row.names = FALSE, col.names = FALSE, quote = FALSE)
      src_sf_wcat <- merge(src_sf, src_sf_cat_merge,
                           by.x = field_to_tif, by.y='val')
      src_sf_convert <- subset(src_sf_wcat, select=c(2,3))
      field_convert <- "cat"
      return_msg <- "Created cat for text field and converted to raster."
    } else if (tb_type_df$data_type %in% c("double precision")) {
      src_sf_double <- src_sf
      src_sf_double$val1 <- src_sf_double[[1]] * d_multiplier
      src_sf_double$val <- round(src_sf_double[[3]], 0)
      src_sf_convert <- subset(src_sf_double, select=c(4,2))
      field_convert <- "val"
      return_msg <- "Multiplied field and converted to raster."
    } else if (tb_type_df$data_type %in% c("integer", "smallint", "boolean")) {
      src_sf_convert <- src_sf
      return_msg <- "Converted integer to raster."
    }

    dadmtools::rasterize_terra(src_sf = src_sf_convert,
                               field = field_convert,
                               template_tif = template_tif,
                               crop_extent = get_gr_skey_extent(dsn = gdb,
                                                                layer=bnd,
                                                                template_tif=template_tif),
                               src_lyr = NULL,
                               out_tif_path = out_tif_path,
                               out_tif_name = dst_tif_name,
                               datatype ='INT4S',
                               nodata = 0)

  }
  print (return_msg)
}
