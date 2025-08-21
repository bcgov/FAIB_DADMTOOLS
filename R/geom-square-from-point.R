#' Add a polyon geometry field (1 hectare squares) to
#' a PostGres table and populate using a point geometry field.
#'@param pg_conn_param named (optional): list - connection parameters (default returned by dadmtools::get_pg_conn_list)
#' @param pg_geomTable  (required): string - PG table with point geometry
#' @param pg_exisiting_geomPoint  (required): Name of point geometry field
#' @param pg_add_geomName (required): string - name of new polygon geometry field
#=================================================

geom_square_from_point <- function(pg_conn_param = get_pg_conn_list(),
                                   pg_geomTable,
                                   pg_exisiting_geomPoint,
                                   pg_add_geomName)
{
  alter_sql <- glue("alter table ", pg_geomTable,
                    " add column ", pg_add_geomName,
                    " geometry(Polygon, 3005);")

  update_sql <- glue("update ", pg_geomTable,
                     " set ", pg_add_geomName, " = st_buffer(",
                     pg_exisiting_geomPoint, ", 50,
                  'endcap=square');")

  run_sql_r(alter_sql, pg_conn_param)

  run_sql_r(update_sql, pg_conn_param)
}
