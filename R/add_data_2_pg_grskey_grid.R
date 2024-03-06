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
  rslt_ind <- gsub("[[:space:]]",'',tolower(rslt_ind)) ## 1 = include(i.e. will add primary key to gr_skey tbl) 0 = not included (i.e. will not add primary key to gr_skey table)
  srctype <- gsub("[[:space:]]",'',tolower(srctype)) ## format of data source i.e. gdb,oracle, postgres, geopackage, raster
  srcpath <- gsub("[[:space:]]",'',tolower(srcpath))## path to input data. Note use bcgw for whse
  srclyr <- gsub("[[:space:]]",'',tolower(srclyr)) ## input layer name
  pk <- gsub("[[:space:]]",'',tolower(pk)) ## primary key field that will be added to resultant table
  suffix <- gsub("[[:space:]]",'',tolower(suffix)) ## suffix to be used in the resultant table
  nsTblm <- gsub("[[:space:]]",'',tolower(nsTblm)) ## name of output non spatial table
  query <- query  ## where clause used to filter input dataset
  flds2keep <- gsub("[[:space:]]",'',tolower(flds2keep)) ## fields to keep in non spatial table
  dataSourceTblName <- glue::glue("{wrkSchema}.{dataSourceTblName}")

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
      print("Create Foreign Table in pG")
      outServerName <- 'oradb'
      fklyr <- createOracleFDWpg(srclyr, oraConnList, connList, outServerName, fdwSchema)

      ####### Importing FDW table into  r######
      print("Importing FDW table into R")
      qry <- getFDWtblSpSQL(srclyr, pk, connList, fdwSchema, where=query)
      print(qry)
      connz <- dbConnect(connList["driver"][[1]],
                       host     = connList["host"][[1]],
                       user     = connList["user"][[1]],
                       dbname   = connList["dbname"][[1]],
                       password = connList["password"][[1]],
                       port     = connList["port"][[1]])
      on.exit(RPostgres::dbDisconnect(connz))
      castList <- c("MULTIPOLYGON","MULTIPOINT","MULTILINE")
      for (i in castList) {
        #ERROR HANDLING
        possibleError <- tryCatch(
          inSF <- st_cast(st_read(connz, query = qry, crs = 3005),i ),
          error=function(e) e
        )
        if(inherits(possibleError, "error")) next else { break }
      }
      print(nrow(inSF))
      #####################Rasterize using TERRA#########
      outTifName <- glue("{nsTblm}.tif")
      inRas <- rasterizeTerra(
        inSrc      = inSF,
        field      = pk,
        template   = grskeyTIF,
        cropExtent = cropExtent,
        outTifpath = outTifpath,
        outTifname = outTifName,
        datatype   ='INT4S'
      )

      ##############################################
      print(paste("Write Non-spatial FDW table:", fklyr, "to PG"))
      faibDataManagement::fdwTbl2PGnoSpatial(
                                              fklyr,
                                              nsTblm,
                                              pk,
                                              outSchema = wrkSchema,
                                              connList  = connList,
                                              attr2keep = flds2keep,
                                              where     = query
      )
      print(paste("Wrote Non-spatial FDW table:", fklyr, "to PG"))
      inSrcTemp <- NULL
      pgConnTemp <- connList
    } else {
      #Write non-spatial table to postgres
      fklyr <- srclyr
      print(paste("Write Non-spatial table:", fklyr, "to PG"))
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
      print(paste("Wrote Non-spatial table:", fklyr, "to PG"))

      #Create tif from input
      outTifName <- glue("{nsTblm}.tif")
      print(paste("Write tif:", outTifName))
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
      print(paste("Wrote tif:", outTifName))
    }
  }

  if(tolower(srctype) == 'raster') {
    outTifName <- glue("{nsTblm}.tif")
    inRas <- srcpath
  }

  inRasbase <- basename(inRas)
  #Tif to PG Raster
  if(importrast2pg) {
    pgRasName <- paste0(rasSchema, '.ras_', substr(inRasbase, 1, nchar(inRasbase)-4))
    cmd <- paste0('raster2pgsql -s 3005 -d -C -r -P -I -M -t 100x100 ',inRas,' ', pgRasName,' | psql -d prov_data')
    print(cmd)
    shell(cmd)
    print('Imported tif to PG')
  }

  # #Convert postgres raster to Non spatial table with gr_skey
  joinTbl <- glue("{wrkSchema}.{nsTblm}_gr_skey")
  joinTblnoschema <- glue("{nsTblm}_gr_skey")
  joinTbl2 <- RPostgres::Id(schema = wrkSchema, table = joinTblnoschema)

  faibDataManagement::df2PG(
                            joinTbl2,
                            faibDataManagement::tif2grskeytbl(
                                                              inRas,
                                                              cropExtent   = cropExtent,
                                                              grskeyTIF    = grskeyTIF,
                                                              maskTif      = maskTif,
                                                              valueColName = pk
                            ),
                            connList
  )
  print('Created PG table from values in tif and gr_skey')

  #Create index on table
  print('Creating index for gr_skey table')
  faibDataManagement::sendSQLstatement(paste0("drop index if exists ", joinTblnoschema, "_grskey_inx;"), connList)
  print('dropped index')
  faibDataManagement::sendSQLstatement(glue::glue("create index {joinTblnoschema}_grskey_inx on {joinTbl}(gr_skey);"),connList)
  faibDataManagement::sendSQLstatement(glue("ANALYZE {joinTbl};"),connList)

  if(rslt_ind == 1) {
    gr_skey_tbl <- glue("{wrkSchema}.{gr_skey_tbl}")
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
    faibDataManagement::sendSQLstatement(paste0("vacuum;"), connList)
    faibDataManagement::sendSQLstatement(glue("ANALYZE {gr_skey_tbl};"), connList)
  }
}

