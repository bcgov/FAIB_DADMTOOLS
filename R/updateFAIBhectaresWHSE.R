#' Update FAIB hectares database from input csv of datasets
#'
#' @param inCSV coming soon
#' @param connList coming soon
#' @param cropExtent coming soon
#' @param fkLookupTbl coming soon
#' @param wrkSchema coming soon
#' @param rasSchema coming soon
#' @param templateRaster coming soon
#' @param dataSourceTblName coming soon
#' @param setwd coming soon
#'
#'
#' @return no return
#' @export
#'
#' @examples coming soon

updateFAIBhectaresWHSE <- function(inCSV = 'D:\\Projects\\provDataProject\\tools\\prov_data_resultant3.csv',
                                   connList = faibDataManagement::get_pg_conn_list(),
                                   oraConnList = faibDataManagement::get_ora_conn_list(),
                                   cropExtent = c(273287.5,1870587.5,367787.5,1735787.5),
                                   fkLookupTbl = 'all_bc_res',
                                   wrkSchema = 'whse',
                                   rasSchema = 'raster',
                                   templateRaster = 'raster.grskey_bc_land',
                                   dataSourceTblName = 'data_sources',
                                   setwd='D:/Projects/provDataProject',
                                   outTifpath = 'D:\\Projects\\provDataProject',
                                   import2pg = FALSE
                                   ){

  inFile <- read.csv(inCSV)
  inFile <- inFile[inFile$inc == 1,]
  dataSourceTblName <- glue::glue("{wrkSchema}.{dataSourceTblName}")

  for (row in 1:nrow(inFile)) {
    gc()
    #Get inputs from input file
    inc <- gsub("[[:space:]]",'',tolower(inFile[row, "inc"])) ##  1 = include(i.e. will not skip) 0 = not included (i.e. will skip)
    rslt_ind <- gsub("[[:space:]]",'',tolower(inFile[row, "rslt_ind"])) ##1 = include(i.e. will add data to provincial resultant) 0 = not included (i.e. will not add data to provincial resultant)
    srctype <- gsub("[[:space:]]",'',tolower(inFile[row, "srctype"])) ##format of data source i.e. gdb,oracle, postgres, geopackage, raster
    srcpath <- gsub("[[:space:]]",'',tolower(inFile[row, "srcpath"]))## path to input data. Note use bcgw for whse
    srclyr <- gsub("[[:space:]]",'',tolower(inFile[row, "srclyr"])) ## input layer name
    pk <- gsub("[[:space:]]",'',tolower(inFile[row, "primarykey"])) ## primary key field that will be added to resultant table
    suffix <- gsub("[[:space:]]",'',tolower(inFile[row, "suffix"])) ## suffix to be used in the resultant table
    nsTblm <- gsub("[[:space:]]",'',tolower(inFile[row, "tblname"])) ## name of output non spatial table
    query <- tolower(inFile[row, "src_query"])  ##where clause used to filter input dataset
    flds2keep <- gsub("[[:space:]]",'',tolower(inFile[row, "fields2keep"])) ## fields to keep in non spatial table

    ##convert whitespace to null when where clause is null
    if (query == '' || is.null(query) || is.na(query)) {
      # print("null is here")
      where_clause <- NULL
    }
    else {where_clause <- inFile[row, "src_query"]}


    #For included rows for csv
    if (inc == 1){
      if(tolower(srctype) != 'raster'){
        if(tolower(srctype) == 'oracle'){
          oraServer <- oraConnList["server"][[1]]
          idir <- oraConnList["user"][[1]]
          orapass <- oraConnList["password"][[1]]
          print("create Foreign Table in pG")
          fklyr <- faibDataManagement::createOracleFDWpg(oraServer, idir, orapass, srclyr,connList)
          print('write non spatial table to pg')
          faibDataManagement::fdwTbl2PGnoSpatial(fklyr, nsTblm,pk,outSchema = wrkSchema,connList=connList,attr2keep=flds2keep,where=where_clause)
          print("wrote non spatial fdw to postgres")
          print(fklyr)
          inSrcTemp <- NULL
          pgConnTemp <- connList
           }

        else {
          #Write none spatial table to postgres
          fklyr <- srclyr
          faibDataManagement::writeNoSpaTbl2PG(srcpath,nsTblm,connList,pk=pk,schema =wrkSchema,lyr= srclyr,where=where_clause,select=flds2keep)
          inSrcTemp <- srcpath
          pgConnTemp <- NULL
          print("wrote non spatial to postgres")}

        #Create tif from input
        outTifName <- glue("{nsTblm}.tif")
        print(outTifName)
        inRas <- rasterizeSF(fklyr,pk,outTifpath = outTifpath, outTifname= outTifName,inSrc = inSrcTemp,pgConnList = pgConnTemp,vecExtent = cropExtent,nodata = 0,where=where_clause)
        #
        # inRas <- faibDataManagement::rasterizeWithGdal(pk,srcpath, srclyr, cropExtent, where_clause = where_clause,outName =nsTblm)
        print("created Tiff")

        #Tif to PG Raster
        if(import2pg){
        inRas <- basename(inRas)
        pgRasName <- paste0(rasSchema,'.ras_',substr(inRas,1,nchar(inRas)-4))
        cmd<-paste0('raster2pgsql -s 3005 -d -C -r -P -I -M -t 100x100 ',inRas,' ', pgRasName,' | psql -d prov_data')
        print(cmd)
        shell(cmd)
        print('imported tif to pg')}

        # #Convert postgres Raster to Non spatial table with gr_skey
        joinTbl <- glue("{wrkSchema}.{nsTblm}_gr_skey")
        joinTbl2 <- Id(schema = wrkSchema, table = glue("{nsTblm}_gr_skey"))
        # faibDataManagement::pgRas2rows(joinTbl,pgRasName ,pk, templateRaster,connList)
        df <- tif2grskeytbl(inRas,cropExtent=cropExtent, valueColName=pk)
        faibDataManagement::df2PG(joinTbl2,df,connList)
        print('converted pg raster to long table')


        # file.remove(inRas)



      }
      #For Rasters
      else{
        # Crop raster to Extent
        inRas <- paste0(srclyr,".tif")
        faibDataManagement::gdalCropRaster(srcpath,cropExtent,inRas )
        print("cropped raster")

        #Tif to PG Raster
        pgRasName <- paste0('raster.ras_',substr(inRas,1,nchar(inRas)-4))
        cmd<-paste0('raster2pgsql -s 3005 -d -I -C -M -t 100x100 ',inRas,' ', pgRasName,' | psql -d prov_data')
        print(cmd)
        shell(cmd)

        #Convert Raster to Non spatial table with gr_skey
         joinTbl <- paste0(wrkSchema, '.',nsTblm, "_gr_skey")
         df <- tif2grskeytbl(inRas,cropExtent=cropExtent, valueColName=pk)

        # faibDataManagement::sendSQLstatement(paste0("drop table if exists ",joinTbl, " CASCADE;"),connList)
        faibDataManagement::df2PG(joinTbl,df,connList)
        # faibDataManagement::pgRas2rows(joinTbl,pgRasName ,pk,templateRaster,connList)


        if(rslt_ind == 1){
          faibDataManagement::updateFKlookupPG(joinTbl,pk,suffix,fkLookupTbl,connList)
          print("created new foreign key lookup table")


          #Update Metadata tables
          faibDataManagement::updateFKfldTablePG(joinTbl,fkLookupTbl,suffix,connList)

          srcpath <- gsub("[[:space:]]",'',tolower(inFile[row, "srcpath"]))
          print('srcpath - 1')

          faibDataManagement::updateFKsrcTblpg(dataSourceTblName,srctype,srcpath,srclyr,pk,suffix,nsTblm,query,inc,rslt_ind,flds2keep)
          print("updated meta data table")

          faibDataManagement::sendSQLstatement(paste0("drop table if exists ",joinTbl, ";"),connList)
          faibDataManagement::sendSQLstatement(paste0("drop table if exists ",fkLookupTbl,"_old;"),connList)
          print("deleted excess tables")
          faibDataManagement::sendSQLstatement(paste0("vacuum;"),connList)

        }
        next

      }
      if(rslt_ind == 1){
        fkLookupTbl <- glue("{wrkSchema}.{fkLookupTbl}")
        faibDataManagement::updateFKlookupPG(joinTbl,pk,suffix,fkLookupTbl,connList)
        print("created new foreign key lookup table")

        #Update Metadata tables
        faibDataManagement::updateFKfldTablePG(nsTblm,fkLookupTbl,suffix,connList)

        srcpath <- gsub("[[:space:]]",'',tolower(inFile[row, "srcpath"]))
        print('srcpath - 1')

        faibDataManagement::updateFKsrcTblpg(dataSourceTblName,srctype,srcpath,srclyr,pk,suffix,nsTblm,query,inc,rslt_ind,flds2keep,connList)
        print("updated data sources table")

        faibDataManagement::sendSQLstatement(paste0("drop table if exists ",joinTbl, ";"),connList)
        faibDataManagement::sendSQLstatement(paste0("drop table if exists ",fkLookupTbl,"_old;"),connList)
        print("deleted excess tables")
        faibDataManagement::sendSQLstatement(paste0("vacuum;"),connList)
      }



    }}
}


