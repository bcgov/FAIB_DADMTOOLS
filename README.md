# FAIB_DATA_MANAGEMENT
Package of common faib data management functions

## Dependencies
 - Read/Write access to a postgreSQL database

 - Installed version of GDAL Version 3.4 or above (https://www.gisinternals.com/index.html)
 
 - Installed version of Oracle Instant client (see  [installation instructions](oracle_fdw_install.md) )
 
 - Installed oracle_fdw PostgreSQL extension (see  [installation instructions](oracle_fdw_install.md)) 

 - Installed version of R Version 4.0 or above (https://cran.r-project.org/bin/windows/base/)
 
 The following R packages need to be installed for this package to work.
 - RPostgres
 - glue
 - terra
 - keyring
 - sf
 
## Install Instructions
```
library(devtools)
install_github("bcgov/FAIB_DATA_MANAGEMENT")
```
 
## Importing Spatial Data into postgres gr_skey tables

1.  Create gr_skey table in postgres with geom by calling  <br> 
    ```gr_skey_tif_2_pg_geom(cropExtent = c(xmin,xmax,ymin,ymax))```<br> 
    
Available options and their corresponding default values are listed below:
 - grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
 - maskTif = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Lands_and_Islandsincluded.tif',
 - cropExtent = c(273287.5,1870587.5,367787.5,1735787.5),
 - outCropTifName = 'D:\\Projects\\provDataProject\\gr_skey_cropped.tif',
 - connList = faibDataManagement::get_pg_conn_list(),
 - pgtblname = "whse.all_bc_gr_skey
    
2.  Fill in input csv file (i.e. [see example](inputsDatasets2load2PG.csv)) <br>
    Column names must match template above
    
3.  Add datasets to postgres from csv input by calling <br> 
    ```add_batch_2_pg_grskey_grid()```  <br>                                                                    Use the rslt_ind option to indicate if the primary key will be added to the gr_skey geometry table or as its own table with a gr_skey attibute. <br> 

Available options and their corresponding default values are listed below:
 - inCSV = 'D:\\Projects\\provDataProject\\tools\\prov_data_resultant3.csv'
 - connList = faibDataManagement::get_pg_conn_list()
 - oraConnList = faibDataManagement::get_ora_conn_list()
 - cropExtent = c(273287.5,1870587.5,367787.5,1735787.5)
 - gr_skey_tbl = 'all_bc_res_gr_skey'
 - wrkSchema = 'whse'
 - rasSchema = 'raster'
 - grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif'
 - maskTif='S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Lands_and_Islandsincluded.tif'
 - dataSourceTblName = 'data_sources'
 - setwd='D:/Projects/provDataProject'
 - outTifpath = 'D:\\Projects\\provDataProject'
 -importrast2pg = FALSE





[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](<Redirect-URL>)
