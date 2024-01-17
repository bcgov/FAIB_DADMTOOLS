# FAIB_DATA_MANAGEMENT
Package of common FAIB data management functions

## Dependencies
 - Read/Write access to a PostgreSQL database (version 12 or above)

 - Installed version of GDAL Version 3.4 or above (https://www.gisinternals.com/index.html)
 
 - Installed version of Oracle Instant client (see  [installation instructions](oracle_fdw_install.md) )

 - Installed version of R Version 4.0 or above (https://cran.r-project.org/bin/windows/base/)
 
 ## PostgreSQL Setup
 - Requires database with the following extensions enabled:
 ```
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_raster;
CREATE EXTENSION oracle_fdw;
 ```
 - Requires the following schemas:
 ```
 CREATE SCHEMA raster;
 CREATE SCHEMA whse;
 ```

 ## R Setup
 - Requires the following R packages:
 ```
 install.packages("RPostgres")
 install.packages("glue")
 install.packages("terra")
 install.packages("keyring")
 install.packages("sf")
 install.packages("devtools")
 ```

## R Setup Environment Variables
Currently defaults to using variables setup with keyring lib.
Example of two common use cases: 
Set up environment variables to connect to "localpsql"
```
library(keyring)
keyring_create("localpsql")
key_set("dbuser", keyring = "localpsql", prompt = 'Postgres keyring dbuser:')
key_set("dbpass", keyring = "localpsql", prompt = 'Postgres keyring password:')
key_set("dbhost", keyring = "localpsql", prompt = 'Postgres keyring host:')
key_set("dbname", keyring = "localpsql", prompt = 'Postgres keyring dbname:')
```
Set up environment variables to connect to "oracle"
```
keyring_create("oracle")
key_set("dbuser", keyring = "oracle", prompt = 'Oracle keyring dbuser:')
key_set("dbpass", keyring = "oracle", prompt = 'Oracle keyring password:')
key_set("dbhost", keyring = "oracle", prompt = 'Oracle keyring host:')
key_set("dbservicename", keyring = "oracle", prompt = 'Oracle keyring serviceName:')
key_set("dbserver", keyring = "oracle", prompt = 'Oracle keyring server:')
```

Example of confirming your imputs:
```
key_get("dbuser", keyring = "localpsql")
key_get("dbpass", keyring = "localpsql")
key_get("dbhost", keyring = "localpsql")
key_get("dbname", keyring = "localpsql")
```

## R Library Library Import Instructions
```
library(devtools)
install_github("bcgov/FAIB_DATA_MANAGEMENT")
library(RPostgres)
library(glue)
library(terra)
library(keyring)
library(sf)
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
    Column names must match template above. Usage:
    - `srctype`: Type of source file. 
        - Options: `gdb, oracle, raster`
    - `srcpath`: Source path.
        - When `srctype = oracle` then `bcgw`
        - When `srctype = gdb or raster` then full path and filename
    - `srclyr`      : Layer name
        - When `srctype = oracle` then schema and layer name, e.g. `WHSE_FOREST_VEGETATION.bec_biogeoclimatic_poly`
    - `primarykey`  :
    - `suffix`      :
    - `tblname`     :
    - `src_query`   :
    - `inc`         :
    - `rslt_ind`    :
    - `fields2keep` : By default, all fields are retained. Use this field to filter fields to keep. 


    
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
