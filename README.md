# FAIB_DATA_MANAGEMENT
Package of common FAIB data management functions

## Dependencies and installation
 - Read/Write access to a PostgreSQL database (version 12 or above) with Postgis and Postgis Raster extension

 - Installed version of GDAL Version 3.4 or above (https://www.gisinternals.com/index.html)
 
 - Installed version of Oracle Instant client (see  [installation instructions](oracle_fdw_install.md) )

 - Installed version of R Version 4.0 or above (https://cran.r-project.org/bin/windows/base/)

 - Requires the following R packages:
 ```
 install.packages("RPostgres")
 install.packages("glue")
 install.packages("terra")
 install.packages("keyring")
 install.packages("sf")
 install.packages("devtools")
 ```
 
 ## PostgreSQL Configuration
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

## R First Time Configuration
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

## Usage
# R library import
```
library(devtools)
install_github("bcgov/FAIB_DATA_MANAGEMENT")
library(RPostgres)
library(glue)
library(terra)
library(keyring)
library(sf)

```


# 1. Importing Spatial Data into postgres gr_skey tables

```
gr_skey_tif_2_pg_geom()
```

The above function creates two tables in PG database. It creates a raster table named `raster.grskey_bc_land` and an additional table (tablename specified by `pgtblname` argument, default is `whse.all_bc_gr_skey`). The second table is the raster table converted to table with a geometry field (`geom`) representing the raster centroids. Function

Function takes the following inputs. Default values listed below:

```
gr_skey_tif_2_pg_geom(
    grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
    maskTif = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Lands_and_Islandsincluded.tif',
    cropExtent = c(273287.5,1870587.5,367787.5,1735787.5), ## c(xmin,xmax,ymin,ymax)
    outCropTifName = 'D:\\Projects\\provDataProject\\gr_skey_cropped.tif', ## output destination tif filename
    connList = faibDataManagement::get_pg_conn_list(),
    pgtblname = 'whse.all_bc_gr_skey'
)
```

# 2.  Fill in configuration input csv file (i.e. [see example](inputsDatasets2load2PG.csv))

    Column names must match template above. Field description:
    - `srctype`: Type of source file. 
        - Allowable options: `gdb, oracle, raster, geopackage, shp`
    - `srcpath`: Source path.
        - When `srctype = oracle` then `bcgw`
        - When `srctype = gdb or raster` then full path and filename
    - `srclyr` : Layer name
        - When `srctype = oracle` then schema and layer name, e.g. `WHSE_FOREST_VEGETATION.bec_biogeoclimatic_poly`
    - `primarykey` : Required source file primary key, must be integer.
    - `suffix` : Used in resultant columns that were kept from the data source (e.g. "vri2021")
    - `tblname` : Postgres destination table name.
        - E.g. `forest_harvesting_restrictions_july2023` TODO schema?
    - `src_query` : Optional argument to filter source layer
        - E.g. `rr_restriction is not null` OR `rr_restriction = '01_National Park'` OR `strgc_land_rsrce_plan_name like '%Klappan%'`
    - `inc` : Required argument whether to include layer when script is ran. 
        - E.g. 0 = exclude, 1 = include
    - `rslt_ind` : Option to add primary key to imported PG gr_skey table
        - 1 = include (i.e. will add primary key to gr_skey tbl)
        - 0 = not included (i.e. will not add primary key to gr_skey table)
    - `fields2keep` : By default, all fields are retained. Use this field to filter fields to keep. Format is comma separated list (no spaces)
        - E.g. `REGEN_OBLIGATION_IND,FREE_GROW_DECLARED_IND,OBJECTID`

    
# 3.  Add datasets to postgres from csv input by calling

```
add_batch_2_pg_grskey_grid()
```

Function takes the following inputs. Default values listed below:

```
add_batch_2_pg_grskey_grid(
    inCSV = 'D:\\Projects\\provDataProject\\tools\\prov_data_resultant3.csv',
    connList = faibDataManagement::get_pg_conn_list(),
    oraConnList = faibDataManagement::get_ora_conn_list(),
    cropExtent = c(273287.5,1870587.5,367787.5,1735787.5), ## c(xmin,xmax,ymin,ymax)
    gr_skey_tbl = 'all_bc_res_gr_skey',
    wrkSchema = 'whse',
    rasSchema = 'raster',
    grskeyTIF = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
    maskTif='S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Lands_and_Islandsincluded.tif',
    dataSourceTblName = 'data_sources',
    setwd='D:/Projects/provDataProject',
    outTifpath = 'D:\\Projects\\provDataProject',
    importrast2pg = FALSE
)
```
Use the `rslt_ind` field in Step 2's configuration input csv to indicate if the primary key will be added to the gr_skey geometry table or as its own table with a gr_skey attibute.





[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](<Redirect-URL>)
