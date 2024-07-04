# dadmtools
Package of common FAIB Data Analysis and Data Management team functions, focusing of functions to import vector into PG in the gr_skey grid lookup table.

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
 install_github("bcgov/FAIB_DADMTOOLS")
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
 - Ensure (pgpass file)[https://www.postgresql.org/docs/current/libpq-pgpass.html] is setup. Either by adding an environment variable PGPASSWORD with your postgres password OR saving the file on Windows at  %APPDATA%\postgresql\pgpass.conf where the file format is:
 `hostname:port:database:username:password`

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

Example of confirming your inputs:
```
## pg variables
key_get("dbuser", keyring = "localpsql")
key_get("dbpass", keyring = "localpsql")
key_get("dbhost", keyring = "localpsql")
key_get("dbname", keyring = "localpsql")

## oracle variables
key_get("dbuser", keyring = "oracle")
key_get("dbpass", keyring = "oracle")
key_get("dbhost", keyring = "oracle")
key_get("dbservicename", keyring = "oracle")
key_get("dbserver", keyring = "oracle")
```

Example of how to update your keyring when your BCGW password changes and you receive an error like this:
```
Error: Failed to fetch row : ERROR:  cannot authenticate connection to foreign Oracle server
DETAIL:  ORA-01017: invalid username/password; logon denied
```
Fix: update your oracle keyring with your new password
```
key_set("dbpass", keyring = "oracle", prompt = 'Oracle keyring password:')
```


## Usage
# R library import
```
library(devtools)
library(dadmtools)
library(RPostgres)
library(glue)
library(terra)
library(keyring)
library(sf)
```


# 1. Importing Spatial Data into postgres gr_skey tables

```
import_gr_skey_tif_to_pg_rast()
```

The above function creates two tables in PG database. It creates a raster table named `raster.grskey_bc_land` and an additional table (tablename specified by `dst_tbl` argument, default is `whse.all_bc_gr_skey`). The second table is the raster table converted to table with a geometry field (`geom`) representing the raster centroids. Note: table specified by `dst_tbl` argument, default is `whse.all_bc_gr_skey`, is the `gr_skey_tbl` argument that should be specificed in combination with `rslt_ind` and `suffix`.

Function takes the following inputs. Default values listed below:

```
import_gr_skey_tif_to_pg_rast(
    template_tif      = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
    mask_tif          = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
    crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5), ## c(xmin,xmax,ymin,ymax)
    out_crop_tif_name = ## no default
    pg_conn_param     = dadmtools::get_pg_conn_list(),
    dst_tbl           = 'whse.all_bc_gr_skey'
)
```

# 2.  Fill in configuration input csv file (i.e. [see example](config_parameters.csv))

Column names must match template above. Field description:
- `src_type`: Type of source file. raster option will only work when the raster matches the spatial resolution (100x100), alignment and projection (BC Albers) of the gr_skey grid. Example gr_skey file: S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif
    - Options: `gdb, oracle, raster, geopackage, gpkg, shapefile, shp`
- `src_path`: Source path.
    - When `srctype = oracle` then `bcgw`
    - When `srctype = gdb or raster or shp or geopackage` then full path and filename
- `src_lyr` : Layer name
    - When `src_type = oracle`, provide oracle schema and layer name, e.g. `WHSE_FOREST_VEGETATION.bec_biogeoclimatic_poly`
    - When `src_type = gdb`, provide layername within the file geodatabase, e.g. `tsa_boundaries_2020`
    - When `src_type = shp|shapefile`, provide the shapefile name without extension, e.g. `k3o_cfa`
    - When `src_type = gpkg|geopackage`, provide the layername within the geopackage, e.g. `FireSeverity_Final`
    - When `src_type = raster`, argument not used in import. It is imported into metadata table.
- `suffix` : Optional argument. Argument ignored unless `rslt_ind = 1`. When `rslt_ind = 1`, suffix used for column name creation in foreign table lookup, e.g. `pgid_<suffix>`
- `dst_schema` : Postgres destination schema name.
    - E.g. `whse`
- `dst_tbl` : Postgres destination table name.
    - E.g. `forest_harvesting_restrictions_july2023`
- `query` : Optional argument to filter source layer. When `src_path = 'bcgw'`, argument is applied to postgres fdw layer. Otherwise, argument used within ogr2ogr call.
    - E.g. `rr_restriction is not null` OR `rr_restriction = '01_National Park'` OR `strgc_land_rsrce_plan_name like '%Klappan%'`
- `inc` : Required argument whether to include layer when script is ran. 
    - 0 = exclude
    - 1 = include
- `rslt_ind` : When set to 1, used in combination with `suffix` and `gr_skey_tbl` (`gr_skey_tbl` is argument to `batch_import_to_pg_gr_skey` and `import_to_pg_gr_skey`). Option to add `pgid_<suffix>` for the specific imported table to previously imported PG `gr_skey_tbl` (Eg. `whse.all_bc_gr_skey`) 
    - 1 = include (i.e. will add primary key to gr_skey_tbl)
    - 0 = not included (i.e. will not add primary key to gr_skey_tbl)
- `flds_to_keep` : By default, all fields are retained. Use this field to filter fields to keep. Format is comma separated list (no spaces)
    - E.g. `REGEN_OBLIGATION_IND,FREE_GROW_DECLARED_IND,OBJECTID`
- `notes` : Notes
    - E.g. `This layer is very important.`
    
# 3.  Add datasets to postgres from csv input by calling

```
batch_import_to_pg_gr_skey()
```

Function takes the following inputs. Default values listed below:

```
batch_import_to_pg_gr_skey(
    in_csv            = 'config_parameters.csv',
    pg_conn_param     = dadmtools::get_pg_conn_list(),
    ora_conn_param    = dadmtools::get_ora_conn_list(),
    crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5), ## c(xmin,xmax,ymin,ymax)
    gr_skey_tbl       = 'whse.all_bc_gr_skey',
    raster_schema     = 'raster',
    template_tif      = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
    mask_tif          = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
    data_src_tbl      = 'whse.data_sources',
    out_tif_path      = ## no default
    import_rast_to_pg = FALSE
)
```
Use the `rslt_ind` field in Step 2's configuration input csv to indicate if the `pgid_<suffix>` primary key will be added to the gr_skey geometry table. 

[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](<Redirect-URL>)
