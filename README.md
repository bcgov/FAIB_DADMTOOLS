# dadmtools
Package of common FAIB Data Analysis and Data Management team functions, focusing on functions to import vector into PG in the gr_skey grid lookup table.

### Install supporting software on PC

#### Postgres
 - Requires PostgreSQL database (version 12 or above). During installation, be sure to install the dependancies for `postgis` and `postgis_raster`. After installation, ensure that the connection details are in the User Variables. For example:
  - variable = PGPORT, value = 5432
  - variable = PGUSER, value = postgres (or whatever user you are using)

- Optional:
Add your postgres password to the User variables. This allows you to sign into psql without providing your password. You can either create an environment variable:
  - PGPASSWORD = your password

#### Oracle Instant Client
 - Follow instructions provided here to install Oracle Instant client (see  [installation instructions](oracle_fdw_install.md) ) in order to get dependencies required for PostgreSQL Oracle Foreign Data Wrapper extension: `oracle_fdw`. Ensure to follow the instructions in the link around adding the two OCI paths to your System Path environment variable or neither the R library or PostgreSQL will be able to connect to the BCGW. Example of paths to add to System Path environment variable:
  - C:\Data\localApps\OCI
  - C:\Data\localApps\OCI\instant_client_23_4

#### GDAL
 - Requires GDAL Version 3.4 or above (https://www.gisinternals.com/index.html). Be sure that GDAL_DATA and GDAL_DRIVER_PATH are installed in the System Variables. For example, 
  - variable = GDAL_DATA, value = C:\Program Files\GDAL\gdal-data
  - variable = GDAL_DRIVER_PATH, value = C:\Program Files\GDAL\gdalplugins

### PostgreSQL Required Extensions and Schemas

1. Once PostgreSQL and dependancies are installed, enable database with the following extensions enabled:
 ```
CREATE EXTENSION postgis;
CREATE EXTENSION postgis_raster;
CREATE EXTENSION oracle_fdw;
 ```

2. Create two required schemas:
 ```
 CREATE SCHEMA raster;
 CREATE SCHEMA whse;
 ```

### Install Packages in R
 - Installed version of R Version 4.0 or above (https://cran.r-project.org/bin/windows/base/)

 - Requires the following R packages:
 ```
 install.packages("RPostgres")
 install.packages("glue")
 install.packages("terra")
 install.packages("keyring")
 install.packages("sf")
 install.packages("devtools")
 library(devtools)
 install_github("bcgov/FAIB_DADMTOOLS")
 ```
 

### Run R scripts to setup raster process
The dadmtools library uses the Windows Credential Manager Keyring to manage passwords. Two keyings are required for most usage within the library. Instructions below show how to create two required keyrings: `localsql` and `oracle`. 

Set up "localpsql" keyring:
```
library(keyring)
keyring_create("localpsql")
key_set("dbuser", keyring = "localpsql", prompt = 'Postgres keyring dbuser:')
key_set("dbpass", keyring = "localpsql", prompt = 'Postgres keyring password:')
key_set("dbhost", keyring = "localpsql", prompt = 'Postgres keyring host:')
key_set("dbname", keyring = "localpsql", prompt = 'Postgres keyring dbname:')
```

Set up "oracle" keyring:
```
keyring_create("oracle")
key_set("dbuser", keyring = "oracle", prompt = 'Oracle keyring dbuser:')
key_set("dbpass", keyring = "oracle", prompt = 'Oracle keyring password:')
key_set("dbhost", keyring = "oracle", prompt = 'Oracle keyring host:')
key_set("dbservicename", keyring = "oracle", prompt = 'Oracle keyring serviceName:')
key_set("dbserver", keyring = "oracle", prompt = 'Oracle keyring server:')
```

Example of confirming your keyring inputs:
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

# 1. Importing Spatial Data into postgres gr_skey tables

```
library(dadmtools)
import_gr_skey_tif_to_pg_rast()
```

The above function creates two tables in PG database. It creates a raster table within the `raster` schema named `raster.grskey_bc_land` and a second table which defaults to `whse.all_bc_gr_skey` and can be specified using the `dst_tbl` argument (format: `schema_name.table_name`). The second table is the raster table converted to a table with a geometry field (`geom`) representing the raster centroids. You can choose either Centroid or Polygon for the geometry type. The function defaults to "Centroid".

Function takes the following inputs. Default values listed below:

```
import_gr_skey_tif_to_pg_rast(
    out_crop_tif_name = , ## no default, provide filename of the written 
    template_tif      = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
    mask_tif          = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
    crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5), ## c(xmin,xmax,ymin,ymax)
    pg_conn_param = dadmtools::get_pg_conn_list(),
    dst_tbl           = 'whse.all_bc_gr_skey',
    rast_sch          = "raster",
    pg_rast_name      = "grskey_bc_land",
    geom_type         = 'Centroid'
)
```

Example function call using all defaults and specifying out_crop_tif_name:
```
import_gr_skey_tif_to_pg_rast(
    out_crop_tif_name = 'C:\\projects\\data\\gr_skey_grid.tif'
)
```

# 2.  To import data layers, first fill in configuration input csv file (i.e. [see example](config_parameters.csv))
The main function within the package, `batch_import_to_pg_gr_skey`, uses an input configuration file which is described below. An example configuation csv file is included within the root directory of this repository and linked above. It is recommended that you edit the provided example configuration file for your usage. The function: `batch_import_to_pg_gr_skey` 

Column names must match template above. Field description:
- `include` : Required argument whether to include layer when script is ran. 
    - 0 = exclude
    - 1 = include
- `overlap_ind` : Required argument indicating whether layer to import contains overlap. If set to TRUE, the function will import duplicate gr_skey raster cells for each overlapping spatial feature with the corresponding pgid to relate to the attribute table.
    - TRUE = Layer to import contains overlaps
    - FALSE = Layer to import does not contain overlaps
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
- `dst_schema` : Postgres destination schema name.
    - E.g. `whse`
- `dst_tbl` : Postgres destination table name.
    - E.g. `forest_harvesting_restrictions_july2023`
- `query` : Optional argument to filter source layer. When `src_path = 'bcgw'`, argument is applied to postgres fdw layer. Otherwise, argument used within ogr2ogr call.
    - E.g. `rr_restriction is not null` OR `rr_restriction = '01_National Park'` OR `strgc_land_rsrce_plan_name like '%Klappan%'`
- `flds_to_keep` : By default, all fields are retained. Use this field to filter fields to keep. Format is comma separated list (no spaces)
    - E.g. `REGEN_OBLIGATION_IND,FREE_GROW_DECLARED_IND,OBJECTID`
- `overlap_group_fields` : 
- `notes` : Notes
    - E.g. `This layer is very important because bee boop.`

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

Example usage using defaults: 
```
batch_import_to_pg_gr_skey(
    in_csv            = 'C:\\projects\\data\\config_parameters.csv',
    out_tif_path      = 'C:\\projects\\data\\'
)
```


[![Lifecycle:Experimental](https://img.shields.io/badge/Lifecycle-Experimental-339999)](<Redirect-URL>)
