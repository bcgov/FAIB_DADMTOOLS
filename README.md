# dadmtools
Package of common FAIB Data Analysis and Data Management team functions, focusing on functions to import vector into PG in the gr_skey grid lookup table.

### 1. Install supporting software on PC

#### Postgres
 - Requires PostgreSQL database (version 12 or above). During installation, be sure to install the dependancies for `postgis` and `postgis_raster`. 

#### Oracle Instant Client
 - Follow instructions provided here to install Oracle Instant client (see  [installation instructions](oracle_fdw_install.md) ) in order to get dependencies required for PostgreSQL Oracle Foreign Data Wrapper extension: `oracle_fdw`. 

#### GDAL
 - Requires GDAL Version 3.4 or above (https://www.gisinternals.com/index.html). 

### 2. Environment Settings

After Postgres installation, ensure that the connection details are in the Environment User Variables. For example:

  - variable = PGPORT, value = 5432
  - variable = PGUSER, value = postgres (or whatever user you are using)

- Optional:
Add your postgres password to the User variables. This allows you to sign into psql without providing your password. You can either create an environment variable:

  - PGPASSWORD = your password

After installed the Oracle Instant Client, ensure to follow the instructions in the link around adding the two OCI paths to your System Path environment variable or neither the R library or PostgreSQL will be able to connect to the BCGW. Example of paths to add to System Path Environment Variable:

  - C:\Data\localApps\OCI
  - C:\Data\localApps\OCI\instant_client_23_4

After GDAL installation, be sure that GDAL_DATA and GDAL_DRIVER_PATH are installed in the System Variables. For example:

  - variable = GDAL_DATA, value = C:\Program Files\GDAL\gdal-data
  - variable = GDAL_DRIVER_PATH, value = C:\Program Files\GDAL\gdalplugins


### 3. PostgreSQL Required Extensions and Schemas

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

### 4. Install Packages in R
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
 

### 5. Set up Database Connections in R 
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

### 6. Importing Spatial Data into postgres

#### dadmtool library function: import_gr_skey_tif_to_pg_rast

Before importing any spatial layers, you must first import a `gr_skey` raster (.tif) into PostgreSQL. This is done using the function: `import_gr_skey_tif_to_pg_rast`.

**Function description**

The function creates two tables in the PostgreSQL database:
1. Raster table
 - Default name: `raster.grskey_bc_land`
 - You can specify a different schema using the `rast_sch` argument and a different table name using the `pg_rast_name` argument.

2. Vector Table (Geometry Representation)
 - Default name: `whse.all_bc_gr_skey`
 - Fields:
  - `gr_skey integer`
  - `geom geometry(Point,3005) OR geom geometry(Polygon,3005)`
 - You can specify a different schema and table name using the `dst_tbl` argument (format: "schema_name.table_name").
 - This table contains a geometry column (geom), representing raster centroids by default.
 - You can choose either "Centroid" or "Polygon" using the `geom_type` argument. The function defaults to "Centroid".

**Function overview**

Example function call using all defaults and specifying out_crop_tif_name:
```
import_gr_skey_tif_to_pg_rast(
    out_crop_tif_name = 'C:\\projects\\data\\gr_skey_grid.tif'
)
```

The function takes the following inputs, with default values listed below. A few notes:

 - `template_tif`: must have a BC Albers (I.e. EPSG: 3005) coordinate reference system. For TSR, it is strongly recommended to provide a gr_skey raster with 100m cell resolution.
 - `mask_tif`: Must have the same crs and resolution as `template_tif`.

```
library(dadmtools)
import_gr_skey_tif_to_pg_rast(
    out_crop_tif_name = , # No default; provide the filename of the output cropped TIFF
    template_tif      = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
    mask_tif          = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
    crop_extent       = c(273287.5,1870587.5,367787.5,1735787.5), ## c(xmin,xmax,ymin,ymax)
    pg_conn_param     = dadmtools::get_pg_conn_list(),
    dst_tbl           = 'whse.all_bc_gr_skey',
    rast_sch          = "raster",
    pg_rast_name      = "grskey_bc_land",
    geom_type         = 'Centroid'
)
```

**Understanding `gr_skey`**
 - The `gr_skey` is a globally unique identifier, where each hectare of land in BC is assigned a static integer value.
 - The table, `whse.all_bc_gr_skey`, is a vector reprentation (Point or Polygon) of the gr_skey raster where each row represents a single raster pixel (i.e., one hectare), identified by its `gr_skey` value.
 - It allows for easy SQL joins using `gr_skey` with other tables imported using the same process and rasterized to the same grid.


#### dadmtool library function: import_to_pg_gr_skey

**Function description**

*Vector Import*
If importing a *vector* (Ex. shp, fgdb, geopackage, etc), the function imports two tables into PostgreSQL:
1. Vector attribute table with no geometry
 - The function, `import_to_pg_gr_skey`, imports the input vector layer attribute table into PostgreSQL.
 - A new unique incrementing integer primary key field is created: `pgid`
2. Raster Attribute Table
The function then rasterizes the input vector layer and converts the raster into a raster attribute table. In other words, each record represent a raster pixel (Ie. one hectare)
  - The `gr_skey` serves as the primary key.
  - The `pgid` field links the raster attribute table to the vector attribute table

The function rasterizes using the gr_skey raster properties (e.g. resolution, cell size) as a template,  defined by argument: `template_tif`.  There is also a option to provide  a mask raster, defined by argument: `mask_tif`, to spatially filter out values from the input data. See below code snippet for an example of importing a vector: `whse_admin_boundaries.adm_nr_districts_sp` into PostgreSQL in the gr_skey format.

*Raster Import*
If importing a *raster* (Ex. Geotiff), the raster is required to have BC Albers coordinate reference system, (Ie. EPSG: 3005), the same grid definition as the `template_tif` and `mask_tif` provided to the `import_gr_skey_tif_to_pg_rast` function and only one band. For TSR, it is recommended to use the gr_skey grid. The function imports *one* table into PostgreSQL.

1. Raster Attribute Table
The function imports the raster attribute table including `gr_skey` and the raster value. 
  - The `gr_skey` serves as the primary key.
  - The `val` is the input raster value


 **Function Example**
Example of importing the vector: whse_admin_boundaries.adm_nr_districts_sp from BCGW into postgres database in gr_skey format:
 
 ```
 import_to_pg_gr_skey(
  src_type             = 'oracle',
  src_path             = 'bcgw',
  src_lyr              = 'whse_admin_boundaries.adm_nr_districts_sp',
  dst_tbl              = 'adm_nr_districts_sp',
  query                = '',
  flds_to_keep         = NA,
  notes                = '',
  overlap_ind          = FALSE,
  overlap_group_fields = '',
  out_tif_path         = 'C:\\projects\\requests\\2025-03-10_test_dadmtools\\output\\',
  pg_conn_param        = dadmtools::get_pg_conn_list(),
  ora_conn_param       = dadmtools::get_ora_conn_list(),
  crop_extent          = c(273287.5, 1870587.5, 367787.5, 1735787.5),
  dst_schema           = 'sandbox',
  raster_schema        = 'raster',
  fdw_schema           = 'load',
  template_tif         = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\bc_01ha_gr_skey.tif',
  mask_tif             = 'S:\\FOR\\VIC\\HTS\\ANA\\workarea\\PROVINCIAL\\BC_Boundary_Terrestrial.tif',
  data_src_tbl         = 'whse.data_sources',
  import_rast_to_pg    = FALSE,
  grskey_schema        = NULL
)
```

The function imported the following two tables into PostgreSQL:

Vector attribute table: `sandbox.adm_nr_districts_sp`


Raster attribute table: `sandbox.adm_nr_districts_sp_gr_skey`

Entity Relationship diagram for newly imported tables:
![Image](https://github.com/user-attachments/assets/802bacd8-1222-4b83-9da5-b36570d271b1)

Since each row in `sandbox.adm_nr_districts_sp_gr_skey` represents one hectare, this structure makes it easy to perform area calculations and join with other table that contain `gr_skey`. Example query calculating area in hectares by Natural Resource District:
```
SELECT
    adm.district_name,
    count(*) as ha
FROM
sandbox.adm_nr_districts_sp adm
JOIN sandbox.adm_nr_districts_sp_gr_skey adm_key ON adm.pgid = adm_key.pgid
GROUP BY 
    adm.district_name
```

In order to import more than one layer at a time, use the batch import function which is explained next.

#### dadmtool library function: batch_import_to_pg_gr_skey

To import many spatial layers, use the function: `batch_import_to_pg_gr_skey` which requires populating a configuration input csv file (i.e. see example [config_parameters.csv](config_parameters.csv))

It is recommended that you edit the provided example configuration file for your usage. 

**Data Dictionary:** 

- `include` : Required argument whether to include layer when script is ran. 
    - 0 = exclude
    - 1 = include
- `overlap_ind` : TRUE or FALSE.  If TRUE, it indicates that the input spatial layer has overlaps and imported <dst_tbl>_gr_skey table will duplicate gr_skey records where spatial overlaps occur.  If FALSE, spatial overlaps will be ignored (i.e only the higher pgid value will be kept when overlaps occur)
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
- `overlap_group_fields` : The field groupings that will be used to handle spatial overlaps. I.e. each unique combination of the specified fields will be rasterized separately.
- `notes` : Notes
    - E.g. `Downloaded layer from URL.. etc.`

#### Add datasets to postgres from csv input by calling

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
