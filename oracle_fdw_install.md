# Installation: Oracle Foreign Data Wrapper

# Table of Contents

[Oracle Instant Client Installation](##Oracle%20Instant%20Client%20Installation)  
[Oracle Foreign Data Wrapper](##Oracle%20Foreign%20Data%20Wrapper)  
[Adding the Extension in PostgreSQL](##Adding%20the%20Extension%20in%20PostgreSQL)  





## Oracle Instant Client Installation

- Oracle Foreign Data Wrappers requires an installation of the Oracle instant client.
- it will not work with the OSGEO4W oci installationas it is incomplete
- you must use the official Oracle OCI install

Download the latest version of the following: instantclient-basiclite-windows.x64-<latest version>.zip

From : https://www.oracle.com/ca-en/database/technologies/instant-client/winx64-64-downloads.html

The following steps assume you downloaded version 21.12 - change to reflect the version you downloaded.
Copy and extract to:

Unzip so that folder: instantclient_21_12
is created.  

Copy this folder to:   

C:\Data\localApps\OCI\instantclient_21_12

Add  
C:\Data\localApps\OCI\instantclient_21_12
C:\Data\localApps\OCI

to **SYSTEMS** path

There is a package for sqlplus on the same site: instantclient-sqlplus-windows.x64-19.10.0.0.0dbru.zip  
This can be useful for testing connections


Downloads are also available on: G:\!Project\U5\SoftWare\PostgreSQL13  



## Oracle Foreign Data Wrapper

Download the version that matches your PostgreSQL version. Hint: expand Assets to see the file names of the zipped executables. At the time of this documentation, it was: 

oracle_fdw-2.5.0-pg14-win64.zip

from: https://github.com/laurenz/oracle_fdw/releases

Unzip to a temp location and then copy from the folder structure:  

copy from:  
- share/extension  
	- oracle_fdw.control  
	- oracle_fdw--1.2.sql  
	
copy to (your verson of):  
- C:\Program Files\PostgreSQL\14\share\extension  

copy from:  
- lib  
	- oracle_fdw.dll 
	
copy to (your verson of):  
- C:\Program Files\PostgreSQL\14\lib

**Stop and restart the PostgreSQL server**


## Adding the Extension in PostgreSQL
in PostgreSQL interactive console (Eg. PgAdmin Query Tool or psql) add the extension:  
\>`CREATE EXTENSION oracle_fdw;`

\>\dx  
<pre>
										 List of installed extensions  
       Name       | Version |   Schema   |                             Description
------------------+---------+------------+---------------------------------------------------------------------  
 oracle_fdw       | 1.2     | public     | foreign data wrapper for Oracle access  
(4 rows)  

</pre>

and you should see the oracle_fdw listed as an added extension.  
