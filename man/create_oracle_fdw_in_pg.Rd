% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/create-oracle-fdw-in-pg.R
\name{create_oracle_fdw_in_pg}
\alias{create_oracle_fdw_in_pg}
\title{Create Oracle Foreign Data Wrapper Table in Postgres}
\usage{
create_oracle_fdw_in_pg(
  in_table,
  ora_conn_param,
  pg_conn_param,
  out_server_name,
  out_schema
)
}
\arguments{
\item{in_table}{oracle table to import (e.g. WHSE_BASEMAPPING.FWA_ASSESSMENT_WATERSHEDS_POLY )}

\item{ora_conn_param}{named list of oracle connection parameters (see get_ora_conn_list() function for more details)}

\item{pg_conn_param}{named list of postgres connection parameters (see get_pg_conn_list() function for more details)}

\item{out_server_name}{name of FDW Server that will be created}

\item{out_schema}{name of schema where foreign table will be imported}
}
\value{
string of output foreign table (schema.in_table)
}
\description{
Create Oracle Foreign Data Wrapper Table in Postgres
}
\examples{
coming soon
}
