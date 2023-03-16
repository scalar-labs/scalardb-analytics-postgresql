# ScalarDB Analytics with PostgreSQL

ScalarDB Analytics with PostgreSQL processes analytical queries on ScalarDB-managed data using PostgreSQL and its Foreign Data Wrapper (FDW) feature.

## Components

ScalarDB Analytics PostgreSQL provides analytical environment as Docker images. This repository gather all required components to build the images. Currently, this repository consists of the following:

- [scalardb_fdw](./scalardb_fdw): a FDW plugin that read data from underlying databases using the ScalarDB library internally.
