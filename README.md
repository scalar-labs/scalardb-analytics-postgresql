# ScalarDB Analytics with PostgreSQL

ScalarDB Analytics with PostgreSQL processes analytical queries on ScalarDB-managed data by using PostgreSQL and its Foreign Data Wrapper (FDW) extension.

## Components

ScalarDB Analytics with PostgreSQL provides a Docker container image that includes all the required components to run. This repository gathers all the required components to build the image. Currently, this repository consists of the following:

- [schema-importer](./schema-importer): CLI application that imports tables definitions from ScalarDB into PostgreSQL using FDW.
- [scalardb_fdw](./scalardb_fdw/docs): an FDW extension that reads data from underlying databases by using the ScalarDB library internally.
- [docker](./docker): Dockerfile to build a docker image containing PostgreSQL with FDW extensions installed.
