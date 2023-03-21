# ScalarDB Analytics with PostgreSQL

ScalarDB Analytics with PostgreSQL processes analytical queries on ScalarDB-managed data by using PostgreSQL and its Foreign Data Wrapper (FDW) extension.

## Components

ScalarDB Analytics with PostgreSQL provides a Docker container image that includes all the required components to run. This repository gathers all the required components to build the image. Currently, this repository consists of the following:

- [scalardb_fdw](./scalardb_fdw): an FDW plugin that reads data from underlying databases using the ScalarDB library internally.
