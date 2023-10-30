# ScalarDB Analytics with PostgreSQL

ScalarDB Analytics with PostgreSQL expands the capabilities of [ScalarDB](https://www.scalar-labs.com/scalardb/) to support various queries, including joins and aggregations, and enables users to run advanced processing, such as ad-hoc analysis.

ScalarDB Analytics with PostgreSQL, as the name suggests, uses PostgreSQL to execute queries on the data that ScalarDB manages, enabling users to perform various queries that PostgreSQL supports. For details, see the [docs](#docs).

## Online documentation

- [How to Install ScalarDB Analytics with PostgreSQL in Your Local Environment by Using Docker](https://scalardb.scalar-labs.com/docs/latest/scalardb-analytics-postgresql/installation/)
- [Getting Started with ScalarDB Analytics with PostgreSQL](https://scalardb.scalar-labs.com/docs/latest/scalardb-analytics-postgresql/getting-started/)
- [Sample Application for ScalarDB Analytics with PostgreSQL](https://scalardb.scalar-labs.com/docs/latest/scalardb-samples/scalardb-analytics-postgresql-sample/README/)

## Components

This repository mainly includes two components. For details, please refer to the sub-directory of each component:

- Schema Importer [(schema-importer/)](./schema-importer) : CLI application that imports database objects from ScalarDB into PostgreSQL.
- ScalarDB FDW [(scalardb_fdw/)](./scalardb_fdw) : PostgreSQL FDW extension that reads data from underlying databases by calling the ScalarDB library via the Java Native Interface.

This repository contains other directories, including:

- [docker/](./docker): Dockerfile to build a Docker image that contains PostgreSQL with the community-provided foreign data wrapper (FDW) extensions installed.
- [docs/](./docs): Documentation written in Markdown.

## Contributing

Although this library is mainly maintained by the Scalar Engineering Team, we appreciate any help. Feel free to open an issue for reporting bugs, suggesting improvements, or requesting new features.

## License

ScalarDB Analytics with PostgreSQL is dual-licensed under both the Apache 2.0 License (found in the LICENSE file in the root directory) and a commercial license. You may select, at your option, one of the above-listed licenses. Regarding the commercial license, please [contact us](https://scalar-labs.com/contact_us/) for more information.
