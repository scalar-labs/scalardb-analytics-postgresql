# Getting Started with ScalarDB Analytics with PostgreSQL

This document explains how to get started with ScalarDB Analytics with PostgreSQL. Here, we assume that you have already installed ScalarDB Analytics with PostgreSQL, and all required services are running. If you don't have such an environment, please follow [the installation document](./installation.md). As ScalarDB Analytics with PostgreSQL executes queries via PostgreSQL, we also assume you already have a `psql` or other PostgreSQL client to send queries to PostgreSQL.

## What is ScalarDB Analytics with PostgreSQL?

ScalarDB, as a universal transaction manager, targets mainly transactional workloads and therefore supports limited subsets of relational queries.

ScalarDB Analytics with PostgreSQL extends the functionality of ScalarDB to process analytical queries on ScalarDB-managed data by using PostgreSQL and its foreign data wrapper (FDW) extension.

ScalarDB Analytics with PostgreSQL mainly consists of two components: PostgreSQL and Schema Importer.

PostgreSQL runs as a service, accepting queries from users to process. FDW extensions are used to read data from the back-end storages managed by ScalarDB. Schema Importer is a tool to import the schema of the ScalarDB database into PostgreSQL so that users can see the identical tables in PostgreSQL side to the ScalarDB side.

## Clone the sample application

## Set up a ScalarDB database

First, you need one or more ScalarDB database to run analytical queries with ScalarDB Analytics with PostgreSQL. If you have your own ScalarDB database, you can skip this section and use your database instead. Otherwise, you can set up a sample database by running the following command.

```shell
$ docker compose run --rm sql-cli --config /etc/scalardb.properties --file /etc/sample_data.sql
```

This command sets up [multiple storage instances](https://scalardb.scalar-labs.com/docs/3.9/multi-storage-transactions/) that consist of DynamoDB, PostgreSQL, and Cassandra. Then, this creates namespaces of `dynamons`, `postgresns`, and `cassandrans` mapped to those storages, creates tables of `dynamons.customer`, `postgresns.orders`, and `cassandrans.lineitem` using [ScalarDB SQL](https://scalardb.scalar-labs.com/docs/3.9/scalardb-sql/getting-started-with-sql/), and load sample data into the tables.

![Multi-storage overview](./images/multi-storage-overview.png)

## Import the schemas from ScalarDB into PostgreSQL

Next, let's import the schemas of the ScalarDB databases into PostgreSQL that processes analytical queries. ScalarDB Analytics with PostgreSQL provides a tool, Schema Importer, for this purpose. It'll get everything in place to run analytical queries for you.

```shell
$ docker compose run --rm schema-importer \
  import \
  --config /etc/scalardb.properties \
  --host analytics \
  --port 5432 \
  --database test \
  --user postgres \
  --password postgres \
  --namespace cassandrans \
  --namespace postgresns \
  --namespace dynamons \
  --config-on-postgres-host /etc/scalardb.properties
```

This creates tables (in precise, views) with the same names as the tables in the ScalarDB databases. In this example, the tables of `dynamons.customer`, `postgresns.orders`, and `cassandrans.lineitem` are created. The column definitions are also identical to the ScalarDB databases. These tables are [foreign tables](https://www.postgresql.org/docs/current/sql-createforeigntable.html) connected to the underlying storage of the ScalarDB databases using FDW. Therefore, you can equate those tables in PostgreSQL with the tables in the ScalarDB databases.

![Imported schema](./images/imported-schema.png)

## Run analytical queries

Now, you have all tables to read the same data in the ScalarDB databases and can run any arbitrary analytical queries supported by PostgreSQL. To run queries, please connect to PostgreSQL with `psql` or other client.

```shell
$ psql -U postgres -h localhost test
Password for user postgres:

> select c_mktsegment, count(*) from dynamons.customer group by c_mktsegment;
 c_mktsegment | count
--------------+-------
 AUTOMOBILE   |     4
 BUILDING     |     2
 FURNITURE    |     1
 HOUSEHOLD    |     2
 MACHINERY    |     1
(5 rows)
```

For details about the sample data and additional practical work, see [the sample application page]().

## Caveats

### Isolation level

ScalarDB Analytics with PostgreSQL reads data with the **Read Committed** isolation level set. This isolation level ensures that the data you read has been committed in the past but does not guarantee that you can read consistent data at a particular point in time.

### Write operations are not supported

ScalarDB Analytics with PostgreSQL only supports read-only queries. `INSERT`, `UPDATE`, and other write operations are not supported.

### Conflicts among FDW extensions

If you have a multi-storage configuration and use JDBC with DynamoDB or CosmosDB, you may encounter an error due to a conflict between the underlying FDW extensions. We plan to fix this issue in the near future. Until we fix this issue, you can avoid the error by reading data in DynamoDB or CosmosDB first, then reading data by using JDBC.
