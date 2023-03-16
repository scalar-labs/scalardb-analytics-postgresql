# ScalarDB FDW

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for [ScalarDB](https://scalar-labs.com/products/scalardb).

## Prerequisite

### JDK

You need to install JDK whose version is compatible with ScalarDB.

Note that since these extions use JNI internally, the dynamic library of the JVM, such as `libjvm.so`, must be found in the library search paths.

### ScalarDB

You have to download the ScalarDB library jar. You can put it in an arbitrary path.

## Build

```
cd scalardb_fdw;
make USE_PGXS=1 install;
```

## Usage

### Example

```sql
CREATE EXTENSION scalardb_fdw;

CREATE SERVER scalardb FOREIGN DATA WRAPPER scalardb_fdw OPTIONS (
    jar_file_path '/path/to/scalardb-3.8.0.jar',
    config_file_path '/path/to/scalardb.properties'
);


CREATE USER MAPPING FOR PUBLIC SERVER scalardb;

CREATE FOREIGN TABLE sample_table (
    pk int OPTIONS(scalardb_partition_key 'true'),
    ck1 int OPTIONS(scalardb_clustering_key 'true'),
    ck2 int OPTIONS(scalardb_clustering_key 'true'),
    boolean_col boolean,
    bigint_col bigint,
    float_col double precision,
    double_col double precision,
    text_col text,
    blob_col bytea
) SERVER scalardb OPTIONS (
    namespace 'ns',
    table_name 'sample_table'
);

select * from sample_table;
```

### Available Options

#### Foreign Server

The following options can be set on a ScalarDB foreign server object:

| Name               | Required     | Type     | Description                                                                                   |
| ------------------ | ------------ | -------- | --------------------------------------------------------------------------------------------- |
| `jar_file_path`    | **REQUIRED** | `string` | The path to the ScalarDB jar file. Note that this jar also must contain all its dependencies. |
| `config_file_path` | **REQURIED** | `string` | The path to the ScalarDB config file.                                                         |
| `max_heap_size`    |              | `string` | The maximux heap size of JVM. The format is the same as `-Xmx`.                               |

#### Foreign Table

The following options can be set on a Cassandra foreign table object:

| Name         | Required     | Type     | Description                                            |
| ------------ | ------------ | -------- | ------------------------------------------------------ |
| `name_space` | **REQUIRED** | `string` | The name of the namespace of the table in the ScalarDB |
| `table_name` | **REQUIRED** | `string` | The name of the table in the ScalarDB                  |

#### Column

| Name                      | Required | Type      | Description                                                                                                                                       |
| ------------------------- | -------- | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------- |
| `scalardb_partition_key`  |          | `boolean` | This specify the column is a partition key in the ScalarDB-side table definition. This is optional, and used to optiomize the query plan if set.  |
| `scalardb_clustering_key` |          | `boolean` | This specify the column is a clustering key in the ScalarDB-side table definition. This is optional, and used to optiomize the query plan if set. |
| `scalardb_index_key`      |          | `boolean` | This specify the column is a index key in the ScalarDB-side table definition. This is optional, and used to optiomize the query plan if set.      |

### Data Type Mapping

| ScalarDB | PostgreSQL       |
| -------- | ---------------- |
| BOOLEAN  | boolean          |
| INT      | int              |
| BIGINT   | bigint           |
| FLOAT    | float            |
| DOUBLE   | double precision |
| TEXT     | text             |
| BLOB     | bytea            |

## Testing

### Setup ScalarDB databases for testing

Before running the tests, you need a running ScalarDB instance and test data loaded into it. You can setup all by executing the following commands.

```
cd tests;
./setup.sh
```

### Run regression tests

```
make USE_PGXS=1 installcheck
```

## Limitations

- This extension aims at enabling analytical query processing on ScalarDB-managed databases. Thus, only reading data from ScalarDB is supported.
- Currently, this extension always reads all recoreds in the underlying table no matter what selection exists in the query.
