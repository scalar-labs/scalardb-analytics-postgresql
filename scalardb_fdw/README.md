# Get started with FDW for ScalarDB

This PostgreSQL extension implements a Foreign Data Wrapper (FDW) for [ScalarDB](https://scalar-labs.com/products/scalardb).

## Prerequisites

You must have the following prerequisites set up in your environment.

### JDK

You must install a version of the Java Development Kit (JDK) that is compatible with ScalarDB. In addition, you must set the `JAVA_HOME` environment variable, which points to your JDK installation directory.

Note that since these extions use JNI internally, the dynamic library of the JVM, such as `libjvm.so`, must be found in the library search paths.

### ScalarDB

You have to download the [ScalarDB](https://scalar-labs.com/products/scalardb) library jar. You can put it in an arbitrary path. Note that the jar file must be a **fat-jar**, which contains all its dependencies.

### PostgreSQL

This extension supports PostgreSQL 13 and above. Please refer to the [PostgreSQL official site](https://www.postgresql.org/docs/current/admin.html) on how to install it.

## Build & Installation

You can build and install this extension by running the following command. You may not need to set `USE_PGXS` if you are familiar with the PostgreSQL extension.

```
make USE_PGXS=1 install
```

### Common Build Errors

#### ld: library not found for -ljvm

Normally, the build script find the path of `libjvm` and set it as a library search path properly. If you still encounte an error like the above, please you may copy or like the `libjvm.so` under the default library paths. For example:

```
ln -s /path/to/your/libjvm.so /usr/lib64/libjvm.so
```

## Usage

### Example

#### 1. Install the Extension

You have to install the extension at first. Please refer to the Build & Installation section.

#### 2. Create an extension

```sql
CREATE EXTENSION scalardb_fdw;
```

#### 3. Create a foreign server

```sql
CREATE SERVER scalardb FOREIGN DATA WRAPPER scalardb_fdw OPTIONS (
    jar_file_path '/path/to/scalardb-3.8.0.jar',
    config_file_path '/path/to/scalardb.properties'
);

```

#### 4. Create a user mapping

```sql
CREATE USER MAPPING FOR PUBLIC SERVER scalardb;
```

#### 5. Create a foreign table

```sql
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
```

#### 6. Run a query you want

```sql
select * from sample_table;
```

### Available Options

#### `CREATE SERVER`

The following options can be set on a ScalarDB foreign server object:

| Name               | Required     | Type     | Description                                                                                   |
| ------------------ | ------------ | -------- | --------------------------------------------------------------------------------------------- |
| `jar_file_path`    | **REQUIRED** | `string` | The path to the ScalarDB jar file. Note that this jar also must contain all its dependencies. |
| `config_file_path` | **REQURIED** | `string` | The path to the ScalarDB config file.                                                         |
| `max_heap_size`    |              | `string` | The maximux heap size of JVM. The format is the same as `-Xmx`.                               |

#### `CREATE USER MAAPING`

Currently, there is no available options for the `CREATE USER MAPPING`.

#### `CREATE FOREIGN SERVER`

The following options can be set on a ScalarDB foreign table object:

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
./test/setup.sh
```

If you want to reset the instances, you can run the following command, then the above setup command again.

```
./test/cleanup.sh
```

### Run regression tests

You can run regression tests by running the following command **after** you have installed the FDW extension.

```console
make USE_PGXS=1 installcheck
```

## Limitations

- This extension aims to enable analytical query processing on ScalarDB-managed databases. Therefore, this extension only supports reading data from ScalarDB.
- Currently, this extension always reads all records from the underlying databases, no matter what selection exists in queries.
