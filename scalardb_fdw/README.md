# ScalarDB FDW

ScalarDB FDW is a PostgreSQL extension that implements a foreign data wrapper (FDW) for [ScalarDB](https://www.scalar-labs.com/scalardb/).

ScalarDB FDW uses the Java Native Interface to directly utilize ScalarDB as a library inside the FDW and read data from external databases via scan operations for ScalarDB.

## Prerequisites

You must have the following prerequisites set up in your environment.

### JDK

You must install a version of the Java Development Kit (JDK) that is compatible with ScalarDB. In addition, you must set the `JAVA_HOME` environment variable, which points to your JDK installation directory.

Note that since these extensions use the Java Native Interface (JNI) internally, you must include the dynamic library of the Java virtual machine (JVM), such as `libjvm.so`, in the library search path.

### PostgreSQL

This extension supports PostgreSQL 13 or later. For details on how to install PostgreSQL, see the official documentation at [Server Administration](https://www.postgresql.org/docs/current/admin.html).

## Build and installation

You can build and install this extension by running the following command.

```console
make install
```

### Common build errors

This section describes some common build errors that you might encounter.

#### ld: library not found for -ljvm

Normally, the build script finds the path for `libjvm.so` and properly sets it as a library search path. However, if you encounter the error `ld: library not found for -ljvm`, please copy the `libjvm.so` file to the default library search path. For example:

```console
ln -s /<PATH_TO_YOUR_LIBJVM_FILE>/libjvm.so /usr/lib64/libjvm.so
```

## Usage

This section provides a usage example and available options for FDW for ScalarDB.

### Example

The following example shows you how to install and create the necessary components, and then run a query by using the FDW extension.

#### 1. Install the extension

For details on how to install the extension, see the [Build and installation](#build-and-installation) section.

#### 2. Create an extension

To create an extension, run the following command:

```sql
CREATE EXTENSION scalardb_fdw;
```

#### 3. Create a foreign server

To create a foreign server, run the following command:

```sql
CREATE SERVER scalardb FOREIGN DATA WRAPPER scalardb_fdw OPTIONS (
    config_file_path '/path/to/scalardb.properties'
);
```

#### 4. Create user mapping

To create user mapping, run the following command:

```sql
CREATE USER MAPPING FOR PUBLIC SERVER scalardb;
```

#### 5. Create a foreign table

To create a foreign table, run the following command:

```sql
CREATE FOREIGN TABLE sample_table (
    pk int,
    ck1 int,
    ck2 int,
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

#### 6. Run a query

To run a query, run the following command:

```sql
select * from sample_table;
```

### Available options

You can set the following options for ScalarDB FDW objects.

#### `CREATE SERVER`

You can set the following options on a ScalarDB foreign server object:

| Name               | Required | Type     | Description                                                     |
| ------------------ | -------- | -------- | --------------------------------------------------------------- |
| `config_file_path` | **Yes**  | `string` | The path to the ScalarDB config file.                           |
| `max_heap_size`    | No       | `string` | The maximum heap size of JVM. The format is the same as `-Xmx`. |

#### `CREATE USER MAPPING`

Currently, no options exist for `CREATE USER MAPPING`.

#### `CREATE FOREIGN SERVER`

The following options can be set on a ScalarDB foreign table object:

| Name         | Required | Type     | Description                                                      |
| ------------ | -------- | -------- | ---------------------------------------------------------------- |
| `namespace` | **Yes**  | `string` | The name of the namespace of the table in the ScalarDB instance. |
| `table_name` | **Yes**  | `string` | The name of the table in the ScalarDB instance.                  |

### Data-type mapping

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

This section describes how to test FDW for ScalarDB.

### Set up a ScalarDB instance for testing

Before testing FDW for ScalarDB, you must have a running ScalarDB instance that contains test data. You can set up the instance and load the test data by running the following commands:

```console
./test/setup.sh
```

If you want to reset the instances, you can run the following command, then the above setup command again.

```console
./test/cleanup.sh
```

### Run regression tests

You can run regression tests by running the following command **after** you have installed the FDW extension.

```console
make installcheck
```

## Limitations

- This extension aims to enable analytical query processing on ScalarDB-managed databases. Therefore, this extension only supports reading data from ScalarDB.
