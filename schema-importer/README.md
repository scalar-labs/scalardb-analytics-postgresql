# Schema Importer

Schema Importer is a CLI tool for automatically configuring PostgreSQL. By using this tool, your PostgreSQL database can have identical database objects, such as namespaces and tables, as your ScalarDB instance.

Schema Importer reads the ScalarDB configuration file, retrieves the schemas of the tables defined in ScalarDB, and creates the corresponding foreign data wrapper external tables and views in that order. For more information, please refer to the [online documentation](https://scalardb.scalar-labs.com/docs/latest/scalardb-analytics-postgresql/getting-started/).

## Build Schema Importer

You can build Schema Importer by using [Gradle](https://scalardb.scalar-labs.com/docs/latest/scalardb-analytics-postgresql/getting-started/). To build Schema Importer, run the following command:

```console
./gradlew build
```

You may want to build a fat JAR file so that you can launch Schema Importer by using `java -jar`. To build the fat JAR, run the following command:

   ```console
   ./gradlew shadowJar
   ```

After you build the fat JAR, you can find the fat JAR file in the `app/build/libs/` directory.

## Run Schema Importer

To run Schema Importer by using the fat JAR file, run the following command:

```console
java -jar <PATH_TO_FAT_JAR_FILE>
```
Available options are as follows: 

| Name                        | Required | Description                                                            | Default                                    |
| --------------------------- | -------- | ---------------------------------------------------------------------- | ------------------------------------------ |
| `--config`                  | **Yes**  | Path to the ScalarDB configuration file                                |                                            |
| `--config-on-postgres-host` | No       | Path to the ScalarDB configuration file on the PostgreSQL-running host | The same value as `--config` will be used. |
| `--namespace`, `-n`         | **Yes**  | Namespaces to import into the analytics instance                       |                                            |
| `--host`                    | No       | PostgreSQL host                                                        | localhost                                  |
| `--port`                    | No       | PostgreSQL port                                                        | 5432                                       |
| `--database`                | No       | PostgreSQL port                                                        | postgres                                   |
| `--user`                    | No       | PostgreSQL user                                                        | postgres                                   |
| `--password`                | No       | PostgreSQL password                                                    |                                            |
| `--debug`                   | No       | Enable debug mode                                                      |                                            |


## Test Schema Importer

To test Schema Importer, run the following command:

```console
./gradlew test
```

## Build a Docker image of Schema Importer


To build a Docker image of Schema Importer, run the following command, replacing `<TAG>` with the tag version of Schema Importer that you want to use:

```console
docker build -t ghcr.io/scalar-labs/scalardb-analytics-postgresql-schema-importer:<TAG> -f ./app/Dockerfile .
```
