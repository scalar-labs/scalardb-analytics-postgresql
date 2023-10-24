# Schema Importer

Schema Importer is a CLI tool for automatically configuring PostgreSQL to enable users have identical database objects, such as namespaces and tables, to ScalarDB in PostgreSQL.

Schema Importer reads the ScalarDB configuration file, retrieves the schemas of the tables defined in ScalarDB, and creates the corresponding FDW external tables and views in that order. Please refer to the [online documentation](https://scalardb.scalar-labs.com/docs/latest/scalardb-analytics-postgresql/getting-started/) for more information.

## Building Schema Importer

Schema Importer is built using [Gradle](https://scalardb.scalar-labs.com/docs/latest/scalardb-analytics-postgresql/getting-started/). To build Schema Importer, run:

```console
./gradlew build
```

You may want to build a fat jar file so that you can launch Schema Importer with `java -jar`. To build the fat jar, run:

   ```console
   ./gradlew shadowJar
   ```

After you build the fat jar, you can find the fat jar file under `app/build/libs/`.

## Run Schema Importer

To run Schema Importer using the fat jar file, run:

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


## Testing Schema Importer

To test Schema Importer, run:

```console
./gradlew test
```

## Building Docker Image


To build Docker image of Schema Importer, run the following command replacing `<TAG>` with a proper tag name:

```console
docker build -t ghcr.io/scalar-labs/scalardb-analytics-postgresql-schema-importer:<TAG> -f ./app/Dockerfile .
```
