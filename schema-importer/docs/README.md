# ScalarDB Analytics with PostgreSQL Schema Importer

## How to build container image

1. Change working directory.
   ```console
   cd schema-importer/
   ```

1. Build jar file.
   ```console
   ./gradlew shadowJar
   ```

1. Build container image.
   ```console
   docker build -t ghcr.io/scalar-labs/scalardb-analytics-postgresql-schema-importer:<TAG> -f ./app/Dockerfile .
   ```
