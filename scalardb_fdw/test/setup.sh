#!/usr/bin/env bash

set -eu

script_dir=$(dirname "$0")

check_cassandra() {
    local count=0
    echo '- waiting for Cassandra container to accept connection'
    while [ "$count" -lt 10 ]; do
        if cqlsh -e 'exit' 2>/dev/null; then
            echo '- Cassandra container is ready'
            return
        fi

        count=$(echo "$count + 1" | bc)
        sleep 1
    done
    echo 'ERROR: Cassandra container has not became ready after 10 seconds' 1>&2
    exit 1
}

check_postgresql() {
    local count=0
    echo '- waiting for PostgreSQL container to accept connection'
    while [ "$count" -lt 10 ]; do
        if env PGPASSWORD=postgres psql -p 5434 -h localhost -U postgres -c 'select 1' >/dev/null 2>&1; then
            echo '- PostgreSQL container is ready'
            return
        fi

        count=$(echo "$count + 1" | bc)
        sleep 1
    done
    echo 'ERROR: PostgreSQL container has not became ready after 10 seconds' 1>&2
    exit 1
}

echo '- launch Cassandra and PostgreSQL containers as ScalarDB backends'
docker compose -f "${script_dir}/docker-compose.yaml" up -d

check_postgresql
check_cassandra

echo '- load ScalarDB schemas'
java -jar "${script_dir}/scalardb-schema-loader-3.8.0.jar" \
    --config "${script_dir}/client.properties" \
    -f "${script_dir}/schema.json" \
    --coordinator

echo '- load test data into ScalarDB backends'
java -jar "${script_dir}/scalardb-sql-cli-3.8.0-all.jar" \
    --config "${script_dir}/client.properties" \
    -f "${script_dir}/test-data.sql"
