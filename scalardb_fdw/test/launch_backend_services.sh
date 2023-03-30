#!/usr/bin/env bash

set -eu -o pipefail

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
    echo 'ERROR: Cassandra container has not become ready after 10 seconds' 1>&2
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
