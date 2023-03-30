#!/usr/bin/env bash

set -eu

script_dir=$(dirname "$0")

echo '- load ScalarDB schemas'
java -jar "${script_dir}/scalardb-schema-loader-3.8.0.jar" \
    --config "${script_dir}/client.properties" \
    -f "${script_dir}/schema.json" \
    --coordinator

echo '- load test data into ScalarDB backends'
java -jar "${script_dir}/scalardb-sql-cli-3.8.0-all.jar" \
    --config "${script_dir}/client.properties" \
    -f "${script_dir}/test-data.sql"
