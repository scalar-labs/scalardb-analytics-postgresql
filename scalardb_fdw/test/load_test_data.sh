#!/usr/bin/env bash

set -eu

script_dir=$(dirname "$0")

namespace_to_check="cassandrans"
table_to_check="test"

check_if_schema_is_loaded() {
    echo '- Check if test data is loaded'
    java -jar "${script_dir}/scalardb-sql-cli-3.8.0-all.jar" \
        --config "${script_dir}/client.properties" \
        --output-format=json\
        --execute="show tables from $namespace_to_check" 2>/dev/null | \
    jq -r .resultset[].tableName | \
    grep "$table_to_check" > /dev/null

    return $?
}

if check_if_schema_is_loaded; then
    echo "- Test data has already been loaded. Skip loading test data"
else
    echo '- Load ScalarDB schemas'
    java -jar "${script_dir}/scalardb-schema-loader-3.8.0.jar" \
        --config "${script_dir}/client.properties" \
        -f "${script_dir}/schema.json" \
        --coordinator 2>/dev/null

    echo '- Load test data into ScalarDB backends'
    java -jar "${script_dir}/scalardb-sql-cli-3.8.0-all.jar" \
        --config "${script_dir}/client.properties" \
        -f "${script_dir}/test-data.sql" 2>/dev/null
fi
