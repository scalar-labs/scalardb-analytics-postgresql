#!/usr/bin/env bash

set -eu

script_dir=$(dirname "$0")
work_dir="$script_dir/work"

scalardb_version=$(make -C "$script_dir/.." scalardb-version)

scalardb_sql_cli_jar="scalardb-sql-cli-${scalardb_version}-all.jar"
scalardb_schema_loader_jar="scalardb-schema-loader-${scalardb_version}.jar"

namespace_to_check="cassandrans"
table_to_check="test"

if ! test -d "$work_dir"; then
    mkdir -p "$work_dir"
fi

if ! test -e "$work_dir/$scalardb_sql_cli_jar";then
    echo '- Downloading scalardb-sql-cli jar file'
    scalardb_sql_cli_url="https://github.com/scalar-labs/scalardb-sql/releases/download/v${scalardb_version}/${scalardb_sql_cli_jar}"
    curl -sLo "$work_dir/$scalardb_sql_cli_jar" "$scalardb_sql_cli_url"
fi

if ! test -e "$work_dir/$scalardb_schema_loader_jar";then
    echo '- Downloading scalardb-schema-loader jar file'
    scalardb_schema_loader_url="https://github.com/scalar-labs/scalardb/releases/download/v${scalardb_version}/${scalardb_schema_loader_jar}"
    curl -sLo "$work_dir/$scalardb_schema_loader_jar" "$scalardb_schema_loader_url"
fi

check_if_schema_is_loaded() {
    echo '- Check if test data is loaded'
    java -jar "$scalardb_sql_cli_jar" \
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
    java -jar "$scalardb_schema_loader_jar" \
        --config "${script_dir}/client.properties" \
        -f "${script_dir}/schema.json" \
        --coordinator 2>/dev/null

    echo '- Load test data into ScalarDB backends'
    java -jar "$scalardb_schema_loader_jar" \
        --config "${script_dir}/client.properties" \
        -f "${script_dir}/test-data.sql" 2>/dev/null
fi
