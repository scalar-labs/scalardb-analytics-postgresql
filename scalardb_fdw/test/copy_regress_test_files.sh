#!/usr/bin/env bash

# Copy regression test files for Postgres 15 (src/*.pg15.sql expected/*.pg15.out) to
# input/ and output/ directory for other versions.

set -eux

sql_file="sql/scalardb_fdw.pg15.sql"
expected_file="expected/scalardb_fdw.pg15.out"

tail -n +4 $sql_file | sed -e "s/:'config_file_path'/'@abs_srcdir@\/test\/client.properties'/" >input/scalardb_fdw.source
tail -n +3 $expected_file | sed -e "s/:'config_file_path'/'@abs_srcdir@\/test\/client.properties'/" >output/scalardb_fdw.source
