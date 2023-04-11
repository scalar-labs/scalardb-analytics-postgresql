#!/usr/bin/env bash

set -eu

script_dir=$(dirname "$0")
properties_path=$(readlink -f "$script_dir/client.properties")
./gradlew test-data-loader:run --args $properties_path
