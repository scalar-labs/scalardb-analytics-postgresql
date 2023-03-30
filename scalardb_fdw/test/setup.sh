#!/usr/bin/env bash

set -eu

script_dir=$(dirname "$0")

"$script_dir/launch_backend_services.sh"

"$script_dir/load_test_data.sh"
