#!/usr/bin/env bash

set -eu

script_dir=$(dirname "$0")

docker compose -f "${script_dir}/docker-compose.yaml" down -v
