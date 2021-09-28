#!/bin/bash

# Build clickhouse-operator
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/..")"

"${PROJECT_ROOT}"/dockerfile/build-docker-files.sh
"${CUR_DIR}"/go_build_metrics_exporter.sh
"${CUR_DIR}"/go_build_operator.sh
"${CUR_DIR}"/run_gocard.sh
"${CUR_DIR}"/run_gosec.sh
