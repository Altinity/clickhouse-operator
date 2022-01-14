#!/bin/bash

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Build clickhouse-operator
OUTPUT_BINARY="${OPERATOR_BIN:-${SRC_ROOT}/dev/bin/clickhouse-operator}"
MAIN_SRC_FILE="${SRC_ROOT}/cmd/operator/main.go"

source "${CUR_DIR}/go_build_universal.sh"
