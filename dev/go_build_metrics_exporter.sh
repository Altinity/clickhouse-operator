#!/bin/bash

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Build metrics-exporter
OUTPUT_BINARY="${METRICS_EXPORTER_BIN:-${SRC_ROOT}/dev/bin/metrics-exporter}"
MAIN_SRC_FILE="${SRC_ROOT}/cmd/metrics_exporter/main.go"

source "${CUR_DIR}/go_build_universal.sh"
