#!/bin/bash

# Delete clickhouse-operator
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

OUTPUT_BINARY="${OPERATOR_BIN}"

rm -f "${OUTPUT_BINARY}"
