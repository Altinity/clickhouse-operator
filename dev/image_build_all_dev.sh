#!/bin/bash

# Build clickhouse-operator
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

"${CUR_DIR}"/image_build_metrics_exporter_dev.sh
"${CUR_DIR}"/image_build_operator_dev.sh
