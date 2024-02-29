#!/bin/bash

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

echo "VERBOSITY=${VERBOSITY}"

# Build clickhouse-operator config files
source "${MANIFESTS_ROOT}/builder/build-clickhouse-operator-configs.sh"
# Build clickhouse-operator install .yaml manifest
source "${MANIFESTS_ROOT}/builder/build-clickhouse-operator-install-yaml.sh"
