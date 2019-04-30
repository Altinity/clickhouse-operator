#!/bin/bash

# Build configuration options

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT=$(realpath ${CUR_DIR}/..)
OPERATOR_BIN=${CUR_DIR}/clickhouse-operator
