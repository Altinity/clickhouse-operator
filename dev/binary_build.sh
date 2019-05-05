#!/bin/bash

# Build clickhouse-operator
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source ${CUR_DIR}/binary_build_config.sh

#CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ${CUR_DIR}/clickhouse-operator ${SRC_ROOT}/cmd/clickhouse-operator
CGO_ENABLED=0 go build -o ${OPERATOR_BIN} ${SRC_ROOT}/cmd/clickhouse-operator

exit $?
