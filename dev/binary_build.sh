#!/bin/bash

# Build clickhouse-operator
# Do not forget to update version

source ./binary_build_config.sh

#CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ${CUR_DIR}/clickhouse-operator ${SRC_ROOT}/cmd/clickhouse-operator
CGO_ENABLED=0 go build -o ${OPERATOR_BIN} ${SRC_ROOT}/cmd/clickhouse-operator

exit $?
