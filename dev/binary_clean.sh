#!/bin/bash

# Delete clickhouse-operator
# Do not forget to update version

source ./binary_build_config.sh

rm -f ${OPERATOR_BIN}
