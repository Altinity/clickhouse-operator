#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
export OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"kube-system"}"
export OPERATOR_VERSION=${OPERATOR_VERSION:-$(cat "$CUR_DIR/../../release")}
export VALIDATE_YAML="${VALIDATE_YAML:-"true"}"
MANIFEST="${CUR_DIR}/clickhouse-operator-install-template.yaml" ${CUR_DIR}/../operator-web-installer/clickhouse-operator-install.sh
