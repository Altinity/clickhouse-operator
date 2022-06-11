#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"kube-system"}"
VALIDATE_YAML="${VALIDATE_YAML:-"true"}"
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

MANIFEST="${CUR_DIR}/clickhouse-operator-install-bundle.yaml" ${CUR_DIR}/../operator-web-installer/clickhouse-operator-install.sh
