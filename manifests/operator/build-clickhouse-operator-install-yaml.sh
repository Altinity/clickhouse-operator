#!/bin/bash

# Build all-sections-included clickhouse-operator installation .yaml manifest with namespace and image parameters

# Full list of available vars is available in ${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh file
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-altinity/metrics-exporter:latest}"

# Paths
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MANIFEST_ROOT="$(realpath ${CUR_DIR}/..)"

# Build installation .yaml manifest - run generator with params
OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
"${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh" > "${CUR_DIR}/clickhouse-operator-install.yaml"
