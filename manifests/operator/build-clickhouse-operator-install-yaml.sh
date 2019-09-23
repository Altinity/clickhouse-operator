#!/bin/bash

# Full list of available vars check in ${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh file

# Here we just build production all-sections-included .yaml manifest with namespace and image parameters
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-altinity/metrics-exporter:latest}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MANIFEST_ROOT=$(realpath ${CUR_DIR}/..)

OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh > ${CUR_DIR}/clickhouse-operator-install.yaml
