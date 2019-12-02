#!/bin/bash

# Build all-sections-included clickhouse-operator installation .yaml manifest with namespace and image parameters

# Full list of available vars is available in ${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh file
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-altinity/metrics-exporter:latest}"

# Paths
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MANIFEST_ROOT="$(realpath ${CUR_DIR}/..)"

# Run generator

#
# Build full manifests
#

# Build prod installation .yaml manifest
OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
"${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh" > "${CUR_DIR}/clickhouse-operator-install.yaml"

# Build templated installation .yaml manifest
OPERATOR_IMAGE="\$OPERATOR_IMAGE" \
METRICS_EXPORTER_IMAGE="\$METRICS_EXPORTER_IMAGE" \
OPERATOR_NAMESPACE="\$OPERATOR_NAMESPACE" \
"${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh" > "${CUR_DIR}/clickhouse-operator-install-template.yaml"

# Build dev installation .yaml manifest
OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
OPERATOR_NAMESPACE="dev" \
"${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh" > "${MANIFEST_ROOT}/dev/clickhouse-operator-install-dev.yaml"

#
# Build part manifests
#

# Build templated installation .yaml manifest - CRD section
MANIFEST_PRINT_CRD="yes" \
MANIFEST_PRINT_RBAC="no" \
MANIFEST_PRINT_DEPLOYMENT="no" \
MANIFEST_PRINT_SERVICE="no" \
\
OPERATOR_IMAGE="\$OPERATOR_IMAGE" \
METRICS_EXPORTER_IMAGE="\$METRICS_EXPORTER_IMAGE" \
OPERATOR_NAMESPACE="\$OPERATOR_NAMESPACE" \
"${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh" > "${CUR_DIR}/clickhouse-operator-install-template-crd.yaml"

# Build templated installation .yaml manifest - RBAC section
MANIFEST_PRINT_CRD="no" \
MANIFEST_PRINT_RBAC="yes" \
MANIFEST_PRINT_DEPLOYMENT="no" \
MANIFEST_PRINT_SERVICE="no" \
\
OPERATOR_IMAGE="\$OPERATOR_IMAGE" \
METRICS_EXPORTER_IMAGE="\$METRICS_EXPORTER_IMAGE" \
OPERATOR_NAMESPACE="\$OPERATOR_NAMESPACE" \
"${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh" > "${CUR_DIR}/clickhouse-operator-install-template-rbac.yaml"

# Build templated installation .yaml manifest - Deployment section
MANIFEST_PRINT_CRD="no" \
MANIFEST_PRINT_RBAC="no" \
MANIFEST_PRINT_DEPLOYMENT="yes" \
MANIFEST_PRINT_SERVICE="no" \
\
OPERATOR_IMAGE="\$OPERATOR_IMAGE" \
METRICS_EXPORTER_IMAGE="\$METRICS_EXPORTER_IMAGE" \
OPERATOR_NAMESPACE="\$OPERATOR_NAMESPACE" \
"${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh" > "${CUR_DIR}/clickhouse-operator-install-template-deployment.yaml"

# Build templated installation .yaml manifest - Service section
MANIFEST_PRINT_CRD="no" \
MANIFEST_PRINT_RBAC="no" \
MANIFEST_PRINT_DEPLOYMENT="no" \
MANIFEST_PRINT_SERVICE="yes" \
\
OPERATOR_IMAGE="\$OPERATOR_IMAGE" \
METRICS_EXPORTER_IMAGE="\$METRICS_EXPORTER_IMAGE" \
OPERATOR_NAMESPACE="\$OPERATOR_NAMESPACE" \
"${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh" > "${CUR_DIR}/clickhouse-operator-install-template-service.yaml"
