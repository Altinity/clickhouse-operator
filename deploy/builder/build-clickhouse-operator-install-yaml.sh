#!/bin/bash

# Build all-sections-included clickhouse-operator installation .yaml manifest with namespace and image parameters

# Paths
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/../..")"
MANIFEST_ROOT="$(realpath ${PROJECT_ROOT}/deploy)"

#
# Setup SOME variables
# Full list of available vars is available in ${MANIFEST_ROOT}/dev/cat-clickhouse-operator-install-yaml.sh file
#

# Namespace to install operator
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"kube-system"}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# Operator's docker image
RELEASE_VERSION=$(cat "${PROJECT_ROOT}/release")
OPERATOR_VERSION="${OPERATOR_VERSION:-"${RELEASE_VERSION}"}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:${OPERATOR_VERSION}"}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:${OPERATOR_VERSION}"}"

# Run generator

#
# Build full manifests
#

# Build namespace:kube-system installation .yaml manifest
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" > "${MANIFEST_ROOT}/operator/clickhouse-operator-install-bundle.yaml"

# Build templated installation .yaml manifest
OPERATOR_IMAGE="\${OPERATOR_IMAGE}" \
METRICS_EXPORTER_IMAGE="\${METRICS_EXPORTER_IMAGE}" \
OPERATOR_NAMESPACE="\${OPERATOR_NAMESPACE}" \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" > "${MANIFEST_ROOT}/operator/clickhouse-operator-install-template.yaml"

# Build v1beta1 bundle and template manifests
"${CUR_DIR}"/build-clickhouse-operator-install-v1beta1-yaml.sh

# Build terraform-templated installation .yaml manifest
cat <<EOF > "${MANIFEST_ROOT}/operator/clickhouse-operator-install-tf.yaml"
#
# Terraform template parameters available:
#
# 1. namespace - namespace to install the operator into and to be watched by the operator.
# 2. password  - password of the clickhouse's user, used by the operator.
#
#
EOF
watchNamespaces="\${namespace}" \
password_sha256_hex="\${sha256(password)}" \
chPassword="\${password}" \
OPERATOR_NAMESPACE="\${namespace}" \
MANIFEST_PRINT_RBAC_NAMESPACED=yes \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" >> "${MANIFEST_ROOT}/operator/clickhouse-operator-install-tf.yaml"

# Build ansible-templated installation .yaml manifest
cat <<EOF > "${MANIFEST_ROOT}/operator/clickhouse-operator-install-ansible.yaml"
#
# Ansible template parameters available:
#
# 1. namespace - namespace to install the operator into and to be watched by the operator.
# 2. password  - password of the clickhouse's user, used by the operator.
#
#
EOF
watchNamespaces="{{ namespace }}" \
password_sha256_hex="{{ password | password_hash('sha256') }}" \
chPassword="{{ password }}" \
OPERATOR_NAMESPACE="{{ namespace }}" \
MANIFEST_PRINT_RBAC_NAMESPACED=yes \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" >> "${MANIFEST_ROOT}/operator/clickhouse-operator-install-ansible.yaml"


# Build partial .yaml manifest(s)
OPERATOR_IMAGE="\${OPERATOR_IMAGE}" \
METRICS_EXPORTER_IMAGE="\${METRICS_EXPORTER_IMAGE}" \
OPERATOR_NAMESPACE="\${OPERATOR_NAMESPACE}" \
MANIFEST_PRINT_CRD="yes" \
MANIFEST_PRINT_RBAC_CLUSTERED="no" \
MANIFEST_PRINT_RBAC_NAMESPACED="no" \
MANIFEST_PRINT_DEPLOYMENT="no" \
MANIFEST_PRINT_SERVICE_METRICS="no" \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" > "${MANIFEST_ROOT}/operator/parts/crd.yaml"
