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
CH_USERNAME_PLAIN="" \
CH_PASSWORD_PLAIN="" \
CH_CREDENTIALS_SECRET_NAME="clickhouse-operator" \
CH_USERNAME_SECRET_PLAIN="clickhouse_operator" \
CH_PASSWORD_SECRET_PLAIN="clickhouse_operator_password" \
MANIFEST_PRINT_RBAC_NAMESPACED=yes \
MANIFEST_PRINT_RBAC_CLUSTERED=yes \
"${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" > "${MANIFEST_ROOT}/operator/clickhouse-operator-install-bundle.yaml"

# Build templated installation .yaml manifest
OPERATOR_IMAGE="\${OPERATOR_IMAGE}" \
OPERATOR_IMAGE_PULL_POLICY="\${OPERATOR_IMAGE_PULL_POLICY}" \
METRICS_EXPORTER_IMAGE="\${METRICS_EXPORTER_IMAGE}" \
METRICS_EXPORTER_IMAGE_PULL_POLICY="\${METRICS_EXPORTER_IMAGE_PULL_POLICY}" \
OPERATOR_NAMESPACE="\${OPERATOR_NAMESPACE}" \
CH_USERNAME_PLAIN="" \
CH_PASSWORD_PLAIN="" \
CH_CREDENTIALS_SECRET_NAME="clickhouse-operator" \
CH_USERNAME_SECRET_PLAIN="clickhouse_operator" \
CH_PASSWORD_SECRET_PLAIN="clickhouse_operator_password" \
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
WATCH_NAMESPACES="\${namespace}" \
CH_USERNAME_PLAIN="" \
CH_PASSWORD_PLAIN="" \
CH_CREDENTIALS_SECRET_NAME="clickhouse-operator" \
CH_USERNAME_SECRET_PLAIN="clickhouse_operator" \
CH_PASSWORD_SECRET_PLAIN="\${password}" \
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
WATCH_NAMESPACES="{{ namespace }}" \
CH_USERNAME_PLAIN="" \
CH_PASSWORD_PLAIN="" \
CH_CREDENTIALS_SECRET_NAME="clickhouse-operator" \
CH_USERNAME_SECRET_PLAIN="clickhouse_operator" \
CH_PASSWORD_SECRET_PLAIN="{{ password }}" \
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

# resolve &Type* to allow properly works IDEA, look details https://youtrack.jetbrains.com/issue/IJPL-67381/Kubernetes-ignores-new-version-of-CRD
yq -i ". | explode(.)" "${MANIFEST_ROOT}/operator/parts/crd.yaml"
