#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-kube-system}"
CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MANIFEST_ROOT=$(realpath ${CUR_DIR}/..)

MANIFEST_PRINT_CRD_RBAC="${MANIFEST_PRINT_CRD_RBAC:-yes}"
MANIFEST_PRINT_DEPLOYMENT="${MANIFEST_PRINT_DEPLOYMENT:-yes}"

if [[ "${MANIFEST_PRINT_CRD_RBAC}" == "yes" ]]; then
    cat ${CUR_DIR}/clickhouse-operator-template-01-section-crd-and-rbac-and-service.yaml | CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
fi
if [[ "${MANIFEST_PRINT_DEPLOYMENT}" == "yes" ]]; then
    echo "---"
    cat ${CUR_DIR}/clickhouse-operator-template-02-section-deployment.yaml               | CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
fi
