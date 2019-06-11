#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-dev}"
CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE:-altinity/clickhouse-operator:dev}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MANIFEST_ROOT=$(realpath ${CUR_DIR}/..)

if [[ "${CHOPERATOR_NAMESPACE}" == "kube-system" ]]; then
    echo "Default k8s namespace 'kube-system' must not be deleted"
    echo "Delete components only"
    kubectl delete --namespace="${CHOPERATOR_NAMESPACE}" -f <(CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" ${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh)
else
    echo "Delete ClickHouse Operator namespace ${CHOPERATOR_NAMESPACE}"
    kubectl delete namespace "${CHOPERATOR_NAMESPACE}"
fi
