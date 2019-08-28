#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source "${CUR_DIR}/dev-config.sh"

if [[ "${OPERATOR_NAMESPACE}" == "kube-system" ]]; then
    echo "Default k8s namespace 'kube-system' must not be deleted"

elif kubectl get namespace "${OPERATOR_NAMESPACE}"; then
    echo "Delete ClickHouse Operator namespace ${OPERATOR_NAMESPACE}"
    kubectl delete namespace "${OPERATOR_NAMESPACE}"

else
    echo "No namespace ${OPERATOR_NAMESPACE} available"
fi
