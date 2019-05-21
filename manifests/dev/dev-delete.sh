#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source ${CUR_DIR}/dev-config.sh

if [[ "${CHOPERATOR_NAMESPACE}" == "kube-system" ]]; then
    echo "Default k8s namespace 'kube-system' must not be deleted"

elif kubectl get namespace "${CHOPERATOR_NAMESPACE}"; then
    echo "Delete ClickHouse Operator namespace ${CHOPERATOR_NAMESPACE}"
    kubectl delete namespace "${CHOPERATOR_NAMESPACE}"

else
    echo "No namespace ${CHOPERATOR_NAMESPACE} available"
fi
