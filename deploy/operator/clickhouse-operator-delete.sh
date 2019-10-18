#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-kube-system}"


if [[ "${CHOPERATOR_NAMESPACE}" == "kube-system" ]]; then
    echo "Default k8s namespace 'kube-system' must not be deleted"
    echo "Delete components only"
    kubectl delete --namespace="${CHOPERATOR_NAMESPACE}" -f clickhouse-operator-install.yaml
else
    echo "Delete ClickHouse Operator namespace ${CHOPERATOR_NAMESPACE}"
    kubectl delete namespace "${CHOPERATOR_NAMESPACE}"
fi
