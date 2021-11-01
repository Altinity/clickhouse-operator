#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"kube-system"}"


if [[ "${OPERATOR_NAMESPACE}" == "kube-system" ]]; then
    echo "Default k8s namespace 'kube-system' must not be deleted"
    echo "Delete components only"
    kubectl delete --namespace="${OPERATOR_NAMESPACE}" -f clickhouse-operator-install-bundle.yaml
else
    echo "Delete ClickHouse Operator namespace ${OPERATOR_NAMESPACE}"
    kubectl delete namespace "${OPERATOR_NAMESPACE}"
fi
