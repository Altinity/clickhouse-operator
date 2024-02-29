#!/bin/bash

NAMESPACE="${NAMESPACE:-"dev"}"

echo "Looking for ${NAMESPACE}/clickhouse-operator logs"
CLICKHOUSE_OPERATOR_POD=$(kubectl -n "${NAMESPACE}" get pod | awk '{print $1}' | grep clickhouse-operator)
if [[ -z "${CLICKHOUSE_OPERATOR_POD}" ]]; then
    echo "clickhouse-operator pod not found in namespace ${NAMESPACE}"
    echo "Abort."
    exit 1
fi
kubectl logs -n "${NAMESPACE}" "${CLICKHOUSE_OPERATOR_POD}" clickhouse-operator -f
