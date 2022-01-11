#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"kube-system"}"
VALIDATE_YAML="${VALIDATE_YAML:-"true"}"
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

echo "Setup ClickHouse Operator into ${OPERATOR_NAMESPACE} namespace"

# Let's setup all clickhouse-operator-related stuff into dedicated namespace
if [[ "${OPERATOR_NAMESPACE}" == "kube-system" ]]; then
    echo "1. No need to create kube-system namespace"
elif kubectl get namespace "${OPERATOR_NAMESPACE}" >/dev/null 2>&1; then
    echo "1. Namespace ${OPERATOR_NAMESPACE}  already exists, install operator into it"
else
    echo "1. Create namespace ${OPERATOR_NAMESPACE} and install operator into it"
    kubectl create namespace "${OPERATOR_NAMESPACE}"
fi

# Setup into dedicated namespace
echo "2. Install operator into ${OPERATOR_NAMESPACE} namespace"
kubectl apply --validate=${VALIDATE_YAML} --namespace="${OPERATOR_NAMESPACE}" -f "${CUR_DIR}/clickhouse-operator-install-bundle.yaml"
