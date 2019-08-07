#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-kube-system}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

echo "Setup ClickHouse Operator into ${CHOPERATOR_NAMESPACE} namespace"

echo "1. Build manifest"
CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" ${CUR_DIR}/build-clickhouse-operator-install-yaml.sh

# Let's setup all clickhouse-operator-related stuff into dedicated namespace
if [[ "${CHOPERATOR_NAMESPACE}" == "kube-system" ]]; then
    echo "2. No need to create kube-system namespace"
elif kubectl get namespace "${CHOPERATOR_NAMESPACE}" >/dev/null 2>&1; then
    echo "2. Namespace ${CHOPERATOR_NAMESPACE}  already exists, install operator into it"
else
    echo "2. Create namespace ${CHOPERATOR_NAMESPACE} and install operator into it"
    kubectl create namespace "${CHOPERATOR_NAMESPACE}"
fi

# Setup into dedicated namespace
echo "3. Install operator into ${CHOPERATOR_NAMESPACE} namespace"
kubectl apply --namespace="${CHOPERATOR_NAMESPACE}" -f clickhouse-operator-install.yaml
