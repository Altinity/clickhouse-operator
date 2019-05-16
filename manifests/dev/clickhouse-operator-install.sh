#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-dev}"
CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE:-altinity/clickhouse-operator:dev}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MANIFEST_ROOT=$(realpath ${CUR_DIR}/..)

echo "Setup ClickHouse Operator into ${CHOPERATOR_NAMESPACE} namespace"

# Let's setup all clickhouse-operator-related stuff into dedicated namespace
kubectl create namespace "${CHOPERATOR_NAMESPACE}"

# Setup into dedicated namespace
kubectl apply --namespace="${CHOPERATOR_NAMESPACE}" -f <(CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" ${CUR_DIR}/cat-clickhouse-operator-yaml.sh)
