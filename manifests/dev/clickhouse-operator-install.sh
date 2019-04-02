#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-dev}"

echo "Setup ClickHouse Operator into ${CHOPERATOR_NAMESPACE} namespace"

# Let's setup all clickhouse-operator-related stuff into dedicated namespace
kubectl create namespace "${CHOPERATOR_NAMESPACE}"

# Setup into dedicated namespace
kubectl apply --namespace="${CHOPERATOR_NAMESPACE}" -f <(cat clickhouse-operator-install-dev-template.yaml | CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst)
