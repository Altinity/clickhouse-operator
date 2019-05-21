#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-kube-system}"


echo "Setup ClickHouse Operator into ${CHOPERATOR_NAMESPACE} namespace"

# Let's setup all clickhouse-operator-related stuff into dedicated namespace
kubectl create namespace "${CHOPERATOR_NAMESPACE}"

# Setup into dedicated namespace
kubectl apply --namespace="${CHOPERATOR_NAMESPACE}" -f clickhouse-operator-install.yaml
