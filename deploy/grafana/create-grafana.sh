#!/bin/bash

GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"
BRANCH="${BRANCH:-master}"
echo "Setup Grafana into ${GRAFANA_NAMESPACE} namespace"

# Let's setup all grafana-related stuff into dedicated namespace called "grafana"
kubectl create namespace "${GRAFANA_NAMESPACE}"

# Setup grafana into dedicated namespace
kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/${BRANCH}/deploy/grafana/grafana.yaml
