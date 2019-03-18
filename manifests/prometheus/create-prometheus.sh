#!/bin/bash

PROMETHEUS_NAMESPACE="${PROMETHEUS_NAMESPACE:-prometheus}"
CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-kube-system}"

echo "Setup Prometheus into ${PROMETHEUS_NAMESPACE} namespace"

# Let's setup all prometheus-related stuff into dedicated namespace called "prometheus"
kubectl create namespace "${PROMETHEUS_NAMESPACE}"

# Create CRD for kind:Prometheus and kind:PrometheusRule Would be handled by prometheus-operator
kubectl apply --namespace="${PROMETHEUS_NAMESPACE}" -f prometheus.crd.yaml
kubectl apply --namespace="${PROMETHEUS_NAMESPACE}" -f prometheusrule.crd.yaml

# Setup prometheus-operator into dedicated namespace. Would manage prometheus instance
kubectl apply --namespace="${PROMETHEUS_NAMESPACE}" -f prometheus-operator.yaml

# Setup Prometheus instance via prometheus-operator into dedicated namespace
kubectl apply --namespace="${PROMETHEUS_NAMESPACE}" -f prometheus.yaml

# Setup "Prometheus - clickhouse-operator" integration.
# Specify endpoint, where Prometheus can gather data from clickhouse-operator
# IMPORTANT: clickhouse-operator should be installed and running prior to this step,
# otherwise Prometheus would not be able to setup integration with clickhouse-operator

if kubectl --namespace="${CHOPERATOR_NAMESPACE}" get service clickhouse-operator-metrics; then
    echo "clickhouse-operator-metrics endpoint found. Configuring integration with clickhouse-operator"
    # clickhouse-operator-metrics service found, can setup integration
    kubectl apply --namespace="${PROMETHEUS_NAMESPACE}" -f prometheus-clickhouse-operator-service-monitor.yaml
else
    echo "Unable to find clickhouse-operator-metrics endpoint. Please setup clickhouse-operator and restart this script."
fi
