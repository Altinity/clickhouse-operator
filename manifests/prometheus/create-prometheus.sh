#!/bin/bash

# Let's setup all prometheus-related stuff into dedicated namespace called "prometheus"
kubectl create namespace prometheus

# Create CRD for kind:Prometheus and kind:PrometheusRule Would be handled by prometheus-operator
kubectl apply --namespace=prometheus -f prometheus.crd.yaml
kubectl apply --namespace=prometheus -f prometheusrule.crd.yaml

# Setup prometheus-operator into dedicated namespace. Would manage prometheus instance
kubectl apply --namespace=prometheus -f prometheus-operator.yaml

# Setup Prometheus instance via prometheus-operator into dedicated namespace
kubectl apply --namespace=prometheus -f prometheus.yaml

# Setup "Prometheus - clickhouse-operator" integration.
# Specify endpoint, where Prometheus can gather data from clickhouse-operator
# IMPORTANT: clickhouse-operator should be installed and running prior to this step,
# otherwise Prometheus would not be able to setup integration with clickhouse-operator

if kubectl --namespace=kube-system get service clickhouse-operator-metrics; then
    # clickhouse-operator-metrics service found, can setup integration
    kubectl apply --namespace=prometheus -f prometheus-clickhouse-operator-service-monitor.yaml
else
    echo "Unable to find clickhouse-operator-metrics endpoint. Please setup clickhouse-operator and restart this script."
fi
