#!/bin/bash

# Let's setup all prometheus-related stuff into dedicated namespace called "prometheus"
kubectl create namespace prometheus

# Setup prometheus-operator into dedicated namespace
kubectl apply --namespace=prometheus -f prometheus-operator.yaml

# Setup Prometheus itself via prometheus-operator into dedicated namespace
kubectl apply --namespace=prometheus -f prometheus.yaml

# Setup "Prometheus - clickhouse-operator" integration.
# Specify endpoint, where Prometheus can gather data from clickhouse-operator
kubectl apply --namespace=prometheus -f prometheus-clickhouse-operator-service-monitor.yaml
