#!/bin/bash

kubectl create namespace prometheus
kubectl apply -f prometheus-operator.yaml -n prometheus
kubectl apply -f prometheus.yaml -n prometheus
kubectl apply -f prometheus-clickhouse-operator-service-monitor.yaml -n prometheus

