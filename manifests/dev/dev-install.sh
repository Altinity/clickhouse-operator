#!/bin/bash

kubectl create namespace dev

# Full dev install in k8s
#kubectl -n dev apply -f ./clickhouse-operator-install.yaml

# Partial dev install
kubectl -n dev apply -f ./custom-resource-definition.yaml
kubectl -n dev apply -f ./rbac-service.yaml
#kubectl -n dev apply -f ./deployment.yaml
