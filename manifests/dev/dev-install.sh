#!/bin/bash

DEV_NAMESPACE="dev"

echo "Create ${DEV_NAMESPACE} namespace"
kubectl create namespace "${DEV_NAMESPACE}"

# Full dev install in k8s
#kubectl -n "${DEV_NAMESPACE}" apply -f ./clickhouse-operator-install.yaml

# Partial dev install
echo "Install operator requirements"
kubectl -n "${DEV_NAMESPACE}" apply -f ./custom-resource-definition.yaml
kubectl -n "${DEV_NAMESPACE}" apply -f ./rbac-service.yaml

# Install operator from Docker Registry (dockerhub or whatever)
kubectl -n "${DEV_NAMESPACE}" apply -f ./deployment-dockerhub-sunsingerus-dev.yaml
