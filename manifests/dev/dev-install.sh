#!/bin/bash

. ./dev-config.sh

echo "Create ${DEV_NAMESPACE} namespace"
kubectl create namespace "${DEV_NAMESPACE}"

if [[ ${INSTALL_FROM_ALTINITY_RELEASE_DOCKERHUB} == "yes" ]]; then
    # Full dev install in k8s
    kubectl -n "${DEV_NAMESPACE}" apply -f ./clickhouse-operator-install.yaml

    # Installation done
    exit $?
fi

# Dev install from all components
echo "Install operator requirements"
kubectl -n "${DEV_NAMESPACE}" apply -f ./custom-resource-definition.yaml
kubectl -n "${DEV_NAMESPACE}" apply -f ./rbac-service.yaml

if [[ ${INSTALL_FROM_PERSONAL_DEVELOP_DOCKERHUB} == "yes" ]]; then
    # Install operator from Docker Registry (dockerhub or whatever)
    kubectl -n "${DEV_NAMESPACE}" apply -f ./deployment-dockerhub-sunsingerus-dev.yaml
fi
