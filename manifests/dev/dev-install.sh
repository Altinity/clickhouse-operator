#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source ${CUR_DIR}/dev-config.sh

echo "Create ${CHOPERATOR_NAMESPACE} namespace"
kubectl create namespace "${CHOPERATOR_NAMESPACE}"

if [[ "${INSTALL_FROM_ALTINITY_RELEASE_DOCKERHUB}" == "yes" ]]; then
    kubectl -n "${CHOPERATOR_NAMESPACE}" apply -f <(CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" ${CUR_DIR}/cat-clickhouse-operator-yaml.sh)

    # Installation done
    exit $?
else
    echo "Install operator requirements"
    echo "CHOPERATOR_NAMESPACE=${CHOPERATOR_NAMESPACE}"
    echo "CHOPERATOR_IMAGE=${CHOPERATOR_IMAGE}"

    kubectl -n "${CHOPERATOR_NAMESPACE}" apply -f <(CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" MANIFEST_PRINT_DEPLOYMENT="no" ${CUR_DIR}/cat-clickhouse-operator-yaml.sh)

    if [[ "${INSTALL_FROM_DEPLOYMENT_MANIFEST}" == "yes" ]]; then
        # Install operator from Docker Registry (dockerhub or whatever)
        kubectl -n "${CHOPERATOR_NAMESPACE}" apply -f <(CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" MANIFEST_PRINT_CRD="no" MANIFEST_PRINT_RBAC="no" ${CUR_DIR}/cat-clickhouse-operator-yaml.sh)
    fi
fi
