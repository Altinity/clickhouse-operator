#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/../..")"
MANIFEST_ROOT="$(realpath ${PROJECT_ROOT}/deploy)"

source "${CUR_DIR}/dev-config.sh"

echo "Create ${OPERATOR_NAMESPACE} namespace"
kubectl create namespace "${OPERATOR_NAMESPACE}" || true

echo "Install operator requirements"
echo "OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE}"
echo "OPERATOR_VERSION=${OPERATOR_VERSION}"
echo "OPERATOR_IMAGE=${OPERATOR_IMAGE}"
echo "METRICS_EXPORTER_NAMESPACE=${METRICS_EXPORTER_NAMESPACE}"
echo "METRICS_EXPORTER_IMAGE=${METRICS_EXPORTER_IMAGE}"
echo "DEPLOY_OPERATOR=${DEPLOY_OPERATOR}"
echo "MINIKUBE=${MINIKUBE}"

export MINIKUBE=${MINIKUBE:-yes}

#
# Run devspace
#
devspace dev --var=OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}" --var=DEVSPACE_DEBUG=delve
