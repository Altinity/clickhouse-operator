#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/../..")"
MANIFEST_ROOT="$(realpath ${PROJECT_ROOT}/deploy)"

source "${CUR_DIR}/dev-config.sh"

echo "Create ${OPERATOR_NAMESPACE} namespace"
kubectl create namespace "${OPERATOR_NAMESPACE}"

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
# Deploy prerequisites - CRDs, RBACs, etc
#
kubectl -n "${OPERATOR_NAMESPACE}" apply -f <( \
    OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
    OPERATOR_VERSION="${OPERATOR_VERSION}" \
    OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
    METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
    METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
    MANIFEST_PRINT_DEPLOYMENT="no" \
    "${MANIFEST_ROOT}/builder/cat-clickhouse-operator-install-yaml.sh" \
)

#
# Deploy operator's deployment
#
case "${DEPLOY_OPERATOR}" in
    "yes" | "release" | "prod" | "latest" | "dev")
        echo "Install operator from Docker Registry (dockerhub or whatever)"
        kubectl -n "${OPERATOR_NAMESPACE}" apply -f <( \
            OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
            OPERATOR_VERSION="${OPERATOR_VERSION}" \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
            METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
            METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
            MANIFEST_PRINT_CRD="no" \
            MANIFEST_PRINT_RBAC_CLUSTERED="no" \
            MANIFEST_PRINT_RBAC_NAMESPACED="no" \
        "${MANIFEST_ROOT}/builder/cat-clickhouse-operator-install-yaml.sh" \
        )
        ;;
    *)
        echo "------------------------------"
        echo "      !!! IMPORTANT !!!       "
        echo "No Operator would be installed"
        echo "------------------------------"
        ;;
esac
