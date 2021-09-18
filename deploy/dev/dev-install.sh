#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

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
    "${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" \
)

#
# Deploy operator's deployment
#
if [[ "${DEPLOY_OPERATOR}" == "yes" ]]; then
    # Install operator from Docker Registry (dockerhub or whatever)
    kubectl -n "${OPERATOR_NAMESPACE}" apply -f <( \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        OPERATOR_VERSION="${OPERATOR_VERSION}" \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
        MANIFEST_PRINT_CRD="no" \
        MANIFEST_PRINT_RBAC="no" \
        "${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" \
    )
else
    echo "------------------------------"
    echo "      !!! IMPORTANT !!!       "
    echo "No Operator would be installed"
    echo "------------------------------"
fi
