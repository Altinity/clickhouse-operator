#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source "${CUR_DIR}/dev-config.sh"

echo "Create ${OPERATOR_NAMESPACE} namespace"
kubectl create namespace "${OPERATOR_NAMESPACE}"

if [[ "${INSTALL_FROM_ALTINITY_RELEASE_DOCKERHUB}" == "yes" ]]; then
    kubectl -n "${OPERATOR_NAMESPACE}" apply -f <( \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
        METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
        "${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" \
        )

    # Installation done
    exit $?
else
    echo "Install operator requirements"
    echo "OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE}"
    echo "OPERATOR_IMAGE=${OPERATOR_IMAGE}"
    echo "METRICS_EXPORTER_NAMESPACE=${METRICS_EXPORTER_NAMESPACE}"
    echo "METRICS_EXPORTER_IMAGE=${METRICS_EXPORTER_IMAGE}"

    kubectl -n "${OPERATOR_NAMESPACE}" apply -f <( \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
        METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
        MANIFEST_PRINT_DEPLOYMENT="no" \
        "${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" \
        )

    if [[ "${INSTALL_FROM_DEPLOYMENT_MANIFEST}" == "yes" ]]; then
        # Install operator from Docker Registry (dockerhub or whatever)
        kubectl -n "${OPERATOR_NAMESPACE}" apply -f <( \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
            OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
            METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
            METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
            MANIFEST_PRINT_CRD="no" \
            MANIFEST_PRINT_RBAC="no" \
            "${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh" \
            )
    fi
fi
