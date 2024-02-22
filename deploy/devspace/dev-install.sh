#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source "${CUR_DIR}/dev-config.sh"

echo "Install operator requirements with the following options:"
echo "OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE}"
echo "OPERATOR_VERSION=${OPERATOR_VERSION}"
echo "OPERATOR_IMAGE=${OPERATOR_IMAGE}"
echo "OPERATOR_IMAGE_PULL_POLICY=${OPERATOR_IMAGE_PULL_POLICY}"
echo "METRICS_EXPORTER_NAMESPACE=${METRICS_EXPORTER_NAMESPACE}"
echo "METRICS_EXPORTER_IMAGE=${METRICS_EXPORTER_IMAGE}"
echo "METRICS_EXPORTER_IMAGE_PULL_POLICY=${METRICS_EXPORTER_IMAGE_PULL_POLICY}"
echo "DEPLOY_OPERATOR=${DEPLOY_OPERATOR}"
echo "MINIKUBE=${MINIKUBE}"

echo "Create namespace to deploy the operator into: ${OPERATOR_NAMESPACE}"
kubectl create namespace "${OPERATOR_NAMESPACE}"

echo "Deploy prerequisites - CRDs, RBACs, etc"
kubectl -n "${OPERATOR_NAMESPACE}" apply -f <(                         \
    OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}"                         \
    OPERATOR_VERSION="${OPERATOR_VERSION}"                             \
    OPERATOR_IMAGE="${OPERATOR_IMAGE}"                                 \
    METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}"         \
    METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}"                 \
    MANIFEST_PRINT_DEPLOYMENT="no"                                     \
    "${MANIFEST_ROOT}/builder/cat-clickhouse-operator-install-yaml.sh" \
)

if [[ "${MINIKUBE}" == "yes" ]]; then
    echo "Preparing for minikube"
    case "${DEPLOY_OPERATOR}" in
        "dev")
            echo "Clean images in minikube"
            echo "  1. ${OPERATOR_IMAGE}"
            echo "  2. ${METRICS_EXPORTER_IMAGE}"
            minikube image rm "${OPERATOR_IMAGE}"
            minikube image rm "${METRICS_EXPORTER_IMAGE}"

            echo "Build images"                             && \
            ${PROJECT_ROOT}/dev/image_build_all_dev.sh      && \
            echo "Load images into minikube:"               && \
            echo "  1. ${OPERATOR_IMAGE}"                   && \
            echo "  2. ${METRICS_EXPORTER_IMAGE}"           && \
            echo "Load images into minikube"                && \
            minikube image load "${OPERATOR_IMAGE}"         && \
            minikube image load "${METRICS_EXPORTER_IMAGE}" && \
            echo "Images loaded"
            ;;
    esac
fi

echo "Deploy operator's deployment"
case "${DEPLOY_OPERATOR}" in
    "yes" | "release" | "prod" | "latest" | "local" | "dev")
        echo "Install operator from Docker Registry (dockerhub or whatever)"
        kubectl -n "${OPERATOR_NAMESPACE}" apply -f <(                                 \
            OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}"                                 \
            OPERATOR_VERSION="${OPERATOR_VERSION}"                                     \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}"                                         \
            OPERATOR_IMAGE_PULL_POLICY="${OPERATOR_IMAGE_PULL_POLICY}"                 \
            CH_USERNAME_SECRET_PLAIN="clickhouse_operator"                             \
            CH_PASSWORD_SECRET_PLAIN="clickhouse_operator_password"                    \
            METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}"                 \
            METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}"                         \
            METRICS_EXPORTER_IMAGE_PULL_POLICY=""${METRICS_EXPORTER_IMAGE_PULL_POLICY} \
            MANIFEST_PRINT_CRD="no"                                                    \
            MANIFEST_PRINT_RBAC_CLUSTERED="no"                                         \
            MANIFEST_PRINT_RBAC_NAMESPACED="no"                                        \
                                                                                       \
            "${MANIFEST_ROOT}/builder/cat-clickhouse-operator-install-yaml.sh"         \
        )
        ;;
    *)
        echo "------------------------------"
        echo "      !!! IMPORTANT !!!       "
        echo "No Operator would be installed"
        echo "------------------------------"
        ;;
esac
