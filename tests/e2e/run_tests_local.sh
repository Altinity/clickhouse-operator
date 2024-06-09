#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

OPERATOR_VERSION="${OPERATOR_VERSION:-"dev"}"
OPERATOR_DOCKER_REPO="${OPERATOR_DOCKER_REPO:-"altinity/clickhouse-operator"}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-"${OPERATOR_DOCKER_REPO}:${OPERATOR_VERSION}"}"
METRICS_EXPORTER_DOCKER_REPO="${METRICS_EXPORTER_DOCKER_REPO:-"altinity/metrics-exporter"}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"${METRICS_EXPORTER_DOCKER_REPO}:${OPERATOR_VERSION}"}"
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-"IfNotPresent"}"
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"test"}"
OPERATOR_INSTALL="${OPERATOR_INSTALL:-"yes"}"
ONLY="${ONLY:-"*"}"
MINIKUBE_RESET="${MINIKUBE_RESET:-""}"
VERBOSITY="${VERBOSITY:-"2"}"

# replace | apply
KUBECTL_MODE="${KUBECTL_MODE:-"replace"}"

echo "What would you like to start. Possible options:"
echo "  1     - test operator"
echo "  2     - test keeper"
echo "  3     - test metrics"
echo -n "Enter command choice (1, 2, 3)"
read COMMAND
# Trim EOL from the command received
COMMAND=$(echo "${COMMAND}" | tr -d '\n\t\r ')
echo "Provided command is: ${COMMAND}"
echo -n "Which means we are going to "
case "${COMMAND}" in
    "1")
        DEFAULT_EXECUTABLE="run_tests_operator.sh"
        echo "test OPERATOR"
        ;;
    "2")
        DEFAULT_EXECUTABLE="run_tests_keeper.sh"
        echo "test KEEPER"
        ;;
    "3")
        DEFAULT_EXECUTABLE="run_tests_metrics.sh"
        echo "test METRICS"
        ;;
    *)
        echo "test OPERATOR"
        ;;
esac

read -p "Press enter to start new release"
echo "Starting new release: ${NEW_RELEASE}"

EXECUTABLE="${EXECUTABLE:-"${DEFAULT_EXECUTABLE}"}"
MINIKUBE_PRELOAD_IMAGES="${MINIKUBE_PRELOAD_IMAGES:-""}"

if [[ ! -z ${MINIKUBE_RESET} ]]; then
    SKIP_K9S=yes ./run_minikube_reset.sh
fi

if [[ ! -z "${MINIKUBE_PRELOAD_IMAGES}" ]]; then
    echo "pre-load images into minikube"
    IMAGES="
    clickhouse/clickhouse-server:22.3
    clickhouse/clickhouse-server:22.6
    clickhouse/clickhouse-server:22.7
    clickhouse/clickhouse-server:22.8
    clickhouse/clickhouse-server:23.3
    clickhouse/clickhouse-server:23.8
    clickhouse/clickhouse-server:latest
    altinity/clickhouse-server:22.8.15.25.altinitystable
    docker.io/zookeeper:3.8.3
    "
    for image in ${IMAGES}; do
        docker pull -q ${image} && \
        echo "pushing to minikube" && \
        minikube image load ${image} --overwrite=false --daemon=true
    done
    echo "images pre-loaded"
fi

echo "Build" && \
VERBOSITY="${VERBOSITY}" ${CUR_DIR}/../../dev/image_build_all_dev.sh && \
echo "Load images" && \
minikube image load "${OPERATOR_IMAGE}" && \
minikube image load "${METRICS_EXPORTER_IMAGE}" && \
echo "Images prepared" && \
OPERATOR_DOCKER_REPO="${OPERATOR_DOCKER_REPO}" \
METRICS_EXPORTER_DOCKER_REPO="${METRICS_EXPORTER_DOCKER_REPO}" \
OPERATOR_VERSION="${OPERATOR_VERSION}" \
IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY}" \
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
OPERATOR_INSTALL="${OPERATOR_INSTALL}" \
ONLY="${ONLY}" \
KUBECTL_MODE="${KUBECTL_MODE}" \
"${CUR_DIR}/${EXECUTABLE}"
