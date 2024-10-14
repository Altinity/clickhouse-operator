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
# Possible options are:
#  1. operator
#  2. keeper
#  3. metrics
WHAT="${WHAT}"

# Possible options are:
#  1. replace
#  2. apply
KUBECTL_MODE="${KUBECTL_MODE:-"apply"}"

#
#
#
function select_test_goal() {
    local specified_goal="${1}"
    if [[ ! -z "${specified_goal}" ]]; then
        echo "Having specified explicitly: ${specified_goal}"
        return 0
    else
        echo "What would you like to start? Possible options:"
        echo "  1     - test operator"
        echo "  2     - test keeper"
        echo "  3     - test metrics"
        echo -n "Enter your choice (1, 2, 3): "
        read COMMAND
        # Trim EOL from the command received
        COMMAND=$(echo "${COMMAND}" | tr -d '\n\t\r ')
        case "${COMMAND}" in
        "1")
            echo "picking operator"
            return 1
            ;;
        "2")
            echo "piking keeper"
            return 2
            ;;
        "3")
            echo "picking metrics"
            return 3
            ;;
        *)
            echo "don't know what '${COMMAND}' is, so picking operator"
            return 1
            ;;
        esac
    fi
}

#
#
#
function goal_name() {
    local goal_code=${1}
    case "${goal_code}" in
        "0")
            echo "${WHAT}"
            ;;
        "1")
            echo "operator"
            ;;
        "2")
            echo "keeper"
            ;;
        "3")
            echo "metrics"
            ;;
        *)
            echo "operator"
            ;;
    esac
}

select_test_goal "${WHAT}"
WHAT=$(goal_name $?)

echo "Provided command is: ${WHAT}"
echo -n "Which means we are going to "
case "${WHAT}" in
    "operator")
        DEFAULT_EXECUTABLE="run_tests_operator.sh"
        echo "test OPERATOR"
        ;;
    "keeper")
        DEFAULT_EXECUTABLE="run_tests_keeper.sh"
        echo "test KEEPER"
        ;;
    "metrics")
        DEFAULT_EXECUTABLE="run_tests_metrics.sh"
        echo "test METRICS"
        ;;
    *)
        echo "exit because I do not know what '${WHAT}' is"
        exit 1
        ;;
esac

TIMEOUT=30
echo "Press <ENTER> to start test immediately (if you agree with specified options)"
echo "In case no input provided tests would start in ${TIMEOUT} seconds automatically"
read -t ${TIMEOUT}

EXECUTABLE="${EXECUTABLE:-"${DEFAULT_EXECUTABLE}"}"
MINIKUBE_PRELOAD_IMAGES="${MINIKUBE_PRELOAD_IMAGES:-""}"

if [[ ! -z "${MINIKUBE_RESET}" ]]; then
    SKIP_K9S="yes" ./run_minikube_reset.sh
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

#
# Build images and run tests
#
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
