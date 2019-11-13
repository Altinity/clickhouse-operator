#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-clickhouse-operator}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# Check whether kubectl is available
function ensure_kubectl() {
    if ! kubectl version > /dev/null; then
        echo "kubectl failed, can not continue"
        exit 1
    fi
}

# Check whether file is available locally
# and download it from github repo if need be
function ensure_file() {
    # Params
    local LOCAL_DIR="$1"
    local FILE="$2"
    local REPO_DIR="$3"

    local LOCAL_FILE="${LOCAL_DIR}/${FILE}"

    if [[ -f "${LOCAL_FILE}" ]]; then
        # File found, all is ok
        :
    else
        download_file "${LOCAL_DIR}" "${FILE}" "${REPO_DIR}"
    fi

    if [[ -f "${LOCAL_FILE}" ]]; then
        # File found, all is ok
        :
    else
        # File not found
        echo "Unable to get ${FILE}"
        exit 1
    fi
}

# Download file from github repo
function download_file() {
    # Params
    local LOCAL_DIR="$1"
    local FILE="$2"
    local REPO_DIR="$3"

    local LOCAL_FILE="${LOCAL_DIR}/${FILE}"

    REPO_URL="${REPO_URL:-https://raw.githubusercontent.com/Altinity/clickhouse-operator}"
    BRANCH="${BRANCH:-master}"
    FILE_URL="${REPO_URL}/${BRANCH}/${REPO_DIR}/${FILE}"

    # Check curl is in place
    if ! curl --version > /dev/null; then
        echo "curl is not available, can not continue"
        exit 1
    fi

    # Download file
    if ! curl --silent "${FILE_URL}" --output "${LOCAL_FILE}"; then
        echo "curl call to download ${FILE_URL} failed, can not continue"
        exit 1
    fi

    # Check file is in place
    if [[ -f "${LOCAL_FILE}" ]]; then
        # File found, all is ok
        :
    else
        # File not found
        echo "Unable to download ${FILE_URL}"
        exit 1
    fi
}

#
# Main
#

ensure_kubectl
ensure_file "${CUR_DIR}" "cat-clickhouse-operator-install-yaml.sh" "deploy/dev"

echo "Setup ClickHouse Operator into ${OPERATOR_NAMESPACE} namespace"

if kubectl get namespace "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
    echo "Namespace ${OPERATOR_NAMESPACE} already exists"
    if kubectl get deployment clickhouse-operator -n "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
        echo "clickhouse-operator is already installed into ${OPERATOR_NAMESPACE} namespace. Abort."
        exit 1
    fi
else
    # No ${OPERATOR_NAMESPACE} namespace found, let's create it
    # Let's setup all clickhouse-operator-related stuff into dedicated namespace
    kubectl create namespace "${OPERATOR_NAMESPACE}"
fi

# Setup into dedicated namespace
kubectl apply --namespace="${OPERATOR_NAMESPACE}" -f <(OPERATOR_IMAGE="${OPERATOR_IMAGE}" OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" /bin/bash "${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh")
