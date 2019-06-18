#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-dev}"
CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE:-altinity/clickhouse-operator:dev}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

function ensure_kubectl() {
    if ! kubectl version; then
        echo "kubectl failed, can not continue"
        exit 1
    fi
}

function ensure_file() {
    local FILE="$1"

    if [[ -f "${FILE}" ]]; then
        # File found, all is ok
        :
    else
        # File not found, try to download it
        if ! curl --version > /dev/null; then
            echo "curl is not available, can not continue"
            exit 1
        fi

        local BASE="$(basename "${FILE}")"
        ALTINITY_REPO_URL="https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/manifests/dev/"
        ALTINITY_REPO_URL="https://raw.githubusercontent.com/Altinity/clickhouse-operator/dev-vladislav/manifests/dev/"
        if ! curl --silent "${ALTINITY_REPO_URL}${BASE}" --output "${FILE}"; then
            echo "curl call to download ${BASE} failed, can not continue"
            exit 1
        fi
    fi

    if [[ -f "${FILE}" ]]; then
        # File found, all is ok
        :
    else
        # File not found
        echo "Unable to download ${FILE}"
        exit 1
    fi
}

ensure_kubectl
ensure_file "${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh"

echo "Setup ClickHouse Operator into ${CHOPERATOR_NAMESPACE} namespace"

# Let's setup all clickhouse-operator-related stuff into dedicated namespace
kubectl create namespace "${CHOPERATOR_NAMESPACE}"

# Setup into dedicated namespace
kubectl apply --namespace="${CHOPERATOR_NAMESPACE}" -f <(CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" "${CUR_DIR}/cat-clickhouse-operator-install-yaml.sh")
