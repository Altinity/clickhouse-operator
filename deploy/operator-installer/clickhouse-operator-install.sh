#!/bin/bash

# Namespace to install operator into
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-clickhouse-operator}"
# Namespace to install metrics-exporter into
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# Github branch for operator install
BRANCH="${BRANCH:-master}"

# Operator's docker image
OPERATOR_IMAGE="${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"
# Metrics exporter's docker image
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-altinity/metrics-exporter:latest}"

#
# Check whether kubectl is available
#
function is_kubectl_available() {
    if ! kubectl version > /dev/null; then
        echo "kubectl is unavailable, can not continue"
        exit 1
    fi
}

#
# Check whether curl is available
#
function is_curl_available() {
    if ! curl --version > /dev/null; then
        echo "curl is unavailable, can not continue"
        exit 1
    fi
}

#
# Check whether wget is available
#
function is_wget_available() {
    if ! wget --version > /dev/null; then
        echo "wget is unavailable, can not continue"
        exit 1
    fi
}

function check_file_getter_available() {
    if curl --version > /dev/null; then
        # curl is available - use it
        :
    elif wget --version > /dev/null; then
        # wget is available - use it
        :
    else
        echo "neither curl nor wget is unavailable, can not continue"
        exit 1
    fi
}


#
# Check whether envsubst is available
#
function checks_envsubst_available() {
    if ! envsubst --version > /dev/null; then
        echo "envsubst is unavailable, can not continue"
        exit 1
    fi
}

#
# Get file
#
function get_file() {
    local URL="$1"

    if curl --version > /dev/null; then
        # curl is available - use it
        curl -s "${URL}"
    elif wget --version > /dev/null; then
        # wget is available - use it
        wget -qO- "${URL}"
    else
        echo "neither curl nor wget is unavailable, can not continue"
        exit 1
    fi
}

##
## Main
##

check_file_getter_available
checks_envsubst_available

echo "Setup ClickHouse Operator into '${OPERATOR_NAMESPACE}' namespace"

if kubectl get namespace "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
    echo "Namespace '${OPERATOR_NAMESPACE}' already exists"
    if kubectl get deployment clickhouse-operator -n "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
        echo "clickhouse-operator is already installed in '${OPERATOR_NAMESPACE}' namespace. Will not install new. Abort."
        exit 1
    else
        echo "Looks like clickhouse-operator is not installed in '${OPERATOR_NAMESPACE}' namespace. Going to install"
    fi
else
    echo "No '${OPERATOR_NAMESPACE}' namespace found. Going to create"
    kubectl create namespace "${OPERATOR_NAMESPACE}"
fi

# Setup clickhouse-operator into specified namespace
get_file https://raw.githubusercontent.com/Altinity/clickhouse-operator/${BRANCH}/deploy/operator/clickhouse-operator-install-template.yaml | \
    OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
    OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
    METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
    METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
    envsubst | kubectl apply --namespace="${OPERATOR_NAMESPACE}" -f -
