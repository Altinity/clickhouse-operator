#!/bin/bash

# Namespace to install operator
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-clickhouse-operator}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# Operator's docker image
OPERATOR_IMAGE="${OPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"
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
# Check whether envsubst is available
#
function is_envsubst_available() {
    if ! envsubst --version > /dev/null; then
        echo "curl is unavailable, can not continue"
        exit 1
    fi
}

##
## Main
##

is_curl_available
is_kubectl_available
is_envsubst_available

echo "Setup ClickHouse Operator into ${OPERATOR_NAMESPACE} namespace"

if kubectl get namespace "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
    echo "Namespace ${OPERATOR_NAMESPACE} already exists"
    if kubectl get deployment clickhouse-operator -n "${OPERATOR_NAMESPACE}" 1>/dev/null 2>/dev/null; then
        echo "clickhouse-operator is already installed into ${OPERATOR_NAMESPACE} namespace. Abort."
        exit 1
    else
        echo "Looks like clickhouse-operator is not installed in ${OPERATOR_NAMESPACE} namespace. Going to install"
    fi
else
    echo "No ${OPERATOR_NAMESPACE} namespace found. Going to create"
    kubectl create namespace "${OPERATOR_NAMESPACE}"
fi

# Setup clickhouse-operator into specified namespace
kubectl apply --namespace="${OPERATOR_NAMESPACE}" -f <( \
    curl -s https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-template.yaml | \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
        METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
        envsubst \
)
