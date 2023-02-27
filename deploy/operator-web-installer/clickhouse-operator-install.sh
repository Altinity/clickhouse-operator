#!/bin/bash

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

#
# Check whether any download tool (curl, wget) is available
#
function check_file_getter_available() {
    if curl --version > /dev/null; then
        # curl is available - use it
        :
    elif wget --version > /dev/null; then
        # wget is available - use it
        :
    else
        echo "neither curl nor wget is available, can not continue"
        exit 1
    fi
}


#
# Check whether envsubst is available
#
function check_envsubst_available() {
    if ! envsubst --version > /dev/null; then
        echo "envsubst is unavailable, can not continue"
        exit 1
    fi
}

#
# Get file
#
function get_file() {
    local url="$1"

    if curl --version > /dev/null; then
        # curl is available - use it
        curl -s "${url}"
    elif wget --version > /dev/null; then
        # wget is available - use it
        wget -qO- "${url}"
    else
        echo "neither curl nor wget is available, can not continue"
        exit 1
    fi
}

function ensure_namespace() {
    local namespace="${1}"
    if kubectl get namespace "${namespace}" 1>/dev/null 2>/dev/null; then
        echo "Namespace '${namespace}' already exists."
    else
        echo "No '${namespace}' namespace found. Going to create."
        kubectl create namespace "${namespace}"
    fi
}

function check_deployment() {
    local namespace="${1}"
    local update=""${2}
    if kubectl get namespace "${namespace}" 1>/dev/null 2>/dev/null; then
        echo "Namespace '${namespace}' exists."
        if kubectl -n "${namespace}" get deployment clickhouse-operator 1>/dev/null 2>/dev/null; then
            echo "clickhouse-operator is already installed in '${namespace}' namespace."
            if [[ "${update}" == "yes" ]]; then
                echo "Will update. Continue."
            else
                echo "Will NOT update. Abort."
                exit 1
            fi
        else
            echo "Looks like clickhouse-operator is not installed in '${namespace}' namespace."
        fi
    else
        echo "No '${namespace}' namespace found. Abort."
        exit 2
    fi
}

function check_http() {
    # ${1,,} converts $1 to lowercase
    if [[ ${1,,} =~ ^https?:// ]]; then
        return 0
    else
        return 1
    fi
}

check_file_getter_available
check_envsubst_available

#
# Setup SOME variables
#
# Full list of available vars is available in cat-clickhouse-operator-install-yaml.sh file
#

# Manifest is expected to be ready-to-use manifest file
MANIFEST="${MANIFEST:-""}"
# Template can have params to substitute
DEFAULT_TEMPLATE="https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/operator/clickhouse-operator-install-template.yaml"
TEMPLATE="${TEMPLATE:-"${DEFAULT_TEMPLATE}"}"
# Namespace to install operator
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"kube-system"}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"
# Operator's docker image
if [[ -z "${OPERATOR_VERSION}" ]]; then
    # Going for default template
    RELEASE_VERSION=$(get_file https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/release)
fi
OPERATOR_VERSION="${OPERATOR_VERSION:-"${RELEASE_VERSION}"}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:${OPERATOR_VERSION}"}"
OPERATOR_IMAGE_PULL_POLICY="${OPERATOR_IMAGE_PULL_POLICY:-"Always"}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:${OPERATOR_VERSION}"}"
METRICS_EXPORTER_IMAGE_PULL_POLICY="${METRICS_EXPORTER_IMAGE_PULL_POLICY:-"Always"}"
# Should existing operator deployment be updated?
UPDATE="${UPDATE:-"yes"}"
VALIDATE_YAML="${VALIDATE_YAML:-"true"}"

##
## Main
##

echo "Setup ClickHouse Operator into '${OPERATOR_NAMESPACE}' namespace."

ensure_namespace "${OPERATOR_NAMESPACE}"
check_deployment "${OPERATOR_NAMESPACE}" "${UPDATE}"


if [[ ! -z "${MANIFEST}" ]]; then
    # Manifest is in place
    echo "Install operator from manifest ${MANIFEST} into ${OPERATOR_NAMESPACE} namespace"
    kubectl apply --validate="${VALIDATE_YAML}" --namespace="${OPERATOR_NAMESPACE}" -f <( \
      cat "${MANIFEST}" | \
      OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
      OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
      OPERATOR_IMAGE_PULL_POLICY="${OPERATOR_IMAGE_PULL_POLICY}" \
      METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
      METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
      METRICS_EXPORTER_IMAGE_PULL_POLICY="${METRICS_EXPORTER_IMAGE_PULL_POLICY}" \
      envsubst \
    )
elif [[ ! -z "${TEMPLATE}" ]]; then
    # Template is in place
    echo "Install operator from template ${TEMPLATE} into ${OPERATOR_NAMESPACE} namespace"
    kubectl apply --validate="${VALIDATE_YAML}" --namespace="${OPERATOR_NAMESPACE}" -f <( \
        get_file "${TEMPLATE}" | \
            OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
            OPERATOR_IMAGE_PULL_POLICY="${OPERATOR_IMAGE_PULL_POLICY}" \
            METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
            METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
            METRICS_EXPORTER_IMAGE_PULL_POLICY="${METRICS_EXPORTER_IMAGE_PULL_POLICY}" \
            envsubst \
    )
else
    echo "Neither manifest not template available. Abort."
fi
