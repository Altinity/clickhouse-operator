#!/bin/bash

# Compose clickhouse-operator .yaml manifest from components

# Paths
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/../..")"

##########################################
##
## clickhouse-operator .yaml configuration
##
##########################################

# Namespace to install operator
CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-kube-system}"

# Operator's docker image
CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"

# Local path to operator's config file to be injected into .yaml
CHOPERATOR_CONFIG_FILE="${PROJECT_ROOT}/config/config.yaml"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/conf.d folder
CHOPERATOR_CONFD_FOLDER="${PROJECT_ROOT}/config/conf.d"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/config.d folder
CHOPERATOR_CONFIGD_FOLDER="${PROJECT_ROOT}/config/config.d"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/users.d folder
CHOPERATOR_USERSD_FOLDER="${PROJECT_ROOT}/config/users.d"

# Local path to folder with operator's .yaml template files which will be injected into .yaml
# as content of /etc/clickhouse-server/templates.d folder
CHOPERATOR_TEMPLATESD_FOLDER="${PROJECT_ROOT}/config/templates.d"


##
## .yaml manifest sections to be rendered
##

# Render operator's CRD
MANIFEST_PRINT_CRD="${MANIFEST_PRINT_CRD:-yes}"

# Render operator's RBAC and other parts needed during operator's install procedure
MANIFEST_PRINT_RBAC="${MANIFEST_PRINT_RBAC:-yes}"

# Render operator's Deployment section. May be not required in case of dev localhost run
MANIFEST_PRINT_DEPLOYMENT="${MANIFEST_PRINT_DEPLOYMENT:-yes}"

##################################
##
##     File handler
##
##################################

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

##################################
##
##     Render .yaml manifest
##
##################################


# Render CRD section
if [[ "${MANIFEST_PRINT_CRD}" == "yes" ]]; then
    ensure_file "${CUR_DIR}/clickhouse-operator-template-01-section-crd.yaml"
    cat "${CUR_DIR}/clickhouse-operator-template-01-section-crd.yaml" | \
        CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
fi

# Render RBAC section
if [[ "${MANIFEST_PRINT_RBAC}" == "yes" ]]; then
    echo "---"
    ensure_file "${CUR_DIR}/clickhouse-operator-template-02-section-rbac-and-service.yaml"
    cat "${CUR_DIR}/clickhouse-operator-template-02-section-rbac-and-service.yaml" | \
        CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
fi

# Render header/beginning of ConfigMap yaml specification:
# apiVersion: v1
# kind: ConfigMap
# metadata:
#  name: ${CONFIGMAP_NAME}
#  namespace: ${CHOPERATOR_NAMESPACE}
# data:
function render_configmap_header() {
    # ConfigMap name
    CM_NAME="$1"
    # Template file with ConfigMap header/beginning
    CM_HEADER_FILE="${CUR_DIR}/clickhouse-operator-template-03-section-configmap-header.yaml"

    ensure_file "${CM_HEADER_FILE}"

    # Render ConfigMap header template with vars substitution
    cat "${CM_HEADER_FILE}" | \
            CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" CONFIGMAP_NAME="${CM_NAME}" envsubst
}

# Render one file section in ConfigMap yaml specification:
# apiVersion: v1
# kind: ConfigMap
# metadata:
#   name: game-config
# data:
#   game.properties: |
#     enemies=aliens
#     lives=3
#
#   ui.properties: |
#     color.good=purple
function render_configmap_data_section_file() {
    FILE_PATH="$1"

    # ConfigMap .data section looks like
    #  config.yaml: |
    #    line 1
    #    line 2
    #    etc
    FILE_NAME="$(basename "${FILE_PATH}")"
    echo "  ${FILE_NAME}: |"
    cat "${FILE_PATH}" | sed 's/^/    /'
    echo ""
}

# Render Deployment and ConfigMap sections
if [[ "${MANIFEST_PRINT_DEPLOYMENT}" == "yes" ]]; then
    if [[ -z "${CHOPERATOR_CONFIG_FILE}" ]]; then
        # No config file specified, render simple deployment
        echo "---"
        ensure_file "${CUR_DIR}/clickhouse-operator-template-04-section-deployment.yaml"
        cat "${CUR_DIR}/clickhouse-operator-template-04-section-deployment.yaml" | \
            CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
    else
        # Config file specified, render all ConfigMaps and then render deployment

        echo "---"
        render_configmap_header "etc-clickhouse-operator-files"
        if [[ -f "${PROJECT_ROOT}/config/config.yaml" ]]; then
            # Render clickhouse-operator config file
            render_configmap_data_section_file "${PROJECT_ROOT}/config/config.yaml"
        else
            # Fetch from github and apply
            # config/config.yaml
            :
        fi

        # Render confd.d files
        echo "---"
        render_configmap_header "etc-clickhouse-operator-confd-files"
        if [[ ! -z "${CHOPERATOR_CONFD_FOLDER}" ]] && [[ -d "${CHOPERATOR_CONFD_FOLDER}" ]] && [[ ! -z "$(ls "${CHOPERATOR_CONFD_FOLDER}")" ]]; then
            for FILE in "${CHOPERATOR_CONFD_FOLDER}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render configd.d files
        echo "---"
        render_configmap_header "etc-clickhouse-operator-configd-files"
        if [[ ! -z "${CHOPERATOR_CONFIGD_FOLDER}" ]] && [[ -d "${CHOPERATOR_CONFIGD_FOLDER}" ]] && [[ ! -z "$(ls "${CHOPERATOR_CONFIGD_FOLDER}")" ]]; then
            for FILE in "${CHOPERATOR_CONFIGD_FOLDER}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        else
            # Fetch from github and apply
            # config/config.d/01-clickhouse-operator-listen.xml
            # config/config.d/01-clickhouse-operator-listen.xml
            :
        fi

        # Render templates.d files
        echo "---"
        render_configmap_header "etc-clickhouse-operator-templatesd-files"
        if [[ ! -z "${CHOPERATOR_TEMPLATESD_FOLDER}" ]] && [[ -d "${CHOPERATOR_TEMPLATESD_FOLDER}" ]] && [[ ! -z "$(ls "${CHOPERATOR_TEMPLATESD_FOLDER}")" ]]; then
            for FILE in "${CHOPERATOR_TEMPLATESD_FOLDER}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render users.d files
        echo "---"
        render_configmap_header "etc-clickhouse-operator-usersd-files"
        if [[ ! -z "${CHOPERATOR_USERSD_FOLDER}" ]] && [[ -d "${CHOPERATOR_USERSD_FOLDER}" ]] && [[ ! -z "$(ls "${CHOPERATOR_USERSD_FOLDER}")" ]]; then
            for FILE in "${CHOPERATOR_USERSD_FOLDER}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        else
            # Fetch from github and apply
            # config/users.d/01-clickhouse-operator-user.xml
            :
        fi

        # Render Deployment
        echo "---"
        ensure_file "${CUR_DIR}/clickhouse-operator-template-04-section-deployment-with-configmap.yaml"
        cat "${CUR_DIR}/clickhouse-operator-template-04-section-deployment-with-configmap.yaml" | \
            CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
    fi
fi
