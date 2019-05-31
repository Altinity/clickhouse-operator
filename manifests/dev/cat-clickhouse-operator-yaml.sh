#!/bin/bash

# Compose clickhouse-operator .yaml manifest from components

# Paths
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MANIFEST_ROOT=$(realpath ${CUR_DIR}/..)
PROJECT_ROOT=$(realpath ${CUR_DIR}/../..)

# clickhouse-operator details
CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-kube-system}"
CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"
CHOPERATOR_CONFIG_FILE="${PROJECT_ROOT}/config/config.yaml"
CHOPERATOR_CONFD_FOLDER="${PROJECT_ROOT}/config/conf.d"
CHOPERATOR_CONFIGD_FOLDER="${PROJECT_ROOT}/config/config.d"
CHOPERATOR_TEMPLATESD_FOLDER="${PROJECT_ROOT}/config/templates.d"
CHOPERATOR_USERSD_FOLDER="${PROJECT_ROOT}/config/users.d"

# .yaml manifest sections to be rendered
MANIFEST_PRINT_CRD="${MANIFEST_PRINT_CRD:-yes}"
MANIFEST_PRINT_RBAC="${MANIFEST_PRINT_RBAC:-yes}"
MANIFEST_PRINT_DEPLOYMENT="${MANIFEST_PRINT_DEPLOYMENT:-yes}"

# Render CRD section
if [[ "${MANIFEST_PRINT_CRD}" == "yes" ]]; then
    cat ${CUR_DIR}/clickhouse-operator-template-01-section-crd.yaml | \
        CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
fi

# Render RBAC section
if [[ "${MANIFEST_PRINT_RBAC}" == "yes" ]]; then
    echo "---"
    cat ${CUR_DIR}/clickhouse-operator-template-02-section-rbac-and-service.yaml | \
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

    # Render ConfigMap header template with vars substitution
    cat ${CM_HEADER_FILE} | \
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
    FILE_PATH=$1
    # ConfigMap .data section looks like
    #  config.yaml: |
    #    line 1
    #    line 2
    #    etc
    FILE_NAME=$(basename "${FILE_PATH}")
    echo "  ${FILE_NAME}: |"
    cat ${FILE_PATH} | sed 's/^/    /'
    echo ""
}

# Render Deployment and ConfigMap sections
if [[ "${MANIFEST_PRINT_DEPLOYMENT}" == "yes" ]]; then
    if [[ -z "${CHOPERATOR_CONFIG_FILE}" ]]; then
        # No config file specified, render simple deployment
        echo "---"
        cat ${CUR_DIR}/clickhouse-operator-template-04-section-deployment.yaml | \
            CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
    else
        # Config file specified, render all ConfigMaps and then render deployment

        # Render clickhouse-operator config file
        echo "---"
        render_configmap_header "etc-clickhouse-operator-files"
        render_configmap_data_section_file "${PROJECT_ROOT}/config/config.yaml"

        # Render confd.d files
        echo "---"
        render_configmap_header "etc-clickhouse-operator-confd-files"
        if [[ ! -z "${CHOPERATOR_CONFD_FOLDER}" ]] && [[ ! -z "$(ls ${CHOPERATOR_CONFD_FOLDER})" ]]; then
            for FILE in ${CHOPERATOR_CONFD_FOLDER}/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render configd.d files
        echo "---"
        render_configmap_header "etc-clickhouse-operator-configd-files"
        if [[ ! -z "${CHOPERATOR_CONFIGD_FOLDER}" ]] && [[ ! -z "$(ls ${CHOPERATOR_CONFIGD_FOLDER})" ]]; then
            for FILE in ${CHOPERATOR_CONFIGD_FOLDER}/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render templates.d files
        echo "---"
        render_configmap_header "etc-clickhouse-operator-templatesd-files"
        if [[ ! -z "${CHOPERATOR_TEMPLATESD_FOLDER}" ]] && [[ ! -z "$(ls ${CHOPERATOR_TEMPLATESD_FOLDER})" ]]; then
            for FILE in ${CHOPERATOR_TEMPLATESD_FOLDER}/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render users.d files
        echo "---"
        render_configmap_header "etc-clickhouse-operator-usersd-files"
        if [[ ! -z "${CHOPERATOR_USERSD_FOLDER}" ]] && [[ ! -z "$(ls ${CHOPERATOR_USERSD_FOLDER})" ]]; then
            for FILE in ${CHOPERATOR_USERSD_FOLDER}/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render Deployment
        echo "---"
        cat ${CUR_DIR}/clickhouse-operator-template-04-section-deployment-with-configmap.yaml | \
            CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
    fi
fi
