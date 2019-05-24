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
CHOPERATOR_CONFIGD_FOLDER="${PROJECT_ROOT}/config/config.d"
CHOPERATOR_USERSD_FOLDER="${PROJECT_ROOT}/config/users.d"

# .yaml manifest sections to be rendered
MANIFEST_PRINT_CRD="${MANIFEST_PRINT_CRD:-yes}"
MANIFEST_PRINT_RBAC="${MANIFEST_PRINT_RBAC:-yes}"
MANIFEST_PRINT_DEPLOYMENT="${MANIFEST_PRINT_DEPLOYMENT:-yes}"

# Render CRD section
if [[ "${MANIFEST_PRINT_CRD}" == "yes" ]]; then
    cat ${CUR_DIR}/clickhouse-operator-template-01-section-crd.yaml | CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
fi

# Render RBAC section
if [[ "${MANIFEST_PRINT_RBAC}" == "yes" ]]; then
    echo "---"
    cat ${CUR_DIR}/clickhouse-operator-template-02-section-rbac-and-service.yaml | CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
fi

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
function render_file_into_configmap() {
    FILE_PATH=$1
    # ConfigMap .data section looks like
    #  config.yaml: |
    #    line 1
    #    line 2
    #    etc
    FILE=$(basename "${FILE_PATH}")
    echo "  ${FILE}: |"
    cat ${FILE_PATH} | sed 's/^/    /'
    echo ""
}

# Render Deployment and ConfigMap sections
if [[ "${MANIFEST_PRINT_DEPLOYMENT}" == "yes" ]]; then
    if [[ -z "${CHOPERATOR_CONFIG_FILE}" ]]; then
        # No config file specified, render simple deployment
        echo "---"
        cat ${CUR_DIR}/clickhouse-operator-template-03-section-deployment.yaml | CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst
    else
        # Config file specified, render deployment with ConfigMap
        echo "---"
        cat ${CUR_DIR}/clickhouse-operator-template-03-section-deployment-with-configmap.yaml | CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" envsubst

        # Render clickhouse-operator config file
        echo "---"
        cat ${CUR_DIR}/clickhouse-operator-template-04-section-configmap-header.yaml | CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" CONFIGMAP_NAME="etc-clickhouse-operator-files" envsubst
        render_file_into_configmap "${PROJECT_ROOT}/config/config.yaml"

        # Render configd.d files
        echo "---"
        cat ${CUR_DIR}/clickhouse-operator-template-04-section-configmap-header.yaml | CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" CONFIGMAP_NAME="etc-clickhouse-operator-configd-files" envsubst
        if [[ ! -z "${CHOPERATOR_CONFIGD_FOLDER}" ]]; then
            for FILE in ${CHOPERATOR_CONFIGD_FOLDER}/*; do
                render_file_into_configmap "${FILE}"
            done
        fi

        # Render users.d files
        echo "---"
        cat ${CUR_DIR}/clickhouse-operator-template-04-section-configmap-header.yaml | CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" CONFIGMAP_NAME="etc-clickhouse-operator-usersd-files" envsubst
        if [[ ! -z "${CHOPERATOR_USERSD_FOLDER}" ]]; then
            for FILE in ${CHOPERATOR_USERSD_FOLDER}/*; do
                render_file_into_configmap "${FILE}"
            done
        fi
    fi
fi

