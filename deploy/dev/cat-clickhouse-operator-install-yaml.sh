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
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# Operator's docker image
RELEASE_VERSION=$(cat ${PROJECT_ROOT}/release)
OPERATOR_VERSION="${OPERATOR_VERSION:-$RELEASE_VERSION}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-altinity/clickhouse-operator:$OPERATOR_VERSION}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-altinity/metrics-exporter:$OPERATOR_VERSION}"

# Local path to operator's config file to be injected into .yaml
OPERATOR_CONFIG_FILE="${OPERATOR_CONFIG_FILE:-${PROJECT_ROOT}/config/config.yaml}"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/conf.d folder
OPERATOR_CONFD_FOLDER="${PROJECT_ROOT}/config/conf.d"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/config.d folder
OPERATOR_CONFIGD_FOLDER="${PROJECT_ROOT}/config/config.d"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/users.d folder
OPERATOR_USERSD_FOLDER="${PROJECT_ROOT}/config/users.d"

# Local path to folder with operator's .yaml template files which will be injected into .yaml
# as content of /etc/clickhouse-server/templates.d folder
OPERATOR_TEMPLATESD_FOLDER="${PROJECT_ROOT}/config/templates.d"


##
## .yaml manifest sections to be rendered
##

# Render operator's CRD
MANIFEST_PRINT_CRD="${MANIFEST_PRINT_CRD:-yes}"

# Render operator's RBAC and other parts needed during operator's install procedure
MANIFEST_PRINT_RBAC="${MANIFEST_PRINT_RBAC:-yes}"

# Render operator's Deployment section. May be not required in case of dev localhost run
MANIFEST_PRINT_DEPLOYMENT="${MANIFEST_PRINT_DEPLOYMENT:-yes}"

# Render operator's Service
MANIFEST_PRINT_SERVICE="${MANIFEST_PRINT_SERVICE:-yes}"

SED_ARGS=""
if [[ "${OPERATOR_NAMESPACE}" == "-" ]]; then
    SED_ARGS="/^  namespace:/ d"
fi

##################################
##
##     File handler
##
##################################

#
# Ensure file `FILE` is in place in `LOCAL_DIR`, if absent, fetch file from `REPO_DIR`
#
function ensure_file() {
    # Params
    local LOCAL_DIR="$1"
    local FILE="$2"
    local GITHUB_REPO_DIR="$3"

    local LOCAL_FILE="${LOCAL_DIR}/${FILE}"

    if [[ -f "${LOCAL_FILE}" ]]; then
        # Local file found, can use it as is
        :
    else
        # Download file into local dir from github dir
        download_file "${LOCAL_DIR}" "${FILE}" "${GITHUB_REPO_DIR}"
    fi

    if [[ -f "${LOCAL_FILE}" ]]; then
        # Local file found - must have been downloaded, can use now
        :
    else
        # File not found
        echo "Unable to get ${FILE}"
        exit 1
    fi
}

#
# Download file `FILE` into `LOCAL_DIR` from `REPO_DIR`
#
function download_file() {
    # Params
    local LOCAL_DIR="$1"
    local FILE="$2"
    local GITHUB_REPO_DIR="$3"

    local LOCAL_FILE="${LOCAL_DIR}/${FILE}"

    GITHUB_REPO_BASE_URL="https://raw.githubusercontent.com/Altinity/clickhouse-operator"
    BRANCH="${BRANCH:-master}"
    FILE_URL="${GITHUB_REPO_BASE_URL}/${BRANCH}/${GITHUB_REPO_DIR}/${FILE}"

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
# Start render section
#
SECTION_RENDER_NUM=0
function section_render_start() {
    SECTION_RENDER_NUM=$((SECTION_RENDER_NUM+1))
}

#
# Print separator, except first time
#
function render_separator() {
    section_render_start

    if [[ "${SECTION_RENDER_NUM}" -gt 1 ]]; then
        echo "---"
    fi
}

##################################
##
##     Render .yaml manifest
##
##################################

REPO_PATH_DEPLOY_DEV_FOLDER="deploy/dev"
REPO_PATH_OPERATOR_CONFIG_FOLDER="config"

# Render CRD section
if [[ "${MANIFEST_PRINT_CRD}" == "yes" ]]; then
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-01-section-crd-01-chi.yaml"
    ensure_file "${CUR_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_DEPLOY_DEV_FOLDER}"
    render_separator
    cat "${CUR_DIR}/${SECTION_FILE_NAME}" | \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        envsubst

    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-01-section-crd-02-chit.yaml"
    ensure_file "${CUR_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_DEPLOY_DEV_FOLDER}"
    render_separator
    cat "${CUR_DIR}/${SECTION_FILE_NAME}" | \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        envsubst

    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-01-section-crd-03-chopconf.yaml"
    ensure_file "${CUR_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_DEPLOY_DEV_FOLDER}"
    render_separator
    cat "${CUR_DIR}/${SECTION_FILE_NAME}" | \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        envsubst
fi

# Render RBAC section
if [[ "${MANIFEST_PRINT_RBAC}" == "yes" ]]; then
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-02-section-rbac.yaml"
    ensure_file "${CUR_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_DEPLOY_DEV_FOLDER}"
    render_separator
    cat "${CUR_DIR}/${SECTION_FILE_NAME}" | \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        envsubst
fi

# Render header/beginning of ConfigMap yaml specification:
# apiVersion: v1
# kind: ConfigMap
# metadata:
#  name: ${CONFIGMAP_NAME}
#  namespace: ${OPERATOR_NAMESPACE}
# data:
function render_configmap_header() {
    # ConfigMap name
    CM_NAME="$1"
    # Template file with ConfigMap header/beginning

    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-03-section-configmap-header.yaml"
    ensure_file "${CUR_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_DEPLOY_DEV_FOLDER}"
    # Render ConfigMap header template with vars substitution
    cat "${CUR_DIR}/${SECTION_FILE_NAME}" | \
            sed "${SED_ARGS}" | \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
            OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
            CONFIGMAP_NAME="${CM_NAME}" \
            envsubst
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
    # Add some spaces to the beginning of each line - proper indent for .yaml file
    # Build ConfigMap section
    FILE_NAME="$(basename "${FILE_PATH}")"
    echo "  ${FILE_NAME}: |"
    cat "${FILE_PATH}" | sed 's/^/    /'
    echo ""
}

# Render Deployment and ConfigMap sections
if [[ "${MANIFEST_PRINT_DEPLOYMENT}" == "yes" ]]; then
    if [[ -z "${OPERATOR_CONFIG_FILE}" ]]; then
        # No config file specified, render simple deployment, w/o ConfigMaps

        SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-04-section-deployment.yaml"
        ensure_file "${CUR_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_DEPLOY_DEV_FOLDER}"
        render_separator
        cat "${CUR_DIR}/${SECTION_FILE_NAME}" | \
            sed "${SED_ARGS}" | \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
            OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
            METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
            METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
            envsubst
    else
        # Config file specified, render all ConfigMaps and then render deployment

        render_separator
        render_configmap_header "etc-clickhouse-operator-files"
        if [[ -f "${PROJECT_ROOT}/config/config.yaml" ]]; then
            # Render clickhouse-operator config file
            render_configmap_data_section_file "${PROJECT_ROOT}/config/config.yaml"
        else
            # Fetch from github and apply
            # config/config.yaml
            download_file "${CUR_DIR}" "config.yaml" "${REPO_PATH_OPERATOR_CONFIG_FOLDER}"
            render_configmap_data_section_file "${CUR_DIR}/config.yaml"
        fi

        # Render confd.d files
        render_separator
        render_configmap_header "etc-clickhouse-operator-confd-files"
        if [[ ! -z "${OPERATOR_CONFD_FOLDER}" ]] && [[ -d "${OPERATOR_CONFD_FOLDER}" ]] && [[ ! -z "$(ls "${OPERATOR_CONFD_FOLDER}")" ]]; then
            for FILE in "${OPERATOR_CONFD_FOLDER}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render configd.d files
        render_separator
        render_configmap_header "etc-clickhouse-operator-configd-files"
        if [[ ! -z "${OPERATOR_CONFIGD_FOLDER}" ]] && [[ -d "${OPERATOR_CONFIGD_FOLDER}" ]] && [[ ! -z "$(ls "${OPERATOR_CONFIGD_FOLDER}")" ]]; then
            for FILE in "${OPERATOR_CONFIGD_FOLDER}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        else
            # Fetch from github and apply
            # config/config.d/01-clickhouse-listen.xml
            # config/config.d/02-clickhouse-logger.xml
            download_file "${CUR_DIR}" "01-clickhouse-listen.xml" "${REPO_PATH_OPERATOR_CONFIG_FOLDER}/config.d"
            download_file "${CUR_DIR}" "02-clickhouse-logger.xml" "${REPO_PATH_OPERATOR_CONFIG_FOLDER}/config.d"
            render_configmap_data_section_file "${CUR_DIR}/01-clickhouse-listen.xml"
            render_configmap_data_section_file "${CUR_DIR}/02-clickhouse-logger.xml"
        fi

        # Render templates.d files
        render_separator
        render_configmap_header "etc-clickhouse-operator-templatesd-files"
        if [[ ! -z "${OPERATOR_TEMPLATESD_FOLDER}" ]] && [[ -d "${OPERATOR_TEMPLATESD_FOLDER}" ]] && [[ ! -z "$(ls "${OPERATOR_TEMPLATESD_FOLDER}")" ]]; then
            for FILE in "${OPERATOR_TEMPLATESD_FOLDER}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render users.d files
        render_separator
        render_configmap_header "etc-clickhouse-operator-usersd-files"
        if [[ ! -z "${OPERATOR_USERSD_FOLDER}" ]] && [[ -d "${OPERATOR_USERSD_FOLDER}" ]] && [[ ! -z "$(ls "${OPERATOR_USERSD_FOLDER}")" ]]; then
            for FILE in "${OPERATOR_USERSD_FOLDER}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        else
            # Fetch from github and apply
            # config/users.d/01-clickhouse-user.xml
            download_file "${CUR_DIR}" "01-clickhouse-user.xml" "${REPO_PATH_OPERATOR_CONFIG_FOLDER}/users.d"
            render_configmap_data_section_file "${CUR_DIR}/01-clickhouse-user.xml"
        fi

        # Render Deployment
        SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-04-section-deployment-with-configmap.yaml"
        ensure_file "${CUR_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_DEPLOY_DEV_FOLDER}"
        render_separator
        cat "${CUR_DIR}/${SECTION_FILE_NAME}" | \
            sed "${SED_ARGS}" | \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
            OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
            METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
            METRICS_EXPORTER_NAMESPACE="${METRICS_EXPORTER_NAMESPACE}" \
            envsubst
    fi
fi

# Render Service section
if [[ "${MANIFEST_PRINT_SERVICE}" == "yes" ]]; then
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-05-section-service.yaml"
    ensure_file "${CUR_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_DEPLOY_DEV_FOLDER}"
    render_separator
    cat "${CUR_DIR}/${SECTION_FILE_NAME}" | \
        sed "${SED_ARGS}" | \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}" \
        OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
        envsubst
fi
