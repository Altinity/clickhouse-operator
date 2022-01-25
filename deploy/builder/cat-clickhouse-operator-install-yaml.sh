#!/bin/bash

# Compose clickhouse-operator .yaml manifest from components

# Paths
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/../..")"
PROJECT_TEMP="$(realpath "${PROJECT_ROOT}/tmp")"

# Relative and abs paths where templates live
TEMPLATES_PATH="deploy/builder/templates-install-bundle"
TEMPLATES_DIR="${PROJECT_ROOT}/${TEMPLATES_PATH}"

# Relative and abs paths where config live
CONFIG_PATH="config"
CONFIG_DIR="${PROJECT_ROOT}/${CONFIG_PATH}"

source "${CUR_DIR}/lib/lib.sh"

##########################################
##
## clickhouse-operator .yaml configuration
##
##########################################

# Namespace where to install operator and metrics-exporter to
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"kube-system"}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# Operator's docker image
RELEASE_VERSION=$(cat "${PROJECT_ROOT}/release")
OPERATOR_VERSION="${OPERATOR_VERSION:-"${RELEASE_VERSION}"}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:${OPERATOR_VERSION}"}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:${OPERATOR_VERSION}"}"

# Render configs into temp config folder
TMP_CONFIG_DIR="${PROJECT_TEMP}/$(date +%s)"

# Local path to operator's config file to be injected into .yaml
TMP_CONFIG_FILE="${TMP_CONFIG_DIR}/config.yaml"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/conf.d folder
TMP_CONFD_DIR="${TMP_CONFIG_DIR}/conf.d"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/config.d folder
TMP_CONFIGD_DIR="${TMP_CONFIG_DIR}/config.d"

# Local path to folder with operator's .yaml template files which will be injected into .yaml
# as content of /etc/clickhouse-server/templates.d folder
TMP_TEMPLATESD_DIR="${TMP_CONFIG_DIR}/templates.d"

# Local path to folder with ClickHouse's .xml configuration files which will be injected into .yaml
# as content of /etc/clickhouse-server/users.d folder
TMP_USERSD_DIR="${TMP_CONFIG_DIR}/users.d"

# Generate and cleanup configs
"${CUR_DIR}"/build-clickhouse-operator-configs.sh "${TMP_CONFIG_DIR}"
function cleanup {
  rm  -rf "${TMP_CONFIG_DIR}"
}
trap cleanup EXIT

##
## .yaml manifest sections to be rendered
##

# Render operator's CRD
MANIFEST_PRINT_CRD="${MANIFEST_PRINT_CRD:-"yes"}"

# Render operator's RBAC (cluster-wide or namespaced)
MANIFEST_PRINT_RBAC_CLUSTERED="${MANIFEST_PRINT_RBAC_CLUSTERED:-"unspecified"}"
MANIFEST_PRINT_RBAC_NAMESPACED="${MANIFEST_PRINT_RBAC_NAMESPACED:-"unspecified"}"
if [[ "${MANIFEST_PRINT_RBAC_CLUSTERED}" == "unspecified" ]] && [[ "${MANIFEST_PRINT_RBAC_NAMESPACED}" == "unspecified" ]]; then
    # We need to have default RBAC version
    MANIFEST_PRINT_RBAC_CLUSTERED="yes"
    MANIFEST_PRINT_RBAC_NAMESPACED="no"
fi

# Render operator's Deployment section. May be not required in case of dev localhost run
MANIFEST_PRINT_DEPLOYMENT="${MANIFEST_PRINT_DEPLOYMENT:-"yes"}"

# Render operator's Service Metrics
MANIFEST_PRINT_SERVICE_METRICS="${MANIFEST_PRINT_SERVICE_METRICS:-"yes"}"

##################################
##
##     Render .yaml manifest
##
##################################

REPO_PATH_TEMPLATES_PATH="${TEMPLATES_PATH}"
REPO_PATH_OPERATOR_CONFIG_DIR="config"

# Render CRD section
if [[ "${MANIFEST_PRINT_CRD}" == "yes" ]]; then
    # Render CHI
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-01-section-crd-01-chi-chit.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    render_separator
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
        KIND="ClickHouseInstallation"             \
        SINGULAR="clickhouseinstallation"         \
        PLURAL="clickhouseinstallations"          \
        SHORT="chi"                               \
        OPERATOR_VERSION="${OPERATOR_VERSION}"    \
        envsubst

    # Render CHIT
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-01-section-crd-01-chi-chit.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    render_separator
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
        KIND="ClickHouseInstallationTemplate"     \
        SINGULAR="clickhouseinstallationtemplate" \
        PLURAL="clickhouseinstallationtemplates"  \
        SHORT="chit"                              \
        OPERATOR_VERSION="${OPERATOR_VERSION}"    \
        envsubst

    # Render CHOp config
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-01-section-crd-02-chopconf.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    render_separator
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
        NAMESPACE="${OPERATOR_NAMESPACE}"       \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}"      \
        OPERATOR_VERSION="${OPERATOR_VERSION}"  \
        envsubst
fi

# Render RBAC section for ClusterRole
if [[ "${MANIFEST_PRINT_RBAC_CLUSTERED}" == "yes" ]]; then
    # Render Account
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-02-section-rbac-01-account.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    render_separator
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
        COMMENT="$(cut_namespace_for_kubectl "${OPERATOR_NAMESPACE}")" \
        NAMESPACE="${OPERATOR_NAMESPACE}"       \
        NAME="clickhouse-operator"              \
        OPERATOR_VERSION="${OPERATOR_VERSION}"  \
        envsubst

    # Render Role
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-02-section-rbac-02-role.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    render_separator
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
        NAMESPACE="${OPERATOR_NAMESPACE}"       \
        COMMENT="#"                             \
        ROLE_KIND="ClusterRole"                 \
        ROLE_NAME="clickhouse-operator-${OPERATOR_NAMESPACE}"         \
        ROLE_BINDING_KIND="ClusterRoleBinding"                        \
        ROLE_BINDING_NAME="clickhouse-operator-${OPERATOR_NAMESPACE}" \
        OPERATOR_VERSION="${OPERATOR_VERSION}"                        \
        envsubst
fi

# Render RBAC section for Role
if [[ "${MANIFEST_PRINT_RBAC_NAMESPACED}" == "yes" ]]; then
    # Render Account
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-02-section-rbac-01-account.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    render_separator
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
        NAMESPACE="${OPERATOR_NAMESPACE}"       \
        NAME="clickhouse-operator"              \
        OPERATOR_VERSION="${OPERATOR_VERSION}"  \
        envsubst

    # Render Role
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-02-section-rbac-02-role.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    render_separator
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
        NAMESPACE="${OPERATOR_NAMESPACE}"       \
        COMMENT=""                              \
        ROLE_KIND="Role"                        \
        ROLE_NAME="clickhouse-operator"         \
        ROLE_BINDING_KIND="RoleBinding"         \
        ROLE_BINDING_NAME="clickhouse-operator" \
        OPERATOR_VERSION="${OPERATOR_VERSION}"  \
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
    CM_NAME="${1}"
    # Template file with ConfigMap header/beginning

    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-03-section-configmap-header.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    # Render ConfigMap header template with vars substitution
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
            COMMENT="$(cut_namespace_for_kubectl "${OPERATOR_NAMESPACE}")" \
            NAMESPACE="${OPERATOR_NAMESPACE}"      \
            NAME="${CM_NAME}"                      \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}"     \
            OPERATOR_VERSION="${OPERATOR_VERSION}" \
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
    FILE_PATH="${1}"

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
    if [[ -z "${TMP_CONFIG_FILE}" ]]; then
        # No config file specified, render simple deployment, w/o ConfigMaps

        SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-04-section-deployment.yaml"
        ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
        render_separator
        cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
            COMMENT="$(cut_namespace_for_kubectl "${OPERATOR_NAMESPACE}")" \
            NAMESPACE="${OPERATOR_NAMESPACE}"                  \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}"                 \
            METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
            OPERATOR_VERSION="${OPERATOR_VERSION}"             \
            envsubst
    else
        # Config file specified, render all ConfigMaps and then render deployment

        render_separator
        render_configmap_header "etc-clickhouse-operator-files"
        if [[ -f "${TMP_CONFIG_DIR}/config.yaml" ]]; then
            # Looks like clickhouse-operator config file is available, let's render it
            render_configmap_data_section_file "${TMP_CONFIG_DIR}/config.yaml"
        else
            # Fetch from github and apply
            # config/config.yaml
            download_file "${CUR_DIR}" "config.yaml" "${REPO_PATH_OPERATOR_CONFIG_DIR}"
            render_configmap_data_section_file "${CUR_DIR}/config.yaml"
        fi

        # Render confd.d files
        render_separator
        render_configmap_header "etc-clickhouse-operator-confd-files"
        if [[ ! -z "${TMP_CONFD_DIR}" ]] && [[ -d "${TMP_CONFD_DIR}" ]] && [[ ! -z "$(ls "${TMP_CONFD_DIR}")" ]]; then
            # Looks like at least one file is available, let's render it
            for FILE in "${TMP_CONFD_DIR}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render configd.d files
        render_separator
        render_configmap_header "etc-clickhouse-operator-configd-files"
        if [[ ! -z "${TMP_CONFIGD_DIR}" ]] && [[ -d "${TMP_CONFIGD_DIR}" ]] && [[ ! -z "$(ls "${TMP_CONFIGD_DIR}")" ]]; then
            # Looks like at least one file is available, let's render it
            for FILE in "${TMP_CONFIGD_DIR}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        else
            # Fetch from github and apply
            # config/config.d/01-clickhouse-listen.xml
            # config/config.d/02-clickhouse-logger.xml
            download_file "${CUR_DIR}" "01-clickhouse-listen.xml" "${REPO_PATH_OPERATOR_CONFIG_DIR}/config.d"
            download_file "${CUR_DIR}" "02-clickhouse-logger.xml" "${REPO_PATH_OPERATOR_CONFIG_DIR}/config.d"
            render_configmap_data_section_file "${CUR_DIR}/01-clickhouse-listen.xml"
            render_configmap_data_section_file "${CUR_DIR}/02-clickhouse-logger.xml"
        fi

        # Render templates.d files
        render_separator
        render_configmap_header "etc-clickhouse-operator-templatesd-files"
        if [[ ! -z "${TMP_TEMPLATESD_DIR}" ]] && [[ -d "${TMP_TEMPLATESD_DIR}" ]] && [[ ! -z "$(ls "${TMP_TEMPLATESD_DIR}")" ]]; then
            # Looks like at least one file is available, let's render it
            for FILE in "${TMP_TEMPLATESD_DIR}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        fi

        # Render users.d files
        render_separator
        render_configmap_header "etc-clickhouse-operator-usersd-files"
        if [[ ! -z "${TMP_USERSD_DIR}" ]] && [[ -d "${TMP_USERSD_DIR}" ]] && [[ ! -z "$(ls "${TMP_USERSD_DIR}")" ]]; then
            # Looks like at least one file is available, let's render it
            for FILE in "${TMP_USERSD_DIR}"/*; do
                render_configmap_data_section_file "${FILE}"
            done
        else
            # Fetch from github and apply
            # config/users.d/01-clickhouse-user.xml
            download_file "${CUR_DIR}" "01-clickhouse-user.xml" "${REPO_PATH_OPERATOR_CONFIG_DIR}/users.d"
            render_configmap_data_section_file "${CUR_DIR}/01-clickhouse-user.xml"
        fi

        # Render Deployment
        SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-04-section-deployment-with-configmap.yaml"
        ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
        render_separator
        cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
            COMMENT="$(cut_namespace_for_kubectl "${OPERATOR_NAMESPACE}")" \
            NAMESPACE="${OPERATOR_NAMESPACE}"                  \
            OPERATOR_IMAGE="${OPERATOR_IMAGE}"                 \
            METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE}" \
            OPERATOR_VERSION="${OPERATOR_VERSION}"             \
            envsubst
    fi
fi

# Render Service Metrics section
if [[ "${MANIFEST_PRINT_SERVICE_METRICS}" == "yes" ]]; then
    SECTION_FILE_NAME="clickhouse-operator-install-yaml-template-05-section-service-metrics.yaml"
    ensure_file "${TEMPLATES_DIR}" "${SECTION_FILE_NAME}" "${REPO_PATH_TEMPLATES_PATH}"
    render_separator
    cat "${TEMPLATES_DIR}/${SECTION_FILE_NAME}" | \
        COMMENT="$(cut_namespace_for_kubectl "${OPERATOR_NAMESPACE}")" \
        NAMESPACE="${OPERATOR_NAMESPACE}"       \
        OPERATOR_IMAGE="${OPERATOR_IMAGE}"      \
        OPERATOR_VERSION="${OPERATOR_VERSION}"  \
        envsubst
fi
