#!/bin/bash

# Compose clickhouse-operator .yaml manifest from components

# Paths
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/../..")"

# Relative and abs paths where templates live
TEMPLATES_PATH="deploy/builder/templates-config"
TEMPLATES_DIR="${PROJECT_ROOT}/${TEMPLATES_PATH}"

# Relative and abs paths where users.d templates live
TEMPLATES_USERSD_PATH="${TEMPLATES_PATH}/users.d"
TEMPLATES_USERSD_DIR="${PROJECT_ROOT}/${TEMPLATES_USERSD_PATH}"

# Relative and abs paths where config live
CONFIG_PATH="config"
CONFIG_DIR="${1:-"${PROJECT_ROOT}/${CONFIG_PATH}"}"

source "${CUR_DIR}/lib/lib.sh"

#
# Renders config file with all variables
#
function render_file() {
    SRC="${1}"
    DST="${2}"
    cat "${SRC}" | \
        watchNamespaces="${watchNamespaces:-""}" \
        chUsername="${chUsername:-"clickhouse_operator"}" \
        chPassword="${chPassword:-"clickhouse_operator_password"}" \
        password_sha256_hex="${password_sha256_hex:-"716b36073a90c6fe1d445ac1af85f4777c5b7a155cea359961826a030513e448"}" \
        envsubst \
        > "${DST}"
}

# Process files in root
# List files only
for f in $(ls -pa "${TEMPLATES_DIR}" | grep -v /); do
    # Source
    SRC_FILE_PATH=$(realpath "${TEMPLATES_DIR}/${f}")
    FILE_NAME=$(basename "${SRC_FILE_PATH}")

    # Destination
    mkdir -p "${CONFIG_DIR}"
    DST_FILE_PATH=$(realpath "${CONFIG_DIR}/${FILE_NAME}")

    #echo "${SRC_FILE_PATH} ======> ${DST_FILE_PATH}"
    render_file "${SRC_FILE_PATH}" "${DST_FILE_PATH}"
done

# Process files in sub-folders
for SUB_TEMPLATES_DIR in $(ls -d "${TEMPLATES_DIR}"/*/); do
    # List files only
    for f in $(ls -pa "${SUB_TEMPLATES_DIR}" | grep -v /); do
        # Source
        SRC_FILE_PATH=$(realpath "${SUB_TEMPLATES_DIR}/${f}")
        SUB_DIR=$(basename "${SUB_TEMPLATES_DIR}")
        FILE_NAME=$(basename "${SRC_FILE_PATH}")

        #Destination
        SUB_CONFIG_DIR=$(realpath "${CONFIG_DIR}/${SUB_DIR}")
        mkdir -p "${SUB_CONFIG_DIR}"
        DST_FILE_PATH=$(realpath "${SUB_CONFIG_DIR}/${FILE_NAME}")

        #echo "${SRC_FILE_PATH} ======> ${DST_FILE_PATH}"
        render_file "${SRC_FILE_PATH}" "${DST_FILE_PATH}"
    done
done
