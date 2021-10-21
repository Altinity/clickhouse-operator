#!/bin/bash

#
# Download file `FILE` into `LOCAL_DIR` from `GITHUB_REPO_DIR`
#
function download_file() {
    # Params
    local LOCAL_DIR="${1}"
    local FILE="${2}"
    local GITHUB_REPO_DIR="${3}"

    # File on local disk
    local LOCAL_FILE="${LOCAL_DIR}/${FILE}"

    # URL where to download file from
    local GITHUB_REPO_BASE_URL="https://raw.githubusercontent.com/Altinity/clickhouse-operator"
    local BRANCH="${BRANCH:-master}"
    local FILE_URL="${GITHUB_REPO_BASE_URL}/${BRANCH}/${GITHUB_REPO_DIR}/${FILE}"

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
# Ensure file `FILE` is in place in `LOCAL_DIR`, if absent, fetch file from `GITHUB_REPO_DIR`
#
function ensure_file() {
    # Params
    local LOCAL_DIR="${1}"
    local FILE="${2}"
    local GITHUB_REPO_DIR="${3}"

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
# Start render section.
# Call this function at the beginning of the new section rendering process
#
# Number of rendered sections. Aux var
SECTIONS_RENDERED_NUM=0
function section_render_start() {
    SECTIONS_RENDERED_NUM=$((SECTIONS_RENDERED_NUM+1))
}

#
# Render section separator, except first time
#
function render_separator() {
    section_render_start

    if [[ "${SECTIONS_RENDERED_NUM}" -gt 1 ]]; then
        echo "---"
    fi
}

#
# Prepare arguments for sed to cut out namespace
#
function cut_namespace_for_kubectl() {
  if [[ "${1}" == "-" ]]; then
      echo "#"
  else
      echo ""
  fi
}
