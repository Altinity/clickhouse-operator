#!/bin/bash

# Find unformatted .go sources

# Exit immediately when a command fails
set -o errexit
# Error on unset variables
set -o nounset
# Only exit with zero if all commands of the pipeline exit successfully
set -o pipefail

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Prepare list of all .go files in the project, but exclude all files from /vendor/ folder
GO_FILES_LIST=$(find "${SRC_ROOT}" -name \*.go -not -path "${SRC_ROOT}/vendor/*" -print)
# Prepare unformatted files list
UNFORMATTED_FILES_LIST=$(gofmt -l ${GO_FILES_LIST})

if [[ ${UNFORMATTED_FILES_LIST} ]]; then
    for FILE in ${UNFORMATTED_FILES_LIST}; do
        echo "${FILE}"
    done
    exit 1
fi

exit 0
