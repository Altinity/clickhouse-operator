#!/bin/bash

# Find unformatted .go sources

# Exit immediately when a command fails
set -e

# Only exit with zero if all commands of the pipeline exit successfully
set -o pipefail

# Error on unset variables
set -u

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source ${CUR_DIR}/binary_build_config.sh

# Prepare list of all .go files in the project, but exclude all files from /vendor/ folder
GO_FILES_LIST=$(find ${SRC_ROOT} -name \*.go -not -path "${SRC_ROOT}/vendor/*" -print)
# Prepare unformatted files list
UNFORMATTED_FILES_LIST=$(gofmt -l ${GO_FILES_LIST})

if [[ ${UNFORMATTED_FILES_LIST} ]]; then
    echo "These files need to pass through 'go fmt'"
    for FILE in ${UNFORMATTED_FILES_LIST}; do
        echo ${FILE}
    done
    exit 1
fi

exit 0
