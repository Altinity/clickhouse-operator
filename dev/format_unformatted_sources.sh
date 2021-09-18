#!/bin/bash

# Find and format unformatted .go sources

# Exit immediately when a command fails
set -o errexit
# Error on unset variables
set -o nounset
# Only exit with zero if all commands of the pipeline exit successfully
set -o pipefail

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Iterate over list of unformatted files and format each of them
"${CUR_DIR}/find_unformatted_sources.sh" | while read -r FILE; do
    go fmt "${FILE}"
done
