#!/bin/bash

# Exit immediately when a command fails
set -o errexit
# Error on unset variables
set -o nounset
# Only exit with zero if all commands of the pipeline exit successfully
set -o pipefail

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

CODE_GENERATOR_DIR_IN_MODULES="${SRC_ROOT}/${MODULES_DIR}/k8s.io/code-generator"
CODE_GENERATOR_DIR_IN_GOPATH="${GOPATH}/src/k8s.io/code-generator"

CODE_GENERATOR_DIR=$( \
    realpath "${CODE_GENERATOR_DIR:-$( \
        cd "${SRC_ROOT}"; \
        ls -d -1 "${CODE_GENERATOR_DIR_IN_MODULES}" 2>/dev/null || echo "${CODE_GENERATOR_DIR_IN_GOPATH}" \
    )}" \
)

echo "Generating code with the following options:"
echo "      SRC_ROOT=${SRC_ROOT}"
echo "      CODE_GENERATOR_DIR=${CODE_GENERATOR_DIR}"
echo ""
echo ""
echo ""

if [[ "${CODE_GENERATOR_DIR}" == "${CODE_GENERATOR_DIR_IN_MODULES}" ]]; then
    echo "Run code generator from modules"
elif [[ "${CODE_GENERATOR_DIR}" == "${CODE_GENERATOR_DIR_IN_GOPATH}" ]]; then
    echo "Run code generator from GOPATH"
else
    echo "Use custom specified CODE_GENERATOR_DIR=${CODE_GENERATOR_DIR}"
fi

bash "${CODE_GENERATOR_DIR}/generate-groups.sh" \
    all \
    github.com/altinity/clickhouse-operator/pkg/client \
    github.com/altinity/clickhouse-operator/pkg/apis \
    "clickhouse.altinity.com:v1"
