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

CODE_GENERATOR_DIR_INSIDE_MODULES="${SRC_ROOT}/vendor/k8s.io/code-generator"
CODE_GENERATOR_DIR_INSIDE_GOPATH="${GOPATH}/src/k8s.io/code-generator"

CODE_GENERATOR_DIR=$( \
    realpath "${CODE_GENERATOR_DIR:-$( \
        cd "${SRC_ROOT}"; \
        ls -d -1 "${CODE_GENERATOR_DIR_INSIDE_MODULES}" 2>/dev/null || echo "${CODE_GENERATOR_DIR_INSIDE_GOPATH}" \
    )}" \
)

echo "Generating code with the following options:"
echo "      SRC_ROOT=${SRC_ROOT}"
echo "      CODE_GENERATOR_DIR=${CODE_GENERATOR_DIR}"

if [[ "${CODE_GENERATOR_DIR}" == "${CODE_GENERATOR_DIR_INSIDE_MODULES}" ]]; then
    echo "MODULES dir ${CODE_GENERATOR_DIR} is used to run code generator from"
elif [[ "${CODE_GENERATOR_DIR}" == "${CODE_GENERATOR_DIR_INSIDE_GOPATH}" ]]; then
    echo "GOPATH dir ${CODE_GENERATOR_DIR} is used to run code generator from"
else
    echo "CUSTOM dir ${CODE_GENERATOR_DIR} is used to run code generator from"
fi

bash "${CODE_GENERATOR_DIR}/generate-groups.sh" \
    all \
    github.com/altinity/clickhouse-operator/pkg/client \
    github.com/altinity/clickhouse-operator/pkg/apis \
    "clickhouse.altinity.com:v1" \
    -o "${SRC_ROOT}/generator"
