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

CODEGEN_PKG=$(realpath "${CODEGEN_PKG:-$(cd "${SRC_ROOT}"; ls -d -1 "${SRC_ROOT}/${MODULES_DIR}/k8s.io/code-generator" 2>/dev/null || echo "${GOPATH}/src/k8s.io/code-generator")}")

#echo "Generating code with the following options:"
#echo "PROJECT_ROOT=${PROJECT_ROOT}"
#echo "CODEGEN_PKG==${CODEGEN_PKG}"

bash "${CODEGEN_PKG}/generate-groups.sh" \
    all \
    github.com/altinity/clickhouse-operator/pkg/client \
    github.com/altinity/clickhouse-operator/pkg/apis \
    "clickhouse.altinity.com:v1"
