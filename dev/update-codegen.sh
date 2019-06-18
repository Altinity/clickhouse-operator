#!/bin/bash

# Exit immediately when a command fails
set -o errexit
# Error on unset variables
set -o nounset
# Only exit with zero if all commands of the pipeline exit successfully
set -o pipefail

PROJECT_ROOT="$(dirname "${BASH_SOURCE}")/.."
CODEGEN_PKG="${CODEGEN_PKG:-$(cd "${PROJECT_ROOT}"; ls -d -1 "${PROJECT_ROOT}/vendor/k8s.io/code-generator" 2>/dev/null || echo "${GOPATH}/src/k8s.io/code-generator")}"

"${PROJECT_ROOT}/vendor/k8s.io/code-generator/generate-groups.sh" all \
    github.com/altinity/clickhouse-operator/pkg/client \
    github.com/altinity/clickhouse-operator/pkg/apis \
    "clickhouse.altinity.com:v1"
