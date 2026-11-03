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

# Possible options for code generator location
GOPATH="${GOPATH:-$(go env GOPATH)}"
CODE_GENERATOR_DIR_INSIDE_MODULES="${SRC_ROOT}/vendor/k8s.io/code-generator"
CODE_GENERATOR_DIR_INSIDE_GOPATH="${GOPATH}/src/k8s.io/code-generator"

# Detect code generator location
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

source "${CODE_GENERATOR_DIR}/kube_codegen.sh"

# gengo v2's package parser logs "W parse.go:769] Making unsupported type entry"
# for every Go type alias it encounters in the package graph (the standard-library
# `any`/`reflect.uncommonType` aliases plus our HookTarget/HookFailurePolicy
# documentation aliases of types.String). The warnings are harmless — gengo doesn't
# need to emit deepcopy for these because no struct field uses them directly. Drop
# them on display so the script's output stays focused on actionable errors.
filter_gengo_noise() {
    grep -Ev '^W.*parse\.go:[0-9]+\] Making unsupported type entry' >&2 || true
}

echo ""
echo "Generate deepcopy helpers for pkg/apis"
kube::codegen::gen_helpers \
    --boilerplate "${SRC_ROOT}/hack/boilerplate.go.txt" \
    "${SRC_ROOT}/pkg/apis" \
    2> >(filter_gengo_noise)

echo ""
echo "Generate client code for clickhouse.altinity.com into ${SRC_ROOT}/pkg/client"
kube::codegen::gen_client \
    --with-watch \
    --one-input-api "clickhouse.altinity.com" \
    --output-dir "${SRC_ROOT}/pkg/client" \
    --output-pkg "${REPO}/pkg/client" \
    --boilerplate "${SRC_ROOT}/hack/boilerplate.go.txt" \
    "${SRC_ROOT}/pkg/apis" \
    2> >(filter_gengo_noise)
