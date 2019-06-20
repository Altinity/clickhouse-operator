#!/bin/bash

# Build clickhouse-operator
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/binary_build_config.sh"

REPO="github.com/altinity/clickhouse-operator"
VERSION=$(cd "${SRC_ROOT}"; cat release)
GIT_SHA=$(cd "${CUR_DIR}"; git rev-parse --short HEAD)

# Build clickhouse-operator install .yaml manifest
"${SRC_ROOT}/manifests/operator/build-clickhouse-operator-install-yaml.sh"

#CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o ${CUR_DIR}/clickhouse-operator ${SRC_ROOT}/cmd/clickhouse-operator
if CGO_ENABLED=0 go build \
    -a \
    -ldflags "-X ${REPO}/pkg/version.Version=${VERSION} -X ${REPO}/pkg/version.GitSHA=${GIT_SHA}" \
    -o "${OPERATOR_BIN}" \
    "${SRC_ROOT}/cmd/manager/main.go"; then
    echo "Build OK"
else
    echo "WARING!"
    echo "BUILD FAILED"
    echo "Check logs for details"
fi

exit $?
