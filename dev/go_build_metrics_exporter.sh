#!/bin/bash

# Build metrics-exporter
# Do not forget to update version

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Build clickhouse-operator install .yaml manifest
"${MANIFESTS_ROOT}/builder/build-clickhouse-operator-configs.sh"
# Build clickhouse-operator install .yaml manifest
"${MANIFESTS_ROOT}/builder/build-clickhouse-operator-install-yaml.sh"

# Prepare modules
# Prepare modules
if [[ "0" == "${BUILD_IN_DOCKER:-0}" ]]; then
    export GO111MODULE=on
    go mod tidy
    go mod "${MODULES_DIR}"
fi

OUTPUT_BINARY="${METRICS_EXPORTER_BIN}"
MAIN_SRC_FILE="${SRC_ROOT}/cmd/metrics_exporter/main.go"

GOOS=${GOOS:-linux}
GOARCH=${GOARCH:-amd64}

if CGO_ENABLED=0 GO111MODULE=on GOOS="${GOOS}" GOARCH="${GOARCH}" go build \
    -mod="${MODULES_DIR}" \
    -a \
    -ldflags " \
        -X ${REPO}/pkg/version.Version=${VERSION} \
        -X ${REPO}/pkg/version.GitSHA=${GIT_SHA}  \
        -X ${REPO}/pkg/version.BuiltAt=${NOW}     \
    " \
    -o "${OUTPUT_BINARY}" \
    "${MAIN_SRC_FILE}"
then
    echo "Build OK"
else
    echo "WARNING! BUILD FAILED!"
    echo "Check logs for details"
    exit 1
fi
