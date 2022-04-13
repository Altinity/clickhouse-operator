#!/bin/bash

# Should be called from go_build_operator.sh or go_build_metrics_exporter.sh

# Build clickhouse-operator install .yaml manifest
source "${MANIFESTS_ROOT}/builder/build-clickhouse-operator-configs.sh"
# Build clickhouse-operator install .yaml manifest
source "${MANIFESTS_ROOT}/builder/build-clickhouse-operator-install-yaml.sh"

# Prepare modules
if [[ ! -d "${SRC_ROOT}/vendor" ]]; then
    export GO111MODULE=on
    go mod tidy
    go mod vendor
fi

GOOS=${GOOS:-linux}
GOARCH=${GOARCH:-amd64}

if [[ ! -z "${GCFLAGS}" ]]; then
    GCFLAGS="-gcflags '${GCFLAGS}'"
fi

if CGO_ENABLED=0 GO111MODULE=on GOOS="${GOOS}" GOARCH="${GOARCH}" go build \
    -mod="vendor" \
    -a \
    -ldflags " \
        -X ${REPO}/pkg/version.Version=${VERSION} \
        -X ${REPO}/pkg/version.GitSHA=${GIT_SHA}  \
        -X ${REPO}/pkg/version.BuiltAt=${NOW}     \
    " \
    ${GCFLAGS} \
    -o "${OUTPUT_BINARY}" \
    "${MAIN_SRC_FILE}"
then
    echo "Build OK"
else
    echo "WARNING! BUILD FAILED!"
    echo "Check logs for details"
    exit 1
fi
