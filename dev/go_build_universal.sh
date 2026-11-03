#!/bin/bash
set -euo pipefail

# Should be called from go_build_operator.sh or go_build_metrics_exporter.sh

# GOFIPS140 cannot be combined with GOEXPERIMENT=boringcrypto or the purego
# build tag (Go FIPS scope spec §1) — they select incompatible crypto modules
# and silently invalidate the FIPS-compatibility claim. Fail fast before
# spending build time on a binary that would mislabel its crypto source.
# Only gate when FIPS is actually being requested — callers that explicitly
# disable FIPS via `GOFIPS140=` (empty) are free to use boringcrypto.
if [[ -n "${GOFIPS140:-}" && -n "${GOEXPERIMENT:-}" && "${GOEXPERIMENT}" == *boringcrypto* ]]; then
    echo "ERROR: GOEXPERIMENT=boringcrypto is incompatible with GOFIPS140; unset one of them"
    exit 1
fi

# Prepare modules
if [[ ! -d "${SRC_ROOT}/vendor" ]]; then
    export GO111MODULE=on
    go mod tidy
    go mod vendor
fi

GOOS=${GOOS:-linux}
GOARCH=${GOARCH:-amd64}

if CGO_ENABLED=0 GO111MODULE=on GOOS="${GOOS}" GOARCH="${GOARCH}" GOFIPS140="${GOFIPS140}" go build \
    -mod="vendor" \
    -a \
    -ldflags " \
        -X ${REPO}/pkg/version.Version=${VERSION} \
        -X ${REPO}/pkg/version.GitSHA=${GIT_SHA}  \
        -X ${REPO}/pkg/version.BuiltAt=${NOW}     \
    " \
    ${GCFLAGS:+-gcflags "${GCFLAGS}"} \
    -o "${OUTPUT_BINARY}" \
    "${MAIN_SRC_FILE}"
then
    echo "Build OK"
else
    echo "WARNING! BUILD FAILED!"
    echo "Check logs for details"
    exit 1
fi
