#!/usr/bin/env bash
set -euo pipefail

# ACVP expected-output reproducibility harness for clickhouse-operator.
#
# Builds either the operator or metrics-exporter binary (default: operator),
# fetches pinned upstream boringssl + acvp-testdata, builds the pinned
# acvptool, then runs check_expected.go against the embedded wrapper.
#
# Override BINARY=metrics-exporter to validate the metrics-exporter binary
# instead. Both binaries embed pkg/util/fips/acvp under the acvp_wrapper
# build tag and dispatch the wrapper when invoked as *-acvp.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"

GO_IMAGE="${GO_IMAGE:-golang:1.26-alpine}"
BORINGSSL_DIR="${BORINGSSL_DIR:-/tmp/boringssl}"
ACVP_TESTDATA_DIR="${ACVP_TESTDATA_DIR:-/tmp/acvp-testdata}"
BORINGSSL_COMMIT="${BORINGSSL_COMMIT:-baaf868e6e8f}"
ACVP_TESTDATA_COMMIT="${ACVP_TESTDATA_COMMIT:-d893de8b8b1c}"
CONFIG_PATH="${CONFIG_PATH:-/work/pkg/util/fips/acvp/acvp_test_fips140v1.26.public.config.json}"

# Which binary to validate. Allowed: clickhouse-operator | metrics-exporter
BINARY="${BINARY:-clickhouse-operator}"
case "${BINARY}" in
  clickhouse-operator)
    CMD_PATH="./cmd/operator"
    ;;
  metrics-exporter)
    CMD_PATH="./cmd/metrics_exporter"
    ;;
  *)
    echo "BINARY must be clickhouse-operator or metrics-exporter; got ${BINARY}" >&2
    exit 2
    ;;
esac

echo "[1/4] Build ${BINARY} (with -tags acvp_wrapper) and ACVP symlink"
docker run --rm \
  -v "${REPO_ROOT}:/work" \
  -w /work \
  "${GO_IMAGE}" \
  sh -lc "export PATH=\$PATH:/usr/local/go/bin && CGO_ENABLED=0 go build -tags acvp_wrapper -o ${BINARY} ${CMD_PATH} && ln -sf ${BINARY} ${BINARY}-acvp"

echo "[2/4] Fetch pinned boringssl and acvp-testdata"
if [[ ! -d "${BORINGSSL_DIR}/.git" ]]; then
  rm -rf "${BORINGSSL_DIR}"
  git clone https://boringssl.googlesource.com/boringssl "${BORINGSSL_DIR}"
fi
git -C "${BORINGSSL_DIR}" fetch --all --tags
git -C "${BORINGSSL_DIR}" checkout "${BORINGSSL_COMMIT}"

if [[ ! -d "${ACVP_TESTDATA_DIR}/.git" ]]; then
  rm -rf "${ACVP_TESTDATA_DIR}"
  git clone https://github.com/geomys/acvp-testdata "${ACVP_TESTDATA_DIR}"
fi
git -C "${ACVP_TESTDATA_DIR}" fetch --all --tags
git -C "${ACVP_TESTDATA_DIR}" checkout "${ACVP_TESTDATA_COMMIT}"

echo "[3/4] Build pinned acvptool"
docker run --rm \
  -v "${BORINGSSL_DIR}:/src" \
  -w /src \
  "${GO_IMAGE}" \
  sh -lc 'export PATH=$PATH:/usr/local/go/bin && go build -o /src/acvptool-pinned ./util/fipstools/acvp/acvptool'

echo "[4/4] Run ACVP expected-output check against ${BINARY}-acvp"
docker run --rm \
  -e ACVP_WRAPPER=1 \
  -e GODEBUG=fips140=on \
  -v "${REPO_ROOT}:/work" \
  -v "${BORINGSSL_DIR}:/tmp/boringssl:ro" \
  -v "${ACVP_TESTDATA_DIR}:/tmp/acvp-testdata:rw" \
  -w /tmp/acvp-testdata \
  "${GO_IMAGE}" \
  sh -lc "export PATH=\$PATH:/usr/local/go/bin && go run /tmp/boringssl/util/fipstools/acvp/acvptool/test/check_expected.go -tool /tmp/boringssl/acvptool-pinned -module-wrappers go:/work/${BINARY}-acvp -tests ${CONFIG_PATH}"
