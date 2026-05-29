#!/bin/bash
# Runs the e2e ACVP responder smoke tests only.
#
# The test module (tests/e2e/test_acvp.py) builds the operator and
# metrics-exporter binaries with `-tags acvp_wrapper`, invokes them via argv0
# dispatch (binary symlinked as `<name>-acvp`), and round-trips ACVP requests
# over stdin/stdout. NO minikube cluster, NO operator image is required —
# the host's Go toolchain and `GOFIPS140=v1.0.0` build env are all the test
# needs.
#
# Full BoringSSL acvptool reproducibility (vector-by-vector comparison
# against geomys/acvp-testdata) lives in pkg/util/fips/acvp/run.sh and is
# reproduced locally per release — this script is the fast pre-flight that
# catches build-tag / argv0-dispatch / FIPS-mode regressions before the
# heavier vector-roundtrip run.
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/test_common.sh"

common_install_pip_requirements
common_export_test_env

RUN_ALL_FLAG=$(common_convert_run_all)

python3 "${COMMON_DIR}/../regression.py" \
    --only="/regression/e2e.test_acvp/${ONLY}" \
    ${RUN_ALL_FLAG} \
    -o short \
    --trim-results on \
    --debug \
    --native
