#!/bin/bash
# Runs the e2e ACVP responder smoke tests only — host-only, NO minikube cluster
# and NO operator image. The host's Go toolchain and `GOFIPS140=v1.0.0` build env
# are all these tests need.
#
# The ACVP scenarios were consolidated into test_operator.py by PR #2031 as
# test_030018 (clickhouse-operator) and test_030019 (metrics-exporter) — they
# build each binary with `-tags acvp_wrapper`, invoke it via argv0 dispatch, and
# round-trip ACVP requests over stdin/stdout. This script runs ONLY those two
# scenarios (via --native, no docker-compose Cluster, no minikube) so `WHAT=all`
# keeps its fast fail-fast crypto pre-flight ahead of the metrics/operator suites.
# They also run inside the full operator suite (test_operator), which is what CI
# executes; this script is the standalone host-only entry point.
#
# Full BoringSSL acvptool reproducibility (vector-by-vector vs geomys/acvp-testdata)
# lives in pkg/util/fips/acvp/run.sh and is reproduced locally per release — this is
# the fast pre-flight that catches build-tag / argv0-dispatch / FIPS-mode regressions.
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/test_common.sh"

common_install_pip_requirements
common_export_test_env

RUN_ALL_FLAG=$(common_convert_run_all)

python3 "${COMMON_DIR}/../regression.py" \
    --only="/regression/e2e.test_operator/test_03001[89]*" \
    ${RUN_ALL_FLAG} \
    -o short \
    --trim-results on \
    --debug \
    --native
