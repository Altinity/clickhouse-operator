#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/test_common.sh"

IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-"Always"}"

common_install_pip_requirements
common_export_test_env

RUN_ALL_FLAG=$(common_convert_run_all)

RETRY_ARGS=()
# Retry failing scenarios in-process (TestFlows native). Applies to single-cluster AND
# dual: a retried fail->pass writes both attempts to the raw log, but merge_dual_results.py
# dedups by scenario (OK wins over Fail), so a rescued test is counted once as passing.
if [[ -n "${RETRY_COUNT}" ]]; then
    RETRY_ARGS+=(--retry "/regression/e2e.test_operator/test_0:,${RETRY_COUNT},,${RETRY_DELAY:-30}")
fi

# Optional untrimmed native raw log for result aggregation. When TF_LOG is set, write
# the full TestFlows log so two concurrent runs can be merged into ONE combined table
# via `tfs transform short`. Unset (single-cluster/CI) -> no --log, argv unchanged.
LOG_ARGS=()
if [[ -n "${TF_LOG:-}" ]]; then
    LOG_ARGS+=(--log "${TF_LOG}")
fi

python3 "${COMMON_DIR}/../regression.py" \
    --only="/regression/e2e.test_operator/${ONLY}" \
    ${RUN_ALL_FLAG} \
    "${RETRY_ARGS[@]}" \
    "${LOG_ARGS[@]}" \
    -o short \
    --trim-results "${TRIM_RESULTS:-on}" \
    --debug \
    --native
