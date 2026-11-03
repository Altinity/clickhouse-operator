#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/test_common.sh"

IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-"Always"}"

common_install_pip_requirements
common_export_test_env

RUN_ALL_FLAG=$(common_convert_run_all)

python3 "${COMMON_DIR}/../regression.py" \
    --only="/regression/e2e.test_metrics_exporter/${ONLY}" \
    ${RUN_ALL_FLAG} \
    --parallel off \
    -o short \
    --trim-results on \
    --debug \
    --native
