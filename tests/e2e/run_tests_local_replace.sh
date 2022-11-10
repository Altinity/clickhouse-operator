#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

KUBECTL_MODE="replace" \
"${CUR_DIR}/run_tests_local.sh"
