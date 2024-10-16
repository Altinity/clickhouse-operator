#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
pip3 install -r "$CUR_DIR/../image/requirements.txt"

export OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"test"}"
export OPERATOR_INSTALL="${OPERATOR_INSTALL:-"yes"}"
export IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-"Always"}"

ONLY="${ONLY:-"*"}"
python3 "$CUR_DIR/../regression.py" --only="/regression/e2e.test_keeper/${ONLY}" --native
