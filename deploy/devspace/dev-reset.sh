#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source "${CUR_DIR}/dev-config.sh"

echo "Reset namespace: ${OPERATOR_NAMESPACE}"
source "${CUR_DIR}/dev-delete.sh" && source "${CUR_DIR}/dev-install.sh"
