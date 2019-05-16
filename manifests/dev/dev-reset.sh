#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source ${CUR_DIR}/dev-config.sh

echo "Reset dev env  via ${CHOPERATOR_NAMESPACE} namespace"
${CUR_DIR}/dev-delete.sh && ${CUR_DIR}/dev-install.sh

