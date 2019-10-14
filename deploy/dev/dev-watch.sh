#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source "${CUR_DIR}/dev-config.sh"

watch -n1 "kubectl -n ${OPERATOR_NAMESPACE} get chi -o wide; echo ''; kubectl -n ${OPERATOR_NAMESPACE} get pod,service,configmap,pv,statefulset,pvc"

