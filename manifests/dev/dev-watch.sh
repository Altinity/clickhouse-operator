#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source ${CUR_DIR}/dev-config.sh

watch -n1 "kubectl -n ${CHOPERATOR_NAMESPACE} get all,configmap,endpoints,pv,pvc"
