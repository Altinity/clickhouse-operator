#!/bin/bash

# This file is not supposed to be run directly
# It must be sourced from task "create" files

if [[ -z "${ZK_NAMESPACE}" ]]; then
    echo "Please specify \$ZK_NAMESPACE"
    exit -1
fi

if [[ -z "${YAML_FILES_LIST}" ]]; then
    echo "Please specify \$YAML_FILES_LIST"
    exit -1
fi

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

kubectl create namespace ${ZK_NAMESPACE}
for FILE in ${YAML_FILES_LIST}; do
    kubectl -n "${ZK_NAMESPACE}" apply -f "${CUR_DIR}/${FILE}"
done
