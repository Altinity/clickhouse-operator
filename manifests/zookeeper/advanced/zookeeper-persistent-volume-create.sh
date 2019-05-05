#!/bin/bash

ZK_NAMESPACE="${ZK_NAMESPACE:-zoons}"
YAML_FILES_LIST="\
01-service-client-access.yaml \
02-headless-service.yaml \
03-pod-disruption-budget.yaml \
04-storageclass-zookeeper.yaml \
05-stateful-set-persistent-volume.yaml\
"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source "${CUR_DIR}/zookeeper-create-universal.sh"
