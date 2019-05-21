#!/bin/bash

CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE:-kube-system}"
CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE:-altinity/clickhouse-operator:latest}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
MANIFEST_ROOT=$(realpath ${CUR_DIR}/..)

CHOPERATOR_IMAGE="${CHOPERATOR_IMAGE}" \
CHOPERATOR_NAMESPACE="${CHOPERATOR_NAMESPACE}" \
${MANIFEST_ROOT}/dev/cat-clickhouse-operator-yaml.sh |  tee ${CUR_DIR}/clickhouse-operator-install.yaml
