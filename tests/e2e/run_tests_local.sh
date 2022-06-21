#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

${CUR_DIR}/../../dev/image_build_all_dev.sh && \
minikube image load "altinity/clickhouse-operator:dev" && \
minikube image load "altinity/metrics-exporter:dev" && \
echo "Images prepared" && \
OPERATOR_DOCKER_REPO="altinity/clickhouse-operator" \
METRICS_EXPORTER_DOCKER_REPO="altinity/metrics-exporter" \
OPERATOR_VERSION="dev" \
IMAGE_PULL_POLICY="IfNotPresent" \
"${CUR_DIR}/run_tests.sh"
