#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-dev}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# yes, no, none, dev, release, prod, latest
DEPLOY_OPERATOR="${DEPLOY_OPERATOR:-"no"}"

echo "DEPLOY_OPERATOR=${DEPLOY_OPERATOR}"

case "${DEPLOY_OPERATOR}" in
    "yes" | "release" | "prod" | "latest")
        # This would be release operator
        OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:latest"}"
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:latest"}"
        ;;
    "local")
        # This would be release operator
        OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:latest"}"
        OPERATOR_IMAGE_PULL_POLICY="${OPERATOR_IMAGE_PULL_POLICY:-"IfNotPresent"}"
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:latest"}"
        METRICS_EXPORTER_IMAGE_PULL_POLICY="${METRICS_EXPORTER_IMAGE_PULL_POLICY:-"IfNotPresent"}"
        ;;
    "dev")
        # This would be dev operator
        OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:dev"}"
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:dev"}"
        ;;
    *)
        echo "No Operator would be installed"
        ;;
esac
