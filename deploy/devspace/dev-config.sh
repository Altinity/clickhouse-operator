#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-dev}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# yes, no, none, dev, release, prod, latest
DEPLOY_OPERATOR="${DEPLOY_OPERATOR:-no}"

echo "DEPLOY_OPERATOR=${DEPLOY_OPERATOR}"

case "${DEPLOY_OPERATOR}" in
    "yes" | "release" | "prod" | "latest")
        # This would be release operator
        OPERATOR_IMAGE="altinity/clickhouse-operator:latest"
        METRICS_EXPORTER_IMAGE="altinity/metrics-exporter:latest"
        ;;
    "dev")
        # This would be dev operator
        OPERATOR_IMAGE="sunsingerus/clickhouse-operator:dev"
        METRICS_EXPORTER_IMAGE="sunsingerus/metrics-exporter:dev"
        ;;
    *)
        echo "No Operator would be installed"
        ;;
esac
