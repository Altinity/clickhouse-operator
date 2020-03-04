#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-dev}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# yes, no
DEPLOY_OPERATOR="${DEPLOY_OPERATOR:-yes}"
# release, dev, custom
DEPLOY_OPERATOR_SOURCE="${DEPLOY_OPERATOR_SOURCE:-custom}"


if [[ "${DEPLOY_OPERATOR_SOURCE}" == "release" ]]; then
    OPERATOR_IMAGE="altinity/clickhouse-operator:latest"
    METRICS_EXPORTER_IMAGE="altinity/metrics-exporter:latest"
    DEPLOY_OPERATOR="yes"
elif [[ "${DEPLOY_OPERATOR_SOURCE}" == "dev" ]]; then
    OPERATOR_IMAGE="sunsingerus/clickhouse-operator:dev"
    METRICS_EXPORTER_IMAGE="sunsingerus/metrics-exporter:dev"
    DEPLOY_OPERATOR="yes"
fi

if [[ "${OPERATOR_VERSION}" != "" ]]; then
    DEPLOY_OPERATOR="yes"
fi