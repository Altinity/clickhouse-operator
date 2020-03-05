#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-dev}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# yes, no
DEPLOY_OPERATOR="${DEPLOY_OPERATOR:-yes}"
# release, dev, custom
DEPLOY_OPERATOR_TYPE="${DEPLOY_OPERATOR_TYPE:-no}"


if \
    [[ "${DEPLOY_OPERATOR_TYPE}" == "release" ]] || \
    [[ "${DEPLOY_OPERATOR_TYPE}" == "prod" ]] || \
    [[ "${DEPLOY_OPERATOR_TYPE}" == "latest" ]]
then
    OPERATOR_IMAGE="altinity/clickhouse-operator:latest"
    METRICS_EXPORTER_IMAGE="altinity/metrics-exporter:latest"
    DEPLOY_OPERATOR="yes"
elif [[ "${DEPLOY_OPERATOR_TYPE}" == "dev" ]]; then
    OPERATOR_IMAGE="sunsingerus/clickhouse-operator:dev"
    METRICS_EXPORTER_IMAGE="sunsingerus/metrics-exporter:dev"
    DEPLOY_OPERATOR="yes"
elif \
    [[ -z "${DEPLOY_OPERATOR_TYPE}" ]] || \
    [[ "${DEPLOY_OPERATOR_TYPE}" == "no" ]] || \
    [[ "${DEPLOY_OPERATOR_TYPE}" == "none" ]]
then
    DEPLOY_OPERATOR="no"
fi

if [[ ! -z "${OPERATOR_VERSION}" ]]; then
    DEPLOY_OPERATOR="yes"
fi
