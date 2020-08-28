#!/bin/bash

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-dev}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# yes, no, none, dev, release, prod, latest
DEPLOY_OPERATOR="${DEPLOY_OPERATOR:-no}"

if  [[ "${DEPLOY_OPERATOR}" == "yes"     ]] || \
    [[ "${DEPLOY_OPERATOR}" == "release" ]] || \
    [[ "${DEPLOY_OPERATOR}" == "prod"    ]] || \
    [[ "${DEPLOY_OPERATOR}" == "latest"  ]]
then
    # This would be release operator
    OPERATOR_IMAGE="altinity/clickhouse-operator:latest"
    METRICS_EXPORTER_IMAGE="altinity/metrics-exporter:latest"
    DEPLOY_OPERATOR="yes"
elif [[ "${DEPLOY_OPERATOR}" == "dev" ]]; then
    # This would be dev operator
    OPERATOR_IMAGE="sunsingerus/clickhouse-operator:dev"
    METRICS_EXPORTER_IMAGE="sunsingerus/metrics-exporter:dev"
    DEPLOY_OPERATOR="yes"
elif [[ -z "${DEPLOY_OPERATOR}"         ]] || \
     [[ "${DEPLOY_OPERATOR}" == "no"    ]] || \
     [[  "${DEPLOY_OPERATOR}" == "none" ]]
then
    # Looks like no operator to be deployed
    DEPLOY_OPERATOR="no"
fi

if [[ ! -z "${OPERATOR_VERSION}" ]]; then
    # Explicit operator version to be deployed
    DEPLOY_OPERATOR="yes"
fi

# Verify DEPLOY_OPERATOR value
if   [[ "${DEPLOY_OPERATOR}" == "yes" ]]; then
    :
elif [[ "${DEPLOY_OPERATOR}" == "no" ]]; then
    :
else
    echo "Unclear, whether to install operator or not, abort"
    exit 1
fi
