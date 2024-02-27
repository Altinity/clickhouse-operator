#!/bin/bash

[[ -n "${CONFIG_LOADED}" ]] && return; CONFIG_LOADED=1;

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
PROJECT_ROOT="$(realpath "${CUR_DIR}/../..")"
MANIFEST_ROOT="$(realpath "${PROJECT_ROOT}/deploy")"

OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"dev"}"
METRICS_EXPORTER_NAMESPACE="${OPERATOR_NAMESPACE}"

# yes, no, none, dev, release, prod, latest
DEPLOY_OPERATOR="${DEPLOY_OPERATOR:-"no"}"
CUR_RELEASE=$(cat "${PROJECT_ROOT}/release")
MINIKUBE="${MINIKUBE:-"yes"}"
VERBOSITY="${VERBOSITY:-"1"}"

case "${DEPLOY_OPERATOR}" in
    "yes" | "release" | "prod" | "latest")
        # This would be release operator
        OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:latest"}"
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:latest"}"
        ;;
    "local")
        # This would be release operator
        OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:${CUR_RELEASE}"}"
        #OPERATOR_IMAGE_PULL_POLICY="${OPERATOR_IMAGE_PULL_POLICY:-"IfNotPresent"}"
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:${CUR_RELEASE}"}"
        #METRICS_EXPORTER_IMAGE_PULL_POLICY="${METRICS_EXPORTER_IMAGE_PULL_POLICY:-"IfNotPresent"}"
        ;;
    "dev")
        # This would be dev operator
        OPERATOR_IMAGE="${OPERATOR_IMAGE:-"altinity/clickhouse-operator:dev"}"
        OPERATOR_IMAGE_PULL_POLICY="${OPERATOR_IMAGE_PULL_POLICY:-"IfNotPresent"}"
        METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"altinity/metrics-exporter:dev"}"
        METRICS_EXPORTER_IMAGE_PULL_POLICY="${METRICS_EXPORTER_IMAGE_PULL_POLICY:-"IfNotPresent"}"
        ;;
    *)
        ;;
esac
