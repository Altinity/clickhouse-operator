#!/bin/bash

echo "External value for \$PROMETHEUS_NAMESPACE=$PROMETHEUS_NAMESPACE"
echo "External value for \$OPERATOR_NAMESPACE=$OPERATOR_NAMESPACE"
echo "External value for \$VALIDATE_YAML=$VALIDATE_YAML"

export PROMETHEUS_NAMESPACE="${PROMETHEUS_NAMESPACE:-prometheus}"
export OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"
# look https://github.com/coreos/prometheus-operator/issues/3168, master branch is not stable
export PROMETHEUS_OPERATOR_BRANCH="${PROMETHEUS_OPERATOR_BRANCH:-release-0.41}"
export ALERT_MANAGER_EXTERNAL_URL="${ALERT_MANAGER_EXTERNAL_URL:-http://localhost:9093}"
# Possible values for "validate yaml" are values from --validate=XXX kubectl option. They are true/false ATM
export VALIDATE_YAML="${VALIDATE_YAML:-true}"

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

echo "OPTIONS"
echo "Setup Prometheus into \$PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} namespace"
echo "Expecting operator in \$OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE} namespace"
echo "Validate .yaml file   \$VALIDATE_YAML=${VALIDATE_YAML}"
echo ""
echo "!!! IMPORTANT !!!"
echo "If you do not agree with specified options, press ctrl-c now"
sleep 10
echo "Apply options now..."

# Let's setup all prometheus-related stuff into dedicated namespace called "prometheus"
kubectl create namespace "${PROMETHEUS_NAMESPACE}" || true

BASE_PATH="https://raw.githubusercontent.com/coreos/prometheus-operator/${PROMETHEUS_OPERATOR_BRANCH}"

echo "Create prometheus-operator's CRDs"
CRD_PATH="${BASE_PATH}/example/prometheus-operator-crd"
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${CRD_PATH}"/monitoring.coreos.com_alertmanagers.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${CRD_PATH}"/monitoring.coreos.com_podmonitors.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${CRD_PATH}"/monitoring.coreos.com_prometheuses.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${CRD_PATH}"/monitoring.coreos.com_prometheusrules.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${CRD_PATH}"/monitoring.coreos.com_servicemonitors.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${CRD_PATH}"/monitoring.coreos.com_thanosrulers.yaml

echo "Setup prometheus-operator into '${PROMETHEUS_NAMESPACE}' namespace."
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f  <( \
    wget -qO- "${BASE_PATH}"/bundle.yaml | \
    PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" \
)

echo "Setup RBAC"
RBAC_PATH="${BASE_PATH}/example/rbac/prometheus"
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
    wget -qO- "${RBAC_PATH}"/prometheus-cluster-role-binding.yaml | \
    PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" \
)
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${RBAC_PATH}"/prometheus-cluster-role.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${RBAC_PATH}"/prometheus-service-account.yaml

echo "Setup Prometheus instance via prometheus-operator into '${PROMETHEUS_NAMESPACE}' namespace"
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
    cat ${CUR_DIR}/prometheus-template.yaml | PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} envsubst \
)

echo "Setup Prometheus <-> clickhouse-operator integration."
# Specify endpoint, where Prometheus can gather data from clickhouse-operator
# IMPORTANT: clickhouse-operator should be installed and running prior to this step,
# otherwise Prometheus would not be able to setup integration with clickhouse-operator

if kubectl --namespace="${OPERATOR_NAMESPACE}" get service clickhouse-operator-metrics; then
    echo "clickhouse-operator-metrics endpoint found. Configuring integration with clickhouse-operator"
    # clickhouse-operator-metrics service found, can setup integration
    kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
        cat ${CUR_DIR}/prometheus-clickhouse-operator-service-monitor.yaml | \
        OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE} sed "s/- kube-system/- ${OPERATOR_NAMESPACE}/" \
    )
    echo ""
    echo "DONE"
else
    echo ""
    echo "ERROR install prometheus. Unable to find service '${OPERATOR_NAMESPACE}/clickhouse-operator-metrics'."
    echo "Please setup clickhouse-operator into ${OPERATOR_NAMESPACE} namespace and restart this script."
    exit 1
fi

echo "Setup Prometheus -> AlertManager -> Slack integration"
if [[ ! -f ${CUR_DIR}/prometheus-sensitive-data.sh ]]; then
    echo "!!! IMPORTANT !!!"
    echo "There is no ${CUR_DIR}/prometheus-sensitive-data.sh file found "
    echo "This means that there will be no Prometheus -> AlertManager -> Slack integration"
    echo "If you do not agree with specified options and would like to have this integration, press ctrl-c now and"
    echo "please copy ${CUR_DIR}/prometheus-sensitive-data.example.sh to ${CUR_DIR}/prometheus-sensitive-data.sh"
    echo "and edit it according to Slack documentation https://api.slack.com/incoming-webhooks"
    sleep 10
    echo "Skip Prometheus -> alertmanager -> Slack integration"
else
    source ${CUR_DIR}/prometheus-sensitive-data.sh
    kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
        cat ${CUR_DIR}/prometheus-alertmanager-template.yaml | \
        envsubst \
    )

    kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f ${CUR_DIR}/prometheus-alert-rules.yaml
fi
echo "Add is done"
