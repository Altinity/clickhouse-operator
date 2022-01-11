#!/bin/bash

echo "External value for \$PROMETHEUS_NAMESPACE=$PROMETHEUS_NAMESPACE"
echo "External value for \$OPERATOR_NAMESPACE=$OPERATOR_NAMESPACE"
echo "External value for \$VALIDATE_YAML=$VALIDATE_YAML"

export PROMETHEUS_NAMESPACE="${PROMETHEUS_NAMESPACE:-prometheus}"
export OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"
export PROMETHEUS_OPERATOR_BRANCH="${PROMETHEUS_OPERATOR_BRANCH:-release-0.50}"
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
if [[ "" == "${NO_WAIT}" ]]; then
  sleep 10
fi
echo "Apply options now..."

# Let's setup all prometheus-related stuff into dedicated namespace called "prometheus"
kubectl create namespace "${PROMETHEUS_NAMESPACE}" || true

BASE_PATH="https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROMETHEUS_OPERATOR_BRANCH}"

echo "Setup prometheus-operator into '${PROMETHEUS_NAMESPACE}' namespace."
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f  <( \
    wget -qO- "${BASE_PATH}"/bundle.yaml | \
    sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" \
)

kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f  <( \
    wget -qO- "${BASE_PATH}"/bundle.yaml | \
    sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" | \
    yq -M eval '. | select(.kind == "Deployment") | .spec.template.spec.containers[0].imagePullPolicy = "IfNotPresent" ' - \
)

echo "Setup RBAC"
RBAC_PATH="${BASE_PATH}/example/rbac/prometheus"
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
    wget -qO- "${RBAC_PATH}"/prometheus-cluster-role-binding.yaml | \
    sed "s/namespace: default/namespace: ${PROMETHEUS_NAMESPACE}/" \
)
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${RBAC_PATH}"/prometheus-cluster-role.yaml
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f \
    "${RBAC_PATH}"/prometheus-service-account.yaml

echo "Setup Prometheus instance via prometheus-operator into '${PROMETHEUS_NAMESPACE}' namespace"
kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
    envsubst < "${CUR_DIR}/prometheus-template.yaml" \
)

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
    source "${CUR_DIR}/prometheus-sensitive-data.sh"
    kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f <( \
        envsubst < "${CUR_DIR}/prometheus-alertmanager-template.yaml" \
    )

    for rules_file in "${CUR_DIR}"/prometheus-alert-rules*.yaml; do
      kubectl --namespace="${PROMETHEUS_NAMESPACE}" apply --validate="${VALIDATE_YAML}" -f "${rules_file}"
    done
fi
echo "Add is done"
