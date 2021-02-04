#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

echo "External value for \$PROMETHEUS_NAMESPACE=$PROMETHEUS_NAMESPACE"
echo "External value for \$OPERATOR_NAMESPACE=$OPERATOR_NAMESPACE"
echo "External value for \$VALIDATE_YAML=$VALIDATE_YAML"

export PROMETHEUS_NAMESPACE="${PROMETHEUS_NAMESPACE:-prometheus}"
export OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-kube-system}"
export PROMETHEUS_OPERATOR_BRANCH="${PROMETHEUS_OPERATOR_BRANCH:-release-0.43}"
export ALERT_MANAGER_EXTERNAL_URL="${ALERT_MANAGER_EXTERNAL_URL:-http://localhost:9093}"
# Possible values for "validate yaml" are values from --validate=XXX kubectl option. They are true/false ATM
export VALIDATE_YAML="${VALIDATE_YAML:-true}"


echo "OPTIONS"
echo "Setup Prometheus into \$PROMETHEUS_NAMESPACE=${PROMETHEUS_NAMESPACE} namespace"
echo "Expecting operator in \$OPERATOR_NAMESPACE=${OPERATOR_NAMESPACE} namespace"
echo "Validate .yaml file   \$VALIDATE_YAML=${VALIDATE_YAML}"
echo ""
echo "!!! IMPORTANT !!!"
echo "If you do not agree with specified options, press ctrl-c now"
sleep 10
echo "Apply options now..."

###########################
##                       ##
##   Functions Section   ##
##                       ##
###########################

##
##
##
function wait_prometheus_to_start() {
    # Fetch Prometheus's name and namespace from params
    local namespace=$1
    local name=$2

    echo -n "Waiting Prometheus '${namespace}/${name}' to start"
    # Check grafana deployment have all pods ready
    while [[ $(kubectl get pods --namespace="${namespace}" -l app=prometheus,prometheus=${name} | grep "Running" | wc -l) == "0" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"

}

function wait_alertmanager_to_start() {
    # Fetch Prometheus's name and namespace from params
    local namespace=$1
    local name=$2

    echo -n "Waiting AlertManager '${namespace}/${name}' to start"
    # Check grafana deployment have all pods ready
    while [[ $(kubectl get pods --namespace="${namespace}" -l app=alertmanager,alertmanager=${name} | grep "Running" | wc -l) == "0" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"

}

# Let's setup all prometheus-related stuff into dedicated namespace called "prometheus"
kubectl create namespace "${PROMETHEUS_NAMESPACE}" || true

BASE_PATH="https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/${PROMETHEUS_OPERATOR_BRANCH}"

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

wait_prometheus_to_start $PROMETHEUS_NAMESPACE prometheus
wait_alertmanager_to_start $PROMETHEUS_NAMESPACE alertmanager
echo "Add is done"
