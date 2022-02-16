#!/bin/bash

echo "External value for \$GRAFANA_NAMESPACE=$GRAFANA_NAMESPACE"
echo "External value for \$GRAFANA_NAME=$GRAFANA_NAME"
echo "External value for \$GRAFANA_ADMIN_USER=$GRAFANA_ADMIN_USER"
echo "External value for \$GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD"
echo "External value for \$GRAFANA_DISABLE_LOGIN_FORM=$GRAFANA_DISABLE_LOGIN_FORM"
echo "External value for \$GRAFANA_DISABLE_SIGNOUT_MENU=$GRAFANA_DISABLE_SIGNOUT_MENU"
echo "External value for \$GRAFANA_OPERATOR_DASHBOARD_NAME=$GRAFANA_OPERATOR_DASHBOARD_NAME"
echo "External value for \$GRAFANA_QUERIES_DASHBOARD_NAME=$GRAFANA_QUERIES_DASHBOARD_NAME"
echo "External value for \$GRAFANA_ZOOKEEPER_DASHBOARD_NAME=$GRAFANA_ZOOKEEPER_DASHBOARD_NAME"
echo "External value for \$GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME=$GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME"
echo "External value for \$GRAFANA_PROMETHEUS_DATASOURCE_NAME=$GRAFANA_PROMETHEUS_DATASOURCE_NAME"
echo "External value for \$PROMETHEUS_URL=$PROMETHEUS_URL"
echo "External value for \$GRAFANA_ROOT_URL=$GRAFANA_ROOT_URL"
echo "External value for \$VALIDATE_YAML=$VALIDATE_YAML"


GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"

GRAFANA_NAME="${GRAFANA_NAME:-grafana}"
GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-admin}"
GRAFANA_DISABLE_LOGIN_FORM="${GRAFANA_DISABLE_LOGIN_FORM:-False}"
GRAFANA_DISABLE_SIGNOUT_MENU="${GRAFANA_DISABLE_SIGNOUT_MENU:-False}"

GRAFANA_OPERATOR_DASHBOARD_NAME="${GRAFANA_OPERATOR_DASHBOARD_NAME:-clickhouse-operator-dashboard}"
GRAFANA_QUERIES_DASHBOARD_NAME=${GRAFANA_QUERIES_DASHBOARD_NAME:-clickhouse-queries-dashboard}
GRAFANA_QUERIES_DASHBOARD_NAME=${GRAFANA_QUERIES_DASHBOARD_NAME:-clickhouse-queries-dashboard}
GRAFANA_ZOOKEEPER_DASHBOARD_NAME=${GRAFANA_ZOOKEEPER_DASHBOARD_NAME:-zookeeper-dashboard}
GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME=${GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME:-clickhouse-keeper-dashboard}

GRAFANA_PROMETHEUS_DATASOURCE_NAME="${GRAFANA_PROMETHEUS_DATASOURCE_NAME:-clickhouse-operator-prometheus}"
PROMETHEUS_URL="${PROMETHEUS_URL:-http://prometheus.prometheus:9090}"
GRAFANA_ROOT_URL="${GRAFANA_ROOT_URL:-http://localhost:3000}"
VALIDATE_YAML=${VALIDATE_YAML:-true}

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

echo "OPTIONS"
echo "\$GRAFANA_NAMESPACE=$GRAFANA_NAMESPACE"
echo "\$GRAFANA_NAME=$GRAFANA_NAME"
echo "\$GRAFANA_ADMIN_USER=$GRAFANA_ADMIN_USER"
echo "\$GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD"
echo "\$GRAFANA_DISABLE_LOGIN_FORM=$GRAFANA_DISABLE_LOGIN_FORM"
echo "\$GRAFANA_DISABLE_SIGNOUT_MENU=$GRAFANA_DISABLE_SIGNOUT_MENU"
echo "\$GRAFANA_OPERATOR_DASHBOARD_NAME=$GRAFANA_OPERATOR_DASHBOARD_NAME"
echo "\$GRAFANA_QUERIES_DASHBOARD_NAME=$GRAFANA_QUERIES_DASHBOARD_NAME"
echo "\$GRAFANA_ZOOKEEPER_DASHBOARD_NAME=$GRAFANA_ZOOKEEPER_DASHBOARD_NAME"
echo "\$GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME=$GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME"
echo "\$GRAFANA_PROMETHEUS_DATASOURCE_NAME=$GRAFANA_PROMETHEUS_DATASOURCE_NAME"
echo "\$PROMETHEUS_URL=$PROMETHEUS_URL"
echo "\$GRAFANA_ROOT_URL=$GRAFANA_ROOT_URL"
echo ""
echo "!!! IMPORTANT !!!"
echo "If you do not agree with specified options, press ctrl-c now"
if [[ "" == "${NO_WAIT}" ]]; then
  sleep 10
fi
echo "Apply options now..."

###########################
##                       ##
##   Functions Section   ##
##                       ##
###########################

##
##
##
function wait_grafana_to_start() {
    # Fetch Grafana's name and namespace from params
    local namespace=$1
    local name=$2

    echo -n "Waiting Grafana '${namespace}/${name}' to start"
    # Check grafana deployment have all pods ready
    while [[ $(kubectl --namespace="${namespace}" get deployments | grep "${name}-deployment" | grep "1/1" | wc -l) == "0" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"
}

##
##
##
function wait_grafana_plugin_ch_datasource_to_start() {
    # Fetch namespace from params
    local namespace=$1

    echo -n "Waiting vertamedia-clickhouse-datasource plugin to start in '${namespace}' namespace"
    while [[ $(kubectl --namespace="${namespace}" get deployments -o='custom-columns=PLUGINS:.spec.template.spec.initContainers[*].env[?(@.name=="GRAFANA_PLUGINS")].value' | grep "vertamedia" | wc -l) == "0" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"
}

##
##
##
function wait_grafana_datasource_to_start() {
    # Fetch namespace and datasource from params
    local namespace=$1
    local datasource=$2

    echo -n "Waiting for Grafana DataSource custom resource '${namespace}/${datasource}'"
    while [[ $(kubectl --namespace="${namespace}" get grafanadatasources "${datasource}" -o'=custom-columns=NAME:.metadata.name,STATUS:.status.message' | grep -i "success" | wc -l) == "0" ]]; do
        printf "."
        sleep 1
    done
    echo "...DONE"
}

###########################
##                       ##
##      Main Section     ##
##                       ##
###########################

if [[ -f "${CUR_DIR}/sensitive-data.sh" ]]; then
  source $CUR_DIR/sensitive-data.sh
fi

echo "Install Grafana"
kubectl --namespace="${GRAFANA_NAMESPACE}" apply -f <( \
    cat ${CUR_DIR}/grafana-cr-template.yaml | \
    GRAFANA_NAME="$GRAFANA_NAME" \
    GRAFANA_ADMIN_USER="$GRAFANA_ADMIN_USER" \
    GRAFANA_ADMIN_PASSWORD="$GRAFANA_ADMIN_PASSWORD" \
    GRAFANA_DISABLE_LOGIN_FORM="$GRAFANA_DISABLE_LOGIN_FORM" \
    GRAFANA_DISABLE_SIGNOUT_MENU="$GRAFANA_DISABLE_SIGNOUT_MENU" \
    OAUTH_CLIENT_ID="$OAUTH_CLIENT_ID" \
    OAUTH_CLIENT_SECRET="$OAUTH_CLIENT_SECRET" \
    OAUTH_DOMAIN="$OAUTH_DOMAIN" \
    OAUTH_ALLOWED_DOMAINS="$OAUTH_ALLOWED_DOMAINS" \
    OAUTH_ALLOWED_ORGANIZATIONS="$OAUTH_ALLOWED_ORGANIZATIONS" \
    OAUTH_ALLOWED_TEAM_IDS="$OAUTH_ALLOWED_TEAM_IDS" \
    envsubst \
)
wait_grafana_to_start "${GRAFANA_NAMESPACE}" "${GRAFANA_NAME}"

#
# Install DataSources and Dashboards
#

echo "Install Prometheus DataSource"
kubectl apply --validate=${VALIDATE_YAML} --namespace="${GRAFANA_NAMESPACE}" -f <( \
    cat ${CUR_DIR}/grafana-data-source-prometheus-cr-template.yaml | \
    GRAFANA_PROMETHEUS_DATASOURCE_NAME="$GRAFANA_PROMETHEUS_DATASOURCE_NAME" \
    PROMETHEUS_URL="$PROMETHEUS_URL" \
    envsubst \
)
wait_grafana_datasource_to_start "${GRAFANA_NAMESPACE}" "${GRAFANA_PROMETHEUS_DATASOURCE_NAME}"
wait_grafana_to_start "${GRAFANA_NAMESPACE}" "${GRAFANA_NAME}"

echo "Install Operator dashboard"
kubectl apply --validate=${VALIDATE_YAML} --namespace="${GRAFANA_NAMESPACE}" -f <( \
    cat ${CUR_DIR}/grafana-dashboard-operator-cr-template.yaml | \
    GRAFANA_DASHBOARD_NAME="$GRAFANA_OPERATOR_DASHBOARD_NAME" \
    GRAFANA_PROMETHEUS_DATASOURCE_NAME="$GRAFANA_PROMETHEUS_DATASOURCE_NAME" \
    envsubst \
)
wait_grafana_plugin_ch_datasource_to_start "${GRAFANA_NAMESPACE}"
wait_grafana_to_start "${GRAFANA_NAMESPACE}" "${GRAFANA_NAME}"

# Install CLickHouse DataSource(s)

# TODO get clickhouse password from Vault-k8s secrets ?
if ! command -v yq &> /dev/null; then
  echo "Install 'yq', see installation instruction in https://github.com/mikefarah/yq/#install"
  exit 1
else
  export OPERATOR_CH_USER=$(yq eval .clickhouse.access.username ${CUR_DIR}/../../../config/config.yaml)
  export OPERATOR_CH_PASS=$(yq eval .clickhouse.access.password ${CUR_DIR}/../../../config/config.yaml)
fi

echo "Create ClickHouse DataSource for each ClickHouseInstallation"
IFS=$'\n'
for LINE in $(kubectl get --all-namespaces chi -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,ENDPOINT:.status.endpoint | tail -n +2); do
    ITEMS=( $(grep -Eo '([^[:space:]]+)' <<<"$LINE") )
    NAMESPACE=${ITEMS[0]}
    CHI=${ITEMS[1]}
    ENDPOINT=${ITEMS[2]}
    PORT=$(kubectl --namespace="${NAMESPACE}" get service -l "clickhouse.altinity.com/app=chop,clickhouse.altinity.com/Service=chi,clickhouse.altinity.com/chi=${CHI}" -o='custom-columns=PORT:.spec.ports[?(@.name=="http")].port' | tail -n 1)

    echo "Ensure system.query_log is in place on each pod in ClickHouseInstallation ${NAMESPACE}/${CHI}"
    for POD in $(kubectl --namespace="${NAMESPACE}" get pods -l "clickhouse.altinity.com/app=chop,clickhouse.altinity.com/chi=${CHI}" -o='custom-columns=NAME:.metadata.name' | tail -n +2); do
        echo "Ensure system.query_log on pod ${NAMESPACE}/${POD}"
        kubectl --namespace="${NAMESPACE}" exec "${POD}" -- \
            clickhouse-client --echo -mn -q 'SELECT hostName(), dummy FROM system.one SETTINGS log_queries=1; SYSTEM FLUSH LOGS'
    done

    GRAFANA_CLICKHOUSE_DATASOURCE_NAME="k8s-${NAMESPACE}-${CHI}"
    CLICKHOUSE_URL="http://${ENDPOINT}:${PORT}"
    echo "Create ClickHouse DataSource for ClickHouseInstallation ${CHI} '${GRAFANA_NAMESPACE}/${GRAFANA_CLICKHOUSE_DATASOURCE_NAME}'"
    kubectl apply --validate=${VALIDATE_YAML} --namespace="${GRAFANA_NAMESPACE}" -f <( \
        cat ${CUR_DIR}/grafana-data-source-clickhouse-cr-template.yaml | \
        GRAFANA_CLICKHOUSE_DATASOURCE_NAME="$GRAFANA_CLICKHOUSE_DATASOURCE_NAME" \
        CLICKHOUSE_URL="$CLICKHOUSE_URL" \
        ENDPOINT="$ENDPOINT" \
        OPERATOR_CH_USER="$OPERATOR_CH_USER" \
        OPERATOR_CH_PASS="$OPERATOR_CH_PASS" \
        envsubst \
    )
    wait_grafana_datasource_to_start "${GRAFANA_NAMESPACE}" "${GRAFANA_CLICKHOUSE_DATASOURCE_NAME}"
done
wait_grafana_to_start "${GRAFANA_NAMESPACE}" "${GRAFANA_NAME}"

echo "Install Queries dashboard"
kubectl apply --validate=${VALIDATE_YAML} --namespace="${GRAFANA_NAMESPACE}" -f <( \
    cat ${CUR_DIR}/grafana-dashboard-queries-cr-template.yaml | \
    GRAFANA_DASHBOARD_NAME="$GRAFANA_QUERIES_DASHBOARD_NAME" \
    GRAFANA_PROMETHEUS_DATASOURCE_NAME="$GRAFANA_PROMETHEUS_DATASOURCE_NAME" \
    envsubst \
)

echo "Install Zookeeper dashboard"
kubectl apply --validate=${VALIDATE_YAML} --namespace="${GRAFANA_NAMESPACE}" -f <( \
    cat ${CUR_DIR}/grafana-dashboard-zookeeper-cr-template.yaml | \
    GRAFANA_ZOOKEEPER_DASHBOARD_NAME="$GRAFANA_ZOOKEEPER_DASHBOARD_NAME" \
    GRAFANA_PROMETHEUS_DATASOURCE_NAME="$GRAFANA_PROMETHEUS_DATASOURCE_NAME" \
    envsubst \
)

echo "Install ClickHouse Keeper dashboard"
kubectl apply --validate=${VALIDATE_YAML} --namespace="${GRAFANA_NAMESPACE}" -f <( \
    cat ${CUR_DIR}/grafana-dashboard-clickhouse-keeper-cr-template.yaml | \
    GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME="$GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME" \
    GRAFANA_PROMETHEUS_DATASOURCE_NAME="$GRAFANA_PROMETHEUS_DATASOURCE_NAME" \
    envsubst '$GRAFANA_CLICKHOUSE_KEEPER_DASHBOARD_NAME $GRAFANA_PROMETHEUS_DATASOURCE_NAME' \
)

wait_grafana_plugin_ch_datasource_to_start "${GRAFANA_NAMESPACE}"
wait_grafana_to_start "${GRAFANA_NAMESPACE}" "${GRAFANA_NAME}"

echo "All is done"
