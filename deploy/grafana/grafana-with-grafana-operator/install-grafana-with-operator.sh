#!/bin/bash

echo "External value for \$GRAFANA_NAMESPACE=$GRAFANA_NAMESPACE"
echo "External value for \$GRAFANA_NAME=$GRAFANA_NAME"
echo "External value for \$GRAFANA_ADMIN_USER=$GRAFANA_ADMIN_USER"
echo "External value for \$GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD"
echo "External value for \$GRAFANA_DISABLE_LOGIN_FORM=$GRAFANA_DISABLE_LOGIN_FORM"
echo "External value for \$GRAFANA_DISABLE_SIGNOUT_MENU=$GRAFANA_DISABLE_SIGNOUT_MENU"
echo "External value for \$GRAFANA_DASHBOARD_NAME=$GRAFANA_DASHBOARD_NAME"
echo "External value for \$GRAFANA_PROMETHEUS_DATASOURCE_NAME=$GRAFANA_PROMETHEUS_DATASOURCE_NAME"
echo "External value for \$GRAFANA_QUERIES_DASHBOARD_NAME=$GRAFANA_QUERIES_DASHBOARD_NAME"
echo "External value for \$PROMETHEUS_URL=$PROMETHEUS_URL"

GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"

GRAFANA_NAME="${GRAFANA_NAME:-grafana}"
GRAFANA_ADMIN_USER="${GRAFANA_ADMIN_USER:-admin}"
GRAFANA_ADMIN_PASSWORD="${GRAFANA_ADMIN_PASSWORD:-admin}"
GRAFANA_DISABLE_LOGIN_FORM="${GRAFANA_DISABLE_LOGIN_FORM:-False}"
GRAFANA_DISABLE_SIGNOUT_MENU="${GRAFANA_DISABLE_SIGNOUT_MENU:-True}"

GRAFANA_DASHBOARD_NAME="${GRAFANA_DASHBOARD_NAME:-clickhouse-operator-dashboard}"
GRAFANA_QUERIES_DASHBOARD_NAME=${GRAFANA_QUERIES_DASHBOARD_NAME:-clickhouse-queries-dashboard}

PROMETHEUS_URL="${PROMETHEUS_URL:-http://prometheus.prometheus:9090}"
GRAFANA_PROMETHEUS_DATASOURCE_NAME="${GRAFANA_PROMETHEUS_DATASOURCE_NAME:-clickhouse-operator-prometheus}"


CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

echo "OPTIONS"
echo "\$GRAFANA_NAMESPACE=$GRAFANA_NAMESPACE"
echo "\$GRAFANA_NAME=$GRAFANA_NAME"
echo "\$GRAFANA_ADMIN_USER=$GRAFANA_ADMIN_USER"
echo "\$GRAFANA_ADMIN_PASSWORD=$GRAFANA_ADMIN_PASSWORD"
echo "\$GRAFANA_DISABLE_LOGIN_FORM=$GRAFANA_DISABLE_LOGIN_FORM"
echo "\$GRAFANA_DISABLE_SIGNOUT_MENU=$GRAFANA_DISABLE_SIGNOUT_MENU"
echo "\$GRAFANA_DASHBOARD_NAME=$GRAFANA_DASHBOARD_NAME"
echo "\$GRAFANA_QUERIES_DASHBOARD_NAME=$GRAFANA_QUERIES_DASHBOARD_NAME"
echo "\$GRAFANA_PROMETHEUS_DATASOURCE_NAME=$GRAFANA_PROMETHEUS_DATASOURCE_NAME"
echo "\$PROMETHEUS_URL=$PROMETHEUS_URL"
echo ""
echo "!!! IMPORTANT !!!"
echo "If you do not agree with specified options, press ctrl-c now"
sleep 10
echo "Apply options now..."


########################################
##                                    ##
## Install Grafana as Custom Resource ##
##                                    ##
########################################


kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f <( \
    cat ${CUR_DIR}/grafana-cr-template.yaml | \
    GRAFANA_NAME="$GRAFANA_NAME" \
    GRAFANA_ADMIN_USER="$GRAFANA_ADMIN_USER" \
    GRAFANA_ADMIN_PASSWORD="$GRAFANA_ADMIN_PASSWORD" \
    GRAFANA_DISABLE_LOGIN_FORM="$GRAFANA_DISABLE_LOGIN_FORM" \
    GRAFANA_DISABLE_SIGNOUT_MENU="$GRAFANA_DISABLE_SIGNOUT_MENU" \
    envsubst \
)

echo "Waiting to start Grafana custom resource $GRAFANA_NAME"
while [[ "0" = $(kubectl get deployments --namespace="${GRAFANA_NAMESPACE}" | grep "${GRAFANA_NAME}-deployment" | grep "1/1" | wc -l) ]]; do
    printf "."
    sleep 1
done
echo "...DONE"

kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f <( \
    cat ${CUR_DIR}/grafana-data-source-cr-template.yaml | \
    GRAFANA_PROMETHEUS_DATASOURCE_NAME="$GRAFANA_PROMETHEUS_DATASOURCE_NAME" \
    PROMETHEUS_URL="$PROMETHEUS_URL" \
    envsubst \
)

echo "Waiting to apply GraganaDatasource custom resource $GRAFANA_PROMETHEUS_DATASOURCE_NAME"
while [[ "0" = $(kubectl get grafanadatasources --namespace="${GRAFANA_NAMESPACE}" -o=custom-columns=NAME:.metadata.name,STATUS:.status.message ${GRAFANA_PROMETHEUS_DATASOURCE_NAME} | grep -i "success" | wc -l) ]]; do
    printf "."
    sleep 1
done

while [[ "0" = $(kubectl get deployments --namespace="${GRAFANA_NAMESPACE}" | grep "${GRAFANA_NAME}-deployment" | grep "1/1" | wc -l) ]]; do
    printf "."
    sleep 1
done
echo "...DONE"

kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f <( \
    cat ${CUR_DIR}/grafana-dashboard-cr-template.yaml | \
    GRAFANA_DASHBOARD_NAME="$GRAFANA_DASHBOARD_NAME" \
    GRAFANA_PROMETHEUS_DATASOURCE_NAME="$GRAFANA_PROMETHEUS_DATASOURCE_NAME" \
    envsubst \
)

echo "Waiting to apply vertamedia-clickhouse-datasource plugin"
while [[ "0" = $(kubectl get deployments --namespace="${GRAFANA_NAMESPACE}" -o='custom-columns=PLUGINS:.spec.template.spec.initContainers[*].env[?(@.name=="GRAFANA_PLUGINS")].value' | grep "vertamedia" | wc -l) ]]; do
    printf "."
    sleep 1
done

while [[ "0" = $(kubectl get deployments --namespace="${GRAFANA_NAMESPACE}" | grep "${GRAFANA_NAME}-deployment" | grep "1/1" | wc -l) ]]; do
    printf "."
    sleep 1
done
echo "...DONE"

# TODO get clickhouse password from Vault-k8s secrets ?
# more precise but required yq
# OPERATOR_CH_USER=$(yq r ${CUR_DIR}/../../../config/config.yaml chUsername)
# OPERATOR_CH_PASS=$(yq r ${CUR_DIR}/../../../config/config.yaml chPassword)

OPERATOR_CH_USER=$(grep chUsername ${CUR_DIR}/../../../config/config.yaml | cut -d " " -f 2-)
OPERATOR_CH_PASS=$(grep chPassword ${CUR_DIR}/../../../config/config.yaml | cut -d " " -f 2-)

IFS=$'\n'
for LINE in $(kubectl get --all-namespaces chi -o custom-columns=NAMESPACE:.metadata.namespace,NAME:.metadata.name,ENDPOINT:.status.endpoint | tail -n +2); do
    ITEMS=( $(grep -Eo '([^[:space:]]+)' <<<"$LINE") )
    NAMESPACE=${ITEMS[0]}
    CHI=${ITEMS[1]}
    ENDPOINT=${ITEMS[2]}
    PORT=$(kubectl get --namespace="${NAMESPACE}" svc -l "clickhouse.altinity.com/app=chop,clickhouse.altinity.com/Service=chi,clickhouse.altinity.com/chi=${CHI}" -o='custom-columns=PORT:.spec.ports[?(@.name=="http")].port' | tail -n 1)

    GRAFANA_CLICKHOUSE_DATASOURCE_NAME="k8s-${NAMESPACE}-${CHI}"
    CLICKHOUSE_URL="http://${ENDPOINT}:${PORT}"

    # create system.query_log in each pod on CHI
    for POD in $(kubectl get --namespace="${NAMESPACE}" pods -l "clickhouse.altinity.com/app=chop,clickhouse.altinity.com/chi=${CHI}" -o='custom-columns=NAME:.metadata.name' | tail -n +2); do
        kubectl exec --namespace="${NAMESPACE}" ${POD} -- clickhouse-client --echo -mn -q 'SELECT hostName(), dummy FROM system.one SETTINGS log_queries=1; SYSTEM FLUSH LOGS'
    done

    kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f <(
        cat ${CUR_DIR}/grafana-data-source-queries-cr-template.yaml | \
        GRAFANA_CLICKHOUSE_DATASOURCE_NAME="$GRAFANA_CLICKHOUSE_DATASOURCE_NAME" \
        CLICKHOUSE_URL="$CLICKHOUSE_URL" \
        ENDPOINT="$ENDPOINT" \
        OPERATOR_CH_USER="$OPERATOR_CH_USER" \
        OPERATOR_CH_PASS="$OPERATOR_CH_PASS" \
        envsubst \
    )

done

echo "Waiting to apply all grafana clickhouse datasources"
sleep 10

kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f <(
    cat ${CUR_DIR}/grafana-dashboard-queries-cr-template.yaml | \
    GRAFANA_DASHBOARD_NAME="$GRAFANA_QUERIES_DASHBOARD_NAME" \
    envsubst \
)
