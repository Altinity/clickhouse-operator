#!/bin/bash

echo "External value for \$GRAFANA_NAMESPACE=$GRAFANA_NAMESPACE"

GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"

echo "OPTIONS"
echo "Setup Grafana into \$GRAFANA_NAMESPACE=${GRAFANA_NAMESPACE} namespace"
echo ""
echo "!!! IMPORTANT !!!"
echo "If you do not agree with specified options, press ctrl-c now"
sleep 30
echo "Apply options now..."

function clean_dir() {
    DIR="$1"

    echo "##############################"
    echo "Clean dir $DIR"
    rm -rfv $DIR
}

##############################
##                          ##
## Install Grafana operator ##
##                          ##
##############################

TMP_DIR=$(mktemp -d)
trap "clean_dir ${TMP_DIR}" SIGHUP SIGINT SIGQUIT SIGFPE SIGKILL SIGALRM SIGTERM

GRAFANA_OPERATOR_DIR="${TMP_DIR}/grafana-operator"
mkdir -p "${GRAFANA_OPERATOR_DIR}"

if [[ ! -z "$(ls -A "${GRAFANA_OPERATOR_DIR}")" ]]; then
     echo "${GRAFANA_OPERATOR_DIR} is not empty. Abort"
     exit 1
fi


git clone "https://github.com/integr8ly/grafana-operator" "${GRAFANA_OPERATOR_DIR}"

echo "Setup Grafana operator into ${GRAFANA_NAMESPACE} namespace"

# Let's setup all grafana-related stuff into dedicated namespace called "grafana"
kubectl create namespace "${GRAFANA_NAMESPACE}"

# Setup grafana-operator into dedicated namespace

# 1. Create the custom resource definitions that the operator uses:
kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f "${GRAFANA_OPERATOR_DIR}/deploy/crds"
# 2. Create the operator roles:
kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f "${GRAFANA_OPERATOR_DIR}/deploy/roles"
# 3. If you want to scan for dashboards in other namespaces you also need the cluster roles:
kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f "${GRAFANA_OPERATOR_DIR}/deploy/cluster_roles"
# 4. Deploy operator itself
kubectl apply --namespace="${GRAFANA_NAMESPACE}" -f "${GRAFANA_OPERATOR_DIR}/deploy/operator.yaml"

clean_dir "${TMP_DIR}"
