#!/bin/bash

echo "External value for \$GRAFANA_NAMESPACE=$GRAFANA_NAMESPACE"
echo "External value for \$GRAFANA_OPERATOR_VERSION=$GRAFANA_OPERATOR_VERSION"

GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"
GRAFANA_OPERATOR_VERSION="${GRAFANA_OPERATOR_VERSION:-v4.8.0}"

echo "Setup Grafana"
echo "OPTIONS"
echo "\$GRAFANA_NAMESPACE=${GRAFANA_NAMESPACE}"
echo "\$GRAFANA_OPERATOR_VERSION=${GRAFANA_OPERATOR_VERSION}"
echo ""
echo "!!! IMPORTANT !!!"
echo "If you do not agree with specified options, press ctrl-c now"
if [[ "" == "${NO_WAIT}" ]]; then
  sleep 10
fi
echo "Apply options now..."

##
##
##
function clean_dir() {
    DIR="$1"

    echo "##############################"
    echo "Clean dir $DIR ..."
    rm -rf "$DIR"
    echo "...DONE"
}

##############################
##                          ##
## Install Grafana operator ##
##                          ##
##############################

# Download grafana-operator sources into temp dir and run all installation scripts from there

TMP_DIR=$(mktemp -d)
GRAFANA_OPERATOR_DIR="${TMP_DIR}/grafana-operator"

# Ensure temp dir in place
mkdir -p "${GRAFANA_OPERATOR_DIR}"

# Temp dir must not contain any data
if [[ -n "$(ls -A "${GRAFANA_OPERATOR_DIR}")" ]]; then
     echo "${GRAFANA_OPERATOR_DIR} is not empty. Abort"
     exit 1
fi

# Temp dir is empty, will clear it upon script termination
trap "clean_dir ${TMP_DIR}" SIGHUP SIGINT SIGQUIT SIGFPE SIGALRM SIGTERM

# Continue with sources
echo "Download Grafana operator sources into ${GRAFANA_OPERATOR_DIR}"
git clone -b ${GRAFANA_OPERATOR_VERSION} --single-branch "https://github.com/grafana-operator/grafana-operator" "${GRAFANA_OPERATOR_DIR}"


echo "Setup Grafana operator into ${GRAFANA_NAMESPACE} namespace"

# Let's setup all grafana-related stuff into dedicated namespace

kubectl create namespace "${GRAFANA_NAMESPACE}" || true

# Setup grafana-operator into dedicated namespace

# 1. Create the custom resource definitions that the operator uses:
kubectl --namespace="${GRAFANA_NAMESPACE}" apply -f <(
 cat "${GRAFANA_OPERATOR_DIR}/deploy/manifests/${GRAFANA_OPERATOR_VERSION}/crds.yaml" | sed "s/namespace: system/namespace: ${GRAFANA_NAMESPACE}/g"
)
# 2. Create the operator roles:
kubectl --namespace="${GRAFANA_NAMESPACE}" apply -f <(
  cat "${GRAFANA_OPERATOR_DIR}/deploy/manifests/${GRAFANA_OPERATOR_VERSION}/rbac.yaml" | sed "s/namespace: system/namespace: ${GRAFANA_NAMESPACE}/g" | sed "s/controller-manager/grafana-operator/g"
)
# 3. Deploy the operator of explicitly specified version
kubectl --namespace="${GRAFANA_NAMESPACE}" apply -f <(
  cat "${GRAFANA_OPERATOR_DIR}/deploy/manifests/${GRAFANA_OPERATOR_VERSION}/deployment.yaml" | sed "s/namespace: system/namespace: ${GRAFANA_NAMESPACE}/g" | sed "s/controller-manager/grafana-operator/g"
)

# Remove downloaded sources
clean_dir "${TMP_DIR}"
