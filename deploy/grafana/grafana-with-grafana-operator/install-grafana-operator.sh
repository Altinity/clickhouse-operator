#!/bin/bash

echo "External value for \$GRAFANA_NAMESPACE=$GRAFANA_NAMESPACE"
echo "External value for \$GRAFANA_OPERATOR_VERSION=$GRAFANA_OPERATOR_VERSION"

GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"
GRAFANA_OPERATOR_VERSION="${GRAFANA_OPERATOR_VERSION:-v5.18.0}"

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
git clone -b ${GRAFANA_OPERATOR_VERSION} --single-branch "https://github.com/grafana/grafana-operator" "${GRAFANA_OPERATOR_DIR}"


echo "Setup Grafana operator into ${GRAFANA_NAMESPACE} namespace"

# Let's setup all grafana-related stuff into dedicated namespace

kubectl create namespace "${GRAFANA_NAMESPACE}" || true

# Setup grafana-operator into dedicated namespace
# Use sed with compatibility for both macOS and Linux
if [[ "$(uname)" == "Darwin" ]]; then
    sed -i '' "s/namespace: system/namespace: ${GRAFANA_NAMESPACE}/g" "${GRAFANA_OPERATOR_DIR}/deploy/kustomize/overlays/namespace_scoped/kustomization.yaml"
else
    sed -i "s/namespace: system/namespace: ${GRAFANA_NAMESPACE}/g" "${GRAFANA_OPERATOR_DIR}/deploy/kustomize/overlays/namespace_scoped/kustomization.yaml"
fi
kubectl kustomize "${GRAFANA_OPERATOR_DIR}/deploy/kustomize/overlays/namespace_scoped" --load-restrictor LoadRestrictionsNone | kubectl apply --server-side -f -

kubectl wait deployment/grafana-operator-controller-manager -n "${GRAFANA_NAMESPACE}" --for=condition=available --timeout=300s

# Remove downloaded sources
clean_dir "${TMP_DIR}"
