#!/bin/bash

echo "External value for \$GRAFANA_NAMESPACE=$GRAFANA_NAMESPACE"
echo "External value for \$GRAFANA_OPERATOR_VERSION=$GRAFANA_OPERATOR_VERSION"

GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"
GRAFANA_OPERATOR_VERSION="${GRAFANA_OPERATOR_VERSION:-v3.9.0}"

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
git clone -b ${GRAFANA_OPERATOR_VERSION} --single-branch "https://github.com/integr8ly/grafana-operator" "${GRAFANA_OPERATOR_DIR}"


echo "Setup Grafana operator into ${GRAFANA_NAMESPACE} namespace"

# Let's setup all grafana-related stuff into dedicated namespace

kubectl create namespace "${GRAFANA_NAMESPACE}" || true

# Setup grafana-operator into dedicated namespace

# 1. Create the custom resource definitions that the operator uses:
kubectl --namespace="${GRAFANA_NAMESPACE}" apply -f "${GRAFANA_OPERATOR_DIR}/deploy/crds"
# 2. Create the operator roles:
kubectl --namespace="${GRAFANA_NAMESPACE}" apply -f "${GRAFANA_OPERATOR_DIR}/deploy/roles"
# 3. If you want to scan for dashboards in other namespaces you also need the cluster roles:
kubectl --namespace="${GRAFANA_NAMESPACE}" apply -f "${GRAFANA_OPERATOR_DIR}/deploy/cluster_roles"
# 4. Deploy the operator of explicitly specified version
kubectl --namespace="${GRAFANA_NAMESPACE}" apply -f <( \
    cat "${GRAFANA_OPERATOR_DIR}/deploy/operatorMasterImage.yaml" | sed -e "s/:master/:${GRAFANA_OPERATOR_VERSION}/g" \
)

# Remove downloaded sources
clean_dir "${TMP_DIR}"
