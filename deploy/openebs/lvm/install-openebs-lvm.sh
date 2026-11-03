#!/bin/bash

function ensure_namespace() {
    local namespace="${1}"
    if kubectl get namespace "${namespace}" 1>/dev/null 2>/dev/null; then
        echo "Namespace '${namespace}' already exists."
    else
        echo "No '${namespace}' namespace found. Going to create."
        kubectl create namespace "${namespace}"
    fi
}


echo "External value for \$OPENEBS_NAMESPACE=$OPENEBS_NAMESPACE"
echo "External value for \$OPENEBS_HELM_VERSION=$OPENEBS_HELM_VERSION"
echo "External value for \$VALIDATE_YAML=$VALIDATE_YAML"
echo "External value for \$CLICKHOUSE_NAMESPACE=$CLICKHOUSE_NAMESPACE"

OPENEBS_NAMESPACE="${OPENEBS_NAMESPACE:-openebs}"
OPENEBS_HELM_VERSION="${OPENEBS_HELM_VERSION:-4.1.1}"
VALIDATE_YAML="${VALIDATE_YAML:-"true"}"
CLICKHOUSE_NAMESPACE="${CLICKHOUSE_NAMESPACE:-ch-test}"

echo "Setup OpenEBS"
echo "OPTIONS"
echo "\$OPENEBS_NAMESPACE=${OPENEBS_NAMESPACE}"
echo "\$OPENEBS_HELM_VERSION=${OPENEBS_HELM_VERSION}"
echo "\$VALIDATE_YAML=${VALIDATE_YAML}"
echo "\$CLICKHOUSE_NAMESPACE=${CLICKHOUSE_NAMESPACE}"
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
## Install openebs.io operator ##
##                          ##
##############################

# Download openebs-operator sources into temp dir and run all installation scripts from there

TMP_DIR=$(mktemp -d)

# Ensure temp dir in place
mkdir -p "${OPENEBS_OPERATOR_DIR}"

# Temp dir must not contain any data
if [[ -n "$(ls -A "${OPENEBS_OPERATOR_DIR}")" ]]; then
     echo "${OPENEBS_OPERATOR_DIR} is not empty. Abort"
     exit 1
fi

# Temp dir is empty, will clear it upon script termination
trap 'clean_dir ${TMP_DIR}' SIGHUP SIGINT SIGQUIT SIGFPE SIGALRM SIGTERM

# Continue with installing help repo
helm repo add openebs https://openebs.github.io/openebs
helm repo update

echo "Setup OpenEBS operator ${OPENEBS_HELM_VERSION} into ${OPENEBS_NAMESPACE} namespace"

# Let's setup all OpenEBS-related stuff into dedicated namespace
## TODO: need to refactor after next OPENEBS-operator release
kubectl delete crd volumesnapshotclasses.snapshot.storage.k8s.io
kubectl delete crd volumesnapshotcontents.snapshot.storage.k8s.io
kubectl delete crd volumesnapshots.snapshot.storage.k8s.io

# Setup OPENEBS-operator into dedicated namespace via kustomize
helm install openebs --namespace "${OPENEBS_NAMESPACE}" openebs/openebs --set engines.replicated.mayastor.enabled=false --set engines.local.zfs.enabled=false --create-namespace --version "${OPENEBS_HELM_VERSION}"

echo -n "Waiting '${OPENEBS_NAMESPACE}/openebs-lvm-localpv-controller' deployment to start"
# Check grafana deployment have all pods ready
while [[ $(kubectl --namespace="${OPENEBS_NAMESPACE}" get deployments | grep "openebs-lvm-localpv-controller" | grep -c "1/1") == "0" ]]; do
    printf "."
    sleep 1
done
echo "...DONE"

# Install the test storage class
kubectl apply -f openebs-lvm-storageclass.yaml -n "${OPENEBS_NAMESPACE}"

# Install a simple Clickhouse instance using openebs
echo "Setup simple Clickhouse into ${OPENEBS_NAMESPACE} namespace using OpenEBS"
ensure_namespace "${CLICKHOUSE_NAMESPACE}"
kubectl apply --validate="${VALIDATE_YAML}" --namespace="${CLICKHOUSE_NAMESPACE}" -f clickhouse-installation-with-openebs.yaml

# Remove downloaded sources
clean_dir "${TMP_DIR}"

