#!/bin/bash

echo "External value for \$MINIO_NAMESPACE=$MINIO_NAMESPACE"
echo "External value for \$MINIO_OPERATOR_VERSION=$MINIO_OPERATOR_VERSION"

MINIO_NAMESPACE="${MINIO_NAMESPACE:-minio}"
MINIO_OPERATOR_VERSION="${MINIO_OPERATOR_VERSION:-v4.1.3}"

echo "Setup minio.io"
echo "OPTIONS"
echo "\$MINIO_NAMESPACE=${MINIO_NAMESPACE}"
echo "\$MINIO_OPERATOR_VERSION=${MINIO_OPERATOR_VERSION}"
echo ""
echo "!!! IMPORTANT !!!"
echo "If you do not agree with specified options, press ctrl-c now"
sleep 10
echo "Apply options now..."

##
##
##
function clean_dir() {
    DIR="$1"

    echo "##############################"
    echo "Clean dir $DIR ..."
    rm -rf $DIR
    echo "...DONE"
}

##############################
##                          ##
## Install minio.io operator ##
##                          ##
##############################

# Download minio-operator sources into temp dir and run all installation scripts from there

TMP_DIR=$(mktemp -d)
MINIO_OPERATOR_DIR="${TMP_DIR}/minio-operator"

# Ensure temp dir in place
mkdir -p "${MINIO_OPERATOR_DIR}"

# Temp dir must not contain any data
if [[ ! -z "$(ls -A "${MINIO_OPERATOR_DIR}")" ]]; then
     echo "${MINIO_OPERATOR_DIR} is not empty. Abort"
     exit 1
fi

# Temp dir is empty, will clear it upon script termination
trap "clean_dir ${TMP_DIR}" SIGHUP SIGINT SIGQUIT SIGFPE SIGKILL SIGALRM SIGTERM

# Continue with sources
echo "Download minio.io operator ${MINIO_OPERATOR_VERSION} sources into ${MINIO_OPERATOR_DIR}"
git clone --depth 1 --branch ${MINIO_OPERATOR_VERSION} "https://github.com/minio/operator" "${MINIO_OPERATOR_DIR}"

echo "Setup minio.io operator ${MINIO_OPERATOR_VERSION} into ${MINIO_NAMESPACE} namespace"

# Let's setup all minio-related stuff into dedicated namespace
## TODO: need to refactor after next minio-operator release

MINIO_KUSTOMIZE_DIR="${MINIO_OPERATOR_DIR}/resources"
sed -i -e "s/name: minio-operator/name: ${MINIO_NAMESPACE}/" $MINIO_KUSTOMIZE_DIR/base/namespace.yaml
sed -i -e "s/namespace: default/namespace: ${MINIO_NAMESPACE}/" $MINIO_KUSTOMIZE_DIR/base/*.yaml
sed -i -e "s/namespace: minio-operator/namespace: ${MINIO_NAMESPACE}/" $MINIO_KUSTOMIZE_DIR/base/*.yaml
sed -i -e "s/namespace: minio-operator/namespace: ${MINIO_NAMESPACE}/" $MINIO_KUSTOMIZE_DIR/kustomization.yaml

# Setup minio-operator into dedicated namespace via kustomize
kubectl --namespace="${MINIO_NAMESPACE}" apply -k "${MINIO_KUSTOMIZE_DIR}"


echo -n "Waiting '${MINIO_NAMESPACE}/minio-opeator' deployment to start"
# Check grafana deployment have all pods ready
while [[ $(kubectl --namespace="${MINIO_NAMESPACE}" get deployments | grep "minio-operator" | grep "1/1" | wc -l) == "0" ]]; do
    printf "."
    sleep 1
done
echo "...DONE"

# Remove downloaded sources
clean_dir "${TMP_DIR}"
