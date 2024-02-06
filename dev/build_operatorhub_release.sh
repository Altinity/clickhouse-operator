#!/bin/bash

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Where community-operators sources are located

DEFAULT_CO_REPO_PATH=~/dev/community-operators
CO_REPO_PATH="${CO_REPO_PATH:-"${DEFAULT_CO_REPO_PATH}"}"
DEFAULT_CO_FOLDER_PATH="${CO_REPO_PATH}/operators/clickhouse"
CO_FOLDER_PATH="${CO_FOLDER_PATH:-"${DEFAULT_CO_FOLDER_PATH}"}"

# Ask to prepare copy of required files
echo "Please check operatorhub repo and clickhouse folder within the repo are available :"
echo "Repo:  ${CO_REPO_PATH}"
echo "Folder:${CO_FOLDER_PATH}"
read -n 1 -r -s -p $'Press enter to continue...\n'

if [[ ! -d "${CO_FOLDER_PATH}" ]]; then
    echo "Folder ${CO_FOLDER_PATH} is not available."
    echo "Abort."
    exit 1
fi

if [[ -z "${PREVIOUS_VERSION}" ]]; then
    echo "PREVIOUS_VERSION is not explicitly specified"
    echo "Trying to figure out PREVIOUS_VERSION from releases"
    PREVIOUS_VERSION=$(cat "${SRC_ROOT}/releases" | head -n1)
    echo "Found the following PREVIOUS_VERSION=$PREVIOUS_VERSION"
else
    echo "PREVIOUS_VERSION=${PREVIOUS_VERSION}"
    echo "PREVIOUS_VERSION explicitly specified, continue"
fi

if [[ -z "${PREVIOUS_VERSION}" ]]; then
    echo "No PREVIOUS_VERSION available."
    echo "Please specify PREVIOUS_VERSION earlier published on operatorhub, like:"
    echo "PREVIOUS_VERSION=0.18.1"
    echo "Abort."
    exit 1
else
    echo "Going to use"
    echo "PREVIOUS_VERSION=${PREVIOUS_VERSION}"
fi

echo "Please, verify correctness of the specified previous version"
read -n 1 -r -s -p $'Press enter to continue...\n'

PREVIOUS_VERSION="${PREVIOUS_VERSION}" ${SRC_ROOT}/deploy/builder/operatorhub.sh

OPERATORHUB_DIR="${SRC_ROOT}/deploy/operatorhub"
DST_MANIFESTS_DIR="${CO_FOLDER_PATH}/${VERSION}/manifests/"
DST_METADATA_DIR="${CO_FOLDER_PATH}/${VERSION}/metadata/"
mkdir -p "${DST_MANIFESTS_DIR}"
mkdir -p "${DST_METADATA_DIR}"
cp -r "${OPERATORHUB_DIR}/${VERSION}/"* "${DST_MANIFESTS_DIR}"
cp -r "${OPERATORHUB_DIR}/metadata/"*   "${DST_METADATA_DIR}"
#cp -r "${SRC_ROOT}/deploy/operatorhub/clickhouse.package.yaml" "${CO_PATH}"

echo "DONE"

# git remote add upstream https://github.com/k8s-operatorhub/community-operators
# git pull --rebase upstream main
# git rebase --skip  (if the conflicts are not true, skip the patches)
# git push --force-with-lease origin main
