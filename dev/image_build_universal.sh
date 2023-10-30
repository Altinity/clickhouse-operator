#!/bin/bash

# Universal docker image builder.
# Should be called from image_build_operator_universal.sh or image_build_metrics_exporter_universal.sh

set -e
DOCKERFILE="${DOCKERFILE_DIR}/Dockerfile"

DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN}"
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH:-no}"
MINIKUBE="${MINIKUBE:-no}"

# Source-dependent options
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${CUR_DIR}/..")"
source "${CUR_DIR}/go_build_config.sh"

# Build clickhouse-operator install .yaml manifest
source "${MANIFESTS_ROOT}/builder/build-clickhouse-operator-configs.sh"
# Build clickhouse-operator install .yaml manifest
source "${MANIFESTS_ROOT}/builder/build-clickhouse-operator-install-yaml.sh"

# Build image with Docker
if [[ "${MINIKUBE}" == "yes" ]]; then
    # We'd like to build for minikube
    eval "$(minikube docker-env)"
fi

ARCHITECTURE=$(uname -m)
# Do nothing if architecture is arm，such as MacOS M1/M2

# We may need to install qemu
if [[ ! "${ARCHITECTURE}" =~ "arm" ]]; then
    if ! docker run --rm --privileged multiarch/qemu-user-static --reset -p yes; then
        sudo apt-get install -y qemu binfmt-support qemu-user-static
        docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
    fi
fi

if [[ "0" == $(docker buildx ls | grep -E 'linux/arm.+\*' | grep -E 'running|inactive') ]]; then
    docker buildx create --use --name multi-platform --platform=linux/amd64,linux/arm64
fi

#
# Build docker command
#

DOCKER_CMD="docker buildx build --progress plain"
if [[ "${DOCKER_IMAGE}" =~ ":dev" || "${MINIKUBE}" == "yes" ]]; then
    DOCKER_CMD="${DOCKER_CMD} --output type=image,name=${DOCKER_IMAGE} --platform=linux/amd64"
else
    DOCKER_CMD="${DOCKER_CMD} --platform=linux/amd64,linux/arm64"
fi

DOCKER_CMD="${DOCKER_CMD} --build-arg VERSION=${VERSION:-dev} --build-arg RELEASE=${RELEASE:-1}"

# Append GC flags if present
if [[ ! -z "${GCFLAGS}" ]]; then
    DOCKER_CMD="--build-arg GCFLAGS='${GCFLAGS}'"
fi

# Append repo push
if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
    DOCKER_CMD="${DOCKER_CMD} --push"
fi

# Finalize docker command
DOCKER_CMD="${DOCKER_CMD} -t ${DOCKER_IMAGE} -f ${DOCKERFILE} ${SRC_ROOT}"

if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
    if [[ -n "${DOCKERHUB_LOGIN}" ]]; then
        echo "Dockerhub login specified: '${DOCKERHUB_LOGIN}', perform login"
        docker login -u "${DOCKERHUB_LOGIN}"
    fi
fi

if ${DOCKER_CMD}; then
    echo "ALL DONE. Docker image published."
else
    echo "FAILED docker build! Abort."
    exit 1
fi
