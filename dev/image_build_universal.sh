#!/bin/bash

# Universal docker image builder.
# Should be called from image_build_operator_universal.sh or image_build_metrics_exporter_universal.sh

set -e
DOCKERFILE="${DOCKERFILE_DIR}/Dockerfile"

DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN}"
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH:-"no"}"
MINIKUBE="${MINIKUBE:-"no"}"

# Source-dependent options
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${CUR_DIR}/..")"
source "${CUR_DIR}/go_build_config.sh"

source "${CUR_DIR}/build_manifests.sh"

# Build image with Docker
if [[ "${MINIKUBE}" == "yes" ]]; then
    # We'd like to build for minikube
    eval "$(minikube docker-env)"
fi

ARCHITECTURE=$(uname -m)
# Do nothing if architecture is arm，such as MacOS M1/M2

# We may need to install qemu
if [[ ! "${ARCHITECTURE}" =~ "arm" ]]; then
    if docker run --rm --privileged multiarch/qemu-user-static --reset -p yes; then
        echo "qemu is in place, continue."
    else
        echo "qemu is not available, nee to install."
        sudo apt-get install -y qemu binfmt-support qemu-user-static
        if docker run --rm --privileged multiarch/qemu-user-static --reset -p yes; then
            echo "qemu installed and available, continue."
        else
            echo "qemu is not installed and not available."
            echo "Abort."
            exit 1
        fi
    fi
fi

if [[ "0" == $(docker buildx ls | grep -E 'linux/arm.+\*' | grep -E 'running|inactive') ]]; then
    echo "Looks like there is no appropriate builderx instance available."
    echo "Create a new builder instance."
    docker buildx create --use --name multi-platform --platform=linux/amd64,linux/arm64
else
    echo "Looks like there is an appropriate builderx instance available."
fi

#
# Build docker command
#

# Base docker build command
DOCKER_CMD="docker buildx build --progress plain"


# Append arch
if [[ "${DOCKER_IMAGE}" =~ ":dev" || "${MINIKUBE}" == "yes" ]]; then
    echo "Building dev images, skip arm arch."
    DOCKER_CMD="${DOCKER_CMD} --platform=linux/amd64 --output type=image,name=${DOCKER_IMAGE}"
else
    echo "Going to build for amd64 and arm64."
    DOCKER_CMD="${DOCKER_CMD} --platform=linux/amd64,linux/arm64"
fi

# Append VERSION and RELEASE
DOCKER_CMD="${DOCKER_CMD} --build-arg VERSION=${VERSION:-dev} --build-arg RELEASE=${RELEASE:-1}"

# Append GC flags if present
if [[ ! -z "${GCFLAGS}" ]]; then
    DOCKER_CMD="--build-arg GCFLAGS='${GCFLAGS}'"
fi

# Append repo push
if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
    DOCKER_CMD="${DOCKER_CMD} --push"
fi

# Append tag, dockerfile and SRC_ROOT
DOCKER_CMD="${DOCKER_CMD} --tag ${DOCKER_IMAGE} --file ${DOCKERFILE} ${SRC_ROOT}"

if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
    if [[ -n "${DOCKERHUB_LOGIN}" ]]; then
        echo "Dockerhub login specified: '${DOCKERHUB_LOGIN}', perform login"
        docker login -u "${DOCKERHUB_LOGIN}"
    fi
fi

if ${DOCKER_CMD}; then
    echo "OK. Build successful."
else
    echo "ERROR. Build has failed."
    echo "Abort"
    exit 1
fi
