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

cat << EOF
########################################
Build vars:
DOCKERHUB_LOGIN=${DOCKERHUB_LOGIN}
DOCKERHUB_PUBLISH=${DOCKERHUB_PUBLISH}
MINIKUBE=${MINIKUBE}
EOF

if [[ "${MINIKUBE}" == "yes" ]]; then
    echo "Going to build on minikube, not on the build host itself."
    echo "Minikube is expected to be run on build host, though"
    eval "$(minikube docker-env)"
fi

# In case architecture of the host we are building on is arm，such as MacOS M1/M2, no need to install qemu
# We may need to install qemu otherwise
ARCHITECTURE=$(uname -m)
if [[ "${ARCHITECTURE}" =~ "arm" ]]; then
    echo "Build host is arm and does not need qemu to be installed"
else
    echo "Need qemu to be installed on build host"
    echo "Check whether qemu is available"
    if docker run --rm --privileged multiarch/qemu-user-static --reset -p yes; then
        echo "qemu is in place, continue."
    else
        echo "qemu is not available, need to install."
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

if docker buildx > /dev/null; then
    echo "docker buildx available, continue"
else
    echo "No docker buildx available. Abort."
    exit 1
fi

DOCKER_BUILDX_NUM=$(docker buildx ls | grep -E 'linux/arm.+\*' | grep -E 'running|inactive' | wc -l)
if [[ "${DOCKER_BUILDX_NUM}" == "0" ]]; then
    echo "Looks like there is no appropriate buildx instance available."
    echo "Create a new buildx instance."
    docker buildx create --use --name multi-platform --platform=linux/amd64,linux/arm64
else
    echo "Looks like there is an appropriate buildx instance available."
fi

#
# Build docker command
#

# Base docker build command
DOCKER_CMD="docker buildx build --progress plain"

# Append arch
if [[ "${DOCKER_IMAGE}" =~ ":dev" || "${MINIKUBE}" == "yes" ]]; then
    echo "Build image (dev) for amd64 only, skip arm arch."
    DOCKER_CMD="${DOCKER_CMD} --platform=linux/amd64 --output type=docker --output type=image,name=${DOCKER_IMAGE}"
else
    echo "Build image for both amd64 and arm64."
    DOCKER_CMD="${DOCKER_CMD} --platform=linux/amd64,linux/arm64"
fi

# Append VERSION and RELEASE
DOCKER_CMD="${DOCKER_CMD} --build-arg VERSION=${VERSION:-dev}"

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

echo "Docker build command ready:"
echo "${DOCKER_CMD}"
echo "Starting docker image build."
echo "Please, wait..."

if ${DOCKER_CMD}; then
    echo "OK. Build successful."
else
    echo "########################"
    echo "ERROR."
    echo "Docker image build has failed."
    echo "Abort"
    exit 1
fi
