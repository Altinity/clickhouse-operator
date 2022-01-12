#!/bin/bash

# Universal docker image builder

# Externally configurable build-dependent options
TAG="${TAG:-sunsingerus/metrics-exporter:dev}"
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN}"
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH:-yes}"
MINIKUBE="${MINIKUBE:-no}"

# Source-dependent options
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${CUR_DIR}/..")"
source "${CUR_DIR}/go_build_config.sh"

DOCKERFILE_DIR="${SRC_ROOT}/dockerfile/metrics-exporter"
DOCKERFILE="${DOCKERFILE_DIR}/Dockerfile"

# Build clickhouse-operator install .yaml manifest
"${MANIFESTS_ROOT}/builder/build-clickhouse-operator-configs.sh"
# Build clickhouse-operator install .yaml manifest
"${MANIFESTS_ROOT}/builder/build-clickhouse-operator-install-yaml.sh"

# Build image with Docker
if [[ "${MINIKUBE}" == "yes" ]]; then
    # We'd like to build for minikube
    eval "$(minikube docker-env)"
fi

if ! docker run --rm --privileged multiarch/qemu-user-static --reset -p yes; then
  sudo apt-get install -y qemu binfmt-support qemu-user-static
  docker run --rm --privileged multiarch/qemu-user-static --reset -p yes
fi

if [[ "0" == $(docker buildx ls | grep "\*" | grep -c "running") ]]; then
  docker buildx create --use --name multi-platform --platform=linux/amd64,linux/arm64
fi

DOCKER_CMD="docker buildx build --progress plain --progress plain"
if [[ "${TAG}" =~ ":dev" ]]; then
 DOCKER_CMD="${DOCKER_CMD} --output type=image,name=${TAG} --platform=linux/amd64"
else
 DOCKER_CMD="${DOCKER_CMD} --platform=linux/amd64,linux/arm64"
fi

if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
  DOCKER_CMD="${DOCKER_CMD} --push"
fi

DOCKER_CMD="${DOCKER_CMD} -t ${TAG} -f ${DOCKERFILE} ${SRC_ROOT}"

if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
    if [[ -n "${DOCKERHUB_LOGIN}" ]]; then
        echo "Dockerhub login specified: '${DOCKERHUB_LOGIN}', perform login"
        docker login -u "${DOCKERHUB_LOGIN}"
    fi
fi

if ${DOCKER_CMD}; then
    echo "ALL DONE docker image published."
else
    echo "FAILED docker build! Abort."
    exit 1
fi
