#!/bin/bash

# Universal docker image builder

# Externally configurable build-dependent options
TAG="${TAG:-sunsingerus/metrics-exporter:dev}"
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN:-sunsingerus}"
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH:-yes}"
MINIKUBE="${MINIKUBE:-no}"

# Source-dependent options
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${CUR_DIR}/..")"
DOCKERFILE_DIR="${SRC_ROOT}/dockerfile/metrics-exporter"
DOCKERFILE="${DOCKERFILE_DIR}/Dockerfile"

# Build clickhouse-operator install .yaml manifest
"${SRC_ROOT}/manifests/operator/build-clickhouse-operator-install-yaml.sh"

# Build image with Docker
if [[ "${MINIKUBE}" == "yes" ]]; then
    # We'd like to build for minikube
    eval $(minikube docker-env)
fi
docker build -t "${TAG}" -f "${DOCKERFILE}" "${SRC_ROOT}"

# Publish image
if [[ "${DOCKERHUB_PUBLISH}" == "yes" ]]; then
    docker login -u "${DOCKERHUB_LOGIN}"
    docker push "${TAG}"
fi
