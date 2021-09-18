#!/bin/bash

# Dev docker image builder

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Externally configurable build-dependent options
TAG="${TAG:-sunsingerus/clickhouse-operator:dev}"
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN}"
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH:-yes}"
MINIKUBE="${MINIKUBE:-no}"

TAG="${TAG}" \
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN}" \
DOCKERHUB_PUBLISH="${DOCKERHUB_PUBLISH}" \
MINIKUBE="${MINIKUBE}" \
"${CUR_DIR}/image_build_operator_universal.sh"
