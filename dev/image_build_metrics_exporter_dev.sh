#!/bin/bash

# Dev docker image builder
set -e

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Externally configurable build-dependent options
TAG="${TAG:-dev}"
DOCKER_IMAGE="sunsingerus/metrics-exporter:${TAG}"
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN}"

source "${CUR_DIR}/image_build_metrics_exporter_universal.sh"
