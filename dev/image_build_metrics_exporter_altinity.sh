#!/bin/bash

# Production docker image builder
set -e

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# Externally configurable build-dependent options
TAG="${TAG:-latest}"
DOCKER_IMAGE="altinity/metrics-exporter:${TAG}"
DOCKERHUB_LOGIN="${DOCKERHUB_LOGIN:-altinitybuilds}"

source "${CUR_DIR}/image_build_metrics_exporter_universal.sh"
