#!/bin/bash

set -e
# Externally configurable build-dependent options
DOCKERFILE_DIR="${SRC_ROOT}/dockerfile/metrics-exporter"

source "${CUR_DIR}/image_build_universal.sh"
