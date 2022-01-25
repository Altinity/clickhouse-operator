#!/bin/bash

set -e
# Externally configurable build-dependent options
DOCKERFILE_DIR="${SRC_ROOT}/dockerfile/operator"

source "${CUR_DIR}/image_build_universal.sh"
