#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/test_common.sh"

IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-"IfNotPresent"}"
# Preload images by default — common_preload_images is a no-op unless this is set.
# Avoids kubelet pull timeouts on fresh minikube clusters.
MINIKUBE_PRELOAD_IMAGES="${MINIKUBE_PRELOAD_IMAGES:-"yes"}"
export MINIKUBE_PRELOAD_IMAGES

common_minikube_reset
common_preload_images "${PRELOAD_IMAGES_METRICS[@]}"
common_build_and_load_images && \
common_run_test_script "run_tests_metrics.sh"
