#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/test_common.sh"

IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY:-"IfNotPresent"}"
POOL_SIZE="${POOL_SIZE:-"25"}"
# Skip image preloading when running a specific test (ONLY=...) to save time.
# When running the full suite, preload all images upfront to avoid per-test pull delays.
# Can be overridden explicitly: MINIKUBE_PRELOAD_IMAGES=yes ONLY=... to force preload.
if [[ -n "${ONLY}" && "${ONLY}" != "*" ]]; then
    MINIKUBE_PRELOAD_IMAGES="${MINIKUBE_PRELOAD_IMAGES:-""}"
    # Skip retries when running a specific test — retries mask real bugs during debugging.
    RETRY_COUNT="${RETRY_COUNT:-""}"
else
    MINIKUBE_PRELOAD_IMAGES="${MINIKUBE_PRELOAD_IMAGES:-"yes"}"
    # For full suite runs, retry 5 times to handle transient resource pressure failures.
    RETRY_COUNT="${RETRY_COUNT:-"5"}"
fi
export POOL_SIZE
export MINIKUBE_PRELOAD_IMAGES
export RETRY_COUNT

common_minikube_reset
common_preload_images "${PRELOAD_IMAGES_ALL[@]}"
common_build_and_load_images && \
common_run_test_script "run_tests_operator.sh"
