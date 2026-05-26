#!/bin/bash

# Common library for test scripts. Source this file, do not execute it.
# Usage: source "${CUR_DIR}/test_common.sh"

COMMON_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# =============================================================================
# Variable defaults (all overridable via environment)
# =============================================================================

# Operator versioning
OPERATOR_VERSION="${OPERATOR_VERSION:-"dev"}"
OPERATOR_DOCKER_REPO="${OPERATOR_DOCKER_REPO:-"altinity/clickhouse-operator"}"
OPERATOR_IMAGE="${OPERATOR_IMAGE:-"${OPERATOR_DOCKER_REPO}:${OPERATOR_VERSION}"}"
METRICS_EXPORTER_DOCKER_REPO="${METRICS_EXPORTER_DOCKER_REPO:-"altinity/metrics-exporter"}"
METRICS_EXPORTER_IMAGE="${METRICS_EXPORTER_IMAGE:-"${METRICS_EXPORTER_DOCKER_REPO}:${OPERATOR_VERSION}"}"

# NOTE: IMAGE_PULL_POLICY is intentionally NOT set here.
# Test runners default to "Always" (CI), local scripts default to "IfNotPresent" (minikube).

# Test execution
OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE:-"test"}"
OPERATOR_INSTALL="${OPERATOR_INSTALL:-"yes"}"
ONLY="${ONLY:-"*"}"
VERBOSITY="${VERBOSITY:-"2"}"
RUN_ALL="${RUN_ALL:-""}"
KUBECTL_MODE="${KUBECTL_MODE:-"apply"}"
NO_CLEANUP="${NO_CLEANUP:-""}"

# Minikube control — defaults set by callers (run_tests_*_local.sh), not here

# FIPS-built operator/exporter images are the default for local e2e runs.
# common_build_and_load_images calls dev/image_build_all_dev.sh, which sources
# dev/go_build_config.sh (GOFIPS140=v1.0.0 default) and routes through
# dev/image_build_universal.sh (passes --build-arg GOFIPS140 to docker). The
# Dockerfile image-prod stage adds ENV GODEBUG=fips140=on (TLS filtering). The only build
# path that produces a non-strict image is deploy/devspace/docker-build.sh
# --debug=delve (target image-debug), which is not reachable from run_tests_*.
# Regression coverage: tests/e2e/test_operator.py::test_010076.

# =============================================================================
# Image lists for preloading into minikube
# =============================================================================

# NOTE: Keep this list in sync with images referenced from:
#   - tests/e2e/manifests/**/*.yaml                   (test-specific manifests)
#   - tests/e2e/manifests/chit/tpl-clickhouse-stable.yaml   (default CLICKHOUSE_TEMPLATE)
#   - tests/e2e/manifests/chit/tpl-clickhouse-23.3.yaml     (clickhouse_template_old)
#   - tests/e2e/manifests/chk/*.yaml                  (keeper tests, incl. FIPS)
# Intentionally EXCLUDED from preload:
#   - clickhouse/clickhouse-server:24.3-broken / :24.822   (meant-to-fail rollback tests)
#   - yandex/clickhouse-server:*                           (opt-in via CLICKHOUSE_TEMPLATE)
#   - altinity/clickhouse-server:22.8.15.25.altinitystable (only in optional tpl-clickhouse-22.8.yaml)
# Quick audit:
#   grep -rhE "image:[[:space:]]+[a-zA-Z0-9._/-]+:[a-zA-Z0-9._-]+" tests/e2e/manifests/ | sort -u

PRELOAD_IMAGES_OPERATOR=(
    # ClickHouse server versions used in manifests and templates
    "clickhouse/clickhouse-server:23.3"        # clickhouse_template_old + older-version compat tests
    "clickhouse/clickhouse-server:23.8"
    "clickhouse/clickhouse-server:24.3"        # also base for 24.3-broken rollback tests
    "clickhouse/clickhouse-server:24.8"
    "clickhouse/clickhouse-server:25.3"
    "clickhouse/clickhouse-server:25.8"
    "clickhouse/clickhouse-server:latest"
    # Altinity builds (default stable template + FIPS)
    "altinity/clickhouse-server:25.8.16.10001.altinitystable"  # default clickhouse_template
    "altinity/clickhouse-server:25.8.16.10002.altinitystable"  # test_010035 auto-recovery upgrade target (manifests/chi/test-035-auto-recovery-2.yaml)
    "altinity/clickhouse-server:25.3.8.30001.altinityfips"     # FIPS CHI (e.g. manifests/chk/test-020008-chi-fips.yaml)
    # ClickHouse Keeper versions used in operator tests (test_010063, test_020008, ...)
    "clickhouse/clickhouse-keeper:25.3"
    "clickhouse/clickhouse-keeper:25.8"
    "altinity/clickhouse-keeper:25.3.8.30001.altinityfips"
    # Zookeeper
    "docker.io/zookeeper:3.8.4"
    # Misc
    "registry.access.redhat.com/ubi8/ubi-minimal:latest"
    "nginx:latest"
    "altinity/clickhouse-backup:stable"
    "altinity/clickhouse-backup:2.4.15"
)

PRELOAD_IMAGES_KEEPER=(
    # ClickHouse server versions
    "clickhouse/clickhouse-server:23.3"
    "clickhouse/clickhouse-server:23.8"
    "clickhouse/clickhouse-server:24.3"
    "clickhouse/clickhouse-server:24.8"
    "clickhouse/clickhouse-server:25.3"
    "clickhouse/clickhouse-server:25.8"
    "clickhouse/clickhouse-server:latest"
    # Altinity builds
    "altinity/clickhouse-server:25.8.16.10001.altinitystable"  # default clickhouse_template
    "altinity/clickhouse-server:25.8.16.10002.altinitystable"  # test_010035 auto-recovery upgrade target
    "altinity/clickhouse-server:25.3.8.30001.altinityfips"     # FIPS CHI (manifests/chk/test-020008-chi-fips.yaml)
    # ClickHouse Keeper versions
    "clickhouse/clickhouse-keeper:25.3"
    "clickhouse/clickhouse-keeper:25.8"
    "clickhouse/clickhouse-keeper:latest-alpine"  # test_clickhouse_keeper_rescale (deploy/clickhouse-keeper/clickhouse-keeper-manually/...-for-test-only.yaml)
    "altinity/clickhouse-keeper:25.3.8.30001.altinityfips"
    # Zookeeper
    "docker.io/zookeeper:3.8.4"
)

PRELOAD_IMAGES_METRICS=(
    "clickhouse/clickhouse-server:23.3"
    "clickhouse/clickhouse-server:25.3"
    "clickhouse/clickhouse-server:latest"
    "altinity/clickhouse-server:25.8.16.10001.altinitystable"  # default clickhouse_template
    "docker.io/zookeeper:3.8.4"                                # metrics_alerts exec's into zookeeper-0 pod
)

# =============================================================================
# Functions
# =============================================================================

# Install Python dependencies needed by TestFlows
function common_install_pip_requirements() {
    pip3 install -r "${COMMON_DIR}/../image/requirements.txt"
}

# Convert RUN_ALL env var to --test-to-end flag.
# Usage: RUN_ALL_FLAG=$(common_convert_run_all)
function common_convert_run_all() {
    if [[ -n "${RUN_ALL}" ]]; then
        echo "--test-to-end"
    fi
}

# Export the standard set of env vars that regression.py / settings.py expects
function common_export_test_env() {
    export OPERATOR_NAMESPACE
    export OPERATOR_INSTALL
    export IMAGE_PULL_POLICY
    export NO_CLEANUP
}

# Reset minikube cluster if MINIKUBE_RESET is set
function common_minikube_reset() {
    if [[ -n "${MINIKUBE_RESET}" ]]; then
        SKIP_K9S="yes" "${COMMON_DIR}/run_minikube_reset.sh"
    fi
}

# Pull images and load them into minikube in parallel.
# Only runs if MINIKUBE_PRELOAD_IMAGES is set.
# Usage: common_preload_images "${PRELOAD_IMAGES_OPERATOR[@]}"
function common_preload_images() {
    if [[ -n "${MINIKUBE_PRELOAD_IMAGES}" ]]; then
        echo "pre-load images into minikube (parallel)"
        local pids=()
        for image in "$@"; do
            (
                docker pull -q "${image}" && \
                echo "pushing ${image} to minikube" && \
                minikube image load "${image}" --overwrite=false --daemon=true && \
                echo "done: ${image}"
            ) &
            pids+=($!)
        done
        local failed=0
        for pid in "${pids[@]}"; do
            wait "${pid}" || { echo "ERROR: a preload job failed (pid ${pid})"; failed=1; }
        done
        if [[ "${failed}" -eq 0 ]]; then
            echo "images pre-loaded"
        else
            echo "WARNING: some images failed to preload"
        fi
    fi
}

# Build operator + metrics-exporter docker images and load them into minikube
function common_build_and_load_images() {
    echo "Build" && \
    VERBOSITY="${VERBOSITY}" "${COMMON_DIR}/../../dev/image_build_all_dev.sh" && \
    echo "Load images" && \
    minikube image load "${OPERATOR_IMAGE}" && \
    minikube image load "${METRICS_EXPORTER_IMAGE}" && \
    echo "Images prepared"
}

# Run a test runner script with all env vars forwarded.
# Usage: common_run_test_script "run_tests_operator.sh"
function common_run_test_script() {
    local script="${1}"
    OPERATOR_DOCKER_REPO="${OPERATOR_DOCKER_REPO}" \
    METRICS_EXPORTER_DOCKER_REPO="${METRICS_EXPORTER_DOCKER_REPO}" \
    OPERATOR_VERSION="${OPERATOR_VERSION}" \
    IMAGE_PULL_POLICY="${IMAGE_PULL_POLICY}" \
    OPERATOR_NAMESPACE="${OPERATOR_NAMESPACE}" \
    OPERATOR_INSTALL="${OPERATOR_INSTALL}" \
    ONLY="${ONLY}" \
    KUBECTL_MODE="${KUBECTL_MODE}" \
    RUN_ALL="${RUN_ALL}" \
    NO_CLEANUP="${NO_CLEANUP}" \
    "${COMMON_DIR}/${script}"
}
