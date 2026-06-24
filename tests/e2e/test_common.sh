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
# Intentionally EXCLUDED from preload (verified — do not re-add):
#   - Meant-to-fail / decoy images (preloading them would defeat the test that rejects them):
#       clickhouse/clickhouse-server:24.3-broken / :24.822     (rollback tests)
#       altinity/clickhouse-server:*.altinityfips-decoy        (test-030008-runtime-decoy)
#       clickhouse/clickhouse-keeper:latest                    (test-020010 non-FIPS rejection)
#   - Opt-in CLICKHOUSE_TEMPLATE overrides, not run by default (manifests/chit/tpl-clickhouse-*.yaml):
#       clickhouse/clickhouse-server:21.3 / 21.8 / 22.1 / 22.2 / 22.3 / 22.6 / 22.7
#       altinity/clickhouse-server:22.8.15.25.altinitystable   (tpl-clickhouse-22.8.yaml)
#       yandex/clickhouse-server:*
# Audit coverage (every default-suite image is listed below):
#   comm -23 <(grep -rhoE "(clickhouse|altinity)/clickhouse-(server|keeper):[A-Za-z0-9._-]+" tests/e2e/manifests/ | sort -u) \
#            <(grep -oE "(clickhouse|altinity)/clickhouse-(server|keeper):[A-Za-z0-9._-]+" tests/e2e/test_common.sh | sort -u)

# Single canonical preload list shared by ALL suites (operator, metrics, keeper).
# One list, not per-suite lists: a per-suite list silently drifts from the manifests
# another suite uses (e.g. the metrics suite once omitted server:24.3/24.8 that
# test-017-multi-version needs, cold-pulling them on every fresh minikube and timing
# out). Preloading the full set everywhere is cheap — common_preload_images runs in
# parallel and skips images already present — and removes that whole class of flake.
PRELOAD_IMAGES_ALL=(
    # ClickHouse server versions used in manifests and templates
    "clickhouse/clickhouse-server:23.3"        # clickhouse_template_old + older-version compat tests
    "clickhouse/clickhouse-server:23.8"
    "clickhouse/clickhouse-server:24.3"        # also base for 24.3-broken rollback tests; test-017-multi-version (metrics)
    "clickhouse/clickhouse-server:24.8"        # test-017-multi-version (metrics)
    "clickhouse/clickhouse-server:25.3"
    "clickhouse/clickhouse-server:25.8"
    "clickhouse/clickhouse-server:latest"
    # Altinity builds (default stable template + FIPS)
    "altinity/clickhouse-server:25.8.16.10001.altinitystable"  # default clickhouse_template
    "altinity/clickhouse-server:25.8.16.10002.altinitystable"  # test_010035 auto-recovery upgrade target (manifests/chi/test-035-auto-recovery-2.yaml)
    "altinity/clickhouse-server:25.3.8.30001.altinityfips"     # FIPS CHI (e.g. manifests/chk/test-020008-chi-fips.yaml)
    # ClickHouse Keeper versions
    "clickhouse/clickhouse-keeper:25.3"
    "clickhouse/clickhouse-keeper:25.8"
    "clickhouse/clickhouse-keeper:latest-alpine"  # test_clickhouse_keeper_rescale (deploy/clickhouse-keeper/clickhouse-keeper-manually/...-for-test-only.yaml)
    "altinity/clickhouse-keeper:25.3.8.30001.altinityfips"
    # Zookeeper
    "docker.io/zookeeper:3.8.4"
    # Misc
    "registry.access.redhat.com/ubi8/ubi-minimal:latest"
    "nginx:latest"
    "altinity/clickhouse-backup:stable"
    "altinity/clickhouse-backup:2.4.15"
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
# Usage: common_preload_images "${PRELOAD_IMAGES_ALL[@]}"
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
    # settings.py resolves the operator version from the `release` file, so the
    # e2e install path requests altinity/clickhouse-operator:<release>. The build
    # always tags images :dev, so without retagging an IfNotPresent install pulls
    # the PUBLISHED <release> image from the registry instead of the freshly-built
    # local one -- silently testing the wrong binary. Retag the local :dev build
    # to :<release> and load that too, so the suite exercises local changes.
    local release
    release="$(tr -d ' \r\n\t' < "${COMMON_DIR}/../../release")"
    echo "Build" && \
    VERBOSITY="${VERBOSITY}" "${COMMON_DIR}/../../dev/image_build_all_dev.sh" && \
    echo "Retag local :dev build as :${release} (match install version)" && \
    docker tag "${OPERATOR_DOCKER_REPO}:dev" "${OPERATOR_DOCKER_REPO}:${release}" && \
    docker tag "${METRICS_EXPORTER_DOCKER_REPO}:dev" "${METRICS_EXPORTER_DOCKER_REPO}:${release}" && \
    echo "Load images" && \
    minikube image load "${OPERATOR_DOCKER_REPO}:dev" && \
    minikube image load "${METRICS_EXPORTER_DOCKER_REPO}:dev" && \
    minikube image load "${OPERATOR_DOCKER_REPO}:${release}" --overwrite=true && \
    minikube image load "${METRICS_EXPORTER_DOCKER_REPO}:${release}" --overwrite=true && \
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
