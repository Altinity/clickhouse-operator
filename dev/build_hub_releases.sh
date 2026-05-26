#!/bin/bash
set -euo pipefail
#
# build_hub_releases.sh — Publishes the operator to OperatorHub.io and OpenShift community catalogs.
#
# What this script does (end-to-end):
#   1. Builds OLM bundle manifests (CSV, CRDs, metadata) from templates via operatorhub.sh
#   2. Syncs local clones of the two community catalog repos to upstream/main
#   3. Copies the generated bundle into each catalog repo under operators/clickhouse/<version>/
#   4. Commits and force-pushes each catalog repo (to your fork, for PR creation)
#   5. Commits the generated hub manifests back to the clickhouse-operator repo
#
# Prerequisites:
#   - Local clones of both catalog repos (see REPO_ROOTS below):
#       ~/dev/community-operators       — OperatorHub.io catalog
#       ~/dev/community-operators-prod  — OpenShift / Red Hat catalog
#     Each must have a git remote named "community" (or $UPSTREAM_REMOTE) pointing to the
#     canonical upstream repo (e.g., k8s-operatorhub/community-operators).
#   - yq installed (used to patch CSV for first-version edge case)
#   - VERSION from the release file (auto-detected by go_build_config.sh)
#   - PREVIOUS_VERSION from the releases file or environment (for spec.replaces in CSV)
#
# Usage:
#   ./dev/build_hub_releases.sh                        # auto-detect PREVIOUS_VERSION from releases file
#   PREVIOUS_VERSION=0.25.6 ./dev/build_hub_releases.sh  # explicit PREVIOUS_VERSION
#
# Environment overrides:
#   CO_REPO_PATH        — path to community-operators clone (default: ~/dev/community-operators)
#   OCP_REPO_PATH       — path to community-operators-prod clone (default: ~/dev/community-operators-prod)
#   UPSTREAM_REMOTE     — git remote name for canonical upstream (default: community)
#   PREVIOUS_VERSION    — version this release replaces in OLM upgrade graph

# Source configuration (sets VERSION, SRC_ROOT, etc. from release file and repo paths)
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# ==================================================================================
# Section 1: Build OLM bundle manifests locally
#
# Generates CSV (ClusterServiceVersion), CRDs, and metadata into deploy/operatorhub/
# using the operatorhub.sh template builder. Requires PREVIOUS_VERSION for spec.replaces
# (the OLM upgrade chain — tells OLM which version this one replaces).
# ==================================================================================

# PREVIOUS_VERSION is an optional env override; under `set -u` we must use
# the `:-` default-empty form to allow the unset case (the script then
# auto-detects from the releases file below).
if [[ -z "${PREVIOUS_VERSION:-}" ]]; then
    echo "PREVIOUS_VERSION is not explicitly specified"
    echo "Trying to figure out PREVIOUS_VERSION from releases"
    PREVIOUS_VERSION=$(head -n1 "${SRC_ROOT}/releases")
    echo "Found: PREVIOUS_VERSION=${PREVIOUS_VERSION}"
else
    echo "PREVIOUS_VERSION=${PREVIOUS_VERSION} (explicitly specified)"
fi

if [[ -z "${PREVIOUS_VERSION}" ]]; then
    echo "No PREVIOUS_VERSION available."
    echo "Please specify PREVIOUS_VERSION used in previous release, like:"
    echo "  PREVIOUS_VERSION=0.25.6 $0"
    exit 1
fi

echo "=================================================================================="
echo ""
echo "VERSION=${VERSION}"
echo "PREVIOUS_VERSION=${PREVIOUS_VERSION}"
echo ""
echo "=================================================================================="
read -n 1 -r -s -p $'Please verify VERSION and PREVIOUS_VERSION. Press enter to build...\n'

# Run the OLM bundle builder (generates CSV + CRDs + metadata in deploy/operatorhub/)
PREVIOUS_VERSION="${PREVIOUS_VERSION}" "${SRC_ROOT}/deploy/builder/operatorhub.sh"

OPERATORHUB_DIR="${SRC_ROOT}/deploy/operatorhub"

# ==================================================================================
# Section 2: Define destination catalog repos
#
# The operator is published to two community catalogs:
#   1. community-operators       — OperatorHub.io (vanilla Kubernetes)
#   2. community-operators-prod  — Red Hat OpenShift certified catalog
# Both follow the same directory layout: operators/clickhouse/<version>/manifests/
# ==================================================================================

REPO_ROOTS=(
    "${CO_REPO_PATH:-${HOME}/dev/community-operators}"
    "${OCP_REPO_PATH:-${HOME}/dev/community-operators-prod}"
)

DESTINATIONS=()
for REPO_ROOT in "${REPO_ROOTS[@]}"; do
    DESTINATIONS+=("${REPO_ROOT}/operators/clickhouse")
done

# Name of the git remote pointing to the canonical upstream in each catalog repo
UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-community}"

# ==================================================================================
# Section 3: Sync destination repos to upstream/main
#
# Fetches all remotes, then hard-resets local main to the upstream's main.
# This ensures we're building on top of the latest published catalog state,
# not an outdated local branch. Repos must already be cloned with the
# "community" (or $UPSTREAM_REMOTE) remote configured.
# ==================================================================================

function prepare_destination_repo() {
    local REPO_ROOT="$1"
    local UPSTREAM="$2"

    echo ""
    echo "Syncing ${REPO_ROOT} from ${UPSTREAM}/main ..."

    git -C "${REPO_ROOT}" fetch --all || { echo "  [ERROR] git fetch failed in ${REPO_ROOT}"; return 1; }

    local upstream_sha
    upstream_sha=$(git -C "${REPO_ROOT}" rev-parse "${UPSTREAM}/main") || {
        echo "  [ERROR] Cannot resolve ${UPSTREAM}/main in ${REPO_ROOT}"
        return 1
    }

    git -C "${REPO_ROOT}" checkout main || { echo "  [ERROR] git checkout main failed in ${REPO_ROOT}"; return 1; }
    git -C "${REPO_ROOT}" reset --hard "${upstream_sha}" || { echo "  [ERROR] git reset --hard failed in ${REPO_ROOT}"; return 1; }

    echo "  [OK]   ${REPO_ROOT} synced to ${UPSTREAM}/main (${upstream_sha})"
}

for REPO_ROOT in "${REPO_ROOTS[@]}"; do
    prepare_destination_repo "${REPO_ROOT}" "${UPSTREAM_REMOTE}"
done

# ==================================================================================
# Section 4: Copy generated bundle into each catalog repo
#
# Creates operators/clickhouse/<version>/manifests/ and metadata/ directories,
# copies the CSV, CRDs, and metadata files from deploy/operatorhub/.
#
# Special case: if this is the FIRST version in a catalog (no prior versions exist),
# spec.replaces is removed from the CSV — there's nothing to replace.
#
# Also copies ci.yaml (catalog-level config, same on every release).
# ==================================================================================

function copy_to_destination() {
    local DST_FOLDER="$1"
    local VERSION="$2"
    local SRC_BUNDLE_DIR="$3"

    local DST_MANIFESTS="${DST_FOLDER}/${VERSION}/manifests/"
    local DST_METADATA="${DST_FOLDER}/${VERSION}/metadata/"

    # Count pre-existing versions before we create anything
    local existing
    existing=$(ls -d "${DST_FOLDER}"/[0-9]*/ 2>/dev/null | wc -l)

    # Ensure target dirs exist and copy bundle files
    mkdir -p "${DST_MANIFESTS}" "${DST_METADATA}"
    cp -r "${SRC_BUNDLE_DIR}/${VERSION}/"* "${DST_MANIFESTS}"
    cp -r "${SRC_BUNDLE_DIR}/metadata/"*   "${DST_METADATA}"

    # First version in this destination — no upgrade path exists yet, drop spec.replaces
    if [[ "${existing}" -eq 0 ]]; then
        local csv_file
        csv_file=$(ls "${DST_MANIFESTS}"*.clusterserviceversion.yaml 2>/dev/null | head -1)
        if [[ -n "${csv_file}" ]]; then
            yq -i 'del(.spec.replaces)' "${csv_file}"
            echo "  [INFO] First version in destination — removed spec.replaces from CSV"
        fi
    fi

    # Copy operator-level ci.yaml (idempotent — same content on every release)
    cp "${SRC_BUNDLE_DIR}/ci.yaml" "${DST_FOLDER}/ci.yaml"

    echo "  [OK]   Copied to: ${DST_FOLDER}"
}

echo ""
echo "Bundle v${VERSION} will be copied to the following destinations:"
for DST in "${DESTINATIONS[@]}"; do
    if [[ -d "${DST}" ]]; then
        echo "  [FOUND]       ${DST}"
    else
        echo "  [WILL CREATE] ${DST}"
    fi
done
echo ""
echo "=================================================================================="
read -n 1 -r -s -p $'Press enter to copy...\n'

for DST in "${DESTINATIONS[@]}"; do
    copy_to_destination "${DST}" "${VERSION}" "${OPERATORHUB_DIR}"
done

# ==================================================================================
# Section 5: Commit and push each catalog repo
#
# Stages the new version directory + ci.yaml, creates a signed commit,
# and force-pushes to origin (your fork). After this, you create PRs
# from your fork to the upstream catalog repos.
# ==================================================================================

function commit_destination_repo() {
    local REPO_ROOT="$1"
    local VERSION="$2"

    git -C "${REPO_ROOT}" add "operators/clickhouse/${VERSION}" || { echo "  [ERROR] git add failed in ${REPO_ROOT}"; return 1; }
    # `|| true` is intentional under `set -e`: ci.yaml may already be tracked
    # and unchanged on a re-run, or absent if upstream removed it — both are
    # tolerable for this informational re-stage. The main version-dir add
    # above is the load-bearing one and is NOT tolerated to fail.
    git -C "${REPO_ROOT}" add "operators/clickhouse/ci.yaml" || true
    git -C "${REPO_ROOT}" commit -s -m "operator clickhouse (${VERSION})" || { echo "  [ERROR] git commit failed in ${REPO_ROOT}"; return 1; }
    git -C "${REPO_ROOT}" push --force || { echo "  [ERROR] git push failed in ${REPO_ROOT}"; return 1; }

    echo "  [OK]   Committed in ${REPO_ROOT}"
}

echo ""
for i in "${!REPO_ROOTS[@]}"; do
    commit_destination_repo "${REPO_ROOTS[$i]}" "${VERSION}"
done

# ==================================================================================
# Section 6: Commit generated hub manifests back to clickhouse-operator repo
#
# The generated deploy/operatorhub/ files are committed to the operator repo itself
# so the build artifacts are tracked in version control.
# ==================================================================================

echo ""
echo "Committing hub manifests to clickhouse-operator repo ..."
git -C "${SRC_ROOT}" add "deploy/operatorhub/" || { echo "  [ERROR] git add failed in ${SRC_ROOT}"; exit 1; }
git -C "${SRC_ROOT}" commit -m "env: hub manifests" || { echo "  [ERROR] git commit failed in ${SRC_ROOT}"; exit 1; }
git -C "${SRC_ROOT}" push altinity || { echo "  [ERROR] git push altinity failed in ${SRC_ROOT}"; exit 1; }
echo "  [OK]   Committed and pushed hub manifests to altinity"

echo ""
echo "DONE"
