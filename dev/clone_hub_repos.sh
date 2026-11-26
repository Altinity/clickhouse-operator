#!/bin/bash
set -euo pipefail
#
# clone_hub_repos.sh — Prepares local clones of the OperatorHub catalog repos
# used by dev/build_hub_releases.sh.
#
# For each catalog repo it:
#   1. Forks the upstream repo into your GitHub account (gh repo fork; no-op if the fork exists)
#   2. Clones the fork into ~/src/github.com/<upstream-owner>/<repo> (or reuses an existing clone)
#   3. Points remote "origin" to your fork and remote "community" to the upstream
#   4. Fetches both remotes
#
# Prerequisites: gh (authenticated), git with SSH access to github.com
#
# Environment overrides:
#   FORK_OWNER       — GitHub account to fork into (default: current gh user)
#   CO_REPO_PATH     — path to community-operators clone
#   OCP_REPO_PATH    — path to community-operators-prod clone
#   UPSTREAM_REMOTE  — remote name for canonical upstream (default: community)

FORK_OWNER="${FORK_OWNER:-$(gh api user --jq .login)}"
UPSTREAM_REMOTE="${UPSTREAM_REMOTE:-community}"

# Keep paths in sync with REPO_ROOTS in dev/build_hub_releases.sh
UPSTREAMS=(
    "k8s-operatorhub/community-operators"
    "redhat-openshift-ecosystem/community-operators-prod"
)
REPO_ROOTS=(
    "${CO_REPO_PATH:-${HOME}/src/github.com/k8s-operatorhub/community-operators}"
    "${OCP_REPO_PATH:-${HOME}/src/github.com/redhat-openshift-ecosystem/community-operators-prod}"
)

function set_remote() {
    local REPO_ROOT="$1"
    local NAME="$2"
    local URL="$3"

    if git -C "${REPO_ROOT}" remote get-url "${NAME}" >/dev/null 2>&1; then
        git -C "${REPO_ROOT}" remote set-url "${NAME}" "${URL}"
    else
        git -C "${REPO_ROOT}" remote add "${NAME}" "${URL}"
    fi
}

function prepare_repo() {
    local UPSTREAM="$1"
    local REPO_ROOT="$2"
    local REPO_NAME="${UPSTREAM#*/}"
    local UPSTREAM_URL="git@github.com:${UPSTREAM}.git"
    local FORK_URL="git@github.com:${FORK_OWNER}/${REPO_NAME}.git"

    echo ""
    echo "=================================================================================="
    echo "Upstream: ${UPSTREAM}"
    echo "Fork:     ${FORK_OWNER}/${REPO_NAME}"
    echo "Path:     ${REPO_ROOT}"
    echo "=================================================================================="

    # Fork (idempotent — gh reports an existing fork and exits 0)
    gh repo fork "${UPSTREAM}" --clone=false --default-branch-only

    if [[ -d "${REPO_ROOT}/.git" ]]; then
        echo "  [INFO] Reusing existing clone"
    else
        mkdir -p "$(dirname "${REPO_ROOT}")"
        # Catalog repos are large; blob-less partial clone is enough for add/commit/push
        git clone --filter=blob:none "${FORK_URL}" "${REPO_ROOT}"
    fi

    set_remote "${REPO_ROOT}" origin "${FORK_URL}"
    set_remote "${REPO_ROOT}" "${UPSTREAM_REMOTE}" "${UPSTREAM_URL}"

    git -C "${REPO_ROOT}" fetch --all --prune
    git -C "${REPO_ROOT}" checkout main
    git -C "${REPO_ROOT}" branch --set-upstream-to=origin/main main

    git -C "${REPO_ROOT}" remote -v
    echo "  [OK]   ${REPO_ROOT} ready"
}

for i in "${!UPSTREAMS[@]}"; do
    prepare_repo "${UPSTREAMS[$i]}" "${REPO_ROOTS[$i]}"
done

echo ""
echo "DONE"
