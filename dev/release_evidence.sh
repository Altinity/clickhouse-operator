#!/usr/bin/env bash
#
# release_evidence.sh — capture release evidence from an already-built image.
#
# Produces three artifacts pinned to the image's immutable manifest-list digest:
#   - <safe-tag>.digest.txt       sha256:... immutable digest of the manifest list
#   - <safe-tag>.sbom.spdx.json   syft SPDX-JSON SBOM (Go modules + system pkgs)
#   - <safe-tag>.manifest.json    raw multi-arch image manifest list
# where <safe-tag> = image-ref with '/' and ':' replaced by '__'.
#
# Usage:
#   ./dev/release_evidence.sh <image-ref> [<output-dir>=./release-evidence/]
#
# Exit codes:
#   0  success
#   2  bad usage
#   3  missing required tool
#   4  digest/manifest capture failed
#   5  SBOM generation failed

set -euo pipefail

log() {
    echo "release_evidence: $*" >&2
}

usage() {
    cat >&2 <<'EOF'
Usage: ./dev/release_evidence.sh <image-ref> [<output-dir>=./release-evidence/]

  <image-ref>   Fully-qualified image reference, e.g.
                docker.io/altinity/clickhouse-operator:0.27.1
  <output-dir>  Directory for artifacts (created if missing).
                Default: ./release-evidence/

Writes <safe-tag>.{digest.txt,sbom.spdx.json,manifest.json} where <safe-tag>
is the image ref with '/' and ':' replaced by '__'.
EOF
}

require_tool() {
    local tool="$1"
    if ! command -v "${tool}" >/dev/null 2>&1; then
        log "missing required tool: ${tool}"
        exit 3
    fi
}

# --- Arg parsing -------------------------------------------------------------

if [[ $# -lt 1 ]] || [[ -z "${1:-}" ]]; then
    usage
    exit 2
fi

IMAGE_REF="$1"
OUT_DIR="${2:-./release-evidence/}"

# Path-safe filename: replace '/' and ':' with '__'.
SAFE_TAG="${IMAGE_REF//\//__}"
SAFE_TAG="${SAFE_TAG//:/__}"
# Restrict to a known-safe charset; other specials (@, spaces, parens, *) -> '_'.
SAFE_TAG="${SAFE_TAG//[^A-Za-z0-9._-]/_}"

# --- Tool checks -------------------------------------------------------------

require_tool docker
require_tool syft
require_tool jq
# Used by the sha256sum-of-manifest fallback in step 2.
require_tool sha256sum
require_tool awk

# `docker buildx` is a plugin; verify it's wired in.
if ! docker buildx version >/dev/null 2>&1; then
    log "missing required tool: docker buildx (plugin)"
    exit 3
fi

# --- Prepare output dir ------------------------------------------------------

mkdir -p "${OUT_DIR}"

MANIFEST_FILE="${OUT_DIR%/}/${SAFE_TAG}.manifest.json"
DIGEST_FILE="${OUT_DIR%/}/${SAFE_TAG}.digest.txt"
SBOM_FILE="${OUT_DIR%/}/${SAFE_TAG}.sbom.spdx.json"

log "image-ref:   ${IMAGE_REF}"
log "output-dir:  ${OUT_DIR}"
log "safe-tag:    ${SAFE_TAG}"

# --- Step 1: capture multi-arch manifest list --------------------------------

log "step 1/3: capturing manifest list -> ${MANIFEST_FILE}"
if ! docker buildx imagetools inspect --raw "${IMAGE_REF}" > "${MANIFEST_FILE}"; then
    log "failed to capture manifest list for ${IMAGE_REF}"
    rm -f "${MANIFEST_FILE}"
    exit 4
fi

# Sanity: manifest must be valid JSON.
if ! jq -e . >/dev/null 2>&1 < "${MANIFEST_FILE}"; then
    log "captured manifest is not valid JSON"
    exit 4
fi

# --- Step 2: capture digest --------------------------------------------------

log "step 2/3: capturing digest -> ${DIGEST_FILE}"
DIGEST=""
# Capture stderr so registry/auth/network failures surface in the fallback log
# instead of being silently swallowed.
inspect_err="$(mktemp)"
if DIGEST="$(docker buildx imagetools inspect "${IMAGE_REF}" --format '{{.Manifest.Digest}}' 2>"${inspect_err}")" \
        && [[ "${DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    log "digest via buildx --format: ${DIGEST}"
else
    # Fallback: digest of a registry manifest is sha256 over its raw bytes.
    log "WARNING: --format digest capture failed; computing from manifest. Buildx stderr was:"
    cat "${inspect_err}" >&2 || true
    if ! DIGEST="sha256:$(sha256sum < "${MANIFEST_FILE}" | awk '{print $1}')"; then
        log "failed to compute fallback digest"
        rm -f "${inspect_err}"
        exit 4
    fi
    if [[ ! "${DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
        log "fallback digest is malformed: ${DIGEST}"
        rm -f "${inspect_err}"
        exit 4
    fi
    log "digest via fallback: ${DIGEST}"
fi
rm -f "${inspect_err}"
printf '%s\n' "${DIGEST}" > "${DIGEST_FILE}"

# --- Step 3: generate SBOM via syft ------------------------------------------

log "step 3/3: generating SBOM -> ${SBOM_FILE}"
if ! syft "${IMAGE_REF}" -o "spdx-json=${SBOM_FILE}"; then
    log "syft SBOM generation failed for ${IMAGE_REF}"
    exit 5
fi

# Sanity: SBOM must be valid JSON.
if ! jq -e . >/dev/null 2>&1 < "${SBOM_FILE}"; then
    log "generated SBOM is not valid JSON"
    exit 5
fi

echo "release_evidence: wrote 3 artifacts for ${IMAGE_REF} -> ${OUT_DIR%/}/"
