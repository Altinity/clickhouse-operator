#!/bin/bash

# Build configuration options

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
# All sources root
SRC_ROOT="$(realpath "${CUR_DIR}/..")"

# Deploy manifests root
MANIFESTS_ROOT="${SRC_ROOT}/deploy"
# Executable commands sources root
CMD_ROOT="${SRC_ROOT}/cmd"
# Packages root
PKG_ROOT="${SRC_ROOT}/pkg"

REPO="github.com/altinity/clickhouse-operator"

# 0.9.3
VERSION=$(cd "${SRC_ROOT}" && cat release)
# 885c3f7
GIT_SHA=$(cd "${SRC_ROOT}" && git rev-parse --short HEAD)
# 2020-03-07 14:54:56
NOW=$(date "+%FT%T")
# Which version of golang to use. Ex.: 1.23.0
GO_VERSION=$(cd "${SRC_ROOT}" && grep '^go ' go.mod | awk '{print $2}')

# NOTE: this file is SOURCED by ~20 callers. We deliberately do NOT enable
# `set -euo pipefail` here because that would silently change the strictness
# of every caller's shell, breaking scripts that reference legitimately-unset
# env vars (e.g. ${VERBOSITY} in build_manifests.sh). Instead, fail-fast on
# the specific HAZARD: empty VERSION / GIT_SHA / GO_VERSION getting baked
# into binaries or manifests. These guards run in the caller's shell and
# abort it via `exit 1` if a critical value is missing.
if [[ -z "${VERSION}" ]]; then
    echo "ERROR: go_build_config.sh: VERSION is empty (missing or unreadable ${SRC_ROOT}/release)" >&2
    exit 1
fi
if [[ -z "${GIT_SHA}" ]]; then
    echo "ERROR: go_build_config.sh: GIT_SHA is empty (not a git repo or git not on PATH at ${SRC_ROOT})" >&2
    exit 1
fi
if [[ -z "${GO_VERSION}" ]]; then
    echo "ERROR: go_build_config.sh: GO_VERSION is empty (missing 'go <ver>' line in ${SRC_ROOT}/go.mod)" >&2
    exit 1
fi

# Go FIPS 140-3 module version. Setting GOFIPS140 at build time links the Go
# FIPS 140-3 cryptographic module (`crypto/fips140` v1.0.0, currently in CMVP
# review — not yet a completed CMVP validation). Runtime mode for GOFIPS140-
# built binaries is `GODEBUG=fips140=on` (TLS filtering); this is the shipped
# default for the operator and metrics-exporter images — no other variant is
# published. See docs/security_hardening.md for the full rationale.
# Pass GOFIPS140= (empty) to disable for local non-FIPS builds.
#
# Avoid GOFIPS140=latest for ship builds — `latest` uses the toolchain's
# in-tree module rather than the frozen snapshot, defeating the reproducibility
# guarantee that the operator's FIPS-compatibility claim relies on. Always pin
# to the explicit version (e.g. v1.0.0).
GOFIPS140="${GOFIPS140-v1.0.0}"

# Runtime FIPS enforcement mode baked into the published image via Dockerfile
# `ENV GODEBUG=fips140=${GODEBUG_FIPS140}`. Accepted values: off|on|only|debug.
#   off   — FIPS module not active; standard stdlib crypto. No filtering, no
#           panics. SHIPPED DEFAULT — preserves pre-0.27.1 runtime behavior
#           even on a GOFIPS140-linked binary. Opt in to on/only at runtime
#           via Pod env to activate the module without rebuilding.
#   on    — FIPS module active. TLS / cipher / signature / cert-chain
#           selection is filtered to FIPS-approved primitives. Direct calls
#           into non-approved primitives outside the TLS layer remain
#           callable (no panic).
#   only  — Strictest. Same as `on` plus any direct invocation of a
#           non-FIPS-approved primitive panics at call time. Best-effort
#           enforcement per Go upstream; documented as "not intended for
#           production" by the Go team but useful for CI gating.
#   debug — Same as `on` plus every call into the FIPS module is logged to
#           stderr. Verbose; diagnostic only.
# Customer runtime overrides via Pod env / Helm `operator.env` / kubectl set env
# still win over the image-baked default.
GODEBUG_FIPS140="${GODEBUG_FIPS140-off}"

# Release evidence capture. Default off; CI workflows set EVIDENCE=yes
# explicitly. Local devs can opt in with `EVIDENCE=yes ./dev/image_build_all.sh ...`
# to also produce SBOM / digest / manifest artifacts under EVIDENCE_DIR.
#
# EVIDENCE_DIR defaults to ./release-evidence/ relative to the repo root;
# the orchestrator dev/release_evidence.sh writes <safe-tag>.{digest,sbom.spdx,manifest}.{txt,json}
# files there. The directory is created on demand by the orchestrator.
EVIDENCE="${EVIDENCE:-no}"
EVIDENCE_DIR="${EVIDENCE_DIR:-./release-evidence}"

RELEASE="1"

# Operator binary name can be specified externally
# Default - put 'clickhouse-operator' into cur dir
OPERATOR_BIN="${OPERATOR_BIN:-"${SRC_ROOT}/dev/bin/clickhouse-operator"}"

# Metrics exporter binary name can be specified externally
# Default - put 'metrics-exporter' into cur dir
METRICS_EXPORTER_BIN="${METRICS_EXPORTER_BIN:-"${SRC_ROOT}/dev/bin/metrics-exporter"}"
