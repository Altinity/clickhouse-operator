#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
BUNDLE_DIR="${1:-${SCRIPT_DIR}/generated}"

command -v yq >/dev/null 2>&1 || {
    echo "verify-certified-bundle: missing required tool: yq" >&2
    exit 2
}

CSV="$(find "${BUNDLE_DIR}/manifests" -maxdepth 1 -name '*.clusterserviceversion.yaml' -print -quit)"
if [[ -z "${CSV}" ]]; then
    echo "verify-certified-bundle: no CSV found" >&2
    exit 2
fi

test "$(yq '.metadata.namespace // ""' "${CSV}")" = ""
test "$(yq '.metadata.annotations.certified // ""' "${CSV}")" != "false"
test -n "$(yq '.metadata.annotations."operators.openshift.io/valid-subscription" // ""' "${CSV}")"
test "$(yq '.spec.relatedImages | length' "${CSV}")" -ge 2

while IFS= read -r image; do
    [[ "${image}" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]] || {
        echo "verify-certified-bundle: image is not digest-pinned: ${image}" >&2
        exit 1
    }
done < <(yq -r '.spec.relatedImages[].image' "${CSV}")

while IFS='=' read -r related_name env_name; do
    export RELATED_NAME="${related_name}" ENV_NAME="${env_name}"
    related_image="$(yq -r '.spec.relatedImages[] | select(.name == strenv(RELATED_NAME)) | .image' "${CSV}")"
    configured_image="$(yq -r '.spec.install.spec.deployments[0].spec.template.spec.containers[0].env[] | select(.name == strenv(ENV_NAME)) | .value' "${CSV}")"
    if [[ -z "${related_image}" || "${configured_image}" != "${related_image}" ]]; then
        echo "verify-certified-bundle: certified default ${related_name} is missing or not configured" >&2
        exit 1
    fi
done <<'DEFAULT_IMAGES'
clickhouse-server=CLICKHOUSE_OPERATOR_DEFAULT_CLICKHOUSE_IMAGE
clickhouse-keeper=CLICKHOUSE_OPERATOR_DEFAULT_KEEPER_IMAGE
clickhouse-log=CLICKHOUSE_OPERATOR_DEFAULT_LOG_IMAGE
DEFAULT_IMAGES

for index in 0 1; do
    deployment_image="$(yq -r ".spec.install.spec.deployments[0].spec.template.spec.containers[${index}].image" "${CSV}")"
    found=false
    while IFS= read -r related_image; do
        if [[ "${deployment_image}" == "${related_image}" ]]; then
            found=true
            break
        fi
    done < <(yq -r '.spec.relatedImages[].image' "${CSV}")
    if [[ "${found}" != true ]]; then
        echo "verify-certified-bundle: deployment image missing from relatedImages: ${deployment_image}" >&2
        exit 1
    fi
done

test -n "$(yq '.annotations."com.redhat.openshift.versions" // ""' "${BUNDLE_DIR}/metadata/annotations.yaml")"

if command -v operator-sdk >/dev/null 2>&1; then
    operator-sdk bundle validate "${BUNDLE_DIR}"
else
    echo "verify-certified-bundle: operator-sdk not installed; structural checks only" >&2
fi

echo "verify-certified-bundle: checks passed"
