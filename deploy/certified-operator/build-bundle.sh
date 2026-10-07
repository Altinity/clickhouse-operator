#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${SCRIPT_DIR}/../..")"

require_tool() {
    command -v "$1" >/dev/null 2>&1 || {
        echo "build-certified-bundle: missing required tool: $1" >&2
        exit 2
    }
}

require_value() {
    local name="$1"
    if [[ -z "${!name:-}" ]]; then
        echo "build-certified-bundle: ${name} is required" >&2
        exit 2
    fi
}

require_digest_ref() {
    local name="$1"
    local value="$2"
    if [[ ! "${value}" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]]; then
        echo "build-certified-bundle: ${name} must be an immutable sha256 image reference" >&2
        exit 2
    fi
}

require_tool envsubst
require_tool mktemp
require_tool realpath
require_tool yq

require_value OPERATOR_IMAGE
require_value METRICS_IMAGE
require_value OPENSHIFT_VERSIONS
: "${VALID_SUBSCRIPTION:=No subscription required; optional commercial support is available from Altinity}"

require_digest_ref OPERATOR_IMAGE "${OPERATOR_IMAGE}"
require_digest_ref METRICS_IMAGE "${METRICS_IMAGE}"

VERSION="${VERSION:-$(<"${SRC_ROOT}/release")}"
PACKAGE_NAME="${PACKAGE_NAME:-altinity-clickhouse-operator}"
CHANNELS="${CHANNELS:-stable}"
DEFAULT_CHANNEL="${DEFAULT_CHANNEL:-stable}"
MIN_KUBE_VERSION="${MIN_KUBE_VERSION:-1.25.0}"
CERT_MERGE="${CERT_MERGE:-true}"
OUTPUT_DIR="${OUTPUT_DIR:-${SCRIPT_DIR}/generated}"

if [[ ! "${PACKAGE_NAME}" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]]; then
    echo "build-certified-bundle: PACKAGE_NAME must be a DNS label" >&2
    exit 2
fi

case "$(realpath -m "${OUTPUT_DIR}")" in
    "${SCRIPT_DIR}"/generated|"${SCRIPT_DIR}"/generated/*) ;;
    *)
        echo "build-certified-bundle: OUTPUT_DIR must be under ${SCRIPT_DIR}/generated" >&2
        exit 2
        ;;
esac

STAGING_DIR="$(mktemp -d)"
trap 'rm -rf "${STAGING_DIR}"' EXIT

# Reuse the established CRD/CSV generator, but direct its output away from the
# community catalog tree. Certification-specific metadata is applied below.
OPERATORHUB_DIR="${STAGING_DIR}" \
    "${SRC_ROOT}/deploy/builder/operatorhub.sh"

SOURCE_MANIFESTS="${STAGING_DIR}/${VERSION}"
CSV_SOURCE="${SOURCE_MANIFESTS}/clickhouse-operator.v${VERSION}.clusterserviceversion.yaml"
CSV_NAME="${PACKAGE_NAME}.v${VERSION}"
CSV_DEST="${OUTPUT_DIR}/manifests/${CSV_NAME}.clusterserviceversion.yaml"

rm -rf "${OUTPUT_DIR}"
mkdir -p "${OUTPUT_DIR}/manifests" "${OUTPUT_DIR}/metadata"
cp "${SOURCE_MANIFESTS}"/*.crd.yaml "${OUTPUT_DIR}/manifests/"
cp "${CSV_SOURCE}" "${CSV_DEST}"
cp "${SRC_ROOT}/deploy/operatorhub/metadata/annotations.yaml" \
    "${OUTPUT_DIR}/metadata/annotations.yaml"

export OPERATOR_IMAGE METRICS_IMAGE OPENSHIFT_VERSIONS VALID_SUBSCRIPTION
export PACKAGE_NAME CHANNELS DEFAULT_CHANNEL MIN_KUBE_VERSION CSV_NAME

yq -i '
    del(.metadata.namespace) |
    .metadata.name = strenv(CSV_NAME) |
    .metadata.annotations.containerImage = strenv(OPERATOR_IMAGE) |
    .metadata.annotations."operators.openshift.io/valid-subscription" = strenv(VALID_SUBSCRIPTION) |
    del(.metadata.annotations.certified) |
    .spec.minKubeVersion = strenv(MIN_KUBE_VERSION) |
    del(.spec.skips) |
    .spec.install.spec.deployments[0].spec.template.spec.securityContext.seccompProfile.type = "RuntimeDefault" |
    .spec.install.spec.deployments[0].spec.template.spec.containers[0].image = strenv(OPERATOR_IMAGE) |
    .spec.install.spec.deployments[0].spec.template.spec.containers[1].image = strenv(METRICS_IMAGE) |
    .spec.install.spec.deployments[0].spec.template.spec.containers[].securityContext = {
        "allowPrivilegeEscalation": false,
        "capabilities": {"drop": ["ALL"]},
        "runAsNonRoot": true
    }
' "${CSV_DEST}"

if [[ -n "${PREVIOUS_VERSION:-}" ]]; then
    export PREVIOUS_CSV="${PACKAGE_NAME}.v${PREVIOUS_VERSION}"
    yq -i '.spec.replaces = strenv(PREVIOUS_CSV)' "${CSV_DEST}"
else
    yq -i 'del(.spec.replaces)' "${CSV_DEST}"
fi

RELATED_FILE="${STAGING_DIR}/related-images.yaml"
yq -n '[
    {"name": "clickhouse-operator", "image": strenv(OPERATOR_IMAGE)},
    {"name": "metrics-exporter", "image": strenv(METRICS_IMAGE)}
]' > "${RELATED_FILE}"

# Additional certified operands/helpers are supplied one per line as
# RELATED_IMAGES='clickhouse-server=registry/name@sha256:...<newline>...'.
# Blank lines are ignored.
while IFS= read -r related; do
    [[ -z "${related}" ]] && continue
    if [[ "${related}" != *=* ]]; then
        echo "build-certified-bundle: malformed RELATED_IMAGES entry: ${related}" >&2
        exit 2
    fi
    RELATED_NAME="${related%%=*}"
    RELATED_IMAGE="${related#*=}"
    if [[ ! "${RELATED_NAME}" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]]; then
        echo "build-certified-bundle: invalid related image name: ${RELATED_NAME}" >&2
        exit 2
    fi
    require_digest_ref RELATED_IMAGE "${RELATED_IMAGE}"
    export RELATED_NAME RELATED_IMAGE
    yq -i '. += [{"name": strenv(RELATED_NAME), "image": strenv(RELATED_IMAGE)}]' "${RELATED_FILE}"
done <<< "${RELATED_IMAGES:-}"

for required_image in clickhouse-server clickhouse-keeper clickhouse-log; do
    export REQUIRED_IMAGE="${required_image}"
    if ! yq -e '[.[] | select(.name == strenv(REQUIRED_IMAGE))] | length == 1' "${RELATED_FILE}" >/dev/null; then
        echo "build-certified-bundle: RELATED_IMAGES is missing required default: ${required_image}" >&2
        exit 2
    fi
done

export DEFAULT_CLICKHOUSE_IMAGE DEFAULT_KEEPER_IMAGE DEFAULT_LOG_IMAGE
DEFAULT_CLICKHOUSE_IMAGE="$(yq -r '.[] | select(.name == "clickhouse-server") | .image' "${RELATED_FILE}")"
DEFAULT_KEEPER_IMAGE="$(yq -r '.[] | select(.name == "clickhouse-keeper") | .image' "${RELATED_FILE}")"
DEFAULT_LOG_IMAGE="$(yq -r '.[] | select(.name == "clickhouse-log") | .image' "${RELATED_FILE}")"

yq -i '
    .spec.install.spec.deployments[0].spec.template.spec.containers[0].env += [
        {"name": "CLICKHOUSE_OPERATOR_DEFAULT_CLICKHOUSE_IMAGE", "value": strenv(DEFAULT_CLICKHOUSE_IMAGE)},
        {"name": "CLICKHOUSE_OPERATOR_DEFAULT_KEEPER_IMAGE", "value": strenv(DEFAULT_KEEPER_IMAGE)},
        {"name": "CLICKHOUSE_OPERATOR_DEFAULT_LOG_IMAGE", "value": strenv(DEFAULT_LOG_IMAGE)}
    ]
' "${CSV_DEST}"

export RELATED_FILE
yq -i '.spec.relatedImages = load(strenv(RELATED_FILE))' "${CSV_DEST}"

yq -i '
    .annotations."operators.operatorframework.io.bundle.package.v1" = strenv(PACKAGE_NAME) |
    .annotations."operators.operatorframework.io.bundle.channels.v1" = strenv(CHANNELS) |
    .annotations."operators.operatorframework.io.bundle.channel.default.v1" = strenv(DEFAULT_CHANNEL) |
    .annotations."com.redhat.openshift.versions" = strenv(OPENSHIFT_VERSIONS)
' "${OUTPUT_DIR}/metadata/annotations.yaml"

if [[ -n "${CERT_PROJECT_ID:-}" ]]; then
    if [[ "${CERT_MERGE}" != true && "${CERT_MERGE}" != false ]]; then
        echo "build-certified-bundle: CERT_MERGE must be true or false" >&2
        exit 2
    fi
    printf '%s\n' "---" "cert_project_id: ${CERT_PROJECT_ID}" "merge: ${CERT_MERGE}" > "${OUTPUT_DIR}/ci.yaml"
fi

echo "build-certified-bundle: generated ${OUTPUT_DIR}"
echo "build-certified-bundle: CSV ${CSV_NAME}"
