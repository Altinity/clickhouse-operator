#!/usr/bin/env bash

set -euo pipefail

PROJECT_ID="${1:-${PROJECT_ID:-}}"
MANIFEST_DIGEST="${2:-${MANIFEST_DIGEST:-}}"
PYXIS_API_TOKEN="${RED_HAT_PYXIS_API_TOKEN:-${PYXIS_API_TOKEN:-}}"
PYXIS_URL="${PYXIS_URL:-https://catalog.redhat.com/api/containers/v1}"

if [[ -z "${PROJECT_ID}" || -z "${MANIFEST_DIGEST}" || -z "${PYXIS_API_TOKEN}" ]]; then
    echo "usage: RED_HAT_PYXIS_API_TOKEN=... $0 <certification-project-id> <sha256:digest>" >&2
    exit 2
fi
if [[ ! "${MANIFEST_DIGEST}" =~ ^sha256:[0-9a-f]{64}$ ]]; then
    echo "check-image-published: invalid manifest digest: ${MANIFEST_DIGEST}" >&2
    exit 2
fi

response="$(mktemp)"
trap 'rm -f "${response}"' EXIT

curl --fail --silent --show-error \
    --header "X-API-KEY: ${PYXIS_API_TOKEN}" \
    --get \
    --data-urlencode "page_size=100" \
    --output "${response}" \
    "${PYXIS_URL}/projects/certification/id/${PROJECT_ID}/images/manifest-list-digest"

if jq -e --arg digest "${MANIFEST_DIGEST}" '
    [.data[]? | select(.manifest_list_digest == $digest)] as $manifests |
    ($manifests | length) > 0 and
    all($manifests[];
        (.images | length) > 0 and
        all(.images[];
            .certified == true and
            any(.repositories[]?; .published == true)
        )
    )
' "${response}" >/dev/null; then
    echo "check-image-published: ${MANIFEST_DIGEST} is certified and published"
    exit 0
fi

echo "check-image-published: ${MANIFEST_DIGEST} is not yet certified and published" >&2
exit 3
