#!/bin/bash

# Only tighten shell options when executed as a script. When sourced (e.g. from
# image_build_universal.sh / go_build_all.sh), leave the caller's options alone —
# `set -o pipefail` here used to abort image builds on a no-match `grep | wc`.
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    set -euo pipefail
fi

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

# VERBOSITY is an optional knob — callers may run with `VERBOSITY=1 build_manifests.sh`
# or leave it unset for default. Under `set -u` the bare `${VERBOSITY}` would abort,
# so use the `:-` default-empty form.
echo "VERBOSITY=${VERBOSITY:-}"

# Build clickhouse-operator config files
source "${MANIFESTS_ROOT}/builder/build-clickhouse-operator-configs.sh"
# Build clickhouse-operator install .yaml manifest
source "${MANIFESTS_ROOT}/builder/build-clickhouse-operator-install-yaml.sh"
