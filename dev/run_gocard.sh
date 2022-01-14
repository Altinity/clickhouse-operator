#!/bin/bash

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

if [[ ! $(command -v goreportcard-cli) ]]; then
    CGO_ENABLED=0 GO111MODULE=on go install -ldflags "-s -w -extldflags '-static'" github.com/gojp/goreportcard/cmd/goreportcard-cli@latest
fi

if [[ ! $(command -v goreportcard-cli) ]]; then
    echo "Unable to find goreportcard-cli in \$PATH"
    exit 1
fi

goreportcard-cli -v -d "${CMD_ROOT}"
goreportcard-cli -v -d "${PKG_ROOT}"
