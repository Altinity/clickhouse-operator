#!/bin/bash

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

goreportcard-cli -v -d "${CMD_ROOT}"
goreportcard-cli -v -d "${PKG_ROOT}"

