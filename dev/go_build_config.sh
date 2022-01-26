#!/bin/bash

# Build configuration options

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
SRC_ROOT="$(realpath "${CUR_DIR}/..")"

MANIFESTS_ROOT="${SRC_ROOT}/deploy"
CMD_ROOT="${SRC_ROOT}/cmd"
PKG_ROOT="${SRC_ROOT}/pkg"

REPO="github.com/altinity/clickhouse-operator"
# 0.9.3
VERSION=$(cd "${SRC_ROOT}"; cat release)
# 885c3f7
GIT_SHA=$(cd "${CUR_DIR}"; git rev-parse --short HEAD)
# 2020-03-07 14:54:56
NOW=$(date "+%FT%T")

RELEASE="1"

# Operator binary name can be specified externally
# Default - put 'clickhouse-operator' into cur dir
OPERATOR_BIN="${OPERATOR_BIN:-${SRC_ROOT}/dev/bin/clickhouse-operator}"

# Metrics exporter binary name can be specified externally
# Default - put 'metrics-exporter' into cur dir
METRICS_EXPORTER_BIN="${METRICS_EXPORTER_BIN:-${SRC_ROOT}/dev/bin/metrics-exporter}"
