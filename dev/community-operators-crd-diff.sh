#!/bin/bash

# Source configuration
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
source "${CUR_DIR}/go_build_config.sh"

CO_PATH=~/dev/community-operators/upstream-community-operators/clickhouse/0.10.0

meld "${MANIFESTS_ROOT}"/dev/clickhouse-operator-install-yaml-template-01-section-crd-01-chi.yaml      "${CO_PATH}"/clickhouseinstallations.clickhouse.altinity.com.crd.yaml
meld "${MANIFESTS_ROOT}"/dev/clickhouse-operator-install-yaml-template-01-section-crd-02-chit.yaml     "${CO_PATH}"/clickhouseinstallationtemplates.clickhouse.altinity.com.crd.yaml
meld "${MANIFESTS_ROOT}"/dev/clickhouse-operator-install-yaml-template-01-section-crd-03-chopconf.yaml "${CO_PATH}"/clickhouseoperatorconfigurations.clickhouse.altinity.com.crd.yaml
