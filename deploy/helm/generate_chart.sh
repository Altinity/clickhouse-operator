#!/bin/bash

VERSION=$(cat ../../release)
CHART="clickhouse-operator"

cat ../operator/clickhouse-operator-install-bundle.yaml | helmify ${CHART}

VERSION=$VERSION yq e '.appVersion = env(VERSION) | 
                       .version = env(VERSION)' -i ${CHART}/Chart.yaml
