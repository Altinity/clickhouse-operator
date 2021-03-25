#!/usr/bin/env bash
set -xeo pipefail
docker run --rm -v ${PWD}:/workdir mikefarah/yq yq w -d '10' -s /workdir/deploy/devspace/images.yq.yaml /workdir/deploy/operator/clickhouse-operator-install.yaml > deploy/devspace/clickhouse-operator-install.yaml
sed -i "s/namespace: kube-system/namespace: ${OPERATOR_NAMESPACE}/" deploy/devspace/clickhouse-operator-install.yaml
