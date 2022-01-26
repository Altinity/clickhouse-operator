#!/bin/bash

set -e
DOCKER_IMAGE=$1
echo "Wait when 'clickhouse-operator' Deployment will ready..."
while [[ "0" == $(kubectl get deploy -n "${OPERATOR_NAMESPACE}" | grep -c -E "clickhouse-operator.+1/1") ]]; do
    sleep 1
done
echo "...Done"

for img in $(docker images "${DOCKER_IMAGE%:*}" -q -f "before=${DOCKER_IMAGE}"); do
    docker image rm -f "${img}" || true
done
