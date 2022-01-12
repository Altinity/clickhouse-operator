#!/usr/bin/env bash
set -xe
DEVSPACE_DEBUG=$1
DOCKER_IMAGE=$2
eval $(go env)
TARGET_PLATFORM=${TARGET_PLATFORM:-${GOHOSTOS}/${GOHOSTARCH}}

if [[ "${DOCKER_IMAGE}" =~ "clickhouse-operator" ]]; then
    DOCKER_FILE=./dockerfile/operator/Dockerfile
else
    DOCKER_FILE=./dockerfile/metrics-exporter/Dockerfile
fi

if [[ "${DEVSPACE_DEBUG}" == "--debug=delve" ]]; then
    docker buildx build --progress plain --output "type=image,name=${DOCKER_IMAGE}" --platform="${TARGET_PLATFORM}" -f ${DOCKER_FILE} --target delve --build-arg GO_GCFLAGS='-N -l' -t ${DOCKER_IMAGE} .
else
    docker buildx build --progress plain --output "type=image,name=${DOCKER_IMAGE}" --platform="${TARGET_PLATFORM}" -f ${DOCKER_FILE} -t ${DOCKER_IMAGE} .
fi

docker images | grep devspace
