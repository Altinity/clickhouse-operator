#!/usr/bin/env bash
set -xe
DEVSPACE_DEBUG=$1
DOCKER_IMAGE=$2
if [[ "${DOCKER_IMAGE}" == "altinity/clickhouse-operator:devspace" ]]; then
    DOCKER_FILE=./dockerfile/operator/Dockerfile
else
    DOCKER_FILE=./dockerfile/metrics-exporter/Dockerfile
fi

if [[ "${DEVSPACE_DEBUG}" == "--debug=delve" ]]; then
    docker build -f ${DOCKER_FILE} --target delve --build-arg GO_GCFLAGS='-N -l' -t ${DOCKER_IMAGE} .
else
    docker build -f ${DOCKER_FILE} -t ${DOCKER_IMAGE} .
fi
