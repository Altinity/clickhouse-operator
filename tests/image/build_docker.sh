#!/bin/bash

OPERATOR_VERSION=${OPERATOR_VERSION:=0.16.0}
OPERATOR_VERSION_OLD=${OPERATOR_VERSION_OLD:=0.15.0}
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:="altinity/clickhouse-server:21.8.altinity_prestable"}
CLICKHOUSE_IMAGE_OLD=${CLICKHOUSE_IMAGE_OLD:="yandex/clickhouse-server:21.3"}

set -e

if ! [ -s "cache" ]; then
    mkdir cache
fi

if ! [ -f "cache/preloaded-images-k8s-v12-v1.21.2-docker-overlay2-amd64.tar.lz4" ]; then
    wget https://storage.googleapis.com/minikube-preloaded-volume-tarballs/preloaded-images-k8s-v12-v1.21.2-docker-overlay2-amd64.tar.lz4 -P ./cache/
fi

echo "Pre-pull images"
if  [[ "$(docker images -q gcr.io/k8s-minikube/kicbase:v0.0.26 2> /dev/null)" == "" ]]
then
    docker pull gcr.io/k8s-minikube/kicbase:v0.0.26
fi

if [[ "$(docker images -q ${CLICKHOUSE_IMAGE} 2> /dev/null)" == "" ]]
then
    docker pull ${CLICKHOUSE_IMAGE}
fi

if [[ "$(docker images -q altinity/metrics-exporter:${OPERATOR_VERSION} 2> /dev/null)" == "" ]]
then
    docker pull altinity/metrics-exporter:${OPERATOR_VERSION}
fi

if [[ "$(docker images -q altinity/clickhouse-operator:${OPERATOR_VERSION} 2> /dev/null)" == "" ]]
then
    docker pull altinity/clickhouse-operator:${OPERATOR_VERSION}
fi

if [[ "$(docker images -q gcr.io/k8s-minikube/storage-provisioner:v5 2> /dev/null)" == "" ]]
then
    docker pull gcr.io/k8s-minikube/storage-provisioner:v5
fi

if [[ "$(docker images -q altinity/metrics-exporter:${OPERATOR_VERSION_OLD} 2> /dev/null)" == "" ]]
then
    docker pull altinity/metrics-exporter:${OPERATOR_VERSION_OLD}
fi

if [[ "$(docker images -q altinity/clickhouse-operator:${OPERATOR_VERSION_OLD} 2> /dev/null)" == "" ]]
then
    docker pull altinity/clickhouse-operator:${OPERATOR_VERSION_OLD}
fi

if [[ "$(docker images -q docker.io/zookeeper:3.6.3 2> /dev/null)" == "" ]]
then
    docker pull docker.io/zookeeper:3.6.3
fi

if [[ "$(docker images -q ${CLICKHOUSE_IMAGE_OLD} 2> /dev/null)" == "" ]]
then
    docker pull ${CLICKHOUSE_IMAGE_OLD}
fi

echo "Save images"
docker save yandex/clickhouse-server:21.3 -o cache/ch_old.dockerimage
docker save gcr.io/k8s-minikube/kicbase:v0.0.26 -o cache/kicbase.dockerimage
docker save ${CLICKHOUSE_IMAGE} -o cache/ch_image.dockerimage
docker save altinity/metrics-exporter:${OPERATOR_VERSION} -o cache/m_expo.dockerimage
docker save altinity/clickhouse-operator:${OPERATOR_VERSION} -o cache/cho.dockerimage
docker save gcr.io/k8s-minikube/storage-provisioner:v5 -o cache/s_prov.dockerimage
docker save altinity/metrics-exporter:${OPERATOR_VERSION_OLD} -o cache/m_expo_old.dockerimage
docker save altinity/clickhouse-operator:${OPERATOR_VERSION_OLD} -o cache/cho_old.dockerimage
docker save docker.io/zookeeper:3.6.3 -o cache/zk.dockerimage

docker build -f Dockerfile -t registry.gitlab.com/altinity-qa/clickhouse/cicd/clickhouse-operator .
