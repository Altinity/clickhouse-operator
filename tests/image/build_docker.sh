#!/bin/bash
CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

OPERATOR_VERSION=${OPERATOR_VERSION:=0.20.0}
OPERATOR_VERSION_OLD=${OPERATOR_VERSION_OLD:=0.19.3}
OPERATOR_IMAGE="altinity/clickhouse-operator:${OPERATOR_VERSION}"
OPERATOR_IMAGE_OLD="altinity/clickhouse-operator:${OPERATOR_VERSION_OLD}"
METRICS_EXPORTER_IMAGE="altinity/metrics-exporter:${OPERATOR_VERSION}"
METRICS_EXPORTER_IMAGE_OLD="altinity/metrics-exporter:${OPERATOR_VERSION_OLD}"
CLICKHOUSE_BACKUP_IMAGE="altinity/clickhouse-backup:2.1.3"
CLICKHOUSE_IMAGE=${CLICKHOUSE_IMAGE:="clickhouse/clickhouse-server:22.8"}
CLICKHOUSE_IMAGE_OLD=${CLICKHOUSE_IMAGE_OLD:="clickhouse/clickhouse-server:22.3"}
CLICKHOUSE_IMAGE_LATEST=${CLICKHOUSE_IMAGE_LATEST:="clickhouse/clickhouse-server:latest"}
CLICKHOUSE_OPERATOR_TESTS_IMAGE=${CLICKHOUSE_OPERATOR_TESTS_IMAGE:="registry.gitlab.com/altinity-public/container-images/clickhouse-operator-test-runner:latest"}
ZOOKEEPER_IMAGE=${ZOOKEEPER_IMAGE:="zookeeper:3.8.1"}

K8S_VERSION=${K8S_VERSION:=1.25.3}
MINIKUBE_PRELOADED_TARBALL="preloaded-images-k8s-v18-v${K8S_VERSION}-docker-overlay2-amd64.tar.lz4"
MINIKUBE_KICBASE_IMAGE=${MINIKUBE_KICBASE_IMAGE:-"gcr.io/k8s-minikube/kicbase:v0.0.35"}
MINIKUBE_STORAGE_IMAGE=${MINIKUBE_STORAGE_IMAGE:="gcr.io/k8s-minikube/storage-provisioner:v20210514"}

MINIO_IMAGE=${MINIO_IMAGE:="minio/minio:RELEASE.2021-06-17T00-10-46Z"}
MINIO_CONSOLE_IMAGE=${MINIO_CONSOLE_IMAGE:="minio/console:latest"}
MINIO_CLIENT_IMAGE=${MINIO_CLIENT_IMAGE:="minio/mc:latest"}
MINIO_OPERATOR_IMAGE=${MINIO_OPERATOR_IMAGE:="minio/operator:v4.1.3"}

PROMETHEUS_RELOADER_IMAGE=${PROMETHEUS_RELOADER_IMAGE:="quay.io/prometheus-operator/prometheus-config-reloader:v0.57.0"}
PROMETHEUS_OPERATOR_IMAGE=${PROMETHEUS_OPERATOR_IMAGE:="quay.io/prometheus-operator/prometheus-operator:v0.57.0"}
PROMETHEUS_IMAGE=${PROMETHEUS_IMAGE:="quay.io/prometheus/prometheus:v2.39.1"}
PROMETHEUS_ALERT_MANAGER_IMAGE=${PROMETHEUS_ALERT_MANAGER_IMAGE:="quay.io/prometheus/alertmanager:v0.24.0"}


set -e

if ! [ -s "${CUR_DIR}/cache" ]; then
    mkdir -p "${CUR_DIR}/cache"
fi

if ! [ -f "${CUR_DIR}/cache/${MINIKUBE_PRELOADED_TARBALL}" ]; then
    wget --progress=bar:force:noscroll "https://storage.googleapis.com/minikube-preloaded-volume-tarballs/v18/v${K8S_VERSION}/${MINIKUBE_PRELOADED_TARBALL}" -P "${CUR_DIR}/cache"
fi

echo "Pre-pull images and save"
ALL_IMAGES=(
  "${CLICKHOUSE_IMAGE} ch_image"
  "${CLICKHOUSE_IMAGE_OLD} ch_old"
  "${CLICKHOUSE_IMAGE_LATEST} ch_latest"
  "${MINIKUBE_KICBASE_IMAGE} kicbase"
  "${MINIKUBE_STORAGE_IMAGE} s_prov"
  "${ZOOKEEPER_IMAGE} zk"
  "${OPERATOR_IMAGE} operator"
  "${OPERATOR_IMAGE_OLD} operator_old"
  "${METRICS_EXPORTER_IMAGE} metrics_exporter"
  "${METRICS_EXPORTER_IMAGE_OLD} metrics_exporter_old"
  "${MINIO_IMAGE} minio"
  "${MINIO_CONSOLE_IMAGE} minio_console"
  "${MINIO_CLIENT_IMAGE} minio_image"
  "${MINIO_OPERATOR_IMAGE} minio_operator"
  "${PROMETHEUS_RELOADER_IMAGE} prometheus_preloader"
  "${PROMETHEUS_OPERATOR_IMAGE} prometheus_operator"
  "${PROMETHEUS_IMAGE} prometheus"
  "${PROMETHEUS_ALERT_MANAGER_IMAGE} alert_manager"
  "${CLICKHOUSE_BACKUP_IMAGE} clickhouse_backup"
  "busybox busybox"
)
for item in  "${ALL_IMAGES[@]}"; do
  img=$(echo "$item" | cut -d " " -f 1)
  file=$(echo "$item" | cut -d " " -f 2)
  if [[ "$(docker images -q "${img}" 2> /dev/null)" == "" ]]
  then
      docker pull "${img}"
  fi
  if [[ ! -f "${CUR_DIR}/cache/${file}.txt" || "${img}" != $(cat "${CUR_DIR}/cache/${file}.txt") ]]; then
    docker save "${img}" -o "${CUR_DIR}/cache/${file}.dockerimage"
    echo "${img}" > "${CUR_DIR}/cache/${file}.txt"
  fi
done

echo "Build ${CLICKHOUSE_OPERATOR_TESTS_IMAGE}"
docker buildx build --platform=linux/amd64 --progress=plain -f "${CUR_DIR}/Dockerfile" --output="type=image,name=${CLICKHOUSE_OPERATOR_TESTS_IMAGE}" "${CUR_DIR}" --push
echo "All done"
