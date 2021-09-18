#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source "${CUR_DIR}/dev-config.sh"

echo "=== Pod ==="
kubectl -n "${OPERATOR_NAMESPACE}" -o wide get pod

echo "=== Service ==="
kubectl -n "${OPERATOR_NAMESPACE}" -o wide get service

echo "=== StatefulSet ==="
kubectl -n "${OPERATOR_NAMESPACE}" -o wide get statefulset

echo "=== ConfigMap ==="
kubectl -n "${OPERATOR_NAMESPACE}" -o wide get configmap

echo "=== StorageClass ==="
kubectl get storageclasses

echo "=== PersistentVolumeClaim ==="
kubectl -n "${OPERATOR_NAMESPACE}" -o wide get persistentvolumeclaims

echo "=== PersistentVolume ==="
kubectl -n "${OPERATOR_NAMESPACE}" -o wide get persistentvolume
