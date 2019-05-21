#!/bin/bash

CUR_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

source ${CUR_DIR}/dev-config.sh

echo "=== Dev Pods ==="
kubectl -n "${CHOPERATOR_NAMESPACE}" -o wide get pod

echo "=== Dev Services ==="
kubectl -n "${CHOPERATOR_NAMESPACE}" -o wide get service

echo "=== Dev Statefule Sets ==="
kubectl -n "${CHOPERATOR_NAMESPACE}" -o wide get statefulset

echo "=== Dev Config Maps ==="
kubectl -n "${CHOPERATOR_NAMESPACE}" -o wide get configmap

echo "=== Dev PVC ==="
kubectl get storageclasses

echo "=== Dev PVC ==="
kubectl -n "${CHOPERATOR_NAMESPACE}" -o wide get persistentvolumeclaims

echo "=== Dev PV ==="
kubectl -n "${CHOPERATOR_NAMESPACE}" -o wide get persistentvolume
