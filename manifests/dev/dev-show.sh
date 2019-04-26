#!/bin/bash

. ./dev-config.sh

echo "=== Dev Pods ==="
kubectl -n "${DEV_NAMESPACE}" -o wide get pod

echo "=== Dev Services ==="
kubectl -n "${DEV_NAMESPACE}" -o wide get service

echo "=== Dev Statefule Sets ==="
kubectl -n "${DEV_NAMESPACE}" -o wide get statefulset

echo "=== Dev Config Maps ==="
kubectl -n "${DEV_NAMESPACE}" -o wide get configmap

echo "=== Dev PVC ==="
kubectl get storageclasses

echo "=== Dev PVC ==="
kubectl -n "${DEV_NAMESPACE}" -o wide get persistentvolumeclaims

echo "=== Dev PV ==="
kubectl -n "${DEV_NAMESPACE}" -o wide get persistentvolume
