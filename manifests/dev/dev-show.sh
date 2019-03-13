#!/bin/bash

echo "=== Dev Pods ==="
kubectl -n dev -o wide get pod

echo "=== Dev Services ==="
kubectl -n dev -o wide get service

echo "=== Dev Statefule Sets ==="
kubectl -n dev -o wide get statefulset

echo "=== Dev Config Maps ==="
kubectl -n dev -o wide get configmap

echo "=== Dev PVC ==="
kubectl get storageclasses

echo "=== Dev PVC ==="
kubectl -n dev -o wide get persistentvolumeclaims

echo "=== Dev PV ==="
kubectl -n dev -o wide get persistentvolume
