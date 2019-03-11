#!/bin/bash

echo "=== Dev Pods ==="
kubectl -n dev -o wide get pod

echo "=== Dev Services ==="
kubectl -n dev -o wide get service

echo "=== Dev Statefule Sets ==="
kubectl -n dev -o wide get statefulset

echo "=== Dev Config Maps ==="
kubectl -n dev -o wide get configmap

