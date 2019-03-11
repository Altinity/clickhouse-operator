#!/bin/bash

echo "=== POD ==="
kubectl -n dev -o wide get pod

echo "=== SERVICE ==="
kubectl -n dev -o wide get service

echo "=== STATEFULE SET ==="
kubectl -n dev -o wide get statefulset

echo "=== CONFIG MAP ==="
kubectl -n dev -o wide get configmap

