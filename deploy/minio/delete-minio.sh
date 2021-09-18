#!/bin/bash

MINIO_NAMESPACE="${MINIO_NAMESPACE:-minio}"

echo "Delete Minio namespace ${MINIO_NAMESPACE}"

kubectl delete namespace "${MINIO_NAMESPACE}"
