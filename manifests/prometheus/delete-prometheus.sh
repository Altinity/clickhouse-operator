#!/bin/bash

PROMETHEUS_NAMESPACE="${PROMETHEUS_NAMESPACE:-prometheus}"

echo "Delete Prometheus namespace ${PROMETHEUS_NAMESPACE}"

kubectl delete namespace "${PROMETHEUS_NAMESPACE}"
