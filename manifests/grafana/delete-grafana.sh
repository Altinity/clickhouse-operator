#!/bin/bash

GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"

echo "Delete Grafana namespace ${GRAFANA_NAMESPACE}"

kubectl delete namespace "${GRAFANA_NAMESPACE}"
