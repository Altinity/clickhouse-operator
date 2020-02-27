#!/bin/bash

GRAFANA_NAMESPACE="${GRAFANA_NAMESPACE:-grafana}"

kubectl --namespace="${GRAFANA_NAMESPACE}" port-forward service/grafana-service 3000

