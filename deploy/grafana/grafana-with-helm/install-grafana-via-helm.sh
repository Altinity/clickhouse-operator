#!/usr/bin/env bash
# Install the operator and Grafana via Helm, wiring the operator's dashboards
# ConfigMap into Grafana's default provider. Use to reproduce a vanilla
# operator + Grafana setup with port-forward access on http://localhost:3000.
kubectl create ns test

helm repo add altinity-clickhouse-operator https://helm.altinity.com
helm install -n test test-operator --set dashboards.enabled=true altinity-clickhouse-operator/altinity-clickhouse-operator

helm repo add grafana https://grafana.github.io/helm-charts
helm install -n test grafana \
  --set dashboardsConfigMaps.default=test-operator-altinity-clickhouse-operator-dashboards \
  --set "dashboardProviders.dashboardproviders\\.yaml.apiVersion=1" \
  --set "dashboardProviders.dashboardproviders\\.yaml.providers[0].name=default" \
  --set "dashboardProviders.dashboardproviders\\.yaml.providers[0].orgId=1" \
  --set "dashboardProviders.dashboardproviders\\.yaml.providers[0].folder=" \
  --set "dashboardProviders.dashboardproviders\\.yaml.providers[0].type=file" \
  --set "dashboardProviders.dashboardproviders\\.yaml.providers[0].disableDeletion=false" \
  --set "dashboardProviders.dashboardproviders\\.yaml.providers[0].editable=true" \
  --set "dashboardProviders.dashboardproviders\\.yaml.providers[0].options.path=/var/lib/grafana/dashboards/default" \
  grafana/grafana

# open http://localhost:3000
kubectl port-forward -n test service/grafana 3000:3000