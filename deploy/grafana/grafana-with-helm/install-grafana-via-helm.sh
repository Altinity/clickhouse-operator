#!/usr/bin/env bash
# reproduce behavior for https://github.com/Altinity/clickhouse-operator/issues/1721
kubectl create ns test

helm repo add altinity-clickhouse-operator https://docs.altinity.com/clickhouse-operator/
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