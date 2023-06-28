# Task: Setup Prometheus monitoring

We are going to setup **Prometheus <-> ClickHouse-operator** integration in k8s environment.
This document assumes k8s cluster already setup and `kubectl` has access to it.

We may have two starting points:
1. Prometheus installation is available and we just need to gather `clickhouse-operator` metrics
1. Need to install Prometheus at first and integrate it with `clickhouse-operator` afterwards

## Prometheus already available
In case we have Prometheus already setup, what we need is to specify `clickhouse-operator`'s HTTP endpoint where Prometheus can gather metrics from `clickhouse-operator`.
Endpoint is a Service of type ClusterIP named as `clickhouse-operator-metrics` It is located in the same namespace as `clickhouse-operator`:
```bash
kubectl get service clickhouse-operator-metrics -n kube-system
```
```text
NAME                          TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)    AGE
clickhouse-operator-metrics   ClusterIP   10.105.15.167   <none>        8888/TCP   95m
```

For debug purposes, we can port-forward it into our local OS as
```bash
kubectl --namespace=kube-system port-forward service/clickhouse-operator-metrics 8888
```
and access raw data with browser by navigating to `http://localhost:8888/metrics`

So, what we need in general, is to point Prometheus to gather data from: 
```text
http://<service/clickhouse-operator-metrics>:8888/metrics
```

## Setup Prometheus and integrate it with clickhouse-operator
In case we do not have Prometheus available, we can setup it directly into k8s and integrate with `clickhouse-operator` 

We are going to setup and manage Prometheus with [Prometheus Operator][prometheus-operator]

We already have [manifests available][deploy-prometheus]. 
We can either run [create-prometheus.sh][create-prometheus.sh] or setup the whole process by hands, in case we need to edit configuration.

  - We'd like to setup all Prometheus-related components into dedicated `namespace`. Let's create it
  ```bash
  kubectl create namespace prometheus
  ```
  
  - Create CRD for prometheus-operator like kind:Prometheus, kind:PrometheusRule etc.
  ```bash
  kubectl --namespace=prometheus apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_prometheuses.yaml
  kubectl --namespace=prometheus apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_prometheusrules.yaml
  kubectl --namespace=prometheus apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_alertmanagers.yaml
  kubectl --namespace=prometheus apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_podmonitors.yaml
  kubectl --namespace=prometheus apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml
  kubectl --namespace=prometheus apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/prometheus-operator-crd/monitoring.coreos.com_thanosrulers.yaml
  ```

  - Create RBAC settings for `prometheus-operator`
  ```bash
  kubectl --namespace=prometheus apply --server-side -f <( \
    wget -qO- https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/rbac/prometheus/prometheus-cluster-role-binding.yaml | \
    sed "s/namespace: default/namespace: prometheus/" \
  )
  kubectl --namespace=prometheus apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/rbac/prometheus/prometheus-cluster-role.yaml
  kubectl --namespace=prometheus apply --server-side -f https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/example/rbac/prometheus/prometheus-service-account.yaml
  ```

  - Setup `prometheus-operator` into dedicated namespace
  ```bash
    kubectl --namespace=prometheus apply --server-side -f  <( \
        wget -qO- https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/master/bundle.yaml | \
        sed "s/namespace: default/namespace: prometheus/" \
    )
  ```
    
  - Setup `prometheus` into dedicated namespace. `prometheus-operator` would be used to create `prometheus` instance
  ```bash
  kubectl apply --namespace=prometheus --server-side -f <(wget -qO- https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/prometheus/prometheus-template.yaml | PROMETHEUS_NAMESPACE=prometheus envsubst)
  ```

  - Setup `alertmanager` slack webhook, look at https://api.slack.com/incoming-webhooks how to enable external webhooks in Slack API
  ```bash
  export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/XXXX/YYYYY/ZZZZZ
  export SLACK_CHANNEL="#alerts-channel-name"
  export PROMETHEUS_NAMESPACE=prometheus
  export ALERT_MANAGER_EXTERNAL_URL=https://your.external-domain.for-alertmanger/    
  kubectl apply --namespace=prometheus -f <( \
    wget -qO- https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/prometheus/prometheus-alertmanager-template.yaml | \
    envsubst \
  )
  ```

  - Setup `clickhouse-operator` alert rules for `prometheus`
  ```bash
  kubectl apply --namespace=prometheus -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/deploy/prometheus/prometheus-alert-rules.yaml
  ```
    
At this point Prometheus and AlertManager is up and running. Also, all kubernetes pods which contains `meta.annotations` `prometheus.io/scrape: 'true'`, `promethus.io/port: NNNN`, `prometheus.io/path: '/metrics'`, `prometheus.io/scheme: http`, will discover via prometheus kubernetes_sd_config job `kubernetes-pods`.

Now we should have Prometheus gathering metrics from `clickhouse-operator`. Let's check it out.
To get access to Prometheus. Port-forward Prometheus to `localhost` as:
```bash
kubectl --namespace=prometheus port-forward service/prometheus 9090
```
and navigate browser to `http://localhost:9090` Prometheus should appear.

To get access to Alert Manager. Port-forward Prometheus to `localhost` as:
```bash
kubectl --namespace=prometheus port-forward service/alertmanager 9093
```
and navigate browser to `http://localhost:9093/alerts` Alert Manager should appear.

We can check whether `clickhouse-operator` is available at `http://localhost:9090/targets`

More Prometheus [docs][prometheus-docs]

[prometheus-operator]: https://coreos.com/operators/prometheus/docs/latest/
[deploy-prometheus]: ../deploy/prometheus/
[create-prometheus.sh]: ../deploy/prometheus/create-prometheus.sh
[prometheus-docs]: https://prometheus.io/docs/introduction/overview/
