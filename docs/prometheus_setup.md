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
  
  - Create CRD for kind:Prometheus and kind:PrometheusRule
  ```bash
  kubectl apply --namespace=prometheus -f prometheus.crd.yaml
  kubectl apply --namespace=prometheus -f prometheusrule.crd.yaml
  ```
     
  - Setup `prometheus-operator` into dedicated namespace
  ```bash
  kubectl apply --namespace=prometheus -f prometheus-operator.yaml
  ```
    
  - Setup `prometheus` into dedicated namespace. `prometheus-operator` would be used to create `prometheus` instance
  ```bash
  kubectl apply --namespace=prometheus -f prometheus.yaml
  ```

At this point Prometheus is up and running. What we need to do - setup integration with `clickhouse-operator`
  
  - Point `prometheus` to gather metrics from `clickhouse-operator`
  ```bash
  kubectl apply --namespace=prometheus -f prometheus-clickhouse-operator-service-monitor.yaml
  ```

Now we should have Prometheus gathering metrics from `clickhouse-operator`. Let's check it out.
Let's get access to Prometheus. Port-forward Prometheus to `localhost` as:
```bash
kubectl --namespace=prometheus port-forward service/prometheus 9090
```
and navigate browser to `http://localhost:9090` Prometheus should appear.

We can check whether `clickhouse-operator` is available at `http://localhost:9090/targets`

More Prometheus [docs][prometheus-docs]

[prometheus-operator]: https://coreos.com/operators/prometheus/docs/latest/
[deploy-prometheus]: ../deploy/prometheus/
[create-prometheus.sh]: ../deploy/prometheus/create-prometheus.sh
[prometheus-docs]: https://prometheus.io/docs/introduction/overview/
