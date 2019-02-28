# Task: Setup Prometheus monitoring

We are going to setup Prometheus in k8s environment.
This document assumes k8s cluster already setup and `kubectl` has access to it.

## Prometheus already available
In case we have Prometheus already setup, what we need is to specify `clickhouse-operator`'s HTTP endpoint where Prometheus can gather metrics from `clickhouse-operator`.
Endpoint is a Service of type ClusterIP named as `clickhouse-operator-metrics` and it is located in the same namespace as `clickhouse-operator`:
```bash
kubectl get service clickhouse-operator-metrics -n kube-system
```
```text
NAME                          TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)    AGE
clickhouse-operator-metrics   ClusterIP   10.105.15.167   <none>        8888/TCP   95m
```

We can port-forward it into our local OS as
```bash
kubectl --namespace kube-system port-forward service/clickhouse-operator-metrics 8888
```
and access raw data with browser by navigating to `http://localhost:8888/metrics`
So, what we need is to point Prometheus to gather data from `http://<service/clickhouse-operator-metrics>:8888/metrics`

In case we do not have Prometheus available, we can setup it directly into k8s and integrate with `clickhouse-operator` 

## Setup Prometheus
We are going to setup and manage Prometheus with [Prometheus Operator](https://coreos.com/operators/prometheus/docs/latest/)

We already have [manifests available](../manifests/prometheus/). 
We can either run [create-prometheus-operator.sh](../manifests/prometheus/create-prometheus-operator.sh) or setup the whole process by hands, in case we need to edit configuration.

  - We'd like to setup all Prometheus-related components into dedicated `namespace`. Let's create it
  ```bash
  kubectl create namespace prometheus
  ```
     
  - Setup `prometheus-operator` into dedicated namespace
  ```bash
  kubectl apply -f prometheus-operator.yaml -n prometheus
  ```
    
  - Setup `prometheus` into dedicated namespace with `prometheus-operator`
  ```bash
  kubectl apply -f prometheus.yaml -n prometheus
  ```
  
  - Point `prometheus` to gather metrics from `clickhouse-operator`
  ```bash
  kubectl apply -f prometheus-clickhouse-operator-service-monitor.yaml -n prometheus
  ```

Now we should have Prometheus up and running/ Let's check it out.
Port-forward Prometheus to `localhost` as:
```bash
kubectl --namespace prometheus port-forward service/prometheus 9090
```
and navigate browser to `http://localhost:9090` Prometheus should appear.

