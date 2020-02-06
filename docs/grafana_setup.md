# Task: Setup Grafana monitoring

We are going to setup **Grafana & Prometheus** integration in k8s environment.
This document assumes k8s cluster already setup and `kubectl` has access to it.
This document assumes Prometheus is already setup and gathers data from `clickhouse-operator`. 
More details on how to setup **Prometheus & ClickHouse-operator** integration are in [Setup Prometheus monitoring doc][prometheus_setup_doc] 

We may have two starting points:
1. Need to install Grafana at first and integrate it with Prometheus afterwards
1. Grafana installation is available and we just need to gather data from Prometheus

## Install Grafana
In case we do not have Grafana available, we can setup it directly into k8s and integrate with Prometheus afterwards. 
We already have [manifests available][grafana_manifest_folder]. 
We can either run [create-grafana.sh][create_grafana_script] or setup the whole process by hands, in case we need to edit configuration.

  - We'd like to setup all Grafana-related components into dedicated `namespace`. Let's create it
  ```bash
  kubectl create namespace grafana
  ```
     
  - Setup `grafana` into dedicated namespace
  ```bash
  kubectl apply --namespace=grafana -f grafana.yaml
  ```

At this point Grafana is up and running. Let's check it out. 
Grafana is running in k8s cluster and is available via Service of type ClusterIP and named as `grafana`.
It is located in the same namespace as Grafana:
```bash
kubectl --namespace=grafana get service grafana 
```
```text
NAME      TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)    AGE
grafana   ClusterIP   10.98.42.192   <none>        3000/TCP   14h
```
Let's get access to Grafana. Port-forward Grafana to `localhost` as:
```bash
kubectl --namespace=grafana port-forward service/grafana 3000
```
and navigate browser to `http://localhost:3000` Grafana should appear.
Login credentials:
 - username: **admin**
 - password: **admin**

Login credentials are specified in [grafana.yaml][grafana_manifest_yaml] as resource of [**kind: Secret**][grafana_manifest_yaml_secret] and is `base64`-encoded.

Grafana is installed by now.

## Setup Grafana & Prometheus integration
In Grafana, navigate to **Data Sources** page with menu or directly as 
```
http://localhost:3000/datasources
```
Add new data source on **Data Sources** page to fetch data from Prometheus.
Data source configuration parameters:
 - Name: **Prometheus** or specify your own 
 - Type: choose **Prometheus**
 - URL: This is the tricky part. We need to specify Prometheus endpoint to gather data from. 
 In case you have your own Prometheus installation, specify its endpoint or ask your admin where to look for. 
 In case Prometheus was setup via [this Setup Prometheus doc][prometheus_setup_doc] we need to specify access to `prometheus` service in k8s cluster.
 In case of both Grafana and Prometheus were setup according to our manuals (both are running in k8s), Prometheus endpoint is available inside k8s cluster as:
 ```text
 http://prometheus.prometheus.svc.cluster.local:9090
 ```
 which can be explained as:
 ```text
 http://<service name>.<namespace>.svc.cluster.local:9090
 ```
 where `svc.cluster.local` is k8s-cluster-dependent part, but it is still rather often called with default `svc.cluster.local` 
 - Access: choose **proxy**, which means Grafana backend will send request to Prometheus, while **direct** means "directly from browser" which will not work in case of k8s-installation.

By now, Prometheus data should be available for Grafana and we can choose nice dashboard to plot data on. Altinity supply recommended [Grafana dashoard][altinity_recommended_dashboard] as additional deliverables. 

## Install Grafana Dashboard

In order to install dashboard:
 1. Navigate to `main menu -> Dashboards -> Import` and pick `Upload .json file`. 
 1. Select recommended [Grafana dashoard][altinity_recommended_dashboard]
 1. Select a Prometheus data source from which data would be fetched  
 1. Click **Import**
 
By now Altinity recommended dashboard should be available for use.  

More Grafana [docs](http://docs.grafana.org/)

[grafana_manifest_folder]: ../deploy/grafana
[grafana_manifest_yaml]: ../deploy/grafana/grafana.yaml
[grafana_manifest_yaml_secret]:  ../deploy/grafana/grafana.yaml#L56
[create_grafana_script]: ../deploy/grafana/create-grafana.sh 
[prometheus_setup_doc]: ./prometheus_setup.md 
[altinity_recommended_dashboard]: ../grafana-dashboard/Altinity_ClickHouse_Operator_dashboard.json 
