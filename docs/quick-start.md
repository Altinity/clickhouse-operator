# Quick Start Guides

# Table of Contents
* [ClickHouse Operator Installation](#clickhouse-operator-installation)
* [Building ClickHouse Operator from Sources](#building-clickhouse-operator-from-sources)
* [Examples](#examples)
  * [Simple Example](#simple-example)
  * [Connect to ClickHouse Database](#connect-to-clickhouse-database)
  * [Simple Persistent Volume Example](#simple-persistent-volume-example)
  * [Custom Deployment with Pod and VolumeClaim Templates](#custom-deployment-with-pod-and-volumeclaim-templates)
  * [Custom Deployment with Specific ClickHouse Configuration](#custom-deployment-with-specific-clickhouse-configuration)

# Prerequisites
1. Operational Kubernetes instance
1. Properly configured `kubectl`

# ClickHouse Operator Installation

Apply `clickhouse-operator` installation manifest. The simplest way - directly from github 
```bash
kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/manifests/operator/clickhouse-operator-install.yaml
```
Operator installation process
```text
customresourcedefinition.apiextensions.k8s.io/clickhouseinstallations.clickhouse.altinity.com created
serviceaccount/clickhouse-operator created
clusterrolebinding.rbac.authorization.k8s.io/clickhouse-operator created
deployment.apps/clickhouse-operator created
service/clickhouse-operator-metrics created
```

Check `clickhouse-operator` is running:
```bash
kubectl get pods -n kube-system
```
```text
NAME                                 READY   STATUS    RESTARTS   AGE
clickhouse-operator-5ddc6d858f-drppt 1/1     Running   0          1m
```

## Building ClickHouse Operator from Sources

Complete instructions on how to build ClickHouse operator from sources as well as how to build a docker image and use it inside `kubernetes` described [here][build_from_sources].

# Examples

## Create Custom Namespace
It is a good practice to have all components run in dedicated namespaces. Let's run examples in `test` namespace
```bash
kubectl create namespace test
```
```text
namespace/test created
```

## Simple example

There are several ready-to-use [examples](./examples/)

Let's installed the simplest one - [1-replica](./examples/01-standard-layout-01-1shard-1repl.yaml)
 
```bash
kubectl apply -n test -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/docs/examples/01-standard-layout-01-1shard-1repl.yaml
```
```text
clickhouseinstallation.clickhouse.altinity.com/example-01 created
```

Installation specification is straightforward and defines 1-replica cluster:
```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "standard-01"
spec:
  configuration:
    clusters:
      - name: "standard-01-1shard-1repl"
        layout:
          type: Standard
          shardsCount: 1
          replicasCount: 1
```

Once cluster is created, there are two checks to be made.

```bash
kubectl get pods -n test
```
```text
NAME                    READY   STATUS    RESTARTS   AGE
chi-b3d29f-a242-0-0-0   1/1     Running   0          10m
```

Watch out for 'Running' status. Also check services created by an operator:

```bash
kubectl get service -n test
```
```text
NAME                    TYPE           CLUSTER-IP       EXTERNAL-IP                          PORT(S)                         AGE
chi-b3d29f-a242-0-0     ClusterIP      None             <none>                               8123/TCP,9000/TCP,9009/TCP      11m
clickhouse-example-01   LoadBalancer   100.64.167.170   abc-123.us-east-1.elb.amazonaws.com   8123:30954/TCP,9000:32697/TCP   11m
```

ClickHouse is up and running!

## Connect to ClickHouse Database

There are two ways to connect to ClickHouse database
1. In case previous command `kubectl get service -n test` reported **EXTERNAL-IP** (abc-123.us-east-1.elb.amazonaws.com in our case) we can directly access ClickHouse with:
```bash
clickhouse-client -h abc-123.us-east-1.elb.amazonaws.com
```
```text
ClickHouse client version 18.14.12.
Connecting to abc-123.us-east-1.elb.amazonaws.com:9000.
Connected to ClickHouse server version 19.4.3 revision 54416.
``` 
1. In case there is not **EXTERNAL-IP** available, we can access ClickHouse from inside Kubernetes cluster
```bash
kubectl -n test exec -it chi-b3d29f-a242-0-0-0 -- clickhouse-client
```
```text
ClickHouse client version 19.4.3.11.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.4.3 revision 54416.
```

## Simple Persistent Volume Example
In case of having Dynamic Volume Provisioning available - ex.: running on AWS - we are able to use PersistentVolumeClaims
Manifest is [available in examples](./examples/02-standard-layout-01-1shard-1repl-simple-persistent-volume.yaml)

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "standard-01-simple-pv"
spec:
  configuration:
    clusters:
      - name: "standard-01-simple-pv"
        layout:
          type: Standard
          shardsCount: 1
          replicasCount: 1
  defaults:
    deployment:
      volumeClaimTemplate: volumeclaim-template
  templates:
    volumeClaimTemplates:
      - name: volumeclaim-template
        persistentVolumeClaim:
          spec:
            accessModes:
              - ReadWriteOnce
            resources:
              requests:
                storage: 500Mi
```

## Custom Deployment with Pod and VolumeClaim Templates

Let's install more complex example with:
1. Deployment specified
1. Pod template
1. VolumeClaim template

Manifest is [available in examples](./examples/02-standard-layout-03-1shard-1repl-deployment-persistent-volume.yaml)

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "standard-02-deployment-pv"
spec:
  configuration:
    clusters:
      - name: "standard-02-deployment-pv"
        deployment:
          podTemplate: clickhouse-with-volume-template
          volumeClaimTemplate: clickhouse-storage-template
        layout:
          type: Standard
          shardsCount: 1
          replicasCount: 1
          
  templates:
    podTemplates:
      - name: clickhouse-with-volume-template
        containers:
          - name: clickhouse-pod
            image: yandex/clickhouse-server:19.3.7
            ports:
              - name: http
                containerPort: 8123
              - name: client
                containerPort: 9000
              - name: interserver
                containerPort: 9009
            volumeMounts:
              - name: clickhouse-storage
                mountPath: /var/lib/clickhouse
                
    volumeClaimTemplates:
      - name: clickhouse-storage-template
        persistentVolumeClaim:
          metadata:
            name: clickhouse-storage
          spec:
            accessModes:
              - ReadWriteOnce
            resources:
              requests:
                storage: 1Gi
```

## Custom Deployment with Specific ClickHouse Configuration
More extended settings example is [available here](./examples/03-settings-01.yaml)
```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "test-custom-settings"
spec:
  configuration:
    users:
      readonly/profile: readonly
      test/profile: default
      test/quota: default
    profiles:
      default/max_memory_usage: "1000000000"
      readonly/readonly: "1"
    quotas:
      default/interval/duration: "3600"
    settings:
      compression/case/method: zstd
    clusters:
      - name: "simple"
        layout:
          type: Standard
          shardsCount: 1
          replicasCount: 1
```

[build_from_sources]: ./operator_build_from_sources.md
