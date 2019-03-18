# Quick Start Guides

## Table of Contents
* [Installing ClickHouse Operator](#installing-clickhouse-operator)
* [Simple deployment with default storage](#simple-deployment-with-default-storage)
* [Custom deployment with Pod and VolumeClaim templates](#custom-deployment-with-pod-and-volumeclaim-templates)
* [Custom deployment using specific ClickHouse configuration](#custom-deployment-using-specific-clickhouse-configuration)

## Installing ClickHouse Operator

```console
$ kubectl apply -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/manifests/operator/clickhouse-operator-install.yaml
serviceaccount/clickhouse-operator created
clusterrolebinding.rbac.authorization.k8s.io/clickhouse-operator created
deployment.apps/clickhouse-operator created

```

```console
$ kubectl get pods -n kube-system
NAME                                        READY   STATUS    RESTARTS   AGE
clickhouse-operator-ddc6fd499-fhxqs         1/1     Running   0          5m22s
```

### Creating a custom namespace
```console
$ kubectl create ns test
namespace/test created
```


## Simple deployment with default storage

### Creating a custom resource object
```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "test2-no-replication"
spec:
  defaults:
    deployment:
      volumeClaimTemplate: default
  configuration:
    clusters:
      - name: "sharded-non-replicated"
        layout:
          type: Standard
          shardsCount: 3
```
```console
$ kubectl apply -n test -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/docs/examples/chi-example-02-default-pv-no-replication.yaml
clickhouseinstallation.clickhouse.altinity.com/test created
```

Once cluster is created, there are two checks to be made.

### Check that pods are running
```console
$ kubectl get pods -n test
NAME             READY   STATUS    RESTARTS   AGE
ch-d3ce483i1-0   1/1     Running   0          2m44s
ch-d3ce483i2-0   1/1     Running   0          2m44s
ch-d3ce483i3-0   1/1     Running   0          2m44s
```

### Listing exposed services
```console
$ kubectl get svc -n test
NAME                  TYPE          CLUSTER-IP   EXTERNAL-IP    PORT(S)                      AGE
d3ce483i1s            ClusterIP     None         <none>         9000/TCP,9009/TCP,8123/TCP   2m32s
d3ce483i2s            ClusterIP     None         <none>         9000/TCP,9009/TCP,8123/TCP   2m32s
test2-no-replication  LoadBalancer  None         10.101.227.182 9000/TCP,9009/TCP,8123/TCP   2m32s
```
ClickHouse is up and running!

### Connecting to the clickhouse database using clickhouse-client

The easiest way to connect to a cluster is to enter a pod and run clickhouse client.
```console
$ kubectl exec -it -n test ch-d3ce483i1-0 clickhouse-client
ClickHouse client version 19.1.6.
Connecting to localhost:9000.
Connected to ClickHouse server version 19.1.6 revision 54413.

ch-d3ce483i1-0.d3ce483i1s.test.svc.cluster.local :) 
```
You can also access the cluster using service names or external IP.

## Custom deployment with Pod and VolumeClaim templates
### Creating a custom resource object
```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "test5"
spec:
  defaults:
    deployment:
      podTemplate: customPodTemplate
      volumeClaimTemplate: customVolumeClaimTemplate
  templates:
    volumeClaimTemplates:
    - name: customVolumeClaimTemplate
      template:
        metadata:
          name: clickhouse-data-test
        spec:
          accessModes:
          - ReadWriteOnce
          resources:
            requests:
              storage: 500Mi
    podTemplates:
    - name: customPodTemplate
      containers:
      - name: clickhouse
        volumeMounts:
        - name: clickhouse-data-test
          mountPath: /var/lib/clickhouse
        image: yandex/clickhouse-server:18.16.1
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "512Mi"
            cpu: "500m"
  configuration:
    clusters:
      - name: "sharded-replicated"
        layout:
          type: Standard
          shardsCount: 1
          replicasCount: 2
      - name: "sharded-non-replicated"
        layout:
          type: Standard
          shardsCount: 2
      - name: "replicated"
        layout:
          type: Standard
          replicasCount: 2
```
```console
$ kubectl apply -n test -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/docs/examples/chi-example-05-custom-templates.yaml
clickhouseinstallation.clickhouse.altinity.com/test5 created
```

## Custom deployment using specific ClickHouse configuration
### Creating a custom resource object
```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "test4"
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
      - name: "sharded-replicated"
        layout:
          type: Standard
          shardsCount: 1
          replicasCount: 2
      - name: "sharded-non-replicated"
        layout:
          type: Standard
          shardsCount: 2
      - name: "replicated"
        layout:
          type: Standard
          replicasCount: 2
```
```console
$ kubectl apply -n test -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/docs/examples/chi-example-04-no-pv-custom-configuration.yaml
clickhouseinstallation.clickhouse.altinity.com/test4 created
```
