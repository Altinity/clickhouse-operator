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
  name: "test3"
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
  templates:
    volumeClaimTemplates:
    - name: default
      template:
        metadata:
          name: USE_DEFAULT_NAME
        spec:
          accessModes:
          - ReadWriteOnce
          resources:
            requests:
              storage: 500Mi
```
```console
$ kubectl apply -n test -f https://raw.githubusercontent.com/Altinity/clickhouse-operator/master/docs/examples/chi-example-02-default-pv-no-replication.yaml
clickhouseinstallation.clickhouse.altinity.com/test created
```
### Listing all StatefulSets within "test" namespace
```console
$ kubectl get sts -n test
NAME           DESIRED   CURRENT   AGE
ch-d3ce483i1   1         1         4s
ch-d3ce483i2   1         1         4s
ch-d3ce483i3   1         1         4s
```
### Listing all PVC within "test" namespace
```console
$ kubectl get pvc -n test
NAME                             STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
clickhouse-data-ch-d3ce483i1-0   Bound    pvc-ac34d202-2553-11e9-a0e6-08002744ab58   500Mi      RWO            standard       21s
clickhouse-data-ch-d3ce483i2-0   Bound    pvc-ac4b7a45-2553-11e9-a0e6-08002744ab58   500Mi      RWO            standard       21s
clickhouse-data-ch-d3ce483i3-0   Bound    pvc-ac655ce1-2553-11e9-a0e6-08002744ab58   500Mi      RWO            standard       21s
```
### Listing ConfigMaps within "test" namespace
```console
$ kubectl get cm -n test
NAME                          DATA   AGE
chi-test3-configd             1      47s
chi-test3-configd-d3ce483i1   1      47s
chi-test3-configd-d3ce483i2   1      47s
chi-test3-configd-d3ce483i3   1      47s
```
### Listing all Services within "test" namespace
```console
$ kubectl get svc -n test
NAME         TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                      AGE
d3ce483i1s   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   2m32s
d3ce483i2s   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   2m32s
d3ce483i3s   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   2m32s
```
### Listing all Pods within "test" namespace
```console
$ kubectl get pods -n test
NAME             READY   STATUS    RESTARTS   AGE
ch-d3ce483i1-0   1/1     Running   0          2m44s
ch-d3ce483i2-0   1/1     Running   0          2m44s
ch-d3ce483i3-0   1/1     Running   0          2m44s
```
### Connecting to the clickhouse database using clickhouse-client
```console
$ kubectl exec -it -n test ch-d3ce483i1-0 clickhouse-client
ClickHouse client version 19.1.6.
Connecting to localhost:9000.
Connected to ClickHouse server version 19.1.6 revision 54413.

ch-d3ce483i1-0.d3ce483i1s.test.svc.cluster.local :) select * from system.clusters;

SELECT *
FROM system.clusters 

┌─cluster───────────────────────────┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name─────────────────┬─host_address─┬─port─┬─is_local─┬─user────┬─default_database─┐
│ sharded-non-replicated            │         1 │            1 │           1 │ ch-d3ce483i1-0.d3ce483i1s │ 10.1.1.48    │ 9000 │        1 │ default │                  │
│ sharded-non-replicated            │         2 │            1 │           1 │ ch-d3ce483i2-0.d3ce483i2s │ 10.1.1.50    │ 9000 │        0 │ default │                  │
│ sharded-non-replicated            │         3 │            1 │           1 │ ch-d3ce483i3-0.d3ce483i3s │ 10.1.1.49    │ 9000 │        0 │ default │                  │
│ test_cluster_two_shards_localhost │         1 │            1 │           1 │ localhost                 │ 127.0.0.1    │ 9000 │        1 │ default │                  │
│ test_cluster_two_shards_localhost │         2 │            1 │           1 │ localhost                 │ 127.0.0.1    │ 9000 │        1 │ default │                  │
│ test_shard_localhost              │         1 │            1 │           1 │ localhost                 │ 127.0.0.1    │ 9000 │        1 │ default │                  │
│ test_shard_localhost_secure       │         1 │            1 │           1 │ localhost                 │ 127.0.0.1    │ 9440 │        0 │ default │                  │
│ test_unavailable_shard            │         1 │            1 │           1 │ localhost                 │ 127.0.0.1    │ 9000 │        1 │ default │                  │
│ test_unavailable_shard            │         2 │            1 │           1 │ localhost                 │ 127.0.0.1    │    1 │        0 │ default │                  │
└───────────────────────────────────┴───────────┴──────────────┴─────────────┴───────────────────────────┴──────────────┴──────┴──────────┴─────────┴──────────────────┘

9 rows in set. Elapsed: 0.014 sec. 

```

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
