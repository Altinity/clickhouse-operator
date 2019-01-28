# Quick start with ClickHouse Operator for Kubernetes

## Deploying on Minikube or AWS with dynamic storage provisioning enabled

1. Installing the operator
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

2. Creating a custom namespace
```console
$ kubectl create ns test
namespace/test created
```

3. Creating a Custom Resource object
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
4. Listing all StatefulSets within "test" namespace
```console
$ kubectl get sts -n test
NAME                            DESIRED   CURRENT   AGE
chi-test3-20c3b0712b0937ac-i1   1         1         9s
chi-test3-20c3b0712b0937ac-i2   1         1         9s
chi-test3-20c3b0712b0937ac-i3   1         1         9s
```
5. Listing all PVC within "test" namespace
```console
$ kubectl get pvc -n test
clickhouse-data-chi-test3-20c3b0712b0937ac-i1-0   Bound    pvc-e0458b42-22e7-11e9-bfcc-08002744ab58   500Mi      RWO            standard       21s
clickhouse-data-chi-test3-20c3b0712b0937ac-i2-0   Bound    pvc-e05b8236-22e7-11e9-bfcc-08002744ab58   500Mi      RWO            standard       21s
clickhouse-data-chi-test3-20c3b0712b0937ac-i3-0   Bound    pvc-e07ebb20-22e7-11e9-bfcc-08002744ab58   500Mi      RWO            standard       20s
```
6. Listing ConfigMaps within "test" namespace
```console
$ kubectl get cm -n test
NAME                DATA   AGE
chi-test3-configd   1      11m
```
7. Listing all Services within "test" namespace
```console
$ kubectl get svc -n test
NAME                                    TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)                      AGE
chi-test3-20c3b0712b0937ac-i1-service   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   12m
chi-test3-20c3b0712b0937ac-i2-service   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   12m
chi-test3-20c3b0712b0937ac-i3-service   ClusterIP   None         <none>        9000/TCP,9009/TCP,8123/TCP   12m
```
8. Listing all Pods within "test" namespace
```console
$ kubectl get pods -n test
NAME                              READY   STATUS    RESTARTS   AGE
chi-test3-20c3b0712b0937ac-i1-0   1/1     Running   0          15m
chi-test3-20c3b0712b0937ac-i2-0   1/1     Running   0          15m
chi-test3-20c3b0712b0937ac-i3-0   1/1     Running   0          15m
```
9. Connecting to the clickhouse database using clickhouse-client
```console
$ kubectl exec -it -n test chi-test3-20c3b0712b0937ac-i1-0 clickhouse-client
ClickHouse client version 19.1.6.
Connecting to localhost:9000.
Connected to ClickHouse server version 19.1.6 revision 54413.

chi-test3-20c3b0712b0937ac-i1-0.chi-test3-20c3b0712b0937ac-i1-service.test.svc.cluster.local :) select * from system.clusters;

SELECT *
FROM system.clusters 

┌─cluster───────────────────────────┬─shard_num─┬─shard_weight─┬─replica_num─┬─host_name────────────────────────────────────────────────────────────────────────────────────┬─host_address─┬─port─┬─is_local─┬─user────┬─default_database─┐
│ sharded-non-replicated            │         1 │            1 │           1 │ chi-test3-20c3b0712b0937ac-i1-0.chi-test3-20c3b0712b0937ac-i1-service.test.svc.cluster.local │ 10.1.0.39    │ 9000 │        1 │ default │                  │
│ sharded-non-replicated            │         2 │            1 │           1 │ chi-test3-20c3b0712b0937ac-i2-0.chi-test3-20c3b0712b0937ac-i2-service.test.svc.cluster.local │ 10.1.0.41    │ 9000 │        0 │ default │                  │
│ sharded-non-replicated            │         3 │            1 │           1 │ chi-test3-20c3b0712b0937ac-i3-0.chi-test3-20c3b0712b0937ac-i3-service.test.svc.cluster.local │ 10.1.0.40    │ 9000 │        0 │ default │                  │
│ test_cluster_two_shards_localhost │         1 │            1 │           1 │ localhost                                                                                    │ 127.0.0.1    │ 9000 │        1 │ default │                  │
│ test_cluster_two_shards_localhost │         2 │            1 │           1 │ localhost                                                                                    │ 127.0.0.1    │ 9000 │        1 │ default │                  │
│ test_shard_localhost              │         1 │            1 │           1 │ localhost                                                                                    │ 127.0.0.1    │ 9000 │        1 │ default │                  │
│ test_shard_localhost_secure       │         1 │            1 │           1 │ localhost                                                                                    │ 127.0.0.1    │ 9440 │        0 │ default │                  │
│ test_unavailable_shard            │         1 │            1 │           1 │ localhost                                                                                    │ 127.0.0.1    │ 9000 │        1 │ default │                  │
│ test_unavailable_shard            │         2 │            1 │           1 │ localhost                                                                                    │ 127.0.0.1    │    1 │        0 │ default │                  │
└───────────────────────────────────┴───────────┴──────────────┴─────────────┴──────────────────────────────────────────────────────────────────────────────────────────────┴──────────────┴──────┴──────────┴─────────┴──────────────────┘

9 rows in set. Elapsed: 0.019 sec. 

```


