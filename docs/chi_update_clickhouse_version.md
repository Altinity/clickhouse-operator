# Update ClickHouse Installation - update ClickHouse version 

## Prerequisites
  1. Assume we have `clickhouse-operator` already installed and running
  
## Install ClickHouseInstallation example
We are going to install everything into `dev` namespace. `clickhouse-operator` is already installed into `dev` namespace

Check pre-initial position.
```bash
kubectl -n dev get all,configmap
```
`clickhouse-operator`-only is available
```text
NAME                                      READY   STATUS    RESTARTS   AGE
pod/clickhouse-operator-5cbc47484-v5cfg   1/1     Running   0          17s
NAME                                  TYPE        CLUSTER-IP      EXTERNAL-IP   PORT(S)    AGE
service/clickhouse-operator-metrics   ClusterIP   10.102.229.22   <none>        8888/TCP   17s
NAME                                  READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/clickhouse-operator   1/1     1            1           17s
NAME                                            DESIRED   CURRENT   READY   AGE
replicaset.apps/clickhouse-operator-5cbc47484   1         1         1       17s
```
Now let's install ClickHouse from provided examples. Manifest file with initial position is [08-clickhouse-version-update-01-initial-position.yaml][08-clickhouse-version-update-01-initial-position.yaml]:
```bash
kubectl -n dev apply -f 08-clickhouse-version-update-01-initial-position.yaml
```
Check initial position. We should have cluster up and running:
```bash
kubectl -n dev get all,configmap
```
ClickHouse is installed, up and running, all 3 pods are running
```text
NAME                        READY   STATUS    RESTARTS   AGE
pod/chi-06791a-28a0-0-0-0   1/1     Running   0          9s
pod/chi-06791a-28a0-0-1-0   1/1     Running   0          9s
pod/chi-06791a-28a0-0-2-0   1/1     Running   0          9s
NAME                                  TYPE           CLUSTER-IP       EXTERNAL-IP   PORT(S)                                        AGE
service/chi-06791a-28a0-0-0           ClusterIP      None             <none>        8123/TCP,9000/TCP,9009/TCP                     9s
service/chi-06791a-28a0-0-1           ClusterIP      None             <none>        8123/TCP,9000/TCP,9009/TCP                     9s
service/chi-06791a-28a0-0-2           ClusterIP      None             <none>        8123/TCP,9000/TCP,9009/TCP                     9s
service/clickhouse-version-update     LoadBalancer   10.111.214.30    <pending>     8123:32444/TCP,9000:30048/TCP,9009:31089/TCP   9s
NAME                                   READY   AGE
statefulset.apps/chi-06791a-28a0-0-0   1/1     9s
statefulset.apps/chi-06791a-28a0-0-1   1/1     9s
statefulset.apps/chi-06791a-28a0-0-2   1/1     9s
NAME                                         DATA   AGE
configmap/chi-06791a-common-configd          2      9s
configmap/chi-06791a-common-usersd           0      9s
configmap/chi-06791a-deploy-confd-28a0-0-0   1      9s
configmap/chi-06791a-deploy-confd-28a0-0-1   1      9s
configmap/chi-06791a-deploy-confd-28a0-0-2   1      9s
```
We expect all Pods to run ClickHouse version `23.3.0` as specified in [initial manifest][initial-manifest].

Let's explore all Pods in order to check available ClickHouse version.
Navigate directly inside each Pod
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-0-0 -- clickhouse-client
```
```text
ClickHouse client version 23.3.0.
Connecting to localhost:9000.
Connected to ClickHouse server version 23.3.0 revision 54413.
```
Repeat for all Pods
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-1-0 -- clickhouse-client
```
```text
ClickHouse client version 23.3.0.
Connecting to localhost:9000.
Connected to ClickHouse server version 23.3.0 revision 54413.
```
And the last Pod
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-2-0 -- clickhouse-client
```
```text
ClickHouse client version 23.3.0.
Connecting to localhost:9000.
Connected to ClickHouse server version 23.3.0 revision 54413.
```
All is fine, all Pods are running `23.3.0`

We'll make Version Update in two steps:
 1. Update one ClickHouse instance (one Pod)
 1. Update all the rest ClickHouse instances (all the rest Pods)
 
## Update one ClickHouse instance

Now let's update version of only one instance of ClickHouse. Let it be the last one.
We can do by explicitly specifying `templates` with different ClickHouse version:
```yaml
                  templates:
                    podTemplate: clickhouse:23.8.0
```
Manifest file with one ClickHouse instance update is [08-clickhouse-version-update-02-apply-update-one.yaml][08-clickhouse-version-update-02-apply-update-one.yaml]:
```bash
kubectl -n dev apply -f 08-clickhouse-version-update-02-apply-update-one.yaml
``` 
And let's check what ClickHouse versions are running over the whole cluster. We expect the last instance to run specific version `23.8.0`. Check the first Pod:
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-0-0 -- clickhouse-client
```
```text
ClickHouse client version 23.3.0.
Connecting to localhost:9000.
Connected to ClickHouse server version 23.3.0 revision 54413.
```
The second Pod:
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-1-0 -- clickhouse-client
```
```text
ClickHouse client version 23.3.0.
Connecting to localhost:9000.
Connected to ClickHouse server version 23.3.0 revision 54413.
```
And the most interesting part - the last one:
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-2-0 -- clickhouse-client
```
```text
ClickHouse client version 23.8.0.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 23.8.0 revision 54415.
```
As we can see - it runs different, explicitly specified version `23.8.0`.
All seems to be good. Let's update the whole cluster now.

## Update the whole cluster

Let's run update - apply `.yaml`.
Manifest file with all ClickHouse instance updated is [08-clickhouse-version-update-03-apply-update-all.yaml][08-clickhouse-version-update-03-apply-update-all.yaml]:
```bash
kubectl -n dev apply -f 08-clickhouse-version-update-03-apply-update-all.yaml
```
And let's check the results - we expect all Pods to have ClickHouse `23.8.0` running. The first Pod
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-0-0 -- clickhouse-client
```
```text
ClickHouse client version 23.8.0.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 23.8.0 revision 54415.```
```
The second Pod
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-1-0 -- clickhouse-client
```
```text
ClickHouse client version 23.8.0.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 23.8.0 revision 54415.
```
And the last Pod
```bash
kubectl -n dev exec -it chi-06791a-28a0-0-2-0 -- clickhouse-client
```
```text
ClickHouse client version 23.8.0.
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 23.8.0 revision 54415.
```
All looks fine.

[08-clickhouse-version-update-01-initial-position.yaml]: ./chi-examples/08-clickhouse-version-update-01-initial-position.yaml
[08-clickhouse-version-update-02-apply-update-one.yaml]: ./chi-examples/08-clickhouse-version-update-02-apply-update-one.yaml
[08-clickhouse-version-update-03-apply-update-all.yaml]: ./chi-examples/08-clickhouse-version-update-03-apply-update-all.yaml
[initial-manifest]: ./chi-examples/08-clickhouse-version-update-01-initial-position.yaml
