# Update ClickHouse Installation - add replication to existing non-replicated cluster 

## Prerequisites
  1. Assume we have `clickhouse-operator` already installed and running
  1. Assume we have `Zookeeper` already installed and running
  
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
Now let's install ClickHouse from provided examples. 
There are two **rolling update** examples presented:
1. Simple stateless cluster: [initial position][stateless_initial_position] and [update][stateless_updated_position]
1. Stateful cluster with Persistent Volumes: [initial position][stateful_initial_position] and [update][stateful_updated_position] 

## Simple Rolling Update Example

Let's go with simple stateless cluster. Manifest file with [initial position][stateless_initial_position]:
```bash
kubectl -n dev apply -f 07-rolling-update-stateless-01-initial-position.yaml
```
Check initial position. We should have cluster up and running:
```bash
kubectl -n dev get all,configmap
```
ClickHouse is installed, up and running, all 4 expected pods are running
```text
NAME                                      READY   STATUS    RESTARTS   AGE
pod/chi-d02eaa-347e-0-0-0                 1/1     Running   0          19s
pod/chi-d02eaa-347e-1-0-0                 1/1     Running   0          19s
pod/chi-d02eaa-347e-2-0-0                 1/1     Running   0          19s
pod/chi-d02eaa-347e-3-0-0                 1/1     Running   0          19s
NAME                                  TYPE           CLUSTER-IP      EXTERNAL-IP   PORT(S)                         AGE
service/chi-d02eaa-347e-0-0           ClusterIP      None            <none>        8123/TCP,9000/TCP,9009/TCP      19s
service/chi-d02eaa-347e-1-0           ClusterIP      None            <none>        8123/TCP,9000/TCP,9009/TCP      19s
service/chi-d02eaa-347e-2-0           ClusterIP      None            <none>        8123/TCP,9000/TCP,9009/TCP      19s
service/chi-d02eaa-347e-3-0           ClusterIP      None            <none>        8123/TCP,9000/TCP,9009/TCP      19s
service/update-setup-replication      LoadBalancer   10.106.139.11   <pending>     8123:31328/TCP,9000:32091/TCP   19s
NAME                                   READY   AGE
statefulset.apps/chi-d02eaa-347e-0-0   1/1     19s
statefulset.apps/chi-d02eaa-347e-1-0   1/1     19s
statefulset.apps/chi-d02eaa-347e-2-0   1/1     19s
statefulset.apps/chi-d02eaa-347e-3-0   1/1     19s
NAME                                         DATA   AGE
configmap/chi-d02eaa-common-configd          2      19s
configmap/chi-d02eaa-common-usersd           0      19s
configmap/chi-d02eaa-deploy-confd-347e-0-0   1      19s
configmap/chi-d02eaa-deploy-confd-347e-1-0   1      19s
configmap/chi-d02eaa-deploy-confd-347e-2-0   1      19s
configmap/chi-d02eaa-deploy-confd-347e-3-0   1      19s
```
Let's explore one Pod in order to check available ClickHouse config files.
Navigate directly inside the Pod:
```bash
kubectl -n dev exec -it chi-d02eaa-347e-0-0-0 -- bash
```
And take a look on ClickHouse config files available
```text
root@chi-d02eaa-347e-0-0-0:/etc/clickhouse-server# ls /etc/clickhouse-server/conf.d/
macros.xml
root@chi-d02eaa-347e-0-0-0:/etc/clickhouse-server# ls /etc/clickhouse-server/config.d
listen.xml  remote_servers.xml
```
Standard minimal set of config files is in place.
All is well.

## Update ClickHouse Installation - add replication
Let's run update and change `.yaml` manifest so we'll have replication available. 

In order to have replication correctly setup, we need to specify `Zookeeper` (which is assumed to be running already) and specify replicas for ClickHouse.
Manifest file with [updates specified][stateless_updated_position]:
```bash
kubectl -n dev apply -f 07-rolling-update-stateless-02-apply-update.yaml
```
And let's watch on how update is rolling over:
```text
NAME                                      READY   STATUS              RESTARTS   AGE
pod/chi-d02eaa-347e-0-0-0                 0/1     ContainerCreating   0          1s
pod/chi-d02eaa-347e-1-0-0                 1/1     Running             0          4m41s
pod/chi-d02eaa-347e-2-0-0                 1/1     Running             0          4m41s
pod/chi-d02eaa-347e-3-0-0                 1/1     Running             0          4m41s
```
As we can see, the first Pod is being updated - `pod/chi-d02eaa-347e-0-0-0`. 
Then we can see how the first replica is being created - `pod/chi-d02eaa-347e-0-1-0`, as well as 
next Pod is being updated - `pod/chi-d02eaa-347e-1-0-0` 
```text
NAME                                      READY   STATUS              RESTARTS   AGE
pod/chi-d02eaa-347e-0-0-0                 1/1     Running             0          21s
pod/chi-d02eaa-347e-0-1-0                 0/1     ContainerCreating   0          2s
pod/chi-d02eaa-347e-1-0-0                 0/1     ContainerCreating   0          2s
pod/chi-d02eaa-347e-2-0-0                 1/1     Running             0          5m1s
pod/chi-d02eaa-347e-3-0-0                 1/1     Running             0          5m1s
```
Update is rolling further.
```text
NAME                                      READY   STATUS              RESTARTS   AGE
pod/chi-d02eaa-347e-0-0-0                 1/1     Running             0          62s
pod/chi-d02eaa-347e-0-1-0                 1/1     Running             0          43s
pod/chi-d02eaa-347e-1-0-0                 1/1     Running             0          43s
pod/chi-d02eaa-347e-1-1-0                 1/1     Running             0          20s
pod/chi-d02eaa-347e-2-0-0                 1/1     Running             0          20s
pod/chi-d02eaa-347e-2-1-0                 0/1     ContainerCreating   0          1s
pod/chi-d02eaa-347e-3-0-0                 0/1     ContainerCreating   0          1s
```
And, after all Pod updated, let's take a look on ClickHouse config files available:
```text
root@chi-d02eaa-347e-0-0-0:/# ls /etc/clickhouse-server/conf.d/
macros.xml
root@chi-d02eaa-347e-0-0-0:/# ls /etc/clickhouse-server/config.d
listen.xml  remote_servers.xml  zookeeper.xml
```
We can see new file added: `zookeeper.xml`. It is Zookeeper configuration for ClickHouse. Let's take a look into:
```text
root@chi-d02eaa-347e-0-0-0:/# cat /etc/clickhouse-server/config.d/zookeeper.xml 
<yandex>
    <zookeeper>
        <node>
            <host>zookeeper.zoo1ns</host>
            <port>2181</port>
        </node>
    </zookeeper>
    <distributed_ddl>
        <path>/clickhouse/update-setup-replication/task_queue/ddl</path>
    </distributed_ddl>
</yandex>
```

## Rolling Update with State Example
Stateful cluster with Persistent Volumes examples are presented as [initial position][stateful_initial_position] and [update][stateful_updated_position]
The structure of the example is the same as for simple example, but Persistent Volumes are used. So this example is better to be run on cloud provider with Dynamic Volumes Provisioning available.

[stateless_initial_position]: ./chi-examples/07-rolling-update-stateless-01-initial-position.yaml
[stateless_updated_position]: ./chi-examples/07-rolling-update-stateless-02-apply-update.yaml

[stateful_initial_position]: ./chi-examples/09-rolling-update-emptydir-01-initial-position.yaml
[stateful_updated_position]: ./chi-examples/09-rolling-update-emptydir-02-apply-update.yaml
