# Setting up Zookeeper

This document describes how to setup ZooKeeper in k8s environment.

Zookeeper installation is available in two options:
1. [Quick start](#quick-start) - just run it quickly and ask no questions
1. [Advanced setup](#advanced-setup) - setup internal details, such as storage class, replicas number, etc 

During ZooKeeper installation the following items are created/configured:
1. [OPTIONAL] Create separate namespace to run Zookeeper in 
1. Create k8s resources (optionally, within namespace):
  * [Service][k8sdoc_service_main] - used to provide central access point to Zookeeper
  * [Headless Service][k8sdoc_service_headless] - used to provide DNS namings
  * [Disruption Balance][k8sdoc_disruption_balance] - used to specify max number of offline pods
  * [OPTIONAL] [Storage Class][k8sdoc_storage_class] - used to specify storage class to be used by Zookeeper for data storage
  * [Stateful Set][k8sdoc_statefulset] - used to manage and scale sets of pods

## Quick start
Quick start is represented in two flavors:
1. With persistent volume - good for AWS. File are located in [deploy/zookeeper/quick-start-persistent-volume][quickstart_persistent] 
1. With local [`emptyDir`][k8sdoc_emptydir] storage - good for standalone local run, however has to true persistence. \
Files are located in [deploy/zookeeper/quick-start-volume-emptyDir][quickstart_emptydir] 

Each quick start flavor provides the following installation options:
1. 1-node Zookeeper cluster (**zookeeper-1-** files). No failover provided.
1. 3-node Zookeeper cluster (**zookeeper-3-** files). Failover provided.

In case you'd like to test with AWS or any other cloud provider, we recommend to go with [deploy/zookeeper/quick-start-persistent-volume][quickstart_persistent] persistent storage.
In case of local test, you'd may prefer to go with [deploy/zookeeper/quick-start-volume-emptyDir][quickstart_emptydir] `emptyDir`.

### Script-based Installation 
In this example we'll go with simple 1-node Zookeeper cluster on AWS and pick [deploy/zookeeper/quick-start-persistent-volume][quickstart_persistent].
Both [create][zookeeper-1-node-create.sh] and [delete][zookeeper-1-node-delete.sh]
shell scripts are available for simplification.  

### Manual Installation
In case you'd like to deploy Zookeeper manually, the following steps should be performed:

### Namespace
Create **namespace**
```bash
kubectl create namespace zoo1ns
```
 
### Zookeeper
Deploy Zookeeper into this namespace
```bash
kubectl apply -f zookeeper-1-node.yaml -n zoo1ns
```

Now Zookeeper should be up and running. Let's [explore Zookeeper cluster](#explore-zookeeper-cluster).

**IMPORTANT** quick-start zookeeper installation are for test purposes mainly.  
For fine-tuned Zookeeper setup please refer to [advanced setup](#advanced-setup) options.  

## Advanced setup
Advanced files are are located in [deploy/zookeeper/advanced][zookeeper-advanced] folder. 
All resources are separated into different files so it is easy to modify them and setup required options.  

Advanced setup is available in two options:
1. With [persistent volume][k8sdoc_persistent_volume]
1. With [emptyDir volume][k8sdoc_emptydir]

Each of these options have both `create` and `delete` scripts provided
1. Persistent volume  [create][zookeeper-persistent-volume-create.sh] and [delete][zookeeper-persistent-volume-delete.sh] scripts
1. EmptyDir volume  [create][zookeeper-volume-emptyDir-create.sh] and [delete][zookeeper-volume-emptyDir-delete.sh] scripts

Step-by-step explanations:

### Namespace
Create **namespace** in which all the rest resources would be created
```bash
kubectl create namespace zoons
```
 
### Zookeeper Service
Create service. This service provides DNS name for client access to all Zookeeper nodes.
```bash
kubectl apply -f 01-service-client-access.yaml -n zoons
```
Should have as a result
```text
service/zookeeper created
```

### Zookeeper Headless Service
Create headless service. This service provides DNS names for all Zookeeper nodes
```bash
kubectl apply -f 02-headless-service.yaml -n zoons
```
Should have as a result
```text
service/zookeeper-nodes created
```

### Disruption Budget
Create budget. Disruption Budget instructs k8s on how many offline Zookeeper nodes can be at any time
```bash
kubectl apply -f 03-pod-disruption-budget.yaml -n zoons
``` 
Should have as a result
```text
poddisruptionbudget.policy/zookeeper-pod-distribution-budget created
```

### Storage Class
This part is not that straightforward and may require communication with k8s instance administrator.

First of all, we need to decide, whether Zookeeper would use [Persistent Volume][k8sdoc_persistent_volume] 
as a storage or just stick to more simple [Volume][k8sdoc_volume] (In doc [emptyDir][k8sdoc_emptydir] type is used)

In case we'd prefer to stick with simpler solution and go with [Volume of type emptyDir][k8sdoc_emptydir], 
we need to go with **emptyDir StatefulSet config** [05-stateful-set-volume-emptyDir.yaml][05-stateful-set-volume-emptyDir.yaml] 
as described in next [Stateful Set unit](#stateful-set). Just move to [it](#stateful-set).

In case we'd prefer to go with [Persistent Volume][k8sdoc_persistent_volume] storage, we need to go 
with **Persistent Volume StatefulSet config** [05-stateful-set-persistent-volume.yaml][05-stateful-set-persistent-volume.yaml]

Shortly, [Storage Class][k8sdoc_storage_class] is used to bind together [Persistent Volumes][k8sdoc_persistent_volume],
which are created either by k8s admin manually or automatically by [Provisioner][k8sdocs_dynamic_provisioning]. In any case, Persistent Volumes are provided externally to an application to be deployed into k8s. 
So, this application has to know **Storage Class Name** to ask for from the k8s in application's claim for new persistent volume - [Persistent Volume Claim][k8sdoc_persistent_volume_claim].
This **Storage Class Name** should be asked from k8s admin and written as application's **Persistent Volume Claim** `.spec.volumeClaimTemplates.storageClassName` parameter in `StatefulSet` configuration. **StatefulSet manifest with emptyDir** [05-stateful-set-volume-emptyDir.yaml](../deploy/zookeeper/zookeeper-manually/advanced/05-stateful-set-volume-emptyDir.yaml) and/or **StatefulSet manifest with Persistent Volume** [05-stateful-set-persistent-volume.yaml](../deploy/zookeeper/zookeeper-manually/advanced/05-stateful-set-persistent-volume.yaml). 

### Stateful Set
Edit **StatefulSet manifest with emptyDir** [05-stateful-set-volume-emptyDir.yaml][05-stateful-set-volume-emptyDir.yaml] 
and/or **StatefulSet manifest with Persistent Volume** [05-stateful-set-persistent-volume.yaml][05-stateful-set-persistent-volume.yaml] 
according to your Storage Preferences.

In case we'd go with [Volume of type emptyDir][k8sdoc_emptydir], ensure `.spec.template.spec.containers.volumes` is in place 
and look like the following:
```yaml
      volumes:
      - name: datadir-volume
        emptyDir:
          medium: "" #accepted values:  empty str (means node's default medium) or Memory
          sizeLimit: 1Gi
```
and ensure `.spec.volumeClaimTemplates` is commented.

In case we'd go with **Persistent Volume** storage, ensure `.spec.template.spec.containers.volumes` is commented 
and ensure `.spec.volumeClaimTemplates` is uncommented.
```yaml
  volumeClaimTemplates:
  - metadata:
      name: datadir-volume
    spec:
      accessModes:
        - ReadWriteOnce
      resources:
        requests:
          storage: 1Gi
## storageClassName has to be coordinated with k8s admin and has to be created as a `kind: StorageClass` resource
      storageClassName: storageclass-zookeeper
```
and ensure **storageClassName** (`storageclass-zookeeper` in this example) is specified correctly, as described 
in [Storage Class](#storage-class) section

As `.yaml` file is ready, just apply it with `kubectl`
```bash
kubectl apply -f 05-stateful-set.yaml -n zoons
```
Should have as a result
```text
statefulset.apps/zookeeper-node created
```

Now we can take a look into Zookeeper cluster deployed in k8s:

## Explore Zookeeper cluster

### DNS names
We are expecting to have ZooKeeper cluster of 3 pods inside `zoons` namespace, named as:
```text
zookeeper-0
zookeeper-1
zookeeper-2
```
Those pods are expected to have short DNS names as:

```text
zookeeper-0.zookeepers.zoons
zookeeper-1.zookeepers.zoons
zookeeper-2.zookeepers.zoons
```

where `zookeepers` is name of [Zookeeper headless service](#zookeeper-headless-service) and `zoons` is name of [Zookeeper namespace](#namespace).

and full DNS names (FQDN) as:
```text
zookeeper-0.zookeepers.zoons.svc.cluster.local
zookeeper-1.zookeepers.zoons.svc.cluster.local
zookeeper-2.zookeepers.zoons.svc.cluster.local
```

### Resources

List pods in Zookeeper's namespace
```bash
kubectl get pod -n zoons
```

Expected output is like the following
```text
NAME             READY   STATUS    RESTARTS   AGE
zookeeper-0      1/1     Running   0          9m2s
zookeeper-1      1/1     Running   0          9m2s
zookeeper-2      1/1     Running   0          9m2s
```

List services
```bash
kubectl get service -n zoons
```

Expected output is like the following
```text
NAME                   TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)                      AGE
zookeeper              ClusterIP   10.108.36.44   <none>        2181/TCP                     168m
zookeepers             ClusterIP   None           <none>        2888/TCP,3888/TCP            31m
```

List statefulsets
```bash
kubectl get statefulset -n zoons
```

Expected output is like the following
```text
NAME            READY   AGE
zookeepers      3/3     10m
```

In case all looks fine Zookeeper cluster is up and running



[k8sdoc_service_main]: https://kubernetes.io/docs/concepts/services-networking/service/
[k8sdoc_service_headless]: https://kubernetes.io/docs/concepts/services-networking/service/#headless-services
[k8sdoc_disruption_balance]: https://kubernetes.io/docs/concepts/workloads/pods/disruptions/
[k8sdoc_storage_class]: https://kubernetes.io/docs/concepts/storage/storage-classes/
[k8sdoc_statefulset]: https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/
[k8sdoc_volume]: https://kubernetes.io/docs/concepts/storage/volumes
[k8sdoc_emptydir]: https://kubernetes.io/docs/concepts/storage/volumes/#emptydir
[k8sdoc_persistent_volume]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/
[k8sdoc_persistent_volume_claim]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims
[k8sdocs_dynamic_provisioning]: https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/

[quickstart_persistent]: ../deploy/zookeeper/zookeeper-manually/quick-start-persistent-volume
[quickstart_emptydir]: ../deploy/zookeeper/zookeeper-manually/quick-start-volume-emptyDir

[zookeeper-1-node-create.sh]: ../deploy/zookeeper/zookeeper-manually/quick-start-persistent-volume/zookeeper-1-node-create.sh
[zookeeper-1-node-delete.sh]: ../deploy/zookeeper/zookeeper-manually/quick-start-persistent-volume/zookeeper-1-node-delete.sh
[zookeeper-advanced]: ../deploy/zookeeper/zookeeper-manually/advanced
[zookeeper-persistent-volume-create.sh]: ../deploy/zookeeper/zookeeper-manually/advanced/zookeeper-persistent-volume-create.sh
[zookeeper-persistent-volume-delete.sh]: ../deploy/zookeeper/zookeeper-manually/advanced/zookeeper-persistent-volume-delete.sh
[zookeeper-volume-emptyDir-create.sh]: ../deploy/zookeeper/zookeeper-manually/advanced/zookeeper-volume-emptyDir-create.sh
[zookeeper-volume-emptyDir-delete.sh]: ../deploy/zookeeper/zookeeper-manually/advanced/zookeeper-volume-emptyDir-delete.sh
[05-stateful-set-volume-emptyDir.yaml]: ../deploy/zookeeper/zookeeper-manually/advanced/05-stateful-set-volume-emptyDir.yaml
[05-stateful-set-persistent-volume.yaml]: ../deploy/zookeeper/zookeeper-manually/advanced/05-stateful-set-persistent-volume.yaml
