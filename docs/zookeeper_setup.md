# Task: Setup Zookeeper

We are going to setup Zookeeper in k8s environment.
This document assumes k8s cluster already setup and `kubectl` has access to it.

What we'll need here:
1. Create k8s components:
  * [Service](https://kubernetes.io/docs/concepts/services-networking/service/) - used to provide central access point to Zookeeper
  * [Headless Service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services) - used to provide DNS namings
  * [Disruption Balance](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/) - used to specify max number of offline pods
  * [Storage Class](https://kubernetes.io/docs/concepts/workloads/pods/disruptions/) - used to specify storage class to be used by Zookeeper for data storage
  * [Stateful Set](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) - used to manage and scale sets of pods    
  
 All files are located in [manifests/zookeeper](../manifests/zookeeper) folder
 
 ## Zookeeper Service
 This service provides DNS name for client access to all Zookeeper nodes.
 Create service.
 ```bash
 kubectl apply -f 01-service-client-access.yaml
 ```
 Should have as a result
 ```text
service/zookeeper created
```

## Zookeeper Headless Service
This service provides DNS names for all Zookeeper nodes
Create service.
```bash
kubectl apply -f 02-headless-service.yaml
```
Should have as a result
```text
service/zookeeper-nodes created
```

## Disruption Budget
Disruption Budget instructs k8s on how many offline Zookeeper nodes can be at any time
Create budget.
```bash
kubectl apply -f 03-pod-disruption-budget.yaml
``` 
Should have as a result
```text
poddisruptionbudget.policy/zookeeper-pod-distribution-budget created
```

## Storage Class
This part is not that straightforward and may require communication with k8s instance administrator.

First of all, we need to deside, whether Zookeeper would use [Persistent Volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) as a storage or just stick to more simple [Volume](https://kubernetes.io/docs/concepts/storage/volumes) (In doc [emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir) type is used)

In case we'd prefer to stick with simpler solution and go with [Volume of type emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir), we are done here and need to adjust [StatefulSet config](../manifests/zookeeper/05-stateful-set.yaml) as described in next [Stateful Set unit](#stateful-set). Just move to [it](#stateful-set).

In case we'd prefer to go with [Persistent Volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) storage, some additional steps have to be done.

Shortly, [Storage Class](https://kubernetes.io/docs/concepts/storage/storage-classes/) is used to bind together [Persistent Volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/),
which are created either by k8s admin manually or automatically by [Provisioner](https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/). In any case, Persistent Volumes are provided externally to an application to be deployed into k8s. So, this application has to know **Storage Class Name** to ask for from the k8s in application's claim for new persistent volume[Persistent Volume Claim](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims).
This **Storage Class Name** should be asked from k8s admin and written as application's **Persistent Volume Claim** parameter. 

## Stateful Set
Edit [05-stateful-set.yaml](../manifests/zookeeper/05-stateful-set.yaml) according to your Storage Preferences.

In case we'd go with [Volume of type emptyDir](https://kubernetes.io/docs/concepts/storage/volumes/#emptydir), ensure `.spec.template.spec.containers.volumes` is in place and looke like the following:
```yaml
      volumes:
      - name: datadir-volume
        emptyDir:
          medium: "" #accepted values:  empty str (means node's default medium) or Memory
          sizeLimit: 1Gi
```
and ensure `.spec.volumeClaimTemplates` is commented.

In case we'd go with **Persistent Volume** storage, ensure `.spec.template.spec.containers.volumes` is commented and ensure `.spec.volumeClaimTemplates` is uncommented.
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
      storageClassName: storageclass-zookeeper
```
and ensure **storgaeClassName** (`storageclass-zookeeper` in this example) is specified correctly, as described in [Storgae Class](#storage-class) section

As `.yaml` file is ready, just apply it with `kubectl`
```bash
kubectl apply -f 05-stateful-set.yaml
```
Should have as a result
```text
statefulset.apps/zookeeper-node created
```

```bash
kubectl get pod
```

```text
NAME                  READY   STATUS    RESTARTS   AGE
zookeeper-node-0      1/1     Running   0          9m2s
zookeeper-node-1      1/1     Running   0          9m2s
zookeeper-node-2      1/1     Running   0          9m2s
```

```bash
kubectl get service
```

```text
NAME                        TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)                      AGE
zookeeper                   ClusterIP   10.108.36.44   <none>        2181/TCP                     168m
zookeeper-nodes             ClusterIP   None           <none>        2888/TCP,3888/TCP            31m
```

```bash
kubectl get statefulset
```

```text
NAME                READY   AGE
zookeeper-node      3/3     10m
```
