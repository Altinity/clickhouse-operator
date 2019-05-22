# Storage

Examples are availabe in [examples](./examples) folder:
1. [Simple Persistent Volume](./examples/02-standard-layout-01-1shard-1repl-simple-persistent-volume.yaml)
1. [Template with Persistent Volume](./examples/02-standard-layout-03-1shard-1repl-deployment-persistent-volume.yaml)
1. AWS-based cluster with data replication and Persistent Volumes [minimal](./examples/04-zookeeper-replication-03-minimal-AWS-persistent-volume.yaml) and [medium](./examples/04-zookeeper-replication-04-medium-AWS-persistent-volume.yaml) Zookeeper installations

## Persistent Volumes
k8s cluster administrator provision storage with `PersistentVolume` objects to users. 
Users claim storage with `PersistentVolumeClaim` objects and then mount claimed `PersistentVolume`s into filesystem with `volumeMounts`+`volumes`.

`PersistentVolume` can be created as:
1. Manual volume provisioning. Cluster administrator manually make calls to storage (cloud) provider to provision new storage volumes, and then create `PersistentVolume` objects to represent those volumes in Kubernetes.
Users claim those `PersistentVolume`s later with `PersistentVolumeClaim`s
1. Dynamic volume provisioning. No need for cluster administrators to pre-provision storage.
Storage resources dynamically provisioned with the provisioner specified by the `StorageClass` object.
`StorageClass`es abstract the underlying storage provider with all parameters (such as disk type or location).
`StorageClass`es use software modules - provisioners that are specific to the storage platform or cloud provider to give Kubernetes access to the physical media being used.

### What is and how to use `StorageClass`

Users refers `StorageClass` by name in the `PersistentVolumeClaim` with `storageClassName` parameter.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mypvc
  namespace: mytestns
spec:
  storageClassName: mystorageclass
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```
Storage class name - `mystorageclass` in this example - is specific for each k8s installation and have to be provided (announced to users) by cluster administrator. 
However, this is not convenient and sometimes we'd like to just use **any** available storage, without bothering to know what storage classes are available in this k8s installation.
The cluster administrator have an option to specify a **default `StorageClass`**. 
When present, the user can create a `PersistentVolumeClaim` without having specifying a `storageClassName`, simplifying the process and reducing required knowledge of the underlying storage provider.

Important notes on `PersistentVolumeClaim` 
1. if `storageClassName` is not specified, **default `StorageClass`** (must be specified by cluster administrator) would be used for provisioning
1. if `storageClassName` is set to an empty string (‘’), no **`StorageClass`** will be used and dynamic provisioning is disabled for this `PersistentVolumeClaim`. 
Available PVs that do not have any `storageClassName` specified  will be considered for binding to the PVC
1. if `storageClassName` is set, then the matching `StorageClass` will be used


## AWS-specific
We can use `kubectl` to check for `StorageClass` objects. Here we use cluster created with `kops`
```bash
kubectl get storageclasses.storage.k8s.io 
```
```text
NAME            PROVISIONER             AGE
default         kubernetes.io/aws-ebs   1d
gp2 (default)   kubernetes.io/aws-ebs   1d
```
We can see two storage classes available:
1. named as **default**
1. named as **gp2** which is the **default `StorageClass`**

We can take a look inside them as: 
```bash
kubectl get storageclasses.storage.k8s.io default -o yaml
kubectl get storageclasses.storage.k8s.io gp2 -o yaml
```
What we can see, that, actually, those `StorageClass`es are equal:
```text
metadata:
  labels:
    k8s-addon: storage-aws.addons.k8s.io
  name: gp2
parameters:
  type: gp2
provisioner: kubernetes.io/aws-ebs
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

```text
metadata:
  labels:
    k8s-addon: storage-aws.addons.k8s.io
  name: default
parameters:
  type: gp2
provisioner: kubernetes.io/aws-ebs
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

What does this mean - we can specify our `PersistentVolumeClaim` object with either: 
1. no `storageClassName` specified (just omit this field) - and in this case **gp2** would be used (because it is the default one) or 
1. specify
```yaml
storageClassName: default
```
and in this case `StorageClass` named as **default** would be used, providing the same result as **gp2** (which is actually the **default `StorageClass`**)


## Pods

Pods use `PersistentVolumeClaim` as **volume**.
`PersistentVolumeClaim` must exist in the same namespace as the pod using the claim.
The cluster inspects the `PersistentVolumeClaim` to find appropriate `PersistentVolume` and mounts that `PersistentVolume` into pod's filesystem via `volumeMounts`.

Pod -> via "volumeMounts: name" refers -> "volumes: name" in Pod or Pod Template as:
```yaml
containers:
  - name: myclickhouse
    image: clickhouse
    volumeMounts:
      - mountPath: "/var/lib/clickhouse"
        name: myvolume
```
This "volume" definition can either be the final object description as:
```yaml
volumes:
  - name: myvolume
    emptyDir: {}
```

```yaml      
volumes:
  - name: myvolume
    hostPath:
      path: /local/path/
```
or can refer to `PersistentVolumeClaim` as:
```yaml
volumes:
  - name: myvolume
    persistentVolumeClaim:
      claimName: myclaim
```
where minimal `PersistentVolumeClaim` can be specified as following:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: myclaim
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: nginx
spec:
  volumes:
  - name: www
    persistentVolumeClaim:
      claimName: myclaim
  containers:
  - name: nginx
    image: k8s.gcr.io/nginx-slim:0.8
    ports:
    - containerPort: 80
      name: web
    volumeMounts:
    - name: www
      mountPath: /usr/share/nginx/html
```
Pay attention, that there is no `storageClassName` specified - meaning this `PersistentVolumeClaim` will claim `PersistentVolume` of explicitly specified **default** `StorageClass`.

More details on [storageClassName][storageClassName]

More details on [PersistentVolumeClaim][PersistentVolumeClaim]

## StatefulSet
`StatefulSet` shortcuts the way, jumping from `volumeMounts` directly to `volumeClaimTemplates`, skipping `volume`.

More details in [StatefulSet description][StatefulSet]

StatefulSet example:
```yaml
apiVersion: v1
kind: Service
metadata:
  name: nginx
  labels:
    app: nginx
spec:
  ports:
  - port: 80
    name: web
  clusterIP: None
  selector:
    app: nginx
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: web
spec:
  serviceName: "nginx"
  replicas: 2
  selector:
    matchLabels:
      app: nginx
  template:
    metadata:
      labels:
        app: nginx
    spec:
      containers:
      - name: nginx
        image: k8s.gcr.io/nginx-slim:0.8
        ports:
        - containerPort: 80
          name: web
        volumeMounts:
        - name: www
          mountPath: /usr/share/nginx/html
  volumeClaimTemplates:
  - metadata:
      name: www
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 1Gi
```  
Pay attention to `.spec.template.spec.containers.volumeMounts`:
```yaml
        volumeMounts:
        - name: www
          mountPath: /usr/share/nginx/html
```
refers directly to:
```yaml
  volumeClaimTemplates:
  - metadata:
      name: www
    spec:
      accessModes: [ "ReadWriteOnce" ]
      resources:
        requests:
          storage: 1Gi
```

[PersistentVolumeClaim]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims
[storageClassName]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class-1
[StatefulSet]: https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/#creating-a-statefulset
[example_aws_pv]: ./examples/chi-example-03-zk-replication-aws-PV.yaml
