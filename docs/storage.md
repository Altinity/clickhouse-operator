# Storage

Examples are available in [examples][chi-examples] folder:
1. [Simple Default Persistent Volume][03-persistent-volume-01-default-volume.yaml]
1. [Pod Template with Persistent Volume][03-persistent-volume-02-pod-template.yaml]
1. AWS-based cluster with data replication and Persistent Volumes [minimal][04-replication-zookeeper-03-minimal-AWS-persistent-volume.yaml] 
and [medium][04-replication-zookeeper-04-medium-AWS-persistent-volume.yaml] Zookeeper installations

## Persistent Volumes
k8s cluster administrator provision storage to applications (users) via `PersistentVolume` objects. 
Applications (users) claim storage with `PersistentVolumeClaim` objects and then mount claimed `PersistentVolume`s into filesystem via `volumeMounts`+`volumes`.

`PersistentVolume` can be created as:
1. **Manual volume provisioning**. Cluster administrator manually make calls to storage (cloud) provider to provision new storage volumes, and then create `PersistentVolume` objects to represent those volumes in Kubernetes.
Users claim those `PersistentVolume`s later via `PersistentVolumeClaim`s
1. **Dynamic volume provisioning**. No need for cluster administrators to pre-provision storage manually.
Storage resources are dynamically provisioned by special software module, called provisioner, which is specified by the `StorageClass` object.
`StorageClass`es abstract the underlying storage provider with all parameters (such as disk type or location).
`StorageClass`es use software modules - provisioners that are specific to the storage platform or cloud provider to give Kubernetes access to the physical media being used.

### What it is and how to use `StorageClass`

Applications (users) refer `StorageClass` by name in the `PersistentVolumeClaim` with `storageClassName` parameter.

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: mypvc
  namespace: mytestns
spec:
  storageClassName: my-storage-class
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
```
Storage class name - `my-storage-class` in this example - is specific for each k8s installation and has to be provided (announced to applications(users)) by cluster administrator. 
However, this is not convenient and sometimes we'd like to just use **any** available storage, without bothering to know what storage classes are available in this k8s installation.
The cluster administrator have an option to specify a **default `StorageClass`**. 
When present, the user can create a `PersistentVolumeClaim` having no `storageClassName` specified, simplifying the process and reducing required knowledge of the underlying storage provider.

Important notes on `PersistentVolumeClaim` 
1. if `storageClassName` is not specified, **default `StorageClass`** (must be specified by cluster administrator) would be used for provisioning
1. if `storageClassName` is set to an empty string (""), no **`StorageClass`** will be used, and thus, dynamic provisioning is efficiently disabled for this `PersistentVolumeClaim`. 
Available PVs that do not have any `storageClassName` specified  will be considered for binding to this PVC
1. if `storageClassName` is set, then the matching `StorageClass` will be used for provisioning


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
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp2
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

```text
metadata:
  labels:
    k8s-addon: storage-aws.addons.k8s.io
  name: default
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp2
reclaimPolicy: Delete
volumeBindingMode: Immediate
```

What does this mean - we can specify our `PersistentVolumeClaim` object with either: 
1. no `storageClassName` specified (just omit this field) - and in this case `StorageClass` named **gp2** would be used (because it is the **default** one) or 
1. specify
```yaml
storageClassName: default
```
and in this case `StorageClass` named **default** would be used. The result would be the same as when `StorageClass` named **gp2** used (which is actually the **default `StorageClass`** in the system)


## Pods

Pods use `PersistentVolumeClaim` as **volume**.
`PersistentVolumeClaim` must exist in the same namespace as the pod using the claim.
The k8s inspects the `PersistentVolumeClaim` to find appropriate `PersistentVolume` and mounts that `PersistentVolume` into pod's filesystem via `volumeMounts`.

A Pod refers "volumes: name" via "volumeMounts: name" in Pod or Pod Template as:
```yaml
# ...
# excerpt from Pod or Pod Template manifest
# ...
containers:
  - name: myclickhouse
    image: clickhouse
    volumeMounts:
      - mountPath: "/var/lib/clickhouse"
        name: my-volume
```
This "volume" definition can either be the final object description of different types, such as:
Volume of type `emptyDir`
```yaml
# ...
# excerpt from manifest
# ...
volumes:
  - name: my-volume
    emptyDir: {}
```
Volume of type `hostPath`
```yaml      
# ...
# excerpt from StatefulSet manifest
# ...
volumes:
  - name: my-volume
    hostPath:
      path: /local/path/
```
or can refer to `PersistentVolumeClaim` as:
```yaml
# ...
# excerpt from manifest
# ...
volumes:
  - name: my-volume
    persistentVolumeClaim:
      claimName: my-claim
```
where minimal `PersistentVolumeClaim` can be specified as following:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```
Pay attention, that there is no `storageClassName` specified - meaning this `PersistentVolumeClaim` will claim `PersistentVolume` of explicitly specified **default** `StorageClass`.

More details on [storageClassName][persistent-volumes-class-1]

More details on [PersistentVolumeClaim][persistentvolumeclaims]

Example on how this `persistentVolumeClaim` named `my-pvc` can be used in Pod spec:
```yaml
apiVersion: v1
kind: Pod
metadata:
  name: nginx
spec:
  volumes:
  - name: www
    persistentVolumeClaim:
      claimName: my-pvc
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

## StatefulSet
`StatefulSet` shortcuts the way, jumping from `volumeMounts` directly to `volumeClaimTemplates`, skipping `volume`.

More details in [StatefulSet description][creating-a-statefulset]

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

## AWS encrypted volumes

As we have discussed in [AWS-specific](#AWS-specific) section, AWS provides **gp2** volumes as default media.
Let's create **encrypted** volume based on the same **gp2** volume.
Specify special `StorageClass`
```yaml
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: encrypted-gp2
provisioner: kubernetes.io/aws-ebs
parameters:
  type: gp2
  fsType: ext4
  encrypted: "true"
reclaimPolicy: Delete
volumeBindingMode: Immediate
```
and use it with `PersistentVolumeClaim`:
```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: encrypted-pvc
spec:
  storageClassName: encrypted-gp2
  accessModes:
    - ReadWriteOnce
  volumeMode: Block
  resources:
    requests:
      storage: 1Gi
```

[chi-examples]: ./chi-examples
[03-persistent-volume-01-default-volume.yaml]: ./chi-examples/03-persistent-volume-01-default-volume.yaml
[03-persistent-volume-02-pod-template.yaml]: ./chi-examples/03-persistent-volume-02-pod-template.yaml
[04-replication-zookeeper-03-minimal-AWS-persistent-volume.yaml]: ./chi-examples/04-replication-zookeeper-03-minimal-AWS-persistent-volume.yaml
[04-replication-zookeeper-04-medium-AWS-persistent-volume.yaml]: ./chi-examples/04-replication-zookeeper-04-medium-AWS-persistent-volume.yaml
[persistentvolumeclaims]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims
[persistent-volumes-class-1]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class-1
[creating-a-statefulset]: https://kubernetes.io/docs/tutorials/stateful-application/basic-stateful-set/#creating-a-statefulset
