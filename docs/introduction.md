# ClickHouse Operator Introduction

## Prerequisites
You may need to have the following items in order to have ClickHouse installation in k8s

1. Persistent Volumes
1. Zookeeper

### Persistent Volumes
ClickHouse needs disk space to keep data. Kubernetes provides [Persistent Volumes][persistent-volumes]
for this purpose.
As it is stated
> A PersistentVolume (PV) is a piece of storage in the cluster that has been provisioned by an administrator.

This means that we have to do some homework in order to provide Persistent Volumes to ClickHouse installation.
PVs can be provided by:

1. system administrator, who is in charge of k8s installation, can prepare required number of PVs
1. Persistent Volume Provisioner, which may be set up in k8s installation in order to [provision volumes dynamically][dynamic-provisioning]

When ClickHouse required some disk storage, in places [Persistent Volume Claim][persistentvolumeclaims]
which specifies desired storage class and size. Each Persistent Volume has class assigned and size provisioned.
So the main bond between software and disk to be provisioned is [Storage Class][storage-classes].

### Zookeeper

In case we'd like to have [data replication][replication] in ClickHouse,
we need to have [Zookeeper][zookeeper] instance accessible by ClickHouse.
There is no requirement to have Zookeeper instance dedicated to serve ClickHouse replication, we just need to have access to running Zookeeper.
However, in case we'd like to have high-available ClickHouse installation, we need to have Zookeeper cluster of at least 3 nodes.
So, we can either use
1. Already existing Zookeeper instance, or
1. [Setup][zookeeper-setup-doc] our own Zookeeper - in most cases inside the same k8s installation.

[persistent-volumes]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/
[dynamic-provisioning]: https://kubernetes.io/docs/concepts/storage/dynamic-provisioning/
[persistentvolumeclaims]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims
[storage-classes]: https://kubernetes.io/docs/concepts/storage/storage-classes/
[replication]: https://clickhouse.yandex/docs/en/operations/table_engines/replication/
[zookeeper]: https://zookeeper.apache.org/
[zookeeper-setup-doc]: ./zookeeper_setup.md
