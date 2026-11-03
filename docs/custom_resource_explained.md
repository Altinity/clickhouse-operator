# ClickHouse Installation Custom Resource explained

Let's describe in details ClickHouse [Custom Resource][custom-resource] \
Full example is available in [99-clickhouseinstallation-max.yaml][99-clickhouseinstallation-max.yaml] file. \
The best way to work with this doc is to open [99-clickhouseinstallation-max.yaml][99-clickhouseinstallation-max.yaml] in separate tab
and look into it along with reading this explanation.  

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "clickhouse-installation-test"
```
Create resource of `kind: "ClickHouseInstallation"` named as `"clickhouse-installation-max"`.
Accessible with `kubectl` as:
```bash
kubectl get clickhouseinstallations.clickhouse.altinity.com 
```
```text
NAME                           AGE
clickhouse-installation-max   23h
``` 

## .spec.defaults
```yaml
  defaults:
    replicasUseFQDN: "no"
    distributedDDL:
      profile: default
    templates:
      podTemplate: clickhouse-v18.16.1
      dataVolumeClaimTemplate: default-volume-claim
      logVolumeClaimTemplate: default-volume-claim
      serviceTemplate: chi-service-template
```
`.spec.defaults` section represents default values for sections below.
  - `.spec.defaults.replicasUseFQDN` - should replicas be specified by FQDN in `<host></host>`
  - `.spec.defaults.distributedDDL` - reference to `<yandex><distributed_ddl></distributed_ddl></yandex>`
  - `.spec.defaults.templates` would be used everywhere where `templates` is needed.  

## .spec.configuration
```yaml
  configuration:
```
`.spec.configuration` section represents sources for ClickHouse configuration files. Be it users, remote servers and etc configuration files. 

## .spec.configuration.zookeeper
```yaml
    zookeeper:
      nodes:
        - host: zookeeper-0.zookeepers.zoo3ns.svc.cluster.local
          port: 2181
        - host: zookeeper-1.zookeepers.zoo3ns.svc.cluster.local
          port: 2181
        - host: zookeeper-2.zookeepers.zoo3ns.svc.cluster.local
          port: 2181
      session_timeout_ms: 30000
      operation_timeout_ms: 10000
      root: /path/to/zookeeper/node
      identity: user:password
```
`.spec.configuration.zookeeper` refers to [&lt;yandex&gt;&lt;zookeeper&gt;&lt;/zookeeper&gt;&lt;/yandex&gt;][server-settings_zookeeper] config section

Alternatively, instead of specifying `nodes` explicitly, you can reference a `ClickHouseKeeperInstallation` resource by name:
```yaml
    zookeeper:
      keeper:
        name: my-keeper           # name of the CHK resource
        namespace: keeper-ns      # optional, defaults to CHI namespace
        serviceType: replicas     # "replicas" (default) or "service"
      session_timeout_ms: 30000
      operation_timeout_ms: 10000
```
The operator resolves the keeper reference to ZooKeeper node addresses automatically, including TLS auto-detection.
See [Keeper Reference](keeper_reference.md) for full details.

## .spec.configuration.profiles
`.spec.configuration.profiles` refers to [&lt;yandex&gt;&lt;profiles&gt;&lt;/profiles&gt;&lt;/yandex&gt;][profiles] settings sections.
```yaml
    profiles:
      readonly/readonly: 1
```

expands into
```xml
      <profiles>
        <readonly>
          <readonly>1</readonly>
        </readonly>
      </profiles>
```

## .spec.configuration.quotas
`.spec.configuration.quotas` refers to [&lt;yandex&gt;&lt;quotas&gt;&lt;/quotas&gt;&lt;/yandex&gt;][quotas] settings sections.
```yaml
    quotas:
      default/interval/duration: 3600
      default/interval/queries: 10000
```

expands into
```xml
      <quotas>
        <default>
          <interval>
              <duration>3600</duration>
              <queries>3600</queries>
          </interval>
        </default>
      </quotas>
```

## .spec.configuration.users
`.spec.configuration.users` refers to [&lt;yandex&gt;&lt;users&gt;&lt;/users&gt;&lt;/yandex&gt;][users] settings sections.
```yaml
  users:
    test/networks/ip:
        - "127.0.0.1"
        - "::/0"
```

expands into
```xml
     <users>
        <test>
          <networks>
            <ip>127.0.0.1</ip>
            <ip>::/0</ip>
          </networks>
        </test>
     </users>
```
when you skip user/password, or setup it as empty value then `chConfigUserDefaultPassword` parameter value from `etc-clickhouse-operator-files` ConfigMap will use. 

## .spec.configuration.settings
```yaml
    settings:
      compression/case/method: "zstd"
#      <compression>
#       <case>
#         <method>zstd</method>
#      </case>
#      </compression>
``` 
`.spec.configuration.settings` refers to [&lt;yandex&gt;&lt;profiles&gt;&lt;/profiles&gt;&lt;users&gt;&lt;/users&gt;&lt;/yandex&gt;][settings] settings sections.

## .spec.configuration.files
```yaml
    files:
      dict1.xml: |
        <yandex>
            <!-- ref to file /etc/clickhouse-data/config.d/source1.csv -->
        </yandex>
      source1.csv: |
        a1,b1,c1,d1
        a2,b2,c2,d2
```
`.spec.configuration.files` allows to introduce custom files to ClickHouse via YAML manifest. 
This can be used in order to create complex custom configurations. One possible usage example is [external dictionary][external_dicts_dict]
```yaml
spec:
  configuration:
    settings:
      dictionaries_config: config.d/*.dict
    files:
      dict_one.dict: |
        <yandex>
          <dictionary>
        <name>one</name>
        <source>
            <clickhouse>
                <host>localhost</host>
                <port>9000</port>
                <user>default</user>
                <password/>
                <db>system</db>
                <table>one</table>
            </clickhouse>
        </source>
        <lifetime>60</lifetime>
        <layout><flat/></layout>
        <structure>
            <id>
                <name>dummy</name>
            </id>
            <attribute>
                <name>one</name>
                <expression>dummy</expression>
                <type>UInt8</type>
                <null_value>0</null_value>
            </attribute>
        </structure>
        </dictionary>
        </yandex>
```

## .spec.configuration.clusters
```yaml
    clusters:
```
`.spec.configuration.clusters` represents array of ClickHouse clusters definitions.

## Clusters and Layouts

ClickHouse instances layout within cluster is described with `.clusters.layout` section
```yaml
      - name: all-counts
        templates:
          podTemplate: clickhouse-v18.16.1
          dataVolumeClaimTemplate: default-volume-claim
          logVolumeClaimTemplate: default-volume-claim
        layout:
          shardsCount: 3
          replicasCount: 2
```
Pod and VolumeClaim templates to be used can be specified explicitly for each replica:
```yaml
        templates:
          podTemplate: clickhouse-v18.16.1
          dataVolumeClaimTemplate: default-volume-claim
          logVolumeClaimTemplate: default-volume-claim
```
`layout` is specified with basic layout dimensions:
```yaml
        layout:
          shardsCount: 3
          replicasCount: 2
```
or with detailed specification of `shards` and `replicas`. \
`shard0` here has `replicasCount` specified, while `shard1` has 3 replicas explicitly specified, with possibility to customized each replica.  
```yaml
      - name: customized
        templates:
          podTemplate: clickhouse-v18.16.1
          dataVolumeClaimTemplate: default-volume-claim
          logVolumeClaimTemplate: default-volume-claim
        layout:
          shards:
            - name: shard0
              replicasCount: 3
              weight: 1
              internalReplication: Disabled
              templates:
                podTemplate: clickhouse-v18.16.1
                dataVolumeClaimTemplate: default-volume-claim
                logVolumeClaimTemplate: default-volume-claim

            - name: shard1
              templates:
                podTemplate: clickhouse-v18.16.1
                dataVolumeClaimTemplate: default-volume-claim
                logVolumeClaimTemplate: default-volume-claim
              replicas:
                - name: replica0
                - name: replica1
                - name: replica2
```
combination is also possible, which is presented in `shard2` specification, where 3 replicas in total are requested with `replicasCount` 
and one of these replicas is explicitly specified with different `podTemplate`:
```yaml
      - name: customized
        templates:
          podTemplate: clickhouse-v18.16.1
          dataVolumeClaimTemplate: default-volume-claim
          logVolumeClaimTemplate: default-volume-claim
        layout:
          shards:
          
            - name: shard2
              replicasCount: 3
              templates:
                podTemplate: clickhouse-v18.16.1
                dataVolumeClaimTemplate: default-volume-claim
                logVolumeClaimTemplate: default-volume-claim
              replicas:
                - name: replica0
                  port: 9000
                  templates:
                    podTemplate: clickhouse-v19.11.3.11
                    dataVolumeClaimTemplate: default-volume-claim
                    logVolumeClaimTemplate: default-volume-claim
```
ClickHouse cluster named `all-counts` represented by layout with 3 shards of 2 replicas each (6 pods total).
Pods will be created and fully managed by the operator.
In ClickHouse config file this would be represented as:
```xml
<yandex>
    <remote_servers>
        <all-counts>
        
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>192.168.1.1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>192.168.1.2</host>
                    <port>9000</port>
                </replica>
            </shard>
            
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>192.168.1.3</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>192.168.1.4</host>
                    <port>9000</port>
                </replica>
            </shard>
            
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>192.168.1.5</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>192.168.1.6</host>
                    <port>9000</port>
                </replica>
            </shard>
            
        </all-counts>
    </remote_servers>
</yandex>
``` 
with full IP and DNS management provided by k8s and operator.

### Layout with shards count specified

```yaml
    - name: shards-only
      layout:
        shardsCount: 3 # replicasCount = 1, by default
```
ClickHouse cluster named `shards-only` represented by layout with 3 shards of 1 replicas each (3 pods total).
Pods will be created and fully managed by the operator.
In ClickHouse config file this would be represented as:
```xml
<yandex>
    <remote_servers>
        <shards-only>
        
            <shard>
                <replica>
                    <host>192.168.1.1</host>
                    <port>9000</port>
                </replica>
            </shard>
            
            <shard>
                <replica>
                    <host>192.168.1.2</host>
                    <port>9000</port>
                </replica>
            </shard>
            
            <shard>
                <replica>
                    <host>192.168.1.3</host>
                    <port>9000</port>
                </replica>
            </shard>
            
        </shards-only>
    </remote_servers>
</yandex>
``` 

### Layout with replicas count specified

```yaml
    - name: replicas-only
      layout:
        replicasCount: 3 # shardsCount = 1, by default
```
ClickHouse cluster named `replicas-only` represented by layout with 1 shard of 3 replicas each (3 pods total).
Pods will be created and fully managed by the operator.
In ClickHouse config file this would be represented as:
```xml
<yandex>
    <remote_servers>
        <replicas-only>
        
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>192.168.1.1</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>192.168.1.2</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>192.168.1.3</host>
                    <port>9000</port>
                </replica>
            </shard>
            
        </replicas-only>
    </remote_servers>
</yandex>
``` 

### Advanced layout techniques
`layout` provides possibility to explicitly define each shard and replica with
`.spec.configuration.clusters.layout.shards`
```yaml
    - name: customized
      layout:
        shards:
          - replicas:
```
so we can specify `shards` and `replicas` explicitly - either all `shards` and `replias` or selectively, 
only those which we'd like to be different from default template. 

Full specification of `replicas` in a shard. Note - no `replicasCount` specified, all replicas are described by `replicas` array:
```yaml        
            - name: shard1
              templates:
                podTemplate: clickhouse-v18.16.1
                dataVolumeClaimTemplate: default-volume-claim
                logVolumeClaimTemplate: default-volume-claim
              replicas:
                - name: replica0
                - name: replica1
                - name: replica2
```
Another example with selectively described replicas. Note - `replicasCount` specified and one replica is described explicitly
```yaml
            - name: shard2
              replicasCount: 3
              templates:
                podTemplate: clickhouse-v18.16.1
                dataVolumeClaimTemplate: default-volume-claim
                logVolumeClaimTemplate: default-volume-claim
              replicas:
                - name: replica0
                  port: 9000
                  templates:
                    podTemplate: clickhouse-v19.11.3.11
                    dataVolumeClaimTemplate: default-volume-claim
                    logVolumeClaimTemplate: default-volume-claim
```

## .spec.backup
**EXPERIMENTAL.**
```yaml
  backup:
    enabled: "yes"
    podTemplate: backup-sidecar  # its clickhouse-backup container is the sidecar's base
    storages:                # storages the backups may be kept in, one per level for now
      - name: main
        secretRef:
          name: clickhouse-backup-storage
    # storage: main          # the storage the sidecar uses; left out, the only one declared
    recovery:                # a new installation only
      backupName: default-prod-nightly-20261003-020000
  configuration:
    clusters:
      - name: scratch
        backup:
          enabled: "no"
```
`.spec.backup` turns on operator-managed backup with [clickhouse-backup](https://github.com/Altinity/clickhouse-backup)
for every cluster of the installation; `.spec.configuration.clusters[].backup` overrides it for one cluster.
A cluster inherits every field it leaves empty from `.spec.backup`, so `enabled: "no"` on a cluster opts it out. For every enabled cluster the operator injects a `clickhouse-backup`
sidecar into each Pod - enabling or disabling it rolls the cluster's Pods - and the cluster can then be targeted by
`ClickHouseBackup`, `ClickHouseBackupSchedule` and `ClickHouseRestore` resources, which run one Job per cluster.
  - `enabled` - turn operator-managed backup on (`"yes"`) or off (`"no"`, the default)
  - `podTemplate` - the pod template whose container named `clickhouse-backup` the sidecar is built on, its `image`, `env`,
    `resources`, `securityContext` and more; only that container is taken, and the Jobs run its image under its identity.
    Left empty, the operator builds the sidecar on its own (see [backup.md](backup.md#customizing-the-sidecar))
  - `storages` - the storages the backups may be kept in, one per level for now, each a `name` (a DNS label of
    at most 45 characters) and a `secretRef`: the `name` of a Secret in the installation's namespace whose keys become
    the sidecar's environment variables and configure the storage, e.g. `REMOTE_STORAGE`, `S3_BUCKET`, `S3_PATH`,
    `S3_ACCESS_KEY`, `S3_SECRET_KEY`; without `secretRef`, the container `podTemplate` names configures the storage.
    A cluster takes the installation's storages along with its own, pairing them by name: a storage of its own that
    the installation declares too takes what it leaves out, such as `secretRef`, from the installation's. Templates
    contribute theirs alike. Pods start without the Secret, but backups and restores fail until it exists.
    The sidecars follow the Secret: once it is created or changed, each restarts by itself within minutes - ClickHouse
    does not - when it runs no backup or restore (see [backup.md](backup.md#secret-changes)). The storage path defaults
    to `backup/{namespace}/{installation}/{cluster}/{shard}`, or the default the operator configuration sets in
    `backup.storagePath`, and no two shards may share one; with several backup-enabled clusters, keep the whole
    sidecar configuration in this Secret and the sidecar container's `env`, as backups and restores refuse to run on
    paths they cannot check, such as those of a configuration file (see [backup.md](backup.md#storage-paths))
  - `storage` - the storage of `storages` the sidecar keeps the backups in. Left out, it is the only storage the same
    level declares, or else the one the level a cluster inherits from uses, so a cluster's own storage wins over the
    installation's; `""` on a cluster leaves it without a storage. Backup takes effect only where the storage a
    cluster uses has a `secretRef`, or `podTemplate` is set. On a cluster that turns backup on, a name no level
    declares aborts the reconcile with `InvalidBackupStorage` before anything changes (see
    [backup.md](backup.md#storages))
  - `recovery.backupName` - for a new installation: the backup it starts from. Once its first reconcile completes,
    the operator restores every backup-enabled cluster from it, once, provided the installation holds no tables yet;
    a cluster may name another backup, or `""` for none. Taken from the installation only, never from a template;
    changing it restarts nothing (see [backup.md](backup.md#recover-a-new-chi-from-a-backup))
  - `recovery.sourceInstallation`, `recovery.sourceNamespace` - the installation, and its namespace, that made the
    backup, when not this one: the backup is read from the storage paths the `{installation}` and `{namespace}`
    macros give there (see [backup.md](backup.md#storage-paths))

The sidecar works on ClickHouse's data volume, so give backup-enabled clusters a `dataVolumeClaimTemplate`.
Without a volume mounted at `/var/lib/clickhouse`, the operator shares an `emptyDir` there between the two containers,
which is lost whenever the Pod is deleted.

See [backup.md](./backup.md) for details.

## .spec.templates.serviceTemplates
```yaml
  templates:
    serviceTemplates:
      - name: chi-service-template
        # generateName understands different sets of macros,
        # depending on the level of the object, for which Service is being created:
        # For CHI/CHK-level Service:
        # {chi} - ClickHouseInstallation name
        # {chk} - ClickHouseKeeperInstallation name
        # {chiID} - short hashed ClickHouseInstallation name 
        # {chkID} - short hashed ClickHouseKeeperInstallation name
        #
        # For Cluster-level Service:
        # {chi} - ClickHouseInstallation name
        # {chk} - ClickHouseKeeperInstallation name
        # {chiID} - short hashed ClickHouseInstallation name
        # {chkID} - short hashed ClickHouseKeeperInstallation name
        # {cluster} - cluster name
        # {clusterID} - short hashed cluster name (BEWARE, this is an experimental feature)
        # {clusterIndex} - 0-based index of the cluster in the CHI (BEWARE, this is an experimental feature)
        #
        # For Shard-level Service:
        # {chi} - ClickHouseInstallation name
        # {chk} - ClickHouseKeeperInstallation name
        # {chiID} - short hashed ClickHouseInstallation name
        # {chkID} - short hashed ClickHouseKeeperInstallation name
        # {cluster} - cluster name
        # {clusterID} - short hashed cluster name (BEWARE, this is an experimental feature)
        # {clusterIndex} - 0-based index of the cluster in the CHI
        # {shard} - shard name
        # {shardID} - short hashed shard name (BEWARE, this is an experimental feature)
        # {shardIndex} - 0-based index of the shard in the cluster 
        #
        # For Replica-level Service:
        # {chi} - ClickHouseInstallation name
        # {chk} - ClickHouseKeeperInstallation name
        # {chiID} - short hashed ClickHouseInstallation name
        # {chkID} - short hashed ClickHouseKeeperInstallation name
        # {cluster} - cluster name
        # {clusterID} - short hashed cluster name 
        # {clusterIndex} - 0-based index of the cluster in the CHI 
        # {shard} - shard name
        # {shardID} - short hashed shard name (BEWARE, this is an experimental feature)
        # {shardIndex} - 0-based index of the shard in the cluster 
        # {replica} - replica name
        # {replicaID} - short hashed replica name 
        # {replicaIndex} - 0-based index of the replica in the shard 
        generateName: "service-{chi}"
        # type ObjectMeta struct from k8s.io/meta/v1
        metadata:
          labels:
            custom.label: "custom.value"
          annotations:
            cloud.google.com/load-balancer-type: "Internal"
            service.beta.kubernetes.io/aws-load-balancer-internal: 0.0.0.0/0
            service.beta.kubernetes.io/azure-load-balancer-internal: "true"
            service.beta.kubernetes.io/openstack-internal-load-balancer: "true"
            service.beta.kubernetes.io/cce-load-balancer-internal-vpc: "true"
        # type ServiceSpec struct from k8s.io/core/v1
        spec:
          ports:
            - name: http
              port: 8123
            - name: client
              port: 9000
          type: LoadBalancer
```
`.spec.templates.serviceTemplates` represents [Service][service] templates
with additional sections, such as:
1. `generateName`

**Important:** `serviceTemplates[].spec` is not merged with operator defaults; for ClickHouseKeeperInstallation replica host Services (`replicaServiceTemplate`), include `spec.publishNotReadyAddresses: true` when overriding `spec`.

**`generateName`** is used to explicitly specify service name to be created. `generateName` provides the following macro substitutions:
1. `{chi}` - ClickHouseInstallation name
2. `{chiID}` - short hashed ClickHouseInstallation name (BEWARE, this is an experimental feature)
3. `{cluster}` - cluster name
4. `{clusterID}` - short hashed cluster name (BEWARE, this is an experimental feature)
5. `{clusterIndex}` - 0-based index of the cluster in the CHI (BEWARE, this is an experimental feature)
6. `{shard}` - shard name
7. `{shardID}` - short hashed shard name (BEWARE, this is an experimental feature)
8. `{shardIndex}` - 0-based index of the shard in the cluster (BEWARE, this is an experimental feature)
9. `{replica}` - replica name
10. `{replicaID}` - short hashed replica name (BEWARE, this is an experimental feature)
11. `{replicaIndex}` - 0-based index of the replica in the shard (BEWARE, this is an experimental feature)

## .spec.templates.volumeClaimTemplates
```yaml
  templates:
    volumeClaimTemplates:
      - name: default-volume-claim
        # type PersistentVolumeClaimSpec struct from k8s.io/core/v1
        spec:
          # 1. If storageClassName is not specified, default StorageClass
          # (must be specified by cluster administrator) would be used for provisioning
          # 2. If storageClassName is set to an empty string (‘’), no storage class will be used
          # dynamic provisioning is disabled for this PVC. Existing, “Available”, PVs
          # (that do not have a specified storageClassName) will be considered for binding to the PVC
          #storageClassName: gold
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 1Gi
```
`.spec.templates.volumeClaimTemplates` represents [PersistentVolumeClaim][persistentvolumeclaims] templates

## .spec.templates.podTemplates
```yaml              
  templates:
    podTemplates:
      # multiple pod templates makes possible to update version smoothly
      # pod template for ClickHouse v18.16.1
      - name: clickhouse-v18.16.1
        # We may need to label nodes with clickhouse=allow label for this example to run
        # See ./label_nodes.sh for this purpose
        zone:
          key: "clickhouse"
          values:
            - "allow"
        # Shortcut version for AWS installations
        #zone:
        #  values:
        #    - "us-east-1a"

        # Possible values for podDistribution are:
        # Unspecified
        # ClickHouseAntiAffinity
        podDistribution:
          - type: ClickHouseAntiAffinity

        # type PodSpec struct {} from k8s.io/core/v1
        spec:
          containers:
            - name: clickhouse
              image: clickhouse/clickhouse-server:24.8
              volumeMounts:
                - name: default-volume-claim
                  mountPath: /var/lib/clickhouse
              resources:
                requests:
                  memory: "64Mi"
                  cpu: "100m"
                limits:
                  memory: "64Mi"
                  cpu: "100m"
```
`.spec.templates.podTemplates` represents [Pod Templates][pod-templates] 
with additional sections, such as:
1. `zone`
1. `podDistribution`

**`zone`** and **`podDistribution`** together define zoned layout of ClickHouse instances over nodes. Internally it is a shortcut to `affinity.nodeAffinity` and `affinity.podAntiAffinity` properly filled.

Example - how to place ClickHouse instances in AWS `us-east-1a` availability zone, ensuring Pods from the same ClickHouse cluster are scheduled on different Kubernetes nodes. 
```yaml
        zone:
          values:
            - "us-east-1a"
        podDistribution:
          - type: ClickHouseAntiAffinity
```
Example - how to place ClickHouse instances on nodes labeled `clickhouse=allow`, preventing Pods from the same ClickHouse cluster from sharing a Kubernetes node. 
```yaml
        zone:
          key: "clickhouse"
          values:
            - "allow"
        podDistribution:
          - type: ClickHouseAntiAffinity
```

[custom-resource]: https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/
[99-clickhouseinstallation-max.yaml]: ./chi-examples/99-clickhouseinstallation-max.yaml
[server-settings_zookeeper]: https://clickhouse.tech/docs/en/operations/server-configuration-parameters/settings/#server-settings_zookeeper
[settings]: https://clickhouse.tech/docs/en/operations/settings/settings/
[quotas]: https://clickhouse.tech/docs/en/operations/quotas/
[profiles]: https://clickhouse.tech/docs/en/operations/settings/settings-profiles/
[users]: https://clickhouse.tech/docs/en/operations/settings/settings-users/
[external_dicts_dict]: https://clickhouse.tech/docs/en/query_language/dicts/external_dicts_dict/
[service]: https://kubernetes.io/docs/concepts/services-networking/service/
[persistentvolumeclaims]: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims
[pod-templates]: https://kubernetes.io/docs/concepts/workloads/pods/pod-overview/#pod-templates 
