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

## .spec.templates.serviceTemplates
```yaml
  templates:
    serviceTemplates:
      - name: chi-service-template
        # generateName understands different sets of macroses,
        # depending on the level of the object, for which Service is being created:
        #
        # For CHI-level Service:
        # 1. {chi} - ClickHouseInstallation name
        # 2. {chiID} - short hashed ClickHouseInstallation name (BEWARE, this is an experimental feature)
        #
        # For Cluster-level Service:
        # 1. {chi} - ClickHouseInstallation name
        # 2. {chiID} - short hashed ClickHouseInstallation name (BEWARE, this is an experimental feature)
        # 3. {cluster} - cluster name
        # 4. {clusterID} - short hashed cluster name (BEWARE, this is an experimental feature)
        # 5. {clusterIndex} - 0-based index of the cluster in the CHI (BEWARE, this is an experimental feature)
        #
        # For Shard-level Service:
        # 1. {chi} - ClickHouseInstallation name
        # 2. {chiID} - short hashed ClickHouseInstallation name (BEWARE, this is an experimental feature)
        # 3. {cluster} - cluster name
        # 4. {clusterID} - short hashed cluster name (BEWARE, this is an experimental feature)
        # 5. {clusterIndex} - 0-based index of the cluster in the CHI (BEWARE, this is an experimental feature)
        # 6. {shard} - shard name
        # 7. {shardID} - short hashed shard name (BEWARE, this is an experimental feature)
        # 8. {shardIndex} - 0-based index of the shard in the cluster (BEWARE, this is an experimental feature)
        #
        # For Replica-level Service:
        # 1. {chi} - ClickHouseInstallation name
        # 2. {chiID} - short hashed ClickHouseInstallation name (BEWARE, this is an experimental feature)
        # 3. {cluster} - cluster name
        # 4. {clusterID} - short hashed cluster name (BEWARE, this is an experimental feature)
        # 5. {clusterIndex} - 0-based index of the cluster in the CHI (BEWARE, this is an experimental feature)
        # 6. {shard} - shard name
        # 7. {shardID} - short hashed shard name (BEWARE, this is an experimental feature)
        # 8. {shardIndex} - 0-based index of the shard in the cluster (BEWARE, this is an experimental feature)
        # 9. {replica} - replica name
        # 10. {replicaID} - short hashed replica name (BEWARE, this is an experimental feature)
        # 11. {replicaIndex} - 0-based index of the replica in the shard (BEWARE, this is an experimental feature)
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

        # Possible values for distribution are:
        # Unspecified
        # OnePerHost
        distribution: "Unspecified"

        # type PodSpec struct {} from k8s.io/core/v1
        spec:
          containers:
            - name: clickhouse
              image: clickhouse/clickhouse-server:23.8
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
1. `distribution`

**`zone`** and **`distribution`** together define zoned layout of ClickHouse instances over nodes. Internally it is a shortcut to `affinity.nodeAffinity` and `affinity.podAntiAffinity` properly filled.

Example - how to place ClickHouse instances in AWS `us-east-1a` availability zone with one ClickHouse per host 
```yaml
        zone:
          values:
            - "us-east-1a"
        distribution: "OnePerHost"
```
Example - how to place ClickHouse instances on nodes labeled as `clickhouse=allow` with one ClickHouse per host 
```yaml
        zone:
          key: "clickhouse"
          values:
            - "allow"
        distribution: "OnePerHost"
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
