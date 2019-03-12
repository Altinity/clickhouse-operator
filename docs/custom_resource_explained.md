# ClickHouse Installation Custom Resource explained

Let's describe in details ClickHouse [Custom Resource](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/)
Full example is available in [examples/clickhouseinstallation-object.yaml](examples/clickhouseinstallation-object.yaml) file.
The best way to work with this doc is to open [examples/clickhouseinstallation-object.yaml](examples/clickhouseinstallation-object.yaml) in separate tab
and look into it along with reading this explanation.  

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "clickhouse-installation-test"
```
Create resource of `kind: "ClickHouseInstallation"` named as `"clickhouse-installation-test"`.
Accessible with `kubectl` as:
```bash
kubectl get clickhouseinstallations.clickhouse.altinity.com 
```
```text
NAME                           AGE
clickhouse-installation-test   23h
``` 

## .spec.defaults
```yaml
  defaults:
    replicasUseFQDN: 0 # 0 - by default, 1 - enabled
    distributedDDL:
      profile: default
    deployment:
      zone:
        matchLabels:
          clickhouse.altinity.com/zone: zone1
      podTemplate: clickhouse-installation
      volumeClaimTemplate: default
```
`.spec.defaults` section represents default values for sections below.
  - `.spec.defaults.replicasUseFQDN` - should replicas be specified by FQDN in `<host></host>`
  - `.spec.defaults.distributedDDL` - referense to `<yandex><distributed_ddl></distributed_ddl></yandex>`
  - `.spec.defaults.deployment` would be used everywhere where `deployment` is needed.  

## .spec.configuration
```yaml
  configuration:
```
`.spec.configuration` section refers to config file used by ClickHouse.

## .spec.configuration.zookeeper
```yaml
    zookeeper:
      nodes:
        - host: zk-statefulset-0.zk-service.default.svc.cluster.local
          port: 2181
        - host: zk-statefulset-1.zk-service.default.svc.cluster.local
          port: 2181
        - host: zk-statefulset-2.zk-service.default.svc.cluster.local
          port: 2181
```
`.spec.configuration.zookeeper` refers to [&lt;yandex&gt;&lt;zookeeper&gt;&lt;/zookeeper&gt;&lt;/yandex&gt;](https://clickhouse.yandex/docs/en/operations/table_engines/replication/) config section

## .spec.configuration.users
```yaml
    users:
      profiles/readonly/readonly: "1"
#      <profiles>
#        <readonly>
#          <readonly>1</readonly>
#        </readonly>
#      </profiles>
```
`.spec.configuration.users` refers to [&lt;yandex&gt;&lt;profiles&gt;&lt;/profiles&gt;&lt;users&gt;&lt;/users&gt;&lt;/yandex&gt;](https://clickhouse.yandex/docs/en/operations/settings/settings/) settings sections.
```yaml
      profiles/readonly/readonly: "1"
```
expands into
```xml
      <profiles>
        <readonly>
          <readonly>1</readonly>
        </readonly>
      </profiles>
```

## .spec.configuration.settings
```yaml
    settings:
      profiles/default/max_memory_usage: "1000000000"
#      <profiles>
#        <default>
#          <max_memory_usage>1000000000</max_memory_usage>
#        </default>
#      </profiles>
      compression/case/method: "zstd"
#      <compression>
#       <case>
#         <method>zstd</method>
#      </case>
#      </compression>
``` 
`.spec.configuration.settings` refers to [&lt;yandex&gt;&lt;profiles&gt;&lt;/profiles&gt;&lt;users&gt;&lt;/users&gt;&lt;/yandex&gt;](https://clickhouse.yandex/docs/en/operations/settings/settings/) settings sections.

## .spec.configuration.clusters
```yaml
    clusters:
```
`.spec.configuration.clusters` represents array of ClickHouse clusters definitions.

## Clusters and Layout Types

### Standard layout with shards and replicas count specified

```yaml
    - name: sharded-replicated
      layout:
        type: Standard
        shardsCount: 3
        replicasCount: 2
      deployment:
        podTemplate: clickhouse-installation
#       scanario: Default
#       zone:
#         matchLabels:
#           clickhouse.altinity.com/zone: zone1
# 
#       values inherited from global .spec.deployment section
#
```
ClickHouse cluster named `sharded-replicated` represented with `Standard` layout with 3 shards of 2 replicas each (6 pods total).
Pods will be created and fully managed by the operator.
In ClickHouse config file this would be represented as:
```xml
<yandex>
    <remote_servers>
        <sharded-replicated>
        
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
            
        </sharded-replicated>
    </remote_servers>
</yandex>
``` 
with full IP and DNS management provided by k8s and operator.

### Standard layout with shards count specified

```yaml
    - name: sharded-non-replicated
      layout:
        type: Standard
        shardsCount: 3 # replicasCount = 1, by default
      deployment:
        zone:
          matchLabels:
            clickhouse.altinity.com/zone: zone2
#       scenario: Default
#       podTemplate: clickhouse-installation
#
#       values inherited from global .spec.deployment section
#
```
ClickHouse cluster named `sharded-non-replicated` represented with `Standard` layout with 3 shards of 1 replicas each (3 pods total).
Pods will be created and fully managed by the operator.
In ClickHouse config file this would be represented as:
```xml
<yandex>
    <remote_servers>
        <sharded-non-replicated>
        
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
            
        </sharded-non-replicated>
    </remote_servers>
</yandex>
``` 

### Standard layout with replicas count specified

```yaml
    - name: replicated
      layout:
        type: Standard
        replicasCount: 4 # shardsCount = 1, by default
#     deployment:
#       podTemplate: clickhouse-installation
#       scanario: Default
#       zone:
#         matchLabels:
#           clickhouse.altinity.com/zone: zone1
# 
#       values inherited from global .spec.deployment section
#
```
ClickHouse cluster named `replicated` represented with `Standard` layout with 1 shard of 4 replicas each (4 pods total).
Pods will be created and fully managed by the operator.
In ClickHouse config file this would be represented as:
```xml
<yandex>
    <remote_servers>
        <replicated>
        
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
                <replica>
                    <host>192.168.1.4</host>
                    <port>9000</port>
                </replica>
            </shard>
            
        </replicated>
    </remote_servers>
</yandex>
``` 

### Advanced layout
Advanced layout provides possibility to explicitly define each shard and replica with
`.spec.configuration.clusters.layout.shards`
```yaml
    - name: customized
#     deployment:
#       scenario: Default
#       zone:
#         matchLabels:
#           clickhouse.altinity.com/zone: zone1
#       podTemplate: clickhouse-installation
#
#       values inherited from global .spec.deployment section
#
      layout:
        type: Advanced
        shards:
```
#### definitionType: ReplicasCount
Provides possibility to specify custom replicas count for this shard
```yaml        
        - definitionType: ReplicasCount
          replicasCount: 2
          weight: 1
#         weight - omitted by default
          internalReplication: Disabled
#         internalReplication: Enabled - default value
          deployment:
            podTemplate: clickhouse-installation
            zone:
              matchLabels:
                clickhouse.altinity.com/zone: zone3
#           scenario: Default
#         
#           values inherited from .spec.configuration.clusters[3].deployment section
#
```
Another example with different number of replicas
```yaml
        - definitionType: ReplicasCount
          replicasCount: 3
#         deployment:
#           scenario: Default
#           zone:
#             matchLabels:
#               clickhouse.altinity.com/zone: zone1
#           podTemplate: clickhouse-installation
#  
#         values inherited from .spec.configuration.clusters[3].deployment section
#
```
#### definitionType: Replicas
Provides possibility to specify custom replicas as array `.spec.configuration.clusters.layout.shards.replicas` with individual replicas specifications.

```yaml  
        - definitionType: Replicas
#         deployment:
#           scenario: Default
#           zone:
#             matchLabels:
#               clickhouse.altinity.com/zone: zone1
#           podTemplate: clickhouse-installation
#
#         values inherited from .spec.configuration.clusters[3].deployment section
#
          replicas:
          - port: 9000
            deployment:
              scenario: Default
#             zone:
#               matchLabels:
#                 clickhouse.altinity.com/zone: zone1
#             podTemplate: clickhouse-installation
#  
#           values inherited from .spec.configuration.clusters[3].shards[2].deployment section
#           
          - deployment:
              scenario: NodeMonopoly # 1 pod (CH server instance) per node (zone can be a set of n nodes) -> podAntiAffinity
              zone:
                matchLabels:
                  clickhouse.altinity.com/zone: zone4
                  clickhouse.altinity.com/kind: ssd
              podTemplate: clickhouse-installation
```
ClickHouse cluster named `customized` represented with `Advanced` layout with 3 shards of 2, 3 and 1 replicas each (6 pods total).
Pods will be created and fully managed by the operator.
In ClickHouse config file this would be represented as:
```xml
<yandex>
    <remote_servers>
        <customized>
        
            <shard>
                <internal_replication>false</internal_replication>
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
                <replica>
                    <host>192.168.1.5</host>
                    <port>9000</port>
                </replica>
            </shard>
            
            <shard>
                <internal_replication>true</internal_replication>
                <replica>
                    <host>192.168.1.6</host>
                    <port>9000</port>
                </replica>
            </shard>
            
        </sharded-replicated>
    </customized>
</yandex>
``` 
With first and last shards having personal non-default (i.e. non `.spec.defaults.deployment`) deployments

## .spec.templates
```yaml
  templates:
```

## .spec.templates.volumeClaimTemplates
```yaml
    volumeClaimTemplates:
    - name: default
      template:
        metadata:
          name: clickhouse-data
        spec:
          accessModes:
          - ReadWriteOnce
          resources:
            requests:
              storage: 100Mi
```
`.spec.templates.volumeClaimTemplates` represents [PersistentVolumeClaim](https://kubernetes.io/docs/concepts/storage/persistent-volumes/#persistentvolumeclaims) templates

## .spec.templates.podTemplates
```yaml              
    podTemplates:
    - name: clickhouse-installation
      volumes:
        - name: clickhouse-data
          mountPath: /var/lib/clickhouse
      containers:
      - name: clickhouse
        image: yandex/clickhouse-server:18.14.19-stable
        resources:
          requests:
            memory: "64Mi"
            cpu: "250m"
          limits:
            memory: "128Mi"
            cpu: "500m"
      - name: client
        image: yandex/clickhouse-client:18.14.19-stable
      - name: logwatcher
        image: path/tologwatcher:1.2.3
```
`.spec.templates.podTemplates` represents [Pod Templates](https://kubernetes.io/docs/concepts/workloads/pods/pod-overview/#pod-templates) templates
