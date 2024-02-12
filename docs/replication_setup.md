
# Setup ClickHouse cluster with data replication

## Prerequisites

1. ClickHouse operator [installed][operator_installation_details.md]
1. Zookeeper [installed][zookeeper_setup.md]


## Manifest

Let's take a look on [example][chi-examples/04-replication-zookeeper-05-simple-PV.yaml], which creates a cluster with 2 shards and 2 replicas and persistent storage.

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"

metadata:
  name: "repl-05"

spec:
  defaults:
    templates: 
      dataVolumeClaimTemplate: default
      podTemplate: clickhouse:19.6
 
  configuration:
    zookeeper:
      nodes:
      - host: zookeeper.zoo1ns
    clusters:
      - name: replicated
        layout:
          shardsCount: 2
          replicasCount: 2

  templates:
    volumeClaimTemplates:
      - name: default
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 500Mi
    podTemplates:
      - name: clickhouse:19.6
        spec:
          containers:
            - name: clickhouse-pod
              image: clickhouse/clickhouse-server:23.8
```


## Replicated table setup

### Macros
Operator provides set of [macros][macros], which are:
 1. `{installation}` -- ClickHouse Installation name
 1. `{cluster}` -- primary cluster name
 1. `{replica}` -- replica name in the cluster, maps to pod service name
 1. `{shard}` -- shard id

ClickHouse also supports internal macros `{database}` and `{table}` that maps to current **database** and **table** respectively.

### Create replicated table

Now we can create [replicated table][replication], using specified macros

```sql
CREATE TABLE events_local on cluster '{cluster}' (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/{installation}/{cluster}/tables/{shard}/{database}/{table}', '{replica}')
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_type, article_id);
```

```sql
CREATE TABLE events on cluster '{cluster}' AS events_local
ENGINE = Distributed('{cluster}', default, events_local, rand());
```

We can generate some data:
```sql
INSERT INTO events SELECT today(), rand()%3, number, 'my title' FROM numbers(100);
```

And check how these data are distributed over the cluster
```sql
SELECT count() FROM events;
SELECT count() FROM events_local;
```

[operator_installation_details.md]: ./operator_installation_details.md
[zookeeper_setup.md]: ./zookeeper_setup.md
[chi-examples/04-replication-zookeeper-05-simple-PV.yaml]: ./chi-examples/04-replication-zookeeper-05-simple-PV.yaml
[macros]: https://clickhouse.yandex/docs/en/operations/server_settings/settings/#macros
[replication]: https://clickhouse.yandex/docs/en/operations/table_engines/replication/
