
# Setup ClickHouse cluster with data replication

## Prerequisites

1. ClickHouse operator [installed](operator_installation_details.md)
1. Zookeeper installed as described in [Zookeeper Setup](zookeeper_setup.md)

## Installation
Let's create ClickHouse installation with replication cluster. Example manifest is located in [examples/chi-example-03-zk-replication.yaml](examples/chi-example-03-zk-replication.yaml)
We'd like to create all resources inside separated namespace, which is convenient to manage.

Create namespace where all replication cluster would live
```bash
kubectl create namespace replcluster
```

Create ClickHouse installation object inside namespace
```bash
kubectl apply -f chi-example-03-zk-replication.yaml -n replcluster
```

## Manifest

```yaml
  configuration:
    zookeeper:
      nodes:
        - host: zookeeper-0.zookeepers.zoons
          port: 2181
        - host: zookeeper-1.zookeepers.zoons
          port: 2181
        - host: zookeeper-2.zookeepers.zoons
          port: 2181
```

Zookeeper nodes are DNS names [assigned to Zookeeper](zookeeper_setup.md#dns-names) cluster as described in details in [Zookeeper Setup](zookeeper_setup.md) docs


Verify cluster - show available ClickHouse cluster from within ClickHouse node instance
```bash
kubectl exec -it -n replcluster ch-ddaf0f1i1-0 -- clickhouse-client -q "select cluster, shard_num, replica_num, host_name, host_address from system.clusters where cluster='replcluster'"
```

where `ch-ddaf0f1i1-0` is pod's name obtained by `kubectl get pod -n replcluster`

Now let's create replicated table.

## Replicated table setup

### Macros
Operator provides set of [macros](https://clickhouse.yandex/docs/en/operations/server_settings/settings/#macros), which are:
1. `{installation}` - which refers to `.metadata.name` of ClickHouse Installation custom resource
1. Multiple macros to specify name (i.e. - `.spec.configuration.clusters.name`) of each cluster, named as <"`.spec.configuration.clusters.name` value">  
1. Multiple macros to specify name of each shard of of each cluster, named as <"`.spec.configuration.clusters.name` value"-shard>
1. `{replica}` - which specifyies replica name of each shard. Replica should be unique within each shard, so we can use pod id for this purpose

For example manifest [examples/chi-example-03-zk-replication.yaml](examples/chi-example-03-zk-replication.yaml) we'll have the following macros specified:
- for the first pod (`ch-ddaf0f1i1-0`)
```xml
    <macros>
        <installation>replcluster</installation>
        <replcluster>replcluster</replcluster>
        <replcluster-shard>1</replcluster-shard>
        <replica>ddaf0f1i1</replica>
    </macros>
```
- for the second pod (`ch-ddaf0f1i2-0`)
```xml
    <macros>
        <installation>replcluster</installation>
        <replcluster>replcluster</replcluster>
        <replcluster-shard>1</replcluster-shard>
        <replica>ddaf0f1i2</replica>
    </macros>
```
- for the third pod (`ch-ddaf0f1i3-0`)
```xml
    <macros>
        <installation>replcluster</installation>
        <replcluster>replcluster</replcluster>
        <replcluster-shard>2</replcluster-shard>
        <replica>ddaf0f1i3</replica>
    </macros>
```
etc for the rest pods.

### Create replicated table

Now we can create [replicated table](https://clickhouse.yandex/docs/en/operations/table_engines/replication/), using specified macros
`CREATE TABLE` query for replicated table looks like the following:
```sql
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/{installation}/{replcluster}/tables/{replcluster-shard}/articles_repl/events_repl_local', '{replica}', event_date, (event_type, article_id), 8192);
```
`CREATE TABLE` query for distributed table looks like the following:
```sql
CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());
```

So, on each of all pod we have to run these queries:
```sql
CREATE DATABASE articles_repl;
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/{installation}/{replcluster}/tables/{replcluster-shard}/articles_repl/events_repl_local', '{replica}', event_date, (event_type, article_id), 8192);
CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());
```

Run queries:
```bash
kubectl exec -it -n replcluster ch-ddaf0f1i1-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl exec -it -n replcluster ch-ddaf0f1i2-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl exec -it -n replcluster ch-ddaf0f1i3-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl exec -it -n replcluster ch-ddaf0f1i4-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl exec -it -n replcluster ch-ddaf0f1i5-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl exec -it -n replcluster ch-ddaf0f1i6-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
```

and insert some test data on any of replicas
```sql
INSERT INTO articles_repl.events_replicated VALUES ('2010-01-02', 1, 1, concat('events_dist_1_', toString(now())));
INSERT INTO articles_repl.events_replicated VALUES ('2010-01-02', 1, 2, concat('events_dist_2_', toString(now())));
INSERT INTO articles_repl.events_replicated VALUES ('2010-01-02', 1, 3, concat('events_dist_3_', toString(now())));

INSERT INTO articles_repl.events_replicated VALUES ('2010-01-02', 2, 1, concat('events_dist_1_', toString(now())));
INSERT INTO articles_repl.events_replicated VALUES ('2010-01-02', 2, 2, concat('events_dist_2_', toString(now())));
INSERT INTO articles_repl.events_replicated VALUES ('2010-01-02', 2, 3, concat('events_dist_3_', toString(now())));
```

and watch how these data are distributed over the cluster
```sql
SELECT count(*) FROM articles_repl.events_replicated;
SELECT count(*) FROM articles_repl.events_repl_local;
```

## Delete cluster

As soon as cluster is not required any more we can delete it from k8s 

- delete ClickHouse Installation namespace with cluster inside
```bash
kubectl delete namespaces replcluster 
```
- delete Zookeeper namespace with Zookeeper inside 
```bash
kubectl delete namespaces zoons 
```
- uninstall operator
```bash
kubectl delete -f clickhouse-operator-install.yaml
```
