
# Setup ClickHouse cluster with data replication

## Prerequisites

1. ClickHouse operator [installed](operator_installation_details.md)
1. Zookeeper installed as described in [Zookeeper Setup](zookeeper_setup.md)

## Installation
Let's create ClickHouse installation with replication cluster. There are two example manifests available - depending on what Zookeeper installation was made:
 1. One-node Zookeeper installation - based in [./examples/04-zookeeper-replication-01-minimal.yaml](./examples/04-zookeeper-replication-01-minimal.yaml)
 1. Three-nodes Zookeeper installation - based in [./examples/04-zookeeper-replication-02-medium.yaml](./examples/04-zookeeper-replication-02-medium.yaml) 
We'd like to create all resources inside separated namespace, which is convenient to manage.

Create namespace where all replication cluster would live
```bash
kubectl create namespace replcluster
```

We'll go here with medium 3-nodes Zookeeper installation.
Create ClickHouse installation object inside namespace:
```bash
kubectl -n replcluster apply -f ./examples/04-zookeeper-replication-02-medium.yaml 
```

## Manifest

```yaml
  configuration:
    zookeeper:
      nodes:
        - host: zookeeper-0.zookeepers.zoo3ns
          port: 2181
        - host: zookeeper-1.zookeepers.zoo3ns
          port: 2181
        - host: zookeeper-2.zookeepers.zoo3ns
          port: 2181
```

Zookeeper nodes are DNS names [assigned to Zookeeper](zookeeper_setup.md#dns-names) cluster as described in details in [Zookeeper Setup](zookeeper_setup.md) docs

Verify cluster - show available ClickHouse cluster from within ClickHouse node instance
```bash
kubectl -n replcluster exec -it chi-e557e4-54a2-0-0-0 -- clickhouse-client -q "select cluster, shard_num, replica_num, host_name, host_address from system.clusters where cluster='replcluster'"
```

where `chi-e557e4-54a2-0-0-0` is pod's name obtained by `kubectl -n replcluster get pod`

Now let's create replicated table.

## Replicated table setup

### Macros
Operator provides set of [macros](https://clickhouse.yandex/docs/en/operations/server_settings/settings/#macros), which are:
 1. `{installation}` - which refers to `.metadata.name` of ClickHouse Installation custom resource
 1. Multiple macros to specify name (i.e. - `.spec.configuration.clusters.name`) of each cluster, named as <"`.spec.configuration.clusters.name` value">  
 1. Multiple macros to specify name of each shard of of each cluster, named as <"`.spec.configuration.clusters.name` value"-shard>
 1. `{replica}` - which specifies replica name of each shard. Replica should be unique within each shard, so we can use StatefulSet name for this purpose

For example, our manifest [./examples/04-zookeeper-replication-02-medium.yaml](./examples/04-zookeeper-replication-02-medium.yaml) we'll have the following macros specified:
- for the first pod (`chi-e557e4-54a2-0-0-0`) we have shard with index `0`
```xml
    <macros>
        <installation>repl-02</installation>
        <replcluster>replcluster</replcluster>
        <replcluster-shard>0</replcluster-shard>
        <replica>chi-e557e4-54a2-0-0</replica>
    </macros>
```
- for the second pod (`chi-e557e4-54a2-0-1-0`) we have shard with index `0` (it is replica of the first pod)
```xml
    <macros>
        <installation>repl-02</installation>
        <replcluster>replcluster</replcluster>
        <replcluster-shard>0</replcluster-shard>
        <replica>chi-e557e4-54a2-0-1</replica>
    </macros>
```
- for the third pod (`chi-e557e4-54a2-1-0-0`) we have shard with index `1`
```xml
    <macros>
        <installation>repl-02</installation>
        <replcluster>replcluster</replcluster>
        <replcluster-shard>1</replcluster-shard>
        <replica>chi-e557e4-54a2-1-0</replica>
    </macros>
```
etc for the rest of the pods.

### Create replicated table

Now we can create [replicated table](https://clickhouse.yandex/docs/en/operations/table_engines/replication/), using specified macros
`CREATE TABLE` query example for replicated table looks like the following (do not copy-paste this, real copy-paste-ready query goes later):
```sql
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/{installation}/{replcluster}/tables/{replcluster-shard}/articles_repl/events_repl_local', '{replica}', event_date, (event_type, article_id), 8192);
```
`CREATE TABLE` query example for distributed table looks like the following (do not copy-paste this, real copy-paste-ready query goes later):
```sql
CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());
```

So, on each of all pod we have to run these queries (copy+paste from here on):
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
kubectl -n replcluster exec -it chi-e557e4-54a2-0-0-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl -n replcluster exec -it chi-e557e4-54a2-0-1-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl -n replcluster exec -it chi-e557e4-54a2-1-0-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl -n replcluster exec -it chi-e557e4-54a2-1-1-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl -n replcluster exec -it chi-e557e4-54a2-2-0-0 -- clickhouse-client
... paste CREATE DATABASE + 2 CREATE TABLE SQLs ...
kubectl -n replcluster exec -it chi-e557e4-54a2-2-1-0 -- clickhouse-client
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
kubectl delete namespaces zoo3ns 
```
- uninstall operator
```bash
kubectl delete -f clickhouse-operator-install.yaml
```
