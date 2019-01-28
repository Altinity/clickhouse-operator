
# Setup ClickHouse cluster with data replication

## Prerequisites

1. ClickHouse operator [installed](operator_installation.md)
1. Zookeeper installed as described in [Zookeeper Setup](zookeeper_setup.md)

Create namespace where all replication cluster would live
```bash
kubectl create namespace replcluster
```

Create ClickHouse installation object inside namespace
```bash
kubectl apply -f chi-example-03-zk-replication.yaml -n replcluster
```


Verify cluster - show available ClickHouse cluster from within ClickHouse node instance
```bash
kubectl exec -it -n replcluster chi-replcluster-7b6a5caf28493899-i1-0 -- clickhouse-client -q "select cluster, shard_num, replica_num, host_name, host_address from system.clusters where cluster='replcluster'"
```

```bash
kubectl exec -it -n replcluster chi-replcluster-7b6a5caf28493899-i1-0 -- clickhouse-client
CREATE DATABASE articles_repl;
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/tables/1/articles_repl/events_repl_local', '1', event_date, (event_type, article_id), 8192);

CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());
======
kubectl exec -it -n replcluster chi-replcluster-7b6a5caf28493899-i2-0 -- clickhouse-client
CREATE DATABASE articles_repl;
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/tables/1/articles_repl/events_repl_local', '2', event_date, (event_type, article_id), 8192);

CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());
======
kubectl exec -it -n replcluster chi-replcluster-7b6a5caf28493899-i3-0 -- clickhouse-client
CREATE DATABASE articles_repl;
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/tables/2/articles_repl/events_repl_local', '1', event_date, (event_type, article_id), 8192);

CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());
======
kubectl exec -it -n replcluster chi-replcluster-7b6a5caf28493899-i4-0 -- clickhouse-client
CREATE DATABASE articles_repl;
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/tables/2/articles_repl/events_repl_local', '2', event_date, (event_type, article_id), 8192);

CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());
======
kubectl exec -it -n replcluster chi-replcluster-7b6a5caf28493899-i5-0 -- clickhouse-client
CREATE DATABASE articles_repl;
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/tables/3/articles_repl/events_repl_local', '1', event_date, (event_type, article_id), 8192);

CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());
======
kubectl exec -it -n replcluster chi-replcluster-7b6a5caf28493899-i6-0 -- clickhouse-client
CREATE DATABASE articles_repl;
CREATE TABLE articles_repl.events_repl_local (
    event_date  Date,
    event_type  Int32,
    article_id  Int32,
    title       String
) engine=ReplicatedMergeTree('/clickhouse/tables/3/articles_repl/events_repl_local', '2', event_date, (event_type, article_id), 8192);

CREATE TABLE IF NOT EXISTS  
   articles_repl.events_replicated AS articles_repl.events_repl_local
ENGINE = Distributed( replcluster, articles_repl, events_repl_local, rand());

```
