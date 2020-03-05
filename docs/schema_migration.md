# Schema maintenance

`clickhouse-operator` automates schema management when cluster is scaled up or down

# Schema auto-creation

After cluster is scaled up and new ClickHouse instances are ready, `clickhouse-operator` automatically creates schema.

If shard is added:
  * Other shards in the cluster are analyzed for distributed and corresponding local tables
  * Databases for local and distributed tables are created
  * Local tables are created
  * Distributed tables are created
  
If replica is added:
  * Other replicas **at the same shard** are analyzed for replicated tables
  * Databases for replicated tables are created
  * Replicated tables are created
  * Then the same logic as to adding shard applies

# Schema auto-deletion

If cluster is scaled down and some shards or replicas are deleted, `clickhouse-operator` drops replicated table to make sure nothing is left in ZooKeeper.
