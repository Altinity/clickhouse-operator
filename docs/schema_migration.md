# Schema migration

`clickhouse-operator` can resize cluster. After cluster is scaled up and new ClickHouse instances are ready, `clickhouse-operator` migrates schema.
Schema is unified among all replicas within each cluster. So, suppose we have the following configuration:
 * **replica1** has table `table1`
 * **replica2** has table `table2`
Please, pay attention, that each replica has its own table. Having specified setup, we decide to add one more replica.
`clickhouse-operator` adds one more ClickHouse instance, and, after scale process completed, all three nodes would have both tables created:
 * **replica1** has tables: `table1`, `table2`
 * **replica2** has tables: `table1`, `table2`
 * **replica3** has tables: `table1`, `table2`

Please note - `clickhouse-operator` does nothing with data, table definition only is migrated. 
However, in case newly added replica is properly configured, ClickHouse itself would replicate data from master to newly added replica.
   