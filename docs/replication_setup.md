
# Setup ClickHouse cluster with data replication

## Prerequisites

1. ClickHouse operator [installed](operator_installation.md)
1. Zookeeper installed as described in [Zookeeper Setup](zookeeper_setup.md)

Create namespace where all replication cluster would live
```bash
kubectl create namespace ch-repl-test
```

Create ClickHouse installation object inside namespace
```bash
kubectl apply -f chi-example-03-zk-replication.yaml -n ch-repl-test
```


Verify cluster - show available ClickHouse cluster from within ClickHouse node instance
```bash
kubectl exec -it -n ch-repl-test chi-ch-test-zk-repl-26b13c85a95c144d-i1-0 -- clickhouse-client -q 'select cluster, shard_num, replica_num, host_name, host_address from system.clusters'
```
