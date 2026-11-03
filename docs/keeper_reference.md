# Referencing ClickHouseKeeper from ClickHouseInstallation

Instead of specifying ZooKeeper/Keeper endpoints explicitly via `host:port` in `spec.configuration.zookeeper.nodes`,
you can reference a `ClickHouseKeeperInstallation` (CHK) resource by name. The operator resolves
the keeper endpoints automatically during reconcile.

## Basic Usage

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: my-chi
spec:
  configuration:
    zookeeper:
      keeper:
        name: my-keeper
    clusters:
      - name: default
        layout:
          shardsCount: 1
          replicasCount: 2
```

The operator discovers all keeper replica services and configures ClickHouse with the correct
ZooKeeper node addresses.

## Keeper Reference Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | (required) | Name of the `ClickHouseKeeperInstallation` resource |
| `namespace` | string | CHI's namespace | Namespace of the CHK resource |
| `serviceType` | string | `replicas` | Endpoint discovery mode (see below) |

### Service Type

- **`replicas`** (default) — Discovers per-host keeper services. Creates one ZooKeeper node per keeper replica. Recommended for production as ClickHouse gets full keeper topology awareness for failover.
- **`service`** — Uses the CHK CR-level headless service as a single ZooKeeper node entry. Simpler but ClickHouse doesn't see individual keeper replicas.

## Combining with Other ZooKeeper Settings

The `keeper` reference can be used alongside other `zookeeper` fields:

```yaml
zookeeper:
  keeper:
    name: my-keeper
  session_timeout_ms: 30000
  operation_timeout_ms: 10000
  root: "/clickhouse/my-cluster"
  identity: "user:password"
```

## Cluster-Level Override

A cluster can specify its own keeper reference that overrides the top-level config.
If a cluster has any `zookeeper` config (own `keeper` ref or own `nodes`), the top-level
config is ignored entirely for that cluster.

```yaml
spec:
  configuration:
    zookeeper:
      keeper:
        name: default-keeper
    clusters:
      - name: cluster-a
        # Uses default-keeper (inherited from top-level)
        layout:
          shardsCount: 2
          replicasCount: 2
      - name: cluster-b
        # Uses its own dedicated keeper
        zookeeper:
          keeper:
            name: dedicated-keeper
            namespace: keeper-namespace
        layout:
          shardsCount: 1
          replicasCount: 3
```

## TLS Auto-Detection

The operator automatically detects whether the keeper is configured with TLS by inspecting
the service port spec:

- Port `2181` or port named `zk` → insecure connection
- Port `2281` or port named `zk-secure` → secure connection (sets `<secure>1</secure>` in ClickHouse config)

If the keeper exposes a secure port, the resolved ZooKeeper nodes will have `secure: true`
set automatically. No manual configuration needed.

## Keeper Readiness

Before resolving endpoints, the operator waits for the referenced CHK's pods to be in
`Running` phase. This handles the case where CHK and CHI are created simultaneously.

The timeout is configurable via the operator config:

```yaml
# ClickHouseOperatorConfiguration
spec:
  reconcile:
    coordination:
      keeper:
        readyTimeout: 120  # seconds (default: 120)
```

If the keeper pods don't become ready within the timeout, the CHI reconcile fails with
an `ErrKeeperNotReady` error and a Kubernetes Event is emitted on the CHI resource.

## Auto-Reconcile on Keeper Changes

The operator can optionally watch for CHK resource changes and automatically trigger
CHI reconcile when the referenced keeper completes a reconcile cycle.

```yaml
# ClickHouseOperatorConfiguration
spec:
  reconcile:
    coordination:
      keeper:
        readyTimeout: 120
        onKeeperResourceUpdate: reconcile  # "none" (default) or "reconcile"
```

When `onKeeperResourceUpdate: reconcile` is set:
- The operator watches all CHK resources in watched namespaces
- When a CHK transitions to `Completed` status, all dependent CHIs are re-reconciled
- This ensures ClickHouse picks up keeper topology changes (e.g., keeper scale-up/down)
- CHI reconcile is only triggered when CHK **completes** (not during InProgress)

## Troubleshooting

### Viewing Resolved Endpoints

The resolved keeper nodes are visible in the CHI's normalized status:

```bash
kubectl get chi my-chi -o json | jq '.status.normalizedCompleted.spec.configuration.zookeeper.nodes'
```

### Viewing Kubernetes Events

Keeper reference resolution failures emit Kubernetes Events:

```bash
kubectl get events --field-selector involvedObject.name=my-chi,reason=ReconcileFailed
```

### Common Issues

| Symptom | Cause | Fix |
|---|---|---|
| CHI stuck in InProgress | CHK pods not running | Check CHK status: `kubectl get chk` |
| CHI reconcile fails with timeout | `readyTimeout` too short | Increase `readyTimeout` in operator config |
| ClickHouse can't connect to keeper | Wrong namespace in keeper ref | Verify `namespace` field or omit for same-namespace |
| Only 1 ZK node resolved | `serviceType: service` used | Switch to `serviceType: replicas` (default) |
| CHI not updated after keeper scale | Watcher not enabled | Set `onKeeperResourceUpdate: reconcile` in operator config |

## Examples

- [Basic keeper reference](chi-examples/04-replication-zookeeper-07-keeper-ref.yaml)
- [All fields example](chi-examples/99-clickhouseinstallation-max.yaml)
- [Operator config with coordination](chi-examples/70-chop-config.yaml)
