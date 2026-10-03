# Backup and Restore

The operator can manage **automated backup and restore** for ClickHouse using
[`clickhouse-backup`](https://github.com/Altinity/clickhouse-backup) through three custom
resources:

| Kind | Short | Purpose |
|------|-------|---------|
| `ClickHouseBackup` | `chb` | One-off backup of a `ClickHouseInstallation` |
| `ClickHouseBackupSchedule` | `chbs` | Recurring backups (managed Kubernetes `CronJob`) |
| `ClickHouseRestore` | `chr` | One-off restore of a remote backup |

These resources reference a `ClickHouseInstallation` (CHI) by name in the same namespace.
The operator reconciles them into Kubernetes `Job`/`CronJob` resources that drive
`clickhouse-backup`. The operator owns those jobs, so they are garbage-collected when the
custom resource is deleted, and their status is reflected back on the custom resource.

## Architecture

`clickhouse-backup` must run **as a sidecar** in the ClickHouse pods, because it needs
local access to `/var/lib/clickhouse`. The operator does **not** inject this sidecar; the
generated jobs only *trigger* it remotely by inserting commands into the
`system.backup_actions` integration table of each target host (the sidecar then executes
`create_remote` / `restore_remote` locally and replicates the result to remote storage).

```
ClickHouseBackupSchedule ──► CronJob ─┐
ClickHouseBackup         ──► Job ──────┼─► clickhouse-client ──► INSERT INTO system.backup_actions
ClickHouseRestore        ──► Job ──────┘        │
                                                ▼
                            clickhouse-backup sidecar (port 7171) ──► S3 / GCS / Azure
```

## Prerequisite: the clickhouse-backup sidecar

Add the sidecar to your CHI via a `podTemplate` and set
`API_CREATE_INTEGRATION_TABLES=true`. See
[`chb-examples/01-prerequisite-chi-with-sidecar.yaml`](chb-examples/01-prerequisite-chi-with-sidecar.yaml).

Key requirements:

* `API_CREATE_INTEGRATION_TABLES=true` — exposes `system.backup_actions`, which the jobs use.
* A configured remote storage (`REMOTE_STORAGE`, `S3_BUCKET`, …).
* For **sharded** clusters, include the `{shard}` macro in the remote path
  (e.g. `S3_PATH: backup/shard-{shard}`) so each shard's backup is stored separately under
  the same backup name.

If no sidecar is detected in the CHI pod templates, the custom resource reports a
`SidecarPresent=False` condition.

## Cluster awareness

ClickHouse replicates `Replicated*MergeTree` data across the replicas of a shard, so a
backup only needs **one replica per shard**:

* `replicaSelection: FirstPerShard` (default) — back up the first replica of each shard.
  Correct and storage-efficient for replicated tables.
* `replicaSelection: AllReplicas` — back up every replica. Use this if the cluster holds
  **non-replicated** (plain `MergeTree`) or local `Distributed` tables, whose data differs
  between replicas; otherwise those tables would only be captured on the first replica.

On **restore** the operator restores both schema and data on the **first replica of each
shard**. For `Replicated*` tables the schema `CREATE` is issued **`ON CLUSTER`** so every
replica is created with an identical Keeper path; native ClickHouse replication then clones
the data to the remaining replicas.

> **Important for replicated restore:** the sidecar must set
> `restore_schema_on_cluster` (env `RESTORE_SCHEMA_ON_CLUSTER`) to the cluster name (or the
> `{cluster}` macro). Without it, clickhouse-backup rewrites the replica path per node and
> the replicas land on **divergent Keeper paths that never sync**. This also requires
> distributed DDL, which the operator configures whenever a CHI uses ZooKeeper/Keeper.

## Restore safety

Restore is the most destructive operation, so the operator follows the conventions of
mature database operators (e.g. CloudNativePG):

1. **Prefer restoring into a fresh, empty CHI.** In-place restore over a live cluster is
   supported but guarded.
2. **Preflight validation** runs before any data is touched and is reported through
   `status.conditions`:
   * the target CHI must exist and be in the `Completed` state;
   * with `validateTopology: true` (default) every target host must be reachable, ensuring
     the full cluster is up before a `ReplicatedMergeTree` restore (mismatched topology is
     the primary cause of Keeper-path corruption).
3. **Overwrite guard** — when `overwrite: false` (default) the restore is **refused** if any
   target host already holds user tables. Set `overwrite: true` to drop and recreate them
   (`clickhouse-backup --rm`).
4. The restore Job is one-shot (`backoffLimit: 0`, `restartPolicy: Never`).

## Examples

* [`01-prerequisite-chi-with-sidecar.yaml`](chb-examples/01-prerequisite-chi-with-sidecar.yaml) — CHI with the sidecar.
* [`02-backup-once.yaml`](chb-examples/02-backup-once.yaml) — one-off `ClickHouseBackup`.
* [`03-backup-schedule.yaml`](chb-examples/03-backup-schedule.yaml) — recurring `ClickHouseBackupSchedule`.
* [`04-restore.yaml`](chb-examples/04-restore.yaml) — `ClickHouseRestore`.

```bash
kubectl apply -f docs/chb-examples/01-prerequisite-chi-with-sidecar.yaml
kubectl apply -f docs/chb-examples/03-backup-schedule.yaml

kubectl get chb,chbs,chr
kubectl get jobs,cronjobs -l clickhouse.altinity.com/app=clickhouse-backup
```

## Authentication

If the trigger jobs need credentials to connect to ClickHouse, reference a Secret with
`CLICKHOUSE_USER` and `CLICKHOUSE_PASSWORD` keys via
`spec.clickHouseCredentialsSecretName` (or `spec.backupTemplate.clickHouseCredentialsSecretName`
for schedules).

## Backup options

`ClickHouseBackup` (and `ClickHouseBackupSchedule.backupTemplate`) support:

- `tables` — clickhouse-backup `--tables` pattern (e.g. `mydb.*`) to back up only matching tables.
- `partitions` — list of partition ids to back up.
- `diffFromRemote` (one-off backup) — name of an existing remote backup to take an **incremental**
  backup against (`--diff-from-remote`); the base backup must still exist remotely.
- `keepLastRemote` — **retention**: keep only the N most recent remote backups; older ones are
  pruned (best-effort, via `system.backup_list` + a `delete remote` action) after each backup.
- `verify` (one-off backup) — run a verification Job after the backup that downloads it and checks
  integrity (no cluster data is touched); the result is surfaced as the `Verified` condition.

## Compression and encryption

These are configured on the **clickhouse-backup sidecar** (not operator fields) and apply to every
backup it runs:

- Compression: `COMPRESSION_FORMAT` (`tar`, `lz4`, `zstd`, `gzip`, …) and `COMPRESSION_USE_MULTI_THREAD`.
- Encryption (object-storage server-side): S3 `S3_SSE` / `SSE_KMS_KEY_ID` / `SSE_CUSTOMER_KEY`,
  Azure `SSE_KEY`, GCS `ENCRYPTION_KEY` (CSEK).

See the sidecar env in
[`chb-examples/01-prerequisite-chi-with-sidecar.yaml`](chb-examples/01-prerequisite-chi-with-sidecar.yaml).

## Bootstrap a new cluster from a backup

Annotate a fresh `ClickHouseInstallation` to auto-restore once it is up:

```yaml
metadata:
  annotations:
    clickhouse.altinity.com/recover-from-backup: "my-backup-name"
    # optional: Secret (CLICKHOUSE_USER/CLICKHOUSE_PASSWORD) for the restore to authenticate
    clickhouse.altinity.com/recover-credentials-secret: "ch-backup-creds"
```

Once the CHI reaches `Completed`, the operator creates a one-time `ClickHouseRestore`
(`<chi>-bootstrap`) and stamps `clickhouse.altinity.com/recovered-from` so the recovery fires
exactly once.

## Monitoring

The operator exports backup/restore metrics on its existing Prometheus endpoint (`:9999/metrics`):
`clickhouse_operator_backups_started` / `_completed` / `_failed`,
`clickhouse_operator_restores_started` / `_completed` / `_failed`,
`clickhouse_operator_backup_duration_seconds`,
`clickhouse_operator_backup_last_success_timestamp`, and
`clickhouse_operator_backup_verifications_failed` (labels: `namespace`, `clickhouse_installation`).
It also emits Kubernetes Events (`kubectl describe chb|chr`) on start, completion and failure.

The repository additionally ships Prometheus alert rules for the `clickhouse-backup` sidecar
([`deploy/prometheus/prometheus-alert-rules-backup.yaml`](../deploy/prometheus/prometheus-alert-rules-backup.yaml)).

## Limitations

* Host service names are resolved from the cluster `layout` (`shardsCount`/`replicasCount`)
  using the default host naming scheme. Clusters defined with explicit shard/replica lists
  or custom host names are a planned follow-up.
* Backup/restore is triggered per host through `system.backup_actions`; the sidecar must be
  reachable on the ClickHouse native port (default `9000`).
