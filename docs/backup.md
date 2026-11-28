# Backup and Restore

> **EXPERIMENTAL.** Operator-managed backup is new in 0.28.0. The `spec.backup` section and the
> `ClickHouseBackup`, `ClickHouseBackupSchedule` and `ClickHouseRestore` resources may change in a
> future release without a migration path. Test backups and restores in your environment before
> relying on them.

The operator can back up and restore a `ClickHouseInstallation` (CHI) with
[`clickhouse-backup`](https://github.com/Altinity/clickhouse-backup):

1. You turn on backup for a CHI with `spec.backup`. A Secret tells where to keep the backups (S3, GCS,
   Azure, ...).
2. The operator adds a `clickhouse-backup` sidecar to every ClickHouse Pod of the CHI.
3. You create resources to back up, to back up on a schedule, and to restore:

| Kind                       | Short  | Purpose                                                        |
|----------------------------|--------|----------------------------------------------------------------|
| `ClickHouseBackup`         | `chb`  | One backup of a CHI, or of one of its clusters                 |
| `ClickHouseBackupSchedule` | `chbs` | Recurring backups, with retention                              |
| `ClickHouseRestore`        | `chr`  | One restore of a backup into a CHI, or into one of its clusters |

Each resource names a CHI in the same namespace. The operator runs the resource itself, with SQL on the
sidecars. It creates no `Job`, `CronJob` or Pod. Each cluster of the CHI gets its own run, and `status`
shows the result of each cluster.

This page is the user guide. [backup-internals.md](backup-internals.md) tells how it works, and gives
the details of every check.

## Requirements

* **ClickHouse credentials for the operator.** Set `clickhouse.access.username` and `password` (or
  `clickhouse.access.secret`) in the operator configuration. The install bundle and the Helm chart set
  them in the Secret `clickhouse-operator`. Without them, no operation starts (`NoClickHouseAccess`).
  See [ClickHouse credentials](backup-internals.md#clickhouse-credentials).
* **A backup storage** that `clickhouse-backup` supports: S3 or S3-compatible (MinIO), GCS, Azure Blob,
  SFTP, FTP and others.
* **A persistent data volume** on every backup-enabled cluster, for example a `dataVolumeClaimTemplate`.
  Size it for the data plus one downloaded copy of a backup. See
  [Data volume](backup-internals.md#data-volume).
* **ZooKeeper or ClickHouse Keeper** to restore `Replicated*` tables, as replication needs it.
* **ClickHouse 23.3 or newer** to restore access entities (users, roles, grants). See
  [Access entities](#access-entities).

## Quick start

```bash
# The examples name no namespace. Apply them all to one namespace, together with
# docs/chk-examples/01-simple-1.yaml, the ClickHouseKeeperInstallation the CHI uses.

# 1. Create the storage Secret and a CHI with backup enabled (edit the Secret first)
kubectl apply -f docs/chb-examples/01-chi-with-backup.yaml

# 2. Back up once, or on a schedule with retention
kubectl apply -f docs/chb-examples/02-backup-once.yaml
kubectl apply -f docs/chb-examples/04-backup-schedule.yaml

# 3. Watch progress
kubectl get chb,chbs,chr
kubectl describe chb backup-demo-once

# 4. Start a new CHI from a backup. First set spec.backup.recovery.backupName to a backup name:
#    the BACKUP column of `kubectl get chb`, LAST-BACKUP of `kubectl get chbs`, or status.backups.
kubectl apply -f docs/chb-examples/07-recover-from-backup.yaml
```

Examples:

* [`01-chi-with-backup.yaml`](chb-examples/01-chi-with-backup.yaml) - storage Secret and a CHI with `spec.backup`
* [`02-backup-once.yaml`](chb-examples/02-backup-once.yaml) - one `ClickHouseBackup`
* [`03-backup-later.yaml`](chb-examples/03-backup-later.yaml) - a backup that starts later, with a deadline to start
* [`04-backup-schedule.yaml`](chb-examples/04-backup-schedule.yaml) - nightly `ClickHouseBackupSchedule` with retention
* [`05-backup-from-schedule.yaml`](chb-examples/05-backup-from-schedule.yaml) - the backup of a schedule, run once now
* [`06-restore.yaml`](chb-examples/06-restore.yaml) - `ClickHouseRestore` into a new CHI
* [`07-recover-from-backup.yaml`](chb-examples/07-recover-from-backup.yaml) - a new CHI that starts from a backup

## Enabling backup

Turn on backup for the whole CHI with `spec.backup`. A cluster can override it in
`spec.configuration.clusters[].backup`:

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: backup-demo
spec:
  backup:
    enabled: "yes"
    # The Secret with the clickhouse-backup storage settings
    storageConfig:
      valueFrom:
        secretKeyRef:
          name: backup-demo-storage
          key: config.yml
  configuration:
    clusters:
      - name: main
        layout:
          shardsCount: 2
          replicasCount: 2
      - name: scratch
        backup:
          # This cluster gets no sidecar and cannot be backed up
          enabled: "no"
```

| Field              | Meaning |
|--------------------|---------|
| `enabled`          | `"yes"` turns on backup. Off by default. Accepts the usual yes/no forms (`"yes"`, `"no"`, `true`, `false`, ...). |
| `storageConfig`    | `valueFrom.secretKeyRef`: the `name` of a Secret in the CHI namespace, and the `key` with the storage settings. Required to enable backup. See [Backup storage](#backup-storage). |
| `podTemplate`      | The pod template with the `clickhouse-backup` container that the sidecar is built on (image, `env`, resources). See [Customizing the sidecar](backup-internals.md#customizing-the-sidecar). |
| `preferredReplica` | The replica index, from 0, that a backup of one replica per shard uses. Default 0. See [Which hosts are involved](backup-internals.md#which-hosts-are-involved). |
| `naming`           | A template for the backup name of each shard, for example `shard{shardNum}-{YYYYMMDDHHIISS}`. Empty gives one name on every shard. See [Backup naming](backup-internals.md#backup-naming). |
| `recovery`         | The backup a new CHI starts from. See [Recover a new CHI from a backup](#recover-a-new-chi-from-a-backup). |

Rules:

* A cluster takes every field it leaves empty from `spec.backup`. A value on the cluster always wins.
* To leave out one cluster, set `enabled: "no"` on it.
* `spec.backup` can come from a `ClickHouseInstallationTemplate`, except `recovery`.
* Turning backup on or off adds or removes the sidecar. This rolls the Pods of that cluster only.
* A change of `podTemplate` or of the sidecar container also rolls the Pods.
* A change of the storage Secret or of `recovery` restarts nothing.

**Without a usable storage, the reconcile stops.** When the Secret is missing, has no `key`, or holds
settings that cannot be read, the operator aborts the reconcile of the whole CHI. It changes nothing.
The CHI status becomes `Aborted` with the reason `BackupConfigInvalid`, and a
`BackupStorageConfigInvalid` warning event names the problem. Fix the Secret or the spec. The operator
checks the Secret again once a minute. See
[Storage Secret](backup-internals.md#storage-secret).

### Backup storage

The storage Secret holds the storage part of the `clickhouse-backup` configuration file, as YAML
(`general`, `s3`, `gcs`, `azblob`, ...). You choose the key name, `config.yml` in the examples. See the
[clickhouse-backup configuration reference](https://github.com/Altinity/clickhouse-backup#default-config)
for all settings. A minimal S3 or MinIO Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: backup-demo-storage
type: Opaque
stringData:
  config.yml: |
    general:
      remote_storage: s3
    s3:
      # With MinIO, http://minio-hl.minio:9000
      endpoint: https://s3.us-east-1.amazonaws.com
      region: us-east-1
      bucket: clickhouse-backups
      # The default, see Storage paths
      path: backup/{namespace}/{installation}/{cluster}/{shard}
      access_key: "..."
      secret_key: "..."
      # Needed by MinIO and most S3-compatible stores
      # force_path_style: true
```

Other storage types use the same scheme. For GCS, set `remote_storage: gcs` and a `gcs` section with
`bucket`, `path` and `credentials_json`. For Azure, set `remote_storage: azblob` and an `azblob` section
with `account_name`, `account_key`, `container` and `path`.

Useful settings:

* **Compression:** `s3.compression_format` (`tar`, `zstd`, `gzip`, ...), and the same for `gcs` and
  `azblob`. Do not use `lz4`: `clickhouse-backup` refuses it, and every backup fails.
* **Server-side encryption:** `s3.sse` (for example `AES256` or `aws:kms`) with `s3.sse_kms_key_id`.
* **`clickhouse.restore_distributed_cluster`:** default `{cluster}`. See
  [Restore steps](backup-internals.md#restore-steps).

The operator controls some settings and ignores your value:

* `general.backups_to_keep_remote` is always `0`. Only the [retention](#retention) of a schedule
  deletes backups.
* `general.backups_to_keep_local` is ignored. The local copy is deleted after the upload.
* The REST API and ClickHouse credentials come from the operator.

The full list is in [What the operator runs](backup-internals.md#what-the-operator-runs).

**Secret changes.** Create or edit the Secret at any time. The sidecars use the new settings for their
next operation, within a few minutes, without a restart. Change it only while no backup or restore
runs. See [Secret changes](backup-internals.md#secret-changes).

### A storage per cluster

A cluster that names its own Secret keeps its backups in its own storage:

```yaml
spec:
  backup:
    enabled: "yes"
    storageConfig:
      valueFrom:
        secretKeyRef:
          name: backup-demo-storage
          key: config.yml
  configuration:
    clusters:
      # Keeps its backups as backup-demo-storage says
      - name: hot
      # Keeps its backups as backup-demo-storage-cold says
      - name: cold
        backup:
          storageConfig:
            valueFrom:
              secretKeyRef:
                name: backup-demo-storage-cold
                key: config.yml
```

To clear an inherited Secret on a cluster, set `secretKeyRef.name: ""` and `enabled: "no"`. The CRD
also requires `key` there, for example `key: config.yml`.

### The sidecar

The operator adds a container named `clickhouse-backup` to every Pod of a backup-enabled cluster. It
runs `clickhouse-backup server`:

* It shares the data volumes of ClickHouse, because it backs up and restores the data files.
* Its REST API listens on port 7171. ClickHouse in the same Pod calls it through the integration
  tables `system.backup_actions` and `system.backup_list`.
* Its API credentials are random. They are in the Secret `<chi-name>-auto-backup-secret`.
* By default it connects to ClickHouse as `default` from localhost. To use another user, add
  `CLICKHOUSE_USERNAME` and `CLICKHOUSE_PASSWORD` to `<chi-name>-auto-backup-secret`. The operator then
  creates that user, with access from localhost only.
* The default image is `altinity/clickhouse-backup:stable`, pulled `Always`. Pin a version with
  `podTemplate`.
* It has no resource requests or limits unless you set them. To keep a `Guaranteed` Pod, give it
  requests equal to its limits.

To set the image, `env`, resources or security context, name a pod template with a `clickhouse-backup`
container in `podTemplate`. See [Customizing the sidecar](backup-internals.md#customizing-the-sidecar)
and [What the operator runs](backup-internals.md#what-the-operator-runs).

### Storage paths

All shards use the same backup name. So each shard keeps its part of a backup under its own storage
path. `clickhouse-backup` expands the path on each host with these macros:

| Macro                 | Value                                               |
|-----------------------|-----------------------------------------------------|
| `{cluster}`           | The cluster name                                    |
| `{shard}`             | The shard name, by default its index in the cluster |
| `{replica}`           | The host name                                       |
| `{all-sharded-shard}` | The index of the host in the CHI                    |
| `{installation}`      | The CHI name                                        |
| `{namespace}`         | The namespace of the CHI                            |

* **Default path:** `backup/{namespace}/{installation}/{cluster}/{shard}`.
* **Default object disk path:** `object_disks/{namespace}/{installation}/{cluster}/{shard}`.
* A path in the storage Secret (`s3.path`, `gcs.path`, ...) or in the sidecar `env` (`S3_PATH`) wins.
* The operator configuration can change the defaults with `backup.storagePath` and
  `backup.storageObjectDiskPath`. Set both, each with `{shard}`. Backups made under the old paths stay
  there, and the operator no longer finds them.

Rules that the operator checks before every backup and restore:

* Each shard of each cluster needs its own storage path and its own object disk path. A
  [naming template](backup-internals.md#backup-naming) lets the shards of one cluster share a path.
* An `AllReplicas` backup needs `{replica}` in both paths. An empty object disk path is not checked.
* A `FirstPerShard` backup must not have `{replica}` in the path.

A refused run ends `Aborted` with the reason `StoragePathConflict`. The message names the shared path.
Keep the default paths unless you need something else. Do not define macros with these names in the
CHI. See [Storage paths](backup-internals.md#storage-paths) for the full check.

### Clusters of a CHI

Most CHIs have one cluster. Then leave `spec.cluster` empty on every resource, and skip this section.

With several clusters:

* A backup, schedule or restore works on every backup-enabled cluster, or only on its `spec.cluster`.
* Each cluster gets its own run. One cluster can fail while the others complete. The backup then ends
  `Failed` with the reason `PartiallyCompleted`.
* All clusters use the same backup name. To recover a whole CHI, use a backup that completed on every
  cluster, such as `status.lastBackupName` of a schedule.
* A backup is not a snapshot. Shards and clusters are backed up at about the same time, not at the same
  instant. Where shards must match, stop writes during the backup.
* Access entities belong to the whole CHI. Only a restore of all clusters restores them.

See [Clusters of a CHI](backup-internals.md#clusters-of-a-chi) for what the clusters share and what a
failed host leaves.

## ClickHouseBackup

Use a `ClickHouseBackup` to make one backup now or at a later time. See
[`02-backup-once.yaml`](chb-examples/02-backup-once.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseBackup"
metadata:
  name: backup-demo-once
spec:
  clickHouseInstallation: backup-demo
```

| Field                     | Default         | Meaning |
|---------------------------|-----------------|---------|
| `clickHouseInstallation`  |                 | The CHI to back up, in the same namespace. Set this or `scheduleRef`. |
| `scheduleRef.name`        |                 | Make one backup with the settings of this schedule. See [Run a schedule now](#run-a-schedule-now). |
| `cluster`                 | all             | Back up only this cluster. |
| `backupName`              | generated       | The backup name. Empty gives `<resource name>-<Unix time>`. |
| `scheduledTime`           | creation time   | The RFC 3339 time to start. See [Back up at a later time](#back-up-at-a-later-time). |
| `startingDeadlineSeconds` | no limit        | How many seconds after the scheduled time the backup can still start. |
| `diffFromRemote`          |                 | Make an [incremental backup](#incremental-backups) on this existing backup. |
| `configOverrides`         |                 | `clickhouse-backup` settings for this backup only. See [Config overrides](#config-overrides). |
| `schemaOnly`              | `false`         | Back up table and database definitions only. |
| `replicaSelection`        | `FirstPerShard` | `FirstPerShard` backs up one replica per shard. `AllReplicas` backs up every replica. |
| `tables`                  | all             | Table patterns to back up, e.g. `["db.table", "db.*"]`. |
| `skipTables`              |                 | Table patterns to leave out, e.g. `["db.tmp_*"]`. |
| `skipTableEngines`        |                 | Table engines to leave out, e.g. `["Kafka", "RabbitMQ"]`. |
| `partitions`              | all             | The partition ids to back up. |
| `activeDeadlineSeconds`   | `86400` (24h)   | The maximum run time on each cluster. |
| `timeouts`                | see [Timeouts](#timeouts) | How long the run waits for commands on the sidecars. |
| `taskID`                  |                 | A new value runs the backup again. See [Rerunning with spec.taskID](#rerunning-with-spectaskid). |

**`replicaSelection`.** `FirstPerShard` is complete for `Replicated*` tables. Use `AllReplicas` when
non-replicated tables hold different data on each replica. `AllReplicas` needs `{replica}` in the
[storage paths](#storage-paths).

**Rules.**

* A backup name starts with a letter or digit. It has only letters, digits, `_`, `-` and `.`, no `..`,
  and at most 128 characters.
* A `backupName` must not end in `-YYYYMMDDHHMMSS`. Schedules use that form.
* Table patterns use letters, digits, `_`, `.`, `*`, `?` and `-`. At most 64 items.
* System databases are never backed up.
* You cannot change `clickHouseInstallation`, `cluster` or `scheduleRef` after creation.
* A backup runs one time. To back up again, create a new resource, set a new `taskID`, or use a schedule.

**A one-off backup is never deleted for you.** Retention does not apply to it. Deleting the resource
keeps the backup in the storage. To delete it, run this SQL on one host of each shard:

```sql
INSERT INTO system.backup_actions(command) VALUES ('delete remote <backup>')
```

To have retention delete it, make it with `scheduleRef` instead.

### Back up at a later time

Set `scheduledTime` to start the backup later. Use RFC 3339 with an offset or `Z`. See
[`03-backup-later.yaml`](chb-examples/03-backup-later.yaml).

```yaml
spec:
  clickHouseInstallation: backup-demo
  scheduledTime: "2026-10-20T02:00:00+02:00"
  startingDeadlineSeconds: 3600
```

* Before the time, the backup is `Pending` with the reason `WaitingForScheduledTime`. You can still
  change the time or delete the backup.
* At the time, the operator checks the backup against the CHI as it is then, and names the backup.
* If the backup cannot start at that time, it starts as soon as possible. Examples: the operator is
  down, the CHI reconcile is not complete, or another operation runs.
* `startingDeadlineSeconds` limits this delay. A backup or cluster run that did not start in time fails
  with `StartingDeadlineExceeded`. Use one minute or more.
* Without `scheduledTime`, the deadline counts from creation.
* A GitOps tool can apply a backup again later. Set `startingDeadlineSeconds` on backups kept in git.

### Run a schedule now

Use `scheduleRef` to make one backup exactly as a schedule makes it, for example before an upgrade. See
[`05-backup-from-schedule.yaml`](chb-examples/05-backup-from-schedule.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseBackup"
metadata:
  name: pre-upgrade
spec:
  scheduleRef:
    name: nightly
```

* The backup takes the CHI, the clusters, the prefix and the options of the schedule.
* You can set only `cluster`, `diffFromRemote`, `scheduledTime`, `startingDeadlineSeconds` and
  `timeouts`. `backupName` and `configOverrides` are refused.
* The name is `<backupNamePrefix>-<YYYYMMDDTHHMMSS>`, with the UTC scheduled time, for example
  `default-backup-demo-nightly-20261012T101507`.
* Retention of the schedule keeps it while it is not older than the oldest scheduled backup that an
  entry keeps. Then retention deletes it.
* With an `incremental` entry, the backup is incremental on the newest backup of the schedule.
* It runs also while the schedule is suspended. It waits while the schedule does not exist yet
  (`ScheduleNotFound`).
* Two such backups with the same scheduled second get the same name, and the second fails.

## ClickHouseBackupSchedule

Use a `ClickHouseBackupSchedule` for recurring backups. Each entry (hourly, daily, ...) keeps its own
number of backups. See [`04-backup-schedule.yaml`](chb-examples/04-backup-schedule.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseBackupSchedule"
metadata:
  name: nightly
spec:
  clickHouseInstallation: backup-demo
  timeZone: Europe/Berlin
  time: "02:30"
  schedules:
    - period: hour
      keep: 24
    - period: daily
      keep: 7
    - period: weekly
      day: 0
      keep: 4
```

| Field                         | Default                        | Meaning |
|-------------------------------|--------------------------------|---------|
| `clickHouseInstallation`      | required                       | The CHI to back up, in the same namespace. |
| `cluster`                     | all                            | Back up only this cluster. |
| `schedules`                   | required                       | The entries. At most one entry per period. |
| `time`                        | `00:00`                        | `HH:MM` in `timeZone`, for all entries. Required with a `daily`, `weekly` or `monthly` entry. |
| `timeZone`                    | UTC                            | The IANA time zone of the entries, e.g. `Europe/Berlin`. |
| `suspend`                     | `false`                        | Start no new runs. A run in progress continues. |
| `startingDeadlineSeconds`     | no limit                       | How many seconds after its scheduled time a late run can still start. |
| `backupNamePrefix`            | `<namespace>-<chi>-<schedule>` | The start of every backup name. You can set it one time only. |
| `retention`                   |                                | Backups to keep in addition to the entries. See [Schedule changes](#schedule-changes). |
| `successfulRunsHistoryLimit`  | `3`                            | How many successful runs `status.recentRuns` keeps. |
| `failedRunsHistoryLimit`      | `1`                            | How many failed runs `status.recentRuns` keeps. |
| `schemaOnly`, `replicaSelection`, `tables`, `skipTables`, `skipTableEngines`, `partitions`, `activeDeadlineSeconds` | | The same as in a [`ClickHouseBackup`](#clickhousebackup). |
| `timeouts`                    | see [Timeouts](#timeouts)      | How long every run waits for commands on the sidecars. |
| `taskID`                      |                                | A new value resets an aborted schedule. |

Each entry of `schedules`:

| Field             | Meaning |
|-------------------|---------|
| `period`          | `minute`, `hour`, `6hours`, `daily`, `weekly` or `monthly`. |
| `day`             | `weekly`: `0`..`6`, `0` is Sunday. `monthly`: `1`..`31`. Required for these two periods only. |
| `keep`            | How many of the newest backups of this entry retention keeps. At least 1. |
| `incremental`     | Make incremental backups for this entry. At most one entry. |
| `configOverrides` | `clickhouse-backup` settings for the runs of this entry. |

### How a schedule runs

* **Scheduled times.** A schedule runs at the scheduled times of all its entries. In the example, that is every hour at
  :30. `hour` uses only the minute of `time`. `6hours` uses the hour modulo 6 and the minute.
  `minute` uses no time.
* **One run per scheduled time.** A run counts for every entry whose scheduled time it is. In the example, the run of
  Sunday 02:30 counts for the weekly, the daily and the hourly entry.
* **The heaviest entry decides.** The entry with the longest period sets `incremental` and
  `configOverrides` for the run.
* **Day of month.** A day after the end of the month means the last day. `day: 31` is the last day of
  every month.
* **Names.** A backup is named `<backupNamePrefix>-YYYYMMDDHHMMSS`, after the scheduled time in `timeZone`,
  for example `default-backup-demo-nightly-20261003023000`. A late run keeps the name of its scheduled time.
* **No overlap.** A scheduled time that comes while a run or another operation of the cluster runs waits for it.
* **Missed scheduled times.** As with a `CronJob`, only the latest missed scheduled time runs. A
  scheduled time older than
  `startingDeadlineSeconds` is skipped with a `ScheduledRunSkipped` warning.
* **Daylight saving time.** A scheduled time at a wall time that does not exist moves to the time the zone
  gives. A wall time that occurs twice runs once.

See [Scheduled times and status](backup-internals.md#scheduled-times-and-status) for the details.

**Example: weekly full backups, hourly incremental backups.**

```yaml
schedules:
  - {period: hour, keep: 24, incremental: true}
  - {period: weekly, time: "00:00", day: 0, keep: 4}
```

| Run                                    | Entries      | Backup |
|----------------------------------------|--------------|--------|
| Sunday 00:00                           | hour, weekly | Full. The weekly entry is the heaviest and is not incremental. |
| Sunday 01:00 to Monday 00:00 (24 runs) | hour         | Incremental on the backup before it. |
| Monday 01:00                           | hour         | Full. The chain has 24 incremental backups, which is the `keep` of the entry. |
| Monday 02:00 to Saturday 23:00         | hour         | Incremental. Retention deletes the oldest hourly backup and rebases the next one, so the chain stays 23 long. |

Retention keeps the 24 newest backups and the 4 newest Sunday 00:00 backups.

### Incremental backups

An incremental backup uploads only the data parts that are not in its base backup.

* **One-off:** `diffFromRemote: <backup>` on a `ClickHouseBackup`. The base must exist on every cluster
  of the backup.
* **Schedule:** the entry with `incremental: true` makes incremental backups on the newest backup of
  the schedule. The first backup is full.
* **No periodic full backup.** When retention deletes the oldest backup, it first rebases the next one.
  The rebased backup becomes full. With `{period: daily, keep: 7, incremental: true}`, the storage
  always has 1 full and 6 incremental backups, and the server uploads only the new parts each day.
* A run makes a full backup when the chain reaches the `keep` of the entry. With rebase, this happens
  only when another entry keeps the base, as in the example above.
* A run makes a full backup also when no base exists on every host.
* **Cost of rebase.** Rebase copies the parts that the next backup still needs, inside the storage.
  On S3 this is a server-side `CopyObject`, with no download and no upload.

**Keep the chain.** A restore of an incremental backup needs all its bases. Retention rebases a backup
before it deletes its base. Other ways to delete backups break the chain: `BACKUPS_TO_KEEP_REMOTE`, a
bucket lifecycle rule, or a manual delete.

### Config overrides

Use `configOverrides` to change `clickhouse-backup` settings for some backups or restores only. For example, keep
the monthly backups in a colder storage class:

```yaml
spec:
  clickHouseInstallation: prod
  schedules:
  - {period: daily,   time: "03:00", keep: 7, incremental: true}
  - {period: weekly,  time: "03:00", day: 0, keep: 4,  configOverrides: [S3_STORAGE_CLASS=STANDARD_IA]}
  - {period: monthly, time: "03:00", day: 1, keep: 12, configOverrides: [S3_STORAGE_CLASS=GLACIER_IR]}
```

* Each item is `KEY=VALUE`, with the environment variable name of the setting. At most 16 items.
* A scheduled run uses the overrides of its heaviest entry.
* A restore uses its own `configOverrides` for every download and restore command.
* The operator refuses settings that move the backup, carry credentials or weaken security. See
  [Config overrides](backup-internals.md#config-overrides) for the list.
* Use instant-retrieval storage classes, not `GLACIER` or `DEEP_ARCHIVE`. Retention and restores must
  read the backups.

### Retention

After each successful run, the run deletes old backups of the schedule on its cluster:

* **Only the backups of this schedule count:** names that start with its `backupNamePrefix`. Retention
  never deletes one-off backups or backups of other schedules.
* **Each entry keeps its newest `keep` backups.** A backup stays while any entry keeps it.
* The newest complete backup always stays.
* A backup made with [`scheduleRef`](#run-a-schedule-now) stays while it is recent.
* **Incomplete backups do not count.** A backup is incomplete when a shard does not list it as
  complete. A shard added later does not count for the backups made before it.
  Retention deletes an incomplete backup once a newer complete backup exists and no upload of it runs.
* A failed retention gives a warning. It does not fail the backup.

**Give each schedule its own prefix.** Two schedules with the same prefix on one storage path delete
each other's backups. The default prefix is unique in one Kubernetes cluster. A prefix you set must be
unique on that storage path.

See [Retention](backup-internals.md#retention) for the full rules.

### Schedule changes

A change of the entries or of `timeZone` can make the next run delete backups that the schedule kept
until then. `spec.retention` controls this:

```yaml
spec:
  schedules:
    - {period: daily, time: "03:00", keep: 7}   # was 02:00, with a weekly entry keep 4
  retention:
    onScheduleChange: Hold                     # the default
    keepBackups:
      - name: default-backup-demo-nightly-20261001020000
        until: "2027-01-01T00:00:00Z"          # kept until then
      - name: default-backup-demo-nightly-20261020T153000  # kept while listed
    # Release the held backups of this change and of all earlier changes:
    # releaseHeld: "2026-11-12T12:00:00Z"
```

| Field                        | Meaning |
|------------------------------|---------|
| `onScheduleChange: Hold`     | The default. The backups that the old entries kept stay until the old entries would delete them. |
| `onScheduleChange: Prune`    | The next run applies the change at once. |
| `keepBackups`                | Backups to keep, as `{name, until}`. Without `until`, a backup stays while it is listed. At most 512. |
| `releaseHeld`                | A `changeID` from `status.retention.previous`. Releases the backups held for it and for all earlier changes. |

`status.clusters[].heldBackups` lists the backups that only a held change keeps, each with the time it
expires. A change of `incremental` is not a change. See
[Schedule changes](backup-internals.md#schedule-changes) for how the expiry is computed.

### Suspend and status

* Set `suspend: true` to stop new runs. A run in progress continues.
* To make one backup now, create a `ClickHouseBackup` with [`scheduleRef`](#run-a-schedule-now).
* `kubectl get chbs` shows the state, the last success and the last backup.

`status.phase`:

* `Active`: the schedule runs. A restore can hold it (reason `SuspendedForRestore`).
* `Suspended`: `spec.suspend` is set.
* `Aborted`: the schedule needs your action. `status.reason` tells why. After a failed run
  (`LastBackupFailed`) the schedule still runs, and the next success makes it `Active` again.

Other status fields:

* `nextScheduleTime`: the next scheduled time.
* `lastBackupName`, `lastSuccessfulTime`: the newest backup that every cluster made. Use it to
  [recover the whole CHI](#recover-a-new-chi-from-a-backup).
* `clusters[]`: the last run, success and failure of each cluster.
* `active`, `recentRuns`: the runs in progress and the finished runs.
* `backups[]`: the backups of the schedule in storage. See [List of backups](#list-of-backups).

## ClickHouseRestore

Use a `ClickHouseRestore` to restore a backup into a CHI. See
[`06-restore.yaml`](chb-examples/06-restore.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseRestore"
metadata:
  name: backup-demo-restore
spec:
  clickHouseInstallation: backup-demo-restored
  backupName: default-backup-demo-nightly-20261003023000
```

| Field                      | Default            | Meaning |
|----------------------------|--------------------|---------|
| `clickHouseInstallation`   | required           | The CHI to restore into, in the same namespace. A new, empty CHI is best. |
| `cluster`                  | all                | Restore only into this cluster. |
| `backupName`               | required           | The backup to restore. |
| `sourceInstallation`       | the target CHI     | The CHI that made the backup, if it is another CHI. See [Restore from another CHI](#restore-from-another-chi). |
| `sourceNamespace`          | the target namespace | The namespace of that CHI, if it is another one. |
| `schemaOnly`               | `false`            | Restore table and database definitions only. |
| `tables`                   | all                | Restore only these table patterns, e.g. `["db.*"]`. |
| `partitions`               | all                | Restore only these partition ids. See [Restore filters](#restore-filters). |
| `skipTables`               |                    | Table patterns to leave out. |
| `skipTableEngines`         |                    | Table engines to leave out. |
| `databaseMapping`          |                    | Restore a database under another name: `{source: target}`. |
| `configOverrides`          |                    | `clickhouse-backup` settings for this restore only, e.g. `[DOWNLOAD_CONCURRENCY=8]`. See [Config overrides](#config-overrides). |
| `rbac`                     | `false`            | Also restore the [access entities](#access-entities). Only with an empty `cluster`. |
| `overwrite`                | `false`            | Without `tables`, recreate the databases of the backup completely. |
| `validateTopology`         | `true`             | Refuse the restore if a target shard does not have the backup. |
| `replicatedCopyToDetached` | `true`             | Download the data to every replica, so that replicas do not fetch it over the network. |
| `resumeDownload`           | `true`             | A rerun continues the downloads of earlier runs. `false` downloads again. |
| `activeDeadlineSeconds`    | `86400` (24h)      | The maximum run time on each cluster. |
| `timeouts`                 | see [Timeouts](#timeouts) | How long the run waits for commands on the sidecars. |
| `taskID`                   |                    | A new value runs a failed restore again. See [Rerunning with spec.taskID](#rerunning-with-spectaskid). |

**The target CHI needs:**

* backup enabled, with the same storage as the source,
* the same cluster names and shard layout as the source.

**What a restore does.** On each cluster:

1. All hosts download the backup. A host without enough disk fails here, before anything changes.
2. Each replica restores the schema. `clickhouse-backup` drops each table of the backup that exists,
   and creates it again.
3. The first replica of each shard restores the data. Replication sends it to the other replicas.
4. After a successful restore, every host deletes its downloaded copy.

See [Restore steps](backup-internals.md#restore-steps) for the commands and the fallbacks.

**`overwrite`.** A restore always recreates the tables of the backup. Other tables stay. With
`overwrite: true` and no `tables`, it recreates the whole databases of the backup. Tables that are not
in the backup are then dropped too.

**A failed restore keeps its downloads.** You can examine them, and a new restore of that backup
continues the download. The `OrchestratorRestoreDownloadsKept` warning names the hosts. A copy uses
as much disk as the backup data. To free the disk, run this on each host:

```sql
INSERT INTO system.backup_actions(command) VALUES ('delete local <backup>')
```

**A copy of a running CHI needs its own `zookeeper.root`** when both CHIs use the same Keeper.
Otherwise a restored `Replicated*` table can join the replication of the source table. A `TRUNCATE` or
`DROP PARTITION` on the copy then also changes the source.

### Restore filters

Use these fields to restore less than the full backup:

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseRestore"
metadata:
  name: backup-demo-restore-events
spec:
  clickHouseInstallation: backup-demo-restored
  backupName: default-backup-demo-nightly-20261003023000
  tables: ["analytics.*"]
  # The data of two months only
  partitions: ["202609", "202610"]
  # Neither the scratch tables nor the queue consumers
  skipTables: ["analytics.tmp_*"]
  skipTableEngines: ["Kafka"]
  # Into a database of another name, next to the live one
  databaseMapping:
    analytics: analytics_restored
```

* **`partitions`** replaces only these partitions in the existing tables:
  * There is no schema step. The tables must exist.
  * `clickhouse-backup` drops each of these partitions, then attaches the parts of the backup.
  * Not with `schemaOnly` or `rbac`.
  * An id has letters, digits, `_` and `-`, as `partition_id` in `system.parts`.
* **`skipTables` and `skipTableEngines`** leave out tables in every step. System databases are always
  left out.
* **`databaseMapping`** creates the tables of each named database in its target database. In the
  example, `analytics` does not change. Rules:
  * A database can be mapped one time only, and not to itself.
  * A target cannot be the target of another mapping, or be mapped itself.
  * System databases cannot be a source or a target.

A restore with a value that fails these checks is `Aborted`.

### Access entities

A backup holds the access entities created with SQL: users, roles, grants, row policies, quotas and
settings profiles.

* `rbac: true` restores them with the schema on every host. Existing entities with the same names are
  replaced.
* `rbac` needs an empty `cluster`, because access entities belong to the whole CHI.
* A [recovery](#recover-a-new-chi-from-a-backup) always restores them.
* While such a restore runs, the backups of its clusters wait.
* ClickHouse 23.3 or newer is necessary. An older ClickHouse loads the entities only after a restart.
* Drop entities that were created again after the backup under the same name before you restore.
  ClickHouse keeps only one of the two.

A restore without `rbac`, or of one cluster, completes with the reason `RBACNotRestored` and a warning.
Users and row policies that exist only as SQL entities are then missing. Restore them with `rbac: true`,
or by hand in the sidecar of every host:

```bash
kubectl exec chi-backup-demo-main-0-0-0 -c clickhouse-backup -- \
  clickhouse-backup restore_remote --rbac-only default-backup-demo-nightly-20261003023000
```

### Restore from another CHI

Set `sourceInstallation` (and `sourceNamespace` when the paths have `{namespace}`) to restore a backup
that another CHI made. The restore reads the backup from the storage paths of that CHI.

* The target needs the same cluster names and shard layout.
* You cannot restore an `AllReplicas` backup into a CHI with another name.
* The restore holds the schedules of the source CHI in the same namespace, so that their retention
  does not delete the backup it reads. Restore outside their backup window.
* Keep the backup names of the two CHIs apart. The default `backupNamePrefix` does this.

See [Restoring into another CHI](backup-internals.md#restoring-into-another-chi) for the checks and refusals.

### Restore safety

Before it changes anything, each restore checks the storage paths and, with `validateTopology`, that
every target host has the backup. Every host then downloads the backup before any host restores.

While a restore runs:

* **Do not change the target CHI.** A rolling update restarts the sidecars and leaves a partial
  restore.
* **Do not change the storage Secret.** Some hosts would restore with the old settings.
* The schedules of its clusters do not start runs (`SuspendedForRestore`).

A restore that fails after it changed something holds the schedules of its CHI
(`SuspendedForFailedRestore`). Delete the failed `ClickHouseRestore` to release them. See
[One operation at a time per cluster](backup-internals.md#one-operation-at-a-time-per-cluster).

We recommend: restore into a new, empty CHI, check it, then move the clients to it.

## Recover a new CHI from a backup

To start a new CHI from a backup, set `spec.backup.recovery`. See
[`07-recover-from-backup.yaml`](chb-examples/07-recover-from-backup.yaml).

```yaml
spec:
  backup:
    enabled: "yes"
    storageConfig:
      valueFrom:
        secretKeyRef:
          name: backup-demo-storage
          key: config.yml
    recovery:
      backupName: default-backup-demo-nightly-20261003023000
      # The CHI that made the backup, if it is not this CHI. The backup is under its storage paths.
      sourceInstallation: backup-demo
      # Its namespace, if it is another one and the storage paths contain {namespace}
      # sourceNamespace: team-a
```

What happens:

1. After the first reconcile of the CHI, the operator creates a `ClickHouseRestore` named
   `<chi>-<cluster>-recovery` for each backup-enabled cluster. It restores every table and the access
   entities.
2. The schedules of these clusters wait until the recovery ends.
3. When all recovery restores complete, the operator sets the annotation
   `clickhouse.altinity.com/recovered-from` to the backup name.
4. After that, `recovery` has no effect. It restarts nothing.

Rules:

* The storage Secret must exist before you apply the CHI.
* Each recovered cluster needs the same name and shard layout as in the source.
* A cluster can name its own backup, or `""` for no recovery:

```yaml
  configuration:
    clusters:
      - name: main
      # This cluster has its own schedule
      - name: events
        backup:
          recovery:
            backupName: default-backup-demo-events-20261003-023000
      # This cluster is new, and in no backup
      - name: scratch
        backup:
          recovery:
            backupName: ""
```

* `recovery` is read from the CHI only, never from a template.
* If the source CHI still runs on the same Keeper, give the new CHI its own `zookeeper.root`.
* With `sourceInstallation`, the recovery holds the schedules of the source CHI while it reads.
  Recover outside their backup window.

**Schedules of a recovered CHI.** When the backup name starts with `<namespace>-<chi>-` of this CHI, the
operator holds every schedule of the CHI with the default prefix (`SharesSourcePrefix`). They could
delete the backups of the source. Set `backupNamePrefix` on each schedule to release it. See
[Two CHIs on one storage path](backup-internals.md#two-chis-on-one-storage-path).

**A failed recovery.** The operator gives a warning event on the CHI. Then do one of these:

* **Try again.** Fix the cause, then delete the failed `<chi>-<cluster>-recovery` restore. The operator
  creates a new one.
* **Restore by hand.** Create a `ClickHouseRestore` with `overwrite: true` (and `rbac: true` for the
  whole CHI). When it completes, set the `clickhouse.altinity.com/recovered-from` annotation. Then
  delete the failed recovery restore.
* **Give up.** Set `recovery.backupName: ""` (also in your manifest), then delete the failed restore.

See [Recovery steps](backup-internals.md#recovery-steps) for the details and the order of these steps.

## Rerunning with spec.taskID

A `ClickHouseBackup` or `ClickHouseRestore` runs once. To run it again, set `spec.taskID` to a new
value. A value that `status.taskID`, `status.taskIDsStarted` or `status.taskIDsCompleted` already has
does nothing.

```yaml
spec:
  clickHouseInstallation: prod
  taskID: "2"   # any new value reruns the backup
```

Or: `kubectl patch chb before-upgrade --type=merge -p '{"spec":{"taskID":"3"}}'`.

| The resource is                | A new `taskID` |
|--------------------------------|----------------|
| A completed backup             | Makes a new backup with a new name. The old backup stays. With `backupName`, change it together with `taskID`. |
| A failed or aborted backup     | Makes the backup again, with the same name, on the clusters where it did not complete. |
| A failed or aborted restore    | Runs again on the clusters where it did not complete, and continues their downloads. |
| A completed restore            | Does nothing (`TaskIDIgnored`). Create a new `ClickHouseRestore`. |
| An aborted schedule            | Validates it again. It starts no run. |
| A pending or running resource  | Does nothing, now or later (`TaskIDIgnored`). |

See [Reruns](backup-internals.md#reruns) for what a rerun deletes and keeps.

## Deleting a backup, restore or schedule

* **A completed backup:** the resource goes. The backup stays in the storage.
* **A backup that did not complete:** the operator stops it and deletes what it left on the hosts.
  What it cannot delete is listed in a `DeletionLeftover` warning, with the SQL to delete it.
* **A restore:** the operator waits for the run in progress to finish, then the resource goes. The
  downloads stay.
* **A schedule:** no new run starts. The backups of the schedule stay in the storage.

The operator holds each resource with a finalizer until it has cleaned up. If the operator is gone,
the resource stays `Terminating`. Then remove the finalizer by hand:
`kubectl patch chb <name> --type=merge -p '{"metadata":{"finalizers":null}}'` (the same for `chr` and
`chbs`). See [Deleting](backup-internals.md#deleting-a-backup-restore-or-schedule).

## Timeouts

`spec.timeouts` sets how long the operator waits for the commands on the sidecars. All fields are
optional. Values are Go durations, such as `"12h"` or `"90m"`.

| Field                | Default | Bounds       | Meaning |
|----------------------|---------|--------------|---------|
| `actionTimeout`      | `12h`   | `1s`..`720h` | The maximum wait for one command on one host. |
| `unreachableTimeout` | `10m`   | `1s`..`168h` | How long the run retries a host that does not answer. |
| `errorResponsesMax`  | `5`     | `1`..`1000`  | How many reads in a row can find no record of a command before it counts as lost. |
| `httpReceiveTimeout` | `4h`    | `1s`..`168h` | The HTTP timeout of each command. Long deletes need it. |

A field that is not set comes from, in order:

1. the resource,
2. for a backup with `scheduleRef`, the schedule,
3. `backup.timeouts` in the [operator configuration](operator_configuration.md#backup),
4. the default above.

```yaml
spec:
  timeouts:
    actionTimeout: 24h        # a slow upload may take a day
    unreachableTimeout: 30m   # ride out a rolling restart of the pods
```

A value out of bounds makes the resource `InvalidSpec`. In the operator configuration, it is ignored.
`activeDeadlineSeconds` (24h by default) limits the whole run of each cluster.

## Status, events and metrics

```bash
kubectl get chb,chbs,chr
kubectl describe chb backup-demo-once             # conditions and events
kubectl get chb backup-demo-once \
  -o jsonpath='{range .status.clusters[*]}{.name}{"\t"}{.phase}{"\t"}{.run.step}{"\n"}{end}'
# The commands that the run of a cluster sent, and how each one ended
kubectl get chb backup-demo-once -o jsonpath='{.status.clusters[0].run.actions}'
kubectl get chbs nightly -o jsonpath='{.status.recentRuns}'
# On a host: what its sidecar ran
kubectl exec chi-backup-demo-main-0-0-0 -c clickhouse -- clickhouse-client -q \
  "SELECT command, status, start, finish, error FROM system.backup_actions ORDER BY start DESC LIMIT 10"
```

**Phase of a backup or restore:**

* `Pending`, `Running`: not all clusters finished.
* `Completed`: all clusters completed.
* `Failed`: a run failed, or only some clusters completed (`PartiallyCompleted`).
* `Aborted`: the operator could not run it. Examples: an invalid spec, a missing or stopped CHI, or a
  passed starting deadline.

`status.clusters[]` has the phase, hosts, times and message of each cluster. `status.backupName` is the
backup made or restored.

### List of backups

`status.backups[]` of a schedule lists its backups in storage, newest first, at most 1024:

```bash
kubectl get chbs nightly -o jsonpath='{range .status.backups[*]}{.cluster}{"\t"}{.name}{"\t"}{.state}{"\t"}{.size}{"\t"}{.base}{"\n"}{end}'
```

| Field     | Meaning |
|-----------|---------|
| `cluster` | The cluster whose storage holds the backup. |
| `name`    | The backup name. With a naming template, the name on the first shard. |
| `created` | The earliest creation time on a shard. |
| `size`    | The sum of the sizes on all hosts, in bytes. |
| `state`   | `Complete`: every shard has it, except shards added after it. `Incomplete`: such a shard does not have it. `Broken`: a host reports it broken. |
| `base`    | The base of an incremental backup. Empty for a full backup. |

* Retention reads the list after each successful run on a cluster and leaves out what it deleted.
  `status.clusters[].backupsListTime` tells when. The operator reads no list between runs: a delete by
  hand shows at the next run.
* Only backups whose name starts with `backupNamePrefix` are listed. A `ClickHouseBackup` with
  `scheduleRef` makes such backups, so they show at the next run of the schedule.
* `status.backups[]` of a `ClickHouseBackup` lists the backup it made on each cluster, read once when the
  run completes (`status.backupsListTime`). It is not listed for `schemaOnly`.

**Conditions.** Every resource has `Validated` and `Ready`. Backups and restores also have `Started`.
Schedules also have `LastBackupSucceeded`. The `reason` tells why, and the `message` gives the details.
Reasons you will see often:

| Reason                         | Meaning |
|--------------------------------|---------|
| `CHINotFound`, `CHINotReady`, `CHIStopped` | The CHI is missing, not reconciled, or stopped. The resource waits. |
| `NoClickHouseAccess`           | The operator has no ClickHouse credentials. See [Requirements](#requirements). |
| `OperationInProgress`          | Another operation runs on the cluster. The resource waits. |
| `CompletedWithoutData`         | The backup completed but holds no data. Check `tables` and `partitions`. |
| `PartiallyCompleted`           | Failed on some clusters, completed on the others. |
| `FailedUnchanged`              | A restore failed before it changed anything. |
| `StoragePathConflict`          | Two shards or clusters share a storage path. See [Storage paths](#storage-paths). |
| `SuspendedForRestore`          | A restore holds the schedule. |

See [Conditions and reasons](backup-internals.md#conditions-and-reasons) for the full list.

**Events.** The operator posts events on each resource: phase changes (`BackupCompleted`,
`RestoreFailed`, ...), the start and end of each cluster, and every command (`OrchestratorCommand`).
Warnings tell what to fix.

**Metrics.** The operator exports these, with the labels `namespace`, `chi` and `cluster`:

* `clickhouse_operator_backups_started`, `clickhouse_operator_backups_completed`,
  `clickhouse_operator_backups_failed`
* `clickhouse_operator_restores_started`, `clickhouse_operator_restores_completed`,
  `clickhouse_operator_restores_failed`
* `clickhouse_operator_backup_duration_seconds`
* `clickhouse_operator_backup_last_success_timestamp`, `clickhouse_operator_backup_last_failure_timestamp`

Alert on the age of the last success of each cluster. The counters start from zero when the operator
starts. The timestamps come back from the status after a restart. They go away for a cluster that no
longer gets backups.

## Limitations and security notes

* **Experimental API.** Fields and behavior can change in a future release.
* **Restrict the sidecar REST API.** It listens on port 7171 with plain HTTP and can run commands in
  the sidecar. Block it with a NetworkPolicy. See [Security](backup-internals.md#security).
* **No consistency across shards or clusters.** A backup is a set of per-shard backups with one name.
* **Give every shard its own storage path.** The default does this. See [Storage paths](#storage-paths).
* **Give every schedule on one storage path its own prefix.** Otherwise they delete each other's backups.
* **Delete backups only through retention.** A lifecycle rule or a manual delete can break incremental
  chains. See [Incremental backups](#incremental-backups).
* **Backups made before a shard was added have no copy of it.** A restore of one into the cluster
  with the new shard fails on that shard.
* **Hosts that leave a schedule keep their backups.** Retention no longer reaches them. See
  [Hosts that leave a schedule](backup-internals.md#hosts-that-leave-a-schedule).
* **Do not change a CHI or its storage Secret during a restore.** See [Restore safety](#restore-safety).
* **Change the credentials in `<chi-name>-auto-backup-secret` only while no backup or restore runs.**
  A run can hang until its deadline. See [Secret changes](backup-internals.md#secret-changes).
* **A restore writes data on one replica per shard.** Non-replicated tables are restored on that
  replica only.
* **The layout must match.** A restore into fewer shards leaves out the data of the extra shards.
* **Clusters that share tables are not independent.** See
  [Clusters that share data](backup-internals.md#clusters-that-share-data).
* **Access entities need ClickHouse 23.3 or newer.**
* **A CHI with several backup-enabled clusters** needs sidecar settings that the operator can read:
  the storage Secret and `env`, not `envFrom`. See [Storage paths](backup-internals.md#storage-paths).
* **Immutable fields are enforced only on Kubernetes 1.25 and newer.** See
  [Validation](backup-internals.md#validation).
* **A deleted operation leaves its running command.** The next operation on that host waits for it.
* **Backup traffic does not follow `security.policy: Enforced` or FIPS settings.** With
  `security.images.policy: FIPSRequired`, use a FIPS build of the sidecar image.
* **`ClickHouseKeeperInstallation` is not covered.**
