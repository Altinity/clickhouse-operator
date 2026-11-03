# Backup and Restore

> **EXPERIMENTAL.** Operator-managed backup is new in 0.28.0. The `spec.backup` section and the
> `ClickHouseBackup`, `ClickHouseBackupSchedule` and `ClickHouseRestore` resources may change in a
> future release without a migration path. Test backups and restores in your environment before
> relying on them.

The operator can back up and restore a `ClickHouseInstallation` (CHI) with
[`clickhouse-backup`](https://github.com/Altinity/clickhouse-backup):

1. You turn backup on for a CHI, or for some of its clusters, with `spec.backup` and declare the
   storage (S3, GCS, Azure, ...) it keeps the backups in, with a Secret describing it.
2. The operator runs a `clickhouse-backup` sidecar in every ClickHouse Pod of those clusters.
3. You create custom resources to back up, schedule backups and restore:

| Kind                       | Short  | Purpose                                                               |
|----------------------------|--------|-----------------------------------------------------------------------|
| `ClickHouseBackup`         | `chb`  | One-off backup of a CHI, or of one of its clusters                    |
| `ClickHouseBackupSchedule` | `chbs` | Recurring backups, with optional retention                            |
| `ClickHouseRestore`        | `chr`  | One-off restore of a backup into a CHI, or one of its clusters        |

Each of them references a CHI by name in the same namespace, and optionally one of its clusters.
The operator turns it into one Kubernetes `Job` per cluster (one `CronJob` per cluster for a
schedule) that drives the sidecars of that cluster, and reports progress back in the resource's
`status`, cluster by cluster. The Jobs and CronJobs are owned by their resource and are
garbage-collected with it; the backups in the storage are not. A new CHI can also start from a
backup, see [Recover a new CHI from a backup](#recover-a-new-chi-from-a-backup).

```
spec.backup / clusters[].backup ──► operator ──► clickhouse-backup sidecar in every Pod ──► backup storage
                                                              ▲
ClickHouseBackup         ──► Job per cluster ─────┐           │
ClickHouseBackupSchedule ──► CronJob per cluster ─┼── REST API, port 7171, via each host's Service
ClickHouseRestore        ──► Job per cluster ─────┘
```

## Quick start

```bash
# The examples name no namespace: kubectl creates each in the namespace given with -n, or else in
# the context's, "default" when it names none. Apply them all to one namespace, together with
# docs/chk-examples/01-simple-1.yaml, the ClickHouseKeeperInstallation the CHI uses.

# 1. Storage Secret + a CHI with backup enabled (edit the Secret first)
kubectl apply -f docs/chb-examples/01-chi-with-backup.yaml

# 2. Back up once, or on a schedule with retention
kubectl apply -f docs/chb-examples/02-backup-once.yaml
kubectl apply -f docs/chb-examples/04-backup-schedule.yaml

# 3. Watch progress
kubectl get chb,chbs,chr
kubectl describe chb backup-demo-once

# 4. Start a new CHI from one of the backups (set spec.backup.recovery.backupName first,
#    see the BACKUP column of `kubectl get chb` or LAST-BACKUP of `kubectl get chbs -o wide`)
kubectl apply -f docs/chb-examples/07-recover-from-backup.yaml
```

Examples:

* [`01-chi-with-backup.yaml`](chb-examples/01-chi-with-backup.yaml) - storage Secret and a CHI with `spec.backup`
* [`02-backup-once.yaml`](chb-examples/02-backup-once.yaml) - one-off, verified `ClickHouseBackup`
* [`03-backup-later.yaml`](chb-examples/03-backup-later.yaml) - a one-off backup due at a later time, with a starting deadline
* [`04-backup-schedule.yaml`](chb-examples/04-backup-schedule.yaml) - nightly `ClickHouseBackupSchedule` with retention
* [`05-backup-from-schedule.yaml`](chb-examples/05-backup-from-schedule.yaml) - the backup of a schedule, made once now
* [`06-restore.yaml`](chb-examples/06-restore.yaml) - `ClickHouseRestore` into a fresh CHI
* [`07-recover-from-backup.yaml`](chb-examples/07-recover-from-backup.yaml) - a new CHI that starts from a backup

## Installations and clusters

A CHI may hold several clusters, and operator-managed backup works on both levels:

* **The CHI is what you address, configure and recover.** Every backup resource names a CHI;
  `spec.backup` configures all of its clusters, each of which may override it; and a new CHI is
  recovered as a whole, from `spec.backup.recovery`.
* **The cluster is what an operation runs on, conflicts on and reports on.** A backup, a schedule
  or a restore works on every backup-enabled cluster of its CHI, or on the one its `spec.cluster`
  names, with a Job (a CronJob for a schedule) of its own for each cluster. That Job reaches only
  the hosts of its cluster, and runs with that cluster's image, pod settings and backup storage. An
  operation running on a cluster makes the next one on that cluster wait, while the other clusters
  go on, and a restore of a cluster holds the schedules of that cluster only - unless it restores
  access entities, which belong to the whole CHI. The resource reports the outcome of each cluster
  in `status.clusters`, besides one overall `phase`.

**One backup name for all clusters.** The Jobs of one backup share its name: a backup of the
whole CHI is stored as one `clickhouse-backup` backup per shard, all of that name, each cluster in
[storage paths](#storage-paths) of its own. A restore of that name restores every cluster, or the one
it names, from that cluster's paths. A cluster whose backup fails does not stop the others: the
backup then ends `Failed` with the reason `PartiallyCompleted`, and each cluster that completed
holds a complete backup that a restore of that cluster can use. The runs of a schedule share a
name the same way, so the backup to recover a whole CHI from is one that completed on every
cluster: the schedule names the last one in `status.lastBackupName`, see
[ClickHouseBackupSchedule](#clickhousebackupschedule) for when it lags behind.

**No consistency across shards or clusters.** A backup is not a snapshot of the CHI.
`clickhouse-backup` freezes one table after another on each host, a Job backs up the hosts of its
cluster one after another, and the Jobs of different clusters run independently of each other.
Data written in the meantime is in the copies of some shards and not in those of others, and the
later a host comes, the newer its copy. The shared name ties the copies together for naming,
retention and restore, and promises nothing more. Where shards or clusters must match - a batch of
inserts spread over several shards, tables in two clusters fed by one pipeline - stop writing to
them for the time of the backup.

**What the clusters of a CHI share.** Some state belongs to the whole CHI, not to one cluster:

* one distributed DDL queue in Keeper for the `ON CLUSTER` queries of every cluster;
* the helper clusters `all-replicated`, `all-sharded` and `all-clusters`, which the operator adds to
  `remote_servers` over the hosts of every cluster: a table created `ON CLUSTER 'all-sharded'`
  lives on every cluster, each of which backs up and restores the share on its own hosts, and a
  `Distributed` table over them reads from every cluster;
* access entities created with SQL (users, roles, grants, row policies, quotas, settings profiles)
  and user-defined functions, when ClickHouse keeps them in Keeper - a `replicated` user directory,
  `user_defined_zookeeper_path` - under one path for the whole CHI;
* `Replicated*` tables and `Replicated` databases whose Keeper path is the same on several
  clusters, a path without `{cluster}`, which replicate across those clusters; and `Distributed`
  tables on one cluster over another.

Restores that touch this state are therefore not independent of the other clusters:

* restoring access entities (`rbac: true`) is allowed only for a restore of the whole CHI, and a
  restore of them - a recovery restore included - holds the schedules and backups of every cluster
  until it has finished, see [Access entities](#access-entities);
* every schema restore recreates the user-defined functions of the backup on the hosts it restores:
  with functions kept in Keeper, a restore of one cluster sets them back for all clusters;
* a restore of a cluster that shares tables with another one, by replication or through
  `Distributed` tables, changes what the other cluster holds or reads, see
  [Limitations](#limitations-and-security-notes).

Users, profiles and quotas defined in the CHI's `configuration` come from its spec, and are not in
a backup. The state of the CHI as a whole also gates every cluster: while the CHI has not
completed its reconcile - `Aborted` because of any one of its clusters, for example - or is
stopped, no Job starts on any of its clusters.

**A CHI with a single cluster** - nearly every CHI - needs none of this: leave `spec.cluster` empty,
and each operation runs one Job, on that cluster, which `status.clusters` lists alone; the default
storage paths fit; and recovery takes one backup name.

## Enabling backup

Backup is enabled per installation with `spec.backup`, and can be overridden per cluster with
`spec.configuration.clusters[].backup`:

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: backup-demo
spec:
  backup:
    enabled: "yes"
    # The storages the backups may be kept in, one per level for now
    storages:
      - name: main
        secretRef:
          name: backup-demo-storage
    # The storage the sidecar uses; left out, the only one declared
    # storage: main
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

| Field                    | Meaning                                                                                                                                                                                                                                                    |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `enabled`                | `"yes"` turns operator-managed backup on. Accepts the usual yes/no forms (`"yes"`, `"no"`, `true`, `false`, ...). Off by default. Takes effect only with a storage that has a `secretRef`, or with `podTemplate`, see below.                               |
| `podTemplate`            | Pod template whose `clickhouse-backup` container the sidecar is built on, see [Customizing the sidecar](#customizing-the-sidecar). `""` on a cluster: the operator's own.                                                                                  |
| `storages`               | Storages for the backups, one per level for now: a `name`, a DNS label of at most 45 characters, and a `secretRef` naming a Secret in the CHI namespace whose keys become environment variables of the sidecar. See [Storages](#storages).                 |
| `storage`                | The storage of `storages` the sidecar keeps the backups in. Left out, the only one the same level declares, or else the one the level a cluster inherits from uses. `""` on a cluster: no storage.                                                         |
| `recovery`               | `backupName`: the backup a new CHI starts from, see [Recover a new CHI from a backup](#recover-a-new-chi-from-a-backup). `""` on a cluster: nothing to recover there. `sourceInstallation`, `sourceNamespace`: the CHI that made the backup, when another; `""` on a cluster: this CHI. |

Inheritance follows `spec.security`: a cluster inherits every field it leaves empty from
`spec.backup`, so a cluster-level value always wins, and takes the `storages` of `spec.backup`
along with its own, see [Storages](#storages). Everything else about the sidecar - its
image, environment, resources, security context - is set in the pod template `podTemplate` names, see
[Customizing the sidecar](#customizing-the-sidecar). Set `enabled: "no"` on a cluster to opt it
out of an installation-wide `enabled: "yes"`, or leave `spec.backup.enabled` unset and enable
single clusters. `spec.backup` may also come from a `ClickHouseInstallationTemplate`
(`useTemplates`), like any other CHI field, except
`recovery`: the operator takes it from the CHI only, so that a template many installations use
never makes each of them restore the same backup.

Give every backup-enabled cluster a persistent data volume, normally a `dataVolumeClaimTemplate`
as in [`01-chi-with-backup.yaml`](chb-examples/01-chi-with-backup.yaml); see
[Data volume](#data-volume) for why.

**Backup needs a storage.** It takes effect only where the storage the cluster uses has a
`secretRef`, or where `podTemplate` is set, on the cluster or inherited from `spec.backup`. Without
either, the sidecar would run the bundled image on the operator's settings alone, which name no
storage, so every backup and restore would fail. Such a cluster runs as one with backup off -
no sidecar and no port 7171, and no `clickhouse_backup` user unless another cluster of the CHI backs
up - and the operator emits a `BackupNoStorage` warning event on the CHI on every reconcile,
naming the clusters. A backup or restore of such a cluster is refused with `NoBackupStorage` and a
schedule of it is held, one of every cluster leaves it out with a warning (see
[Which hosts are involved](#which-hosts-are-involved)), and a recovery waits for it (see
[Recover a new CHI from a backup](#recover-a-new-chi-from-a-backup)). Declaring a storage with a
`secretRef`, or setting `podTemplate`, turns backup on with the next reconcile, which rolls the
cluster's Pods once. Setting `enabled: "no"` on a cluster that inherited backup unasked settles it as
well, and changes no Pod. The operator looks at these fields only: a configuration file the hosts'
pod template mounts at `/etc/clickhouse-backup`, or settings a mutating admission webhook injects, do
not count. To use those, name the sidecar's pod template in `podTemplate`, even one whose
`clickhouse-backup` container sets nothing.

**Enabling or disabling backup rolls that cluster's Pods only.** Adding or removing the sidecar
changes the cluster's Pod template, so the operator performs its usual rolling update, host by host.
Changing `podTemplate`, which Secret the cluster's storage names - in its `secretRef`, or by
switching to another storage - or the sidecar's container in its pod template rolls the Pods the same
way; `recovery` is no sidecar setting, and changing it restarts nothing. Changing the contents of the
storage Secret does not roll the Pods either: it restarts the sidecars alone, see
[Secret changes](#secret-changes). The other clusters of the CHI are not restarted: they only
reload their users configuration live when the operator adds or removes the `clickhouse_backup`
user (see below).

## What the operator runs

For every Pod of a cluster with backup enabled, the operator adds a container named
`clickhouse-backup` that runs `clickhouse-backup server`:

* **REST API on port 7171**, container port named `backup-api`. The port is also published on each
  host's Service, which is how the backup and restore Jobs reach every host. Only Services of type
  `ClusterIP` (the default, headless included) get the port: a host Service template of type
  `LoadBalancer`, `NodePort` or `ExternalName`, or a `ClusterIP` Service with `externalIPs`, does
  not, so the plain-HTTP API is never published outside the Kubernetes cluster. The operator warns
  about such hosts (`BackupAPIUnpublished` events), and the Jobs cannot reach them, so their backups
  and restores fail. Expose ClickHouse to external clients through a CHI-level Service instead. The
  API can run commands in the sidecar: restrict access to it, see
  [Limitations and security notes](#limitations-and-security-notes).
* **Credentials Secret `<chi-name>-auto-backup-secret`.** The operator creates it once per CHI with
  random values and the CHI as owner, so it is deleted with the CHI. It holds the REST API basic-auth
  credentials (`API_USERNAME`, `API_PASSWORD`) and the ClickHouse credentials of the sidecar
  (`CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`), the same for every cluster of the CHI. Nothing in
  it needs to be configured; the Jobs read it directly. A sidecar that starts while this Secret is
  missing does not serve its REST API, which would let in anybody without credentials, but waits
  for the Secret, leaving its Pod Ready (see [Secret changes](#secret-changes)).
* **ClickHouse user `clickhouse_backup`.** The operator adds this user to the CHI, with the password
  from the Secret above and access restricted to localhost (`127.0.0.1`, `::1`): it logs in only from
  inside its own Pod, the sidecar's. ClickHouse counts every address of the Pod as local, so a client
  in the same Pod gets in through the Pod IP or the host Service too, while no other Pod does. The
  operator reads the password from the Secret while it normalizes the CHI; if the Secret exists
  but cannot be read, the CHI is aborted with `[BackupSecretUnresolved]` rather than reconciled
  without the user. It stays aborted until the next reconcile with a readable Secret - after a
  spec edit, an operator restart, or auto-recovery when a Pod turns Ready. Without backup
  enabled, a user of that name in the CHI is an ordinary user.
* **Local connection to ClickHouse.** The sidecar connects to `localhost` on the host's native TCP
  port, or on the secure TCP port with TLS when the host only exposes the secure one.
* **Schema restored replica by replica.** `RESTORE_SCHEMA_ON_CLUSTER` is set, empty, so that
  nothing turns it on: a restore creates the tables on every replica from their definitions
  in the backup, not `ON CLUSTER`, which would make `clickhouse-backup` re-point `Distributed`
  tables at the restored cluster (see [ClickHouseRestore](#clickhouserestore)). The replicas of a
  restored `Replicated*` table still share one Keeper path. This needs ZooKeeper or ClickHouse
  Keeper configured for the CHI, as replication does anyway.
* **Tables restored under their own names.** `RESTORE_DATABASE_MAPPING` and `RESTORE_TABLE_MAPPING`
  are set, empty, so that a restore creates every table under the database and name it has in the
  backup: the [overwrite guard](#restore-safety) checks those names, and a mapping would make the
  restore drop and replace other tables than the ones it checked.
* **Access entities reloaded live.** `CLICKHOUSE_RESTART_COMMAND` is set to
  `sql:SYSTEM RELOAD USERS`, so ClickHouse picks up restored users, roles and grants without a
  restart; the `clickhouse-backup` default, `systemctl restart clickhouse-server`, cannot work in a
  sidecar. See [Access entities](#access-entities).
* **One operation at a time.** `API_ALLOW_PARALLEL` is set to `false`, so a sidecar runs one
  backup or restore command at a time and refuses another meanwhile, which the Jobs wait out.
* **Default storage paths.** When the path of the storage in use is not set, the sidecar uses
  `backup/{namespace}/{installation}/{cluster}/{shard}`, and
  `object_disks/{namespace}/{installation}/{cluster}/{shard}` as the object disk path, or the paths the operator configuration sets, see
  [Storage paths](#storage-paths). A path set in the Secret or in the container's `env` is used as it
  is, and with a configuration file mounted into the sidecar no default is applied.
* **Same volumes as ClickHouse.** The sidecar mounts every volume the `clickhouse` container mounts,
  so it sees all data disks. See [Data volume](#data-volume).
* **Security context.** The sidecar runs with the `securityContext` of its container in its pod
  template when that sets one, and otherwise with a copy of the ClickHouse container's, as is -
  `readOnlyRootFilesystem`, capabilities and all - so Pods that pass Pod Security admission or a
  policy engine without the sidecar still pass with it. A user the copy leaves open stays open, for
  the platform to assign, as OpenShift does. On plain Kubernetes a context that requires
  `runAsNonRoot` without naming a user makes the `clickhouse-backup` image, which runs as root by
  default, fail to start, so the Pod never becomes Ready and the rollout stops at the first host.
  Name the user in the `clickhouse-backup` container's `securityContext` then, or at the pod level of
  the pod template the hosts run. The sidecar gets no resource requests or limits unless its
  container sets them; set them where a policy or a `LimitRange` requires them on every container.
  Without them, enabling backup turns a ClickHouse Pod of QoS class `Guaranteed` into `Burstable`, as
  `Guaranteed` takes CPU and memory requests equal to the limits on every container: the Pod loses
  the lowest OOM-kill priority and, under the `static` CPU manager policy, the exclusive CPUs of its
  ClickHouse container. Give the sidecar's container requests equal to its limits to keep it
  `Guaranteed`.
* **Local copies removed after upload.** `BACKUPS_TO_KEEP_LOCAL` is set to `-1`, so a backup's local
  copy is deleted from the data disk once it has been uploaded. Local copies are hard links that pin
  old data parts and would otherwise grow without bound.
* **Secret changes followed.** Both Secrets the sidecar takes its environment from are mounted into
  the sidecar as well, read-only under `/var/run/secrets/clickhouse-backup/`, and a liveness probe
  restarts the sidecar alone once they change, see [Secret changes](#secret-changes).

The sidecar environment is assembled, from lowest to highest precedence, from: the keys of the
storage Secret, the operator's own settings listed above, and the `env` of the sidecar's
container in its pod template. A Secret key that collides with an operator setting is
therefore ignored. Nine settings are the exception: the operator always sets `API_LISTEN`
(`0.0.0.0:7171`), `API_SECURE` (`false`), `API_USERNAME` and `API_PASSWORD` (from the credentials
Secret), `RESTORE_SCHEMA_ON_CLUSTER`, `RESTORE_DATABASE_MAPPING`, `RESTORE_TABLE_MAPPING`,
`API_ALLOW_PARALLEL` and `CLICKHOUSE_RESTART_COMMAND` as above, over the container's own, as the
Jobs rely on them - they reach every sidecar on port 7171 over plain HTTP with those credentials, a
restore `ON CLUSTER` would re-point `Distributed` tables, a mapped restore would replace tables the
overwrite guard never checked, two commands at once on one sidecar would share its environment,
and restored access entities would not load. The default storage paths are no such setting: they
fill in a path left unset only.

A Kubernetes Service in the namespace sets `<SERVICE>_PORT=tcp://<ip>:<port>` in every container,
so a Service named `sftp` sets `SFTP_PORT`, which `clickhouse-backup` fails to parse, refusing its
whole configuration. The sidecar sets `CLICKHOUSE_PORT` itself, and starts the server without any
`*_PORT` variable that holds such a link; a port set in the Secret or in the container's `env` is
kept.

### Customizing the sidecar

The sidecar is built on the `clickhouse-backup` container of the pod template `podTemplate` names -
the sidecar's pod template - at the installation level or for one cluster. The operator keeps what
the container sets and fills in what it lacks; without `podTemplate` it builds the sidecar on its
own. Only that container is taken, not the other containers, the volumes or the pod settings of the
template, so it may be a pod template of its own or the one the cluster's Pods run. Every host of the
cluster builds its sidecar on that one container, whatever pod template it runs, and takes the pod
settings, such as the `imagePullSecrets` a private sidecar image needs, from its own pod template.

```yaml
spec:
  backup:
    enabled: "yes"
    podTemplate: backup-sidecar
    storages:
      - name: main
        secretRef:
          name: backup-demo-storage
  templates:
    podTemplates:
      - name: backup-sidecar
        spec:
          containers:
            - name: clickhouse-backup
              image: altinity/clickhouse-backup:2.8.1
              resources:
                requests:
                  cpu: 100m
                  memory: 256Mi
```

A `ClickHouseInstallationTemplate` may carry both `spec.backup.podTemplate` and the pod template it
names, to give every installation that uses it the same sidecar.

* **Yours:** `image` (over the bundled default, `altinity/clickhouse-backup:2.8.1`), `env` (over the
  storage Secret and the operator's defaults), `envFrom`, `resources`, `securityContext`,
  `volumeMounts` (of volumes the Pods have), `command`, `args`, `livenessProbe`. An `envFrom` of the
  container's own sets only what neither the storage Secret, added after it, nor any `env`
  entry sets. Without a `securityContext` the sidecar copies the ClickHouse container's; without
  `resources` it has none, which makes a `Guaranteed` Pod `Burstable`, see
  [What the operator runs](#what-the-operator-runs).
* **Pinned:** the nine settings above are set over the container's own.
* **Rejected:** a literal `API_SECURE` that turns TLS on: the Jobs speak plain HTTP, so the CHI is
  aborted with `InvalidPodTemplate` before anything changes, rather than having TLS turned off
  unasked. So is a `podTemplate` naming a pod template that does not exist, that has none or two
  `clickhouse-backup` containers, or whose container uses a volume only that pod template declares.
  So is a `clickhouse-backup` container in any other pod template the hosts of a backup-enabled
  cluster run: their Pods run the operator's sidecar under that name, so name that pod template in
  `podTemplate` instead.

The ClickHouse container of a pod template is the one named `clickhouse`, else its first container
that is none of the operator's sidecars (`clickhouse-backup`, `clickhouse-log`), so the sidecar may
be listed anywhere. A pod template holding nothing but the sidecar gets the default ClickHouse
container.

The Jobs follow the sidecar of one host of their cluster - the first host of its first shard: its
image and pull policy, and the user, group, `runAsNonRoot` and seccomp profile of its
`securityContext`, besides the `imagePullSecrets` and `serviceAccountName` of the pod template the
host runs. Changing the container rolls the cluster's Pods.

A `command` or `args` of the sidecar's container needs the credentials Secret to start, keeps the
service links and gets no default storage paths; its own `livenessProbe` stops it following Secret
changes; an `envFrom` of its own other than the storage Secret, unprefixed, hides its storage
paths from the Jobs, see [Storage paths](#storage-paths).

A `clickhouse-backup` container in a pod template a cluster with backup off runs is that cluster's
own, run as written.

### Data volume

`clickhouse-backup` works on ClickHouse's files directly: ClickHouse freezes the data parts as
hard links inside its data directory and the sidecar uploads them from there, while verification
and restores download into that directory for ClickHouse to attach. The sidecar therefore has to
share the volume that holds `/var/lib/clickhouse` with the `clickhouse` container, which it does by
mounting the same volumes.

When the `clickhouse` container mounts nothing at `/var/lib/clickhouse` (or at a directory above
it), there is no volume to share: the data lives in the container's own filesystem. The operator
then adds an `emptyDir` volume mounted at `/var/lib/clickhouse` in both containers, so backups and
restores still work. An `emptyDir` survives container restarts but not the Pod: its data is lost
whenever the Pod is deleted, evicted or moved to another node. That is fine for tests; production
clusters need a PersistentVolumeClaim, e.g. a `dataVolumeClaimTemplate`, sized for the data plus
one downloaded copy of a backup.

## Backup storage

Where a cluster keeps its backups - the storage type, the bucket or container, the path - and the
credentials to reach them are configured by a Secret in the CHI namespace, the storage Secret, named
in the `secretRef` of the [storage](#storages) the cluster uses.
Each key becomes an environment variable of the sidecar and is read by `clickhouse-backup` as its
configuration - see the
[clickhouse-backup configuration reference](https://github.com/Altinity/clickhouse-backup#default-config)
for every option. A minimal S3 or MinIO Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: backup-demo-storage
type: Opaque
stringData:
  REMOTE_STORAGE: "s3"
  # With MinIO, http://minio-hl.minio:9000
  S3_ENDPOINT: "https://s3.us-east-1.amazonaws.com"
  S3_REGION: "us-east-1"
  S3_BUCKET: "clickhouse-backups"
  # The default, see Storage paths
  S3_PATH: "backup/{namespace}/{installation}/{cluster}/{shard}"
  S3_ACCESS_KEY: "..."
  S3_SECRET_KEY: "..."
  # Needed by MinIO and most S3-compatible stores
  # S3_FORCE_PATH_STYLE: "true"
```

Other storage types use the same scheme, for example `REMOTE_STORAGE: "gcs"` with `GCS_BUCKET`,
`GCS_PATH`, `GCS_CREDENTIALS_JSON`, or `REMOTE_STORAGE: "azblob"` with `AZBLOB_ACCOUNT_NAME`,
`AZBLOB_ACCOUNT_KEY`, `AZBLOB_CONTAINER`, `AZBLOB_PATH`. Each cluster takes the Secret of the
storage it uses, so clusters may keep their backups in different buckets, or with different
providers.

Useful `clickhouse-backup` settings to consider in the Secret:

* Leave `BACKUPS_TO_KEEP_REMOTE` unset (`0`). It prunes every backup regardless of who made
  it; use the schedule's [`retention.keepLast`](#retention) instead, which only touches that
  schedule's backups.
* Compression, per storage: `S3_COMPRESSION_FORMAT` (`tar`, `zstd`, `gzip`, ...),
  `GCS_COMPRESSION_FORMAT`, `AZBLOB_COMPRESSION_FORMAT`. Not `lz4`: ClickHouse data is
  lz4-compressed already, so `clickhouse-backup` refuses that configuration, its sidecar never
  serves its API, and every backup and restore of the cluster fails.
* Server-side encryption: `S3_SSE` (e.g. `AES256` or `aws:kms`) with `S3_SSE_KMS_KEY_ID`; see the
  clickhouse-backup reference for GCS and Azure.

`BACKUPS_TO_KEEP_LOCAL` is set by the operator (see above), so a value in this Secret is ignored;
set it in the `env` of the sidecar's container in its pod template to keep local copies.

The Pods start without the Secret. When it does not exist - not created yet, or misspelled in
`secretRef` - the sidecar starts without a storage configuration, ClickHouse is unaffected,
the operator emits a `BackupStorageSecretMissing` warning event on the CHI on every reconcile,
naming the Secret, its storage and the cluster, and backups and restores fail until the Secret is in
place. Once it is created, the sidecars pick it up by themselves, as they do any later change of it.

### Storages

`storages` declares the storages the backups may be kept in, each under a `name` of its own -
a DNS label of at most 45 characters, unique - and `storage` names the one the sidecar keeps the
backups in. For now each level - `spec.backup` or the `backup` of a cluster, of the CHI or of a
`ClickHouseInstallationTemplate` - declares one storage at most.

```yaml
spec:
  backup:
    enabled: "yes"
    storages:
      - name: main
        secretRef:
          name: backup-demo-storage
  configuration:
    clusters:
      # Keeps its backups in main
      - name: hot
      # Keeps its backups in cold
      - name: cold
        backup:
          storages:
            - name: cold
              secretRef:
                name: backup-demo-storage-cold
```

* **Storages pair by name.** A cluster takes the storages of `spec.backup` along with its own, its
  own replacing one of the same name, but for a `secretRef` it leaves out, which it takes from that
  one. A `ClickHouseInstallationTemplate` contributes its storages alike, the CHI's replacing the
  template's.
* **A level's own storage wins.** Left out, `storage` is the only storage the same level declares,
  or else the one the level a cluster inherits from uses. So the storage a cluster declares wins over
  the CHI's, and the CHI's over a template's: above, cluster `cold` keeps its backups in `cold`, and
  every other cluster in `main`. Where one level declares several storages, `storage` must name one.
* **`storage: ""` on a cluster** leaves the cluster without a storage: its backup stays off, unless
  `podTemplate` configures the sidecar, see [Enabling backup](#enabling-backup).
* **A storage without `secretRef`** is configured by the `clickhouse-backup` container of the pod
  template `podTemplate` names; without `podTemplate`, a cluster that uses it has no backup storage.
* **An unknown or ambiguous storage aborts the CHI.** When the `storage` of a cluster that turns
  backup on, its own or inherited, names a storage that no level declares, or a level declares
  several without naming one, the reconcile of the CHI is aborted with `InvalidBackupStorage` before
  anything changes: no Pod restarts. Turning backup off there instead would remove the sidecars and
  restart every host over a typo. The CHI stays aborted until it is edited, or its template is, which
  reaches the CHI only on its next reconcile as `template.chi.policy` allows.

The Jobs and CronJobs of a cluster carry the storage it uses in the label
`clickhouse.altinity.com/backup-storage`, and the backup resources report it in
`status.clusters[].storage`, see [Status, events and metrics](#status-events-and-metrics).

### Storage paths

Every shard holds different data, but a backup has one name on all of them, so each shard of each
cluster keeps its part of a backup under a storage path of its own. `clickhouse-backup` expands the
path on each host with the ClickHouse macros the operator defines there: `{cluster}` (the cluster
name), `{shard}` (the shard name, by default its index in the cluster), `{replica}` (the host name),
`{installation}` (the CHI name) and `{namespace}` (the namespace of the CHI). A macro of one of these
names that the CHI's own configuration defines takes precedence on the hosts, while the Jobs check
the paths with the operator's values: do not redefine them.

**Default.** Without a path for the storage in use - `S3_PATH`, `GCS_PATH`, `AZBLOB_PATH`, ... -
the sidecar uses `backup/{namespace}/{installation}/{cluster}/{shard}`, and, for tables on object
disks, `object_disks/{namespace}/{installation}/{cluster}/{shard}` as the object disk path
(`S3_OBJECT_DISK_PATH`, `GCS_OBJECT_DISK_PATH`, `AZBLOB_OBJECT_DISK_PATH`). A path you set always
wins. The namespace and the CHI name keep apart the backups of every CHI of one Kubernetes cluster
on a shared storage: with paths that expand alike for two CHIs, each lists, restores and deletes the
backups of the other by name, and two backups of one name would merge into one.

**Configured defaults.** The operator configuration sets the defaults, in the same macros:

```yaml
backup:
  storagePath: "eu1/backup/{namespace}/{installation}/{cluster}/{shard}"
  storageObjectDiskPath: "eu1/object_disks/{namespace}/{installation}/{cluster}/{shard}"
```

A fixed prefix of your own, as `eu1/` here, keeps apart the installations of several Kubernetes
clusters on one storage, CHIs of one name in one namespace among them. The sidecar takes the defaults on its command line: changing
them rolls the Pods of every backup-enabled cluster, and leaves the backups made under the old paths
where they are, where restores, retention and incremental backups no longer find them. Set both,
each with `{shard}`, and neither a prefix of the other.

**Checked before anything changes.** Every backup and restore Job expands the paths of all
backup-enabled clusters of the CHI, not only of its own cluster, and fails without changing
anything unless:

* **every shard of its cluster has a storage path of its own**, which no other shard uses, of its
  cluster or of another one on the same storage (the same `REMOTE_STORAGE` and bucket, container,
  endpoint or address). Without `{shard}` the shards of a cluster would write over each other's part
  of a backup, and without `{cluster}` two clusters on one bucket would mix theirs: as they share the
  backup name, a restore of one cluster could restore the other's data. One cluster with one shard
  needs no macro at all, and clusters on different buckets need no `{cluster}`. A path that two other
  clusters share fails the Jobs of those clusters, not this one;
* **every shard has an object disk path of its own** the same way, when one is set. Deleting a
  backup - by retention, or by hand - deletes its object disk data under
  `<object disk path>/<backup name>`, so with a shared object disk path, deleting the backup of one
  shard, or of one cluster, would delete the object disk data of every other shard's copy of it;
* **the path has no `{replica}`** for a `FirstPerShard` backup: every replica of a shard restores
  its schema from its own path, where only the first replica's backup would be;
* **the path has `{replica}`** for an `AllReplicas` backup
  (`backup/{namespace}/{installation}/{cluster}/{shard}/{replica}`),
  as every replica then uploads its own copy under the same name.

A restore takes the path as it is. Under `{replica}`, every replica restores its schema from its own
copy, and the first replica of each shard the data from its copy, so an `AllReplicas` backup can be
restored, while a backup of the first replica of each shard only cannot: the other replicas find no
copy at their paths. `validateTopology` refuses that before anything changes; without it, the Job
only warns.

Settings left unset count as the defaults `clickhouse-backup` gives them, and paths are compared as
it cleans them, so `backup//{shard}/` and `backup/{shard}` are one path. To check, the Job's Pod
takes from the `env` of each cluster's sidecar, as its pod template sets it, and from
its storage Secret only the keys that locate the storage (storage type, bucket,
container, endpoint, address, port and paths) and, from the Secret alone,
`CLICKHOUSE_BACKUP_CONFIG` - never its credentials. Kubernetes passes the Secret's keys to the Pod:
the operator itself only checks that the Secret exists.

**When the check cannot tell.** The paths of a cluster are unknown to the check when:

* a path holds a macro other than the five above, which the Job cannot expand;
* neither the container's `env` nor the storage Secret sets `REMOTE_STORAGE`, as with a
  configuration file of the sidecar, or it names a storage type whose paths the check does not know,
  such as `custom`;
* the sidecar may take settings the operator cannot see: from a configuration file, on a volume
  mounted at `/etc/clickhouse-backup`, named by `CLICKHOUSE_BACKUP_CONFIG` in the container's `env`
  or the storage Secret, or named on the command line of the sidecar's container in its pod
  template - a `command` or `args` with `-c` or `--config`, or any argument
  holding `--config`, as a shell wrapper passes it on; from an `envFrom` of that container; or
  from a setting that locates the storage but is taken from anything but a Secret or ConfigMap key,
  such as a `fieldRef`, or refers to another variable with `$(...)`.

In a CHI with one backup-enabled cluster, the Job then logs a warning and goes on. With several, the
Jobs of their clusters run at the same time, and two of them writing one backup to one path would
leave a mix of both, listed as complete: a Job that cannot tell the paths of its own cluster fails
without changing anything, and one that cannot tell those of another cluster leaves it out of the
check, with a warning - that cluster's own Jobs fail. Configure the sidecars of a CHI with several
backup-enabled clusters through the storage Secret and `env` only, without a configuration
file named anywhere.

A configuration file built into a custom `clickhouse-backup` image is invisible to the operator. The
sidecar then takes from the file what its environment leaves unset, and applies no default path,
while the check takes the defaults: with such an image, set every setting that locates the storage,
paths included, in the storage Secret or `env`.

**Restoring into another CHI.** The target CHI finds a backup by expanding its paths with its own
macros, so it needs the same cluster names and shard layout. A backup of a CHI of another name, or
of another namespace where the paths hold `{namespace}`, lies under the paths of that CHI: name it in
`sourceInstallation`, and its namespace in `sourceNamespace`, of the
[ClickHouseRestore](#clickhouserestore), or of `spec.backup.recovery` for a
[recovery](#recover-a-new-chi-from-a-backup). The restore then reads the backup from the paths those
macros give there, through clickhouse-backup commands that override both paths for that command
alone. The paths of the target must be known to the Job (see above): a configuration file of the
sidecar fails such a restore before it changes anything, and paths that hold neither macro are read
as they are, with a warning. With `validateTopology: false` the restore does not check first that
every host finds the backup there. An `AllReplicas` backup cannot be restored into a CHI of another
name, as `{replica}` - the host name - differs there.

clickhouse-backup tells backups apart by name alone, whatever their path, in the cache where it
keeps what it read of them, and reuses a local copy of a backup it is to download. Before anything
changes, such a restore therefore refuses a host that holds a local copy of the backup, or a backup
of the same name at its own storage path - unless both CHIs have one storage path, their object disk
paths alone differing - or, unless `validateTopology` is `false`, a backup named as the one the
backup is incremental from: the host would restore that one in place of the backup it reads. Keep
the backup names of the two CHIs apart, as the default `backupNamePrefix` of their schedules does,
unless their names joined with dashes coincide.
It also refuses an object disk path that is the same for both CHIs while the storage path is not:
deleting the copy it downloads would delete the object disk data of the backup.

The copies of the schema the preflight downloads are deleted before the restore steps, and a Job
that fails deletes the copies it downloaded. One stopped at its `activeDeadlineSeconds`, or evicted,
leaves them: the next restore of the backup from the same paths then refuses the hosts that hold
one, until it is deleted with `clickhouse-backup delete local <backup>` in their sidecar.

A CHI restored from a backup it reads from its own paths - a copy of the same name and namespace, or
one whose paths hold neither macro - then shares the storage path with the CHI it was restored from,
and so does every schedule of either. Their backups are told apart by name only:
[retention](#retention) prunes the schedule's series, the backups named
`<backupNamePrefix>-YYYYMMDD-HHMMSS`, and a run refuses a name that is already in the
storage. Two schedules with the same `backupNamePrefix` on one storage path therefore delete each
other's backups, and fail each other's runs when they are due at the same time. The default prefix,
`<namespace>-<chi>-<schedule>`, keeps apart the schedules of one Kubernetes cluster, unless their
names joined with dashes coincide (see below); an explicit prefix must be unique among all
schedules that write to the same storage path.

A CHI of the same name in the same namespace of another Kubernetes cluster - a disaster recovery
copy - gets the same default prefixes: give the schedules of such a copy a `backupNamePrefix` of
their own from the start. As a safeguard, when a CHI is
[recovered](#recover-a-new-chi-from-a-backup), or is to be, from a backup whose name starts with its
own `<namespace>-<chi>-` - as the default prefix of each of its schedules does, and so that of each
schedule of the CHI the backup came from - every schedule of the CHI that uses the default prefix,
leaving `backupNamePrefix` unset, is held, not only the one whose prefix the backup carries: the
source's other schedules write to the same paths as well. Their CronJobs stay suspended, `Ready` is
`False` with the reason `SharesSourcePrefix`, and a warning event says so. A name alone cannot tell
whose backup it is: schedule `eu-nightly` of CHI `prod` and schedule `nightly` of CHI `prod-eu` both
make `<namespace>-prod-eu-nightly-...`, so a CHI `prod` recovered from a backup of a CHI `prod-eu` of
the same namespace has its schedules held as well, until they get a `backupNamePrefix`.

The hold starts as soon as `spec.backup.recovery` names such a backup, before any restore, and stays
when a recovery restore fails, whether or not it changed anything. It ends for a schedule when you
set its `backupNamePrefix` - to a prefix of its own, or, once the CHI's storage points at a path of
its own and the sidecars have picked that up (see [Secret changes](#secret-changes)), even to the
default value: setting it is the sign that the choice was made. The choice is final, as the prefix
can be set once and is immutable after (see [ClickHouseBackupSchedule](#clickhousebackupschedule)):
choose it deliberately. Before the recovery is recorded,
[giving the recovery up](#recover-a-new-chi-from-a-backup) ends it for every schedule; after that,
only `backupNamePrefix` does. Any other schedule of a recovered CHI reports the possibly shared path
as a warning event and in its `Validated` condition (reason `CHIRecovered`).

### Secret changes

The sidecar takes its environment from two Secrets: the storage Secret and
`<chi-name>-auto-backup-secret`. Kubernetes resolves the environment of a container only when it
starts the container, and `spec.restart: "RollingUpdate"` restarts ClickHouse in its Pod, not the
sidecar. The operator therefore mounts both Secrets into the sidecar too, which Kubernetes keeps up
to date, and gives the sidecar a liveness probe that compares them with the environment it started
with. Once they differ, the probe fails, and Kubernetes restarts the sidecar alone, resolving its
environment anew; ClickHouse keeps running. Creating the storage Secret after the Pods,
editing it, or deleting and recreating it with other contents therefore takes effect by itself:

* **Within a few minutes.** Kubernetes updates a mounted Secret about once a minute, and the probe
  fails for 90 seconds before the restart, so that a sidecar that started just before such an update
  is not restarted for nothing.
* **Not during an operation of the sidecar.** The probe asks the sidecar's REST API first and waits
  while the sidecar runs an operation, and for a minute after it finished one, so that a restart
  does not interrupt its backup or restore - a restore is not resumed - nor a Job between two of its
  steps on that host. A sidecar whose API does not answer is restarted anyway: no Job can be driving
  it. The probe knows of its own sidecar only, while a Job drives the hosts of its cluster one after
  another: the sidecar of a host the Job has not reached yet, or left more than a minute ago, may
  restart meanwhile. A backup Job waits for it, but goes on with the new values on that host and the
  old ones on the hosts before - and fails on new credentials; a restore Job that finds a sidecar
  restarted between two of its steps fails as partially applied. Change these Secrets only while no
  backup or restore runs.
* **The Pod is briefly not Ready.** For the second or two the sidecar takes to restart, the CHI's
  Service sends no new connections to the Pod. The host Services, which replication, distributed
  queries and the backup Jobs use, publish Pods that are not Ready and are unaffected. The restart
  counts in the Pod's `RESTARTS`, and a `Liveness probe failed` event names the changed key. A
  second change within ten minutes or so delays that restart by Kubernetes' restart back-off,
  keeping the Pod not Ready for that long.
* **A changed or added key counts, a removed one does not.** A key removed from the Secret, with
  nothing else changed, stays in the sidecar's environment until the sidecar restarts for another
  reason; set it to its default value instead - for a path, the one the sidecar defaults to
  (see [Storage paths](#storage-paths)), as an empty one names the root of
  the storage, and for `CLICKHOUSE_BACKUP_CONFIG` an empty one, which names no file - not even one
  a custom image ships. A deleted Secret leaves the sidecars running with the configuration they
  have.
* **Only these two Secrets are followed.** A key the sidecar sets otherwise - an operator setting or
  the container's own `env` - is not compared, as the Secret does not set it, and an `env` entry
  taken from another Secret or a ConfigMap changes at the next restart of the sidecar only.

When `<chi-name>-auto-backup-secret` is deleted, the running sidecars keep their credentials, which
ClickHouse still accepts, and the probe restarts nothing while the Secret is missing, as a sidecar
restarted without it would lack them. The operator notices the deletion and reconciles the CHI,
which creates the Secret anew, with new random values; so does any reconcile that finds the Secret
missing or lacking a key, whatever started it, except for a suspended CHI, whose Secret is left as
it is until the CHI is resumed. ClickHouse reloads the new password live, and the sidecars of every
cluster restart with the new credentials as above - backups and restores fail until they have.
Backup and restore Jobs need the Secret too and do not start while it is missing. A sidecar that
starts while the Secret is missing serves no REST API, as an API without credentials would let in
anybody, but waits until the Secret is back, leaving its Pod Ready.

## ClickHouseBackup

A one-off backup. See [`02-backup-once.yaml`](chb-examples/02-backup-once.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseBackup"
metadata:
  name: backup-demo-once
spec:
  clickHouseInstallation: backup-demo
```

| Field                     | Default         | Meaning                                                                                                                                                                                                                      |
|---------------------------|-----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `clickHouseInstallation`  | one of the two  | CHI to back up, in the same namespace. Exactly one of it and `scheduleRef` is set.                                                                                                                                           |
| `scheduleRef.name`        |                 | Make the backup of this `ClickHouseBackupSchedule` once, with its CHI, clusters, options and `backupNamePrefix`, see [Run a schedule now](#run-a-schedule-now).                                                              |
| `cluster`                 | all             | Back up only this cluster. Empty means every cluster with backup enabled, each in a Job of its own; with `scheduleRef`, every cluster the schedule backs up.                                                                 |
| `backupName`              | generated       | Backup name, the same on every cluster; generated from the resource name and the time it is due when empty, see `status.backupName`. Refused with `scheduleRef`, or if it ends like a schedule's, `-YYYYMMDD-HHMMSS`.        |
| `scheduledTime`           | when created    | RFC 3339 time, with its offset, the backup is due at, see [Back up at a later time](#back-up-at-a-later-time).                                                                                                               |
| `startingDeadlineSeconds` |                 | How late, from the time it is due, the backup may start on a cluster; unset, it waits as long as it takes.                                                                                                                   |
| `diffFromRemote`          |                 | Make this an [incremental backup](#incremental-backups) on top of this existing backup.                                                                                                                                      |
| `verify`                  | `false`         | [Verify](#verification) the backup after it is uploaded.                                                                                                                                                                     |
| `schemaOnly`              | `false`         | Back up table and database definitions only, no data.                                                                                                                                                                        |
| `replicaSelection`        | `FirstPerShard` | `FirstPerShard` backs up one replica of each shard, which is complete for `Replicated*` tables. `AllReplicas` backs up every replica, needed when a cluster holds non-replicated tables whose data differs between replicas. |
| `tables`                  | all             | `clickhouse-backup --tables` pattern, e.g. `db.table`, `db.*`, comma-separated.                                                                                                                                              |
| `partitions`              | all             | List of partition ids to back up.                                                                                                                                                                                            |
| `activeDeadlineSeconds`   | `86400` (24h)   | How long each Job may run before it is stopped and the backup of its cluster fails.                                                                                                                                          |

The spec is validated when the resource is created: backup names (`backupName`, `diffFromRemote`,
`backupNamePrefix`) start with a letter or digit, contain only letters, digits and `_ - .`, never two
dots in a row (`clickhouse-backup` would strip them and store the backup under another name), and are
at most 128 characters - a schedule's `backupNamePrefix` at most 112, leaving room for the timestamp.
A one-off backup's `backupName` must not end like the backups of a schedule, `-YYYYMMDD-HHMMSS`: the
[retention](#retention) of a schedule of that prefix would take it for one of its own.
`tables` may only contain letters, digits and `_ . * ? , -` (up to 1024 characters), and each
partition id must match `^[A-Za-z0-9_-]{1,128}$`. The on/off fields of backups, schedules and
restores (`verify`, `schemaOnly`, `suspend`, `overwrite`, `validateTopology`, `rbac`) take the yes/no
forms `spec.backup.enabled` takes - `true`, `false`, `"yes"`, `"no"`, `1`, `0` and the like - and any
other value, a number with a fraction, a list or an object included, is refused like an invalid name.
Here, as on schedules and restores, `clickHouseInstallation` and `cluster` cannot be changed
afterwards, nor can a backup's `scheduleRef` (`clickHouseInstallation, scheduleRef and cluster are
immutable; create a new resource`), which Kubernetes enforces from 1.25 on. Whatever the spec says
later, the operator works on the clusters it recorded in `status.clusters` when it validated the
resource.

A backup runs once. To back up again, create a new resource (or use a schedule). Deleting a
`ClickHouseBackup` removes its Jobs, not the backup in the storage.

### Run a schedule now

A `ClickHouseBackup` that names a schedule in `scheduleRef` makes the backup that schedule makes,
once - before an upgrade, say, exactly as the nightly backup would. See
[`05-backup-from-schedule.yaml`](chb-examples/05-backup-from-schedule.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseBackup"
metadata:
  name: pre-upgrade
spec:
  scheduleRef:
    name: nightly
  # verify: true
```

* It takes the CHI, the clusters (unless `cluster` names one of them), the `backupNamePrefix` and
  every backup option of the schedule (`schemaOnly`, `replicaSelection`, `tables`, `partitions`,
  `activeDeadlineSeconds`) as they are when the backup is validated, and records them in
  `status.schedule`: an edit of the schedule afterwards changes nothing about it. None of these
  options can be set on the backup itself, nor can `backupName`; its own fields, `cluster` (one of
  the clusters the schedule backs up), `verify`, `diffFromRemote`, `scheduledTime` and
  `startingDeadlineSeconds`, keep their meaning.
* It is always named `<backupNamePrefix>-<UTC time it is due, as YYYYMMDDTHHMMSS>`, e.g.
  `default-backup-demo-nightly-20261012T101507`: a `backupName` is refused with `scheduleRef`
  (`a backup that runs a schedule is named with the schedule's backupNamePrefix: backupName is not
  set with scheduleRef`), so the name always tells the schedule the backup came from. The `T` keeps
  it out of the schedule's series: [retention](#retention) never deletes it, and creating such
  backups never pushes one of the schedule's out - delete it by hand when it is no longer needed.
  The prefix also lets the [recovery prefix guard](#storage-paths) tell where it came from.
* It runs as any `ClickHouseBackup` does, also while the schedule is suspended or held for a restore:
  it is not part of the series those protect. A schedule that does not exist yet - both applied at
  once - is waited for (`ScheduleNotFound`), as is one whose spec the operator finds invalid
  (`InvalidSpec`); a cron expression or `timeZone` only the API server refuses does not stop it, as
  the backup uses neither. A `cluster` the schedule does not back up refuses the backup
  (`ClusterNotFound`).
* Two of them due in the same second get the same name: on a cluster both back up, the second fails,
  before it changes anything, as the backup exists already. Delete the failed one and create it
  again without `scheduledTime`, which names it after its new creation time, or give each its own
  `scheduledTime`. Likewise, one kept in git that may be created again gets a new name only without
  `scheduledTime`, from the time it is created; with it, it gets its former name again, and fails
  while the backup of that name is still in the storage.

It replaces `kubectl create job --from=cronjob/...`, which still works: such a Job names its backup
after the time it runs, as one of the series, so the schedule's retention counts it, it ignores
every hold, and the operator never learns its name.

### Back up at a later time

A backup with `scheduledTime` waits for that time, given in RFC 3339 with its offset (or `Z`). See
[`03-backup-later.yaml`](chb-examples/03-backup-later.yaml).

```yaml
spec:
  clickHouseInstallation: backup-demo
  scheduledTime: "2026-10-20T02:00:00+02:00"
  startingDeadlineSeconds: 3600
```

Until then it stays `Pending`, its `Validated` condition `False` with the reason
`WaitingForScheduledTime` and the time in UTC; `scheduledTime` can still be changed, or the backup
deleted. At that time it is validated against the CHI as it is then, and named after that time, as
`<resource name>-<Unix time>` or, with `scheduleRef`, `<backupNamePrefix>-<YYYYMMDDTHHMMSS>`. It
runs once; for recurring backups, use a schedule.

A backup that cannot start then - the operator is down, the CHI has not completed its reconcile,
another operation runs - starts as soon as it can. `startingDeadlineSeconds` bounds that: a backup
not validated within that many seconds of the time it was due - an invalid spec included - fails
with `StartingDeadlineExceeded`, telling what it waited for, as does each cluster whose Job is not
created by then. Without `scheduledTime` the deadline counts from the backup's creation, which
bounds a backup waiting for a CHI that never becomes ready. The operator checks the time a second
after a backup is due, so a deadline of a few seconds may fail a backup that is on time: give it a
minute or more. Edit neither field once the backup is validated: its clusters still waiting would
count from the edited values. Applied after its `scheduledTime`, as a GitOps tool may re-apply a
backup it keeps, a backup starts at once unless its deadline has passed: keep
`startingDeadlineSeconds` on backups kept in git.

## ClickHouseBackupSchedule

Recurring backups through Kubernetes `CronJob`s. See
[`04-backup-schedule.yaml`](chb-examples/04-backup-schedule.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseBackupSchedule"
metadata:
  name: nightly
spec:
  clickHouseInstallation: backup-demo
  schedule: "0 2 * * *"
  retention:
    keepLast: 7
```

| Field                                                                             | Default                               | Meaning                                                                                                                                                                                                                       |
|-----------------------------------------------------------------------------------|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `clickHouseInstallation`                                                          | required                              | CHI to back up, in the same namespace.                                                                                                                                                                                        |
| `cluster`                                                                         | all                                   | Back up only this cluster. Empty means every cluster with backup enabled, each through a CronJob of its own.                                                                                                                  |
| `schedule`                                                                        | required                              | Cron expression, as in a Kubernetes `CronJob`.                                                                                                                                                                                |
| `timeZone`                                                                        | the controller manager's, usually UTC | IANA time zone `schedule` is read in, e.g. `Europe/Berlin`, as the `CronJob`'s, which Kubernetes before 1.25 ignores. Backup names carry UTC times either way.                                                                |
| `suspend`                                                                         | `false`                               | Stop starting new runs. A run already in progress continues.                                                                                                                                                                  |
| `concurrencyPolicy`                                                               | `Forbid`                              | What to do when a run is due while the previous one still runs: `Forbid` starts it only once that one has finished, and not at all past `startingDeadlineSeconds`; `Allow` starts it anyway; `Replace` stops the running one. |
| `startingDeadlineSeconds`                                                         |                                       | How late a missed run may still start.                                                                                                                                                                                        |
| `successfulJobsHistoryLimit`                                                      | `3`                                   | Finished successful Jobs to keep, at least 1: the operator learns from its Job which backup a run made and whether it holds data.                                                                                             |
| `failedJobsHistoryLimit`                                                          | `1`                                   | Failed Jobs to keep, at least 1: the operator learns from its Job why a run failed.                                                                                                                                           |
| `schemaOnly`, `replicaSelection`, `tables`, `partitions`, `activeDeadlineSeconds` |                                       | As in [`ClickHouseBackup`](#clickhousebackup); `activeDeadlineSeconds` bounds each Job of a run.                                                                                                                              |
| `backupNamePrefix`                                                                | `<namespace>-<chi>-<schedule>`        | Backup names are `<prefix>-<UTC time the run was due>`, e.g. `default-backup-demo-nightly-20261003-020000`. Also scopes [retention](#retention). Can be set once and is immutable after, see below.                           |
| `retention.keepLast`                                                              | `0` (keep all)                        | [Retention](#retention): how many of this schedule's backups with data, and apart from them without, to keep.                                                                                                                 |

A schedule runs one CronJob for each cluster it backs up, named after the schedule and the cluster,
with the cluster's own Pod settings; `concurrencyPolicy` and the history limits apply to each of
them. All of them follow the same `schedule`, so the runs of one slot make one backup name on every
cluster, and each cluster's runs prune that cluster's backups, see [Retention](#retention). A
cluster that joins - added to the CHI, backup enabled - gets its CronJob once the CHI has completed
its reconcile; a cluster that leaves loses it (see
[Hosts that leave a schedule](#hosts-that-leave-a-schedule)).

The default prefix joins the namespace, CHI and schedule names, shortened with a hash past 112
characters, so that schedules of the same name of different CHIs, or of CHIs of the same name in
different namespaces, get different prefixes; see [Storage paths](#storage-paths) for why that
matters.

`backupNamePrefix` can be set once and is immutable after (`backupNamePrefix can be set once and is
immutable after: a changed prefix would leave the former series unpruned; create a new resource`),
which Kubernetes enforces from 1.25 on: [retention](#retention) knows the current prefix only, and
would never prune the backups of a former series. Set it when the schedule is created, or later only
on a schedule that has not run yet, such as one held for a [recovery](#storage-paths), which setting
it releases: on a schedule that has run, any prefix but the default changes the one its runs so far
used. To change the prefix of a schedule that has run, create a new schedule. Either way, the
backups of the former prefix stay in the storage until they are deleted by hand. Choose an explicit
prefix deliberately, unique among the schedules that write to the same storage path.

Each run names its backup after the time it was due - the minute of its schedule slot, which the
CronJob controller puts in the name of every Job it creates - not after the time it happens to start.
A run late by a few minutes still makes `default-backup-demo-nightly-20261003-020000`, and the
operator knows the name of every backup the schedule makes. The Job reads its name from the
`job-name` label of its Pod, which Kubernetes sets on every version the operator supports, besides
`batch.kubernetes.io/job-name` from 1.27 on. To make a schedule's backup at once, see
[Run a schedule now](#run-a-schedule-now).

Scheduled backups are always full backups; `diffFromRemote` and `verify` are only available on a
`ClickHouseBackup`. The status tells how the runs went, for each cluster in `status.clusters`:

* `cronJobName`: the cluster's CronJob; `storage`: the [storage](#storages) its runs keep their
  backups in, as the CronJob was last built - for a cluster that lost its CronJob, where its unpruned
  backups lie (see [Hosts that leave a schedule](#hosts-that-leave-a-schedule));
* `lastScheduleTime` and `lastSuccessfulTime`: when the CronJob last started a run and last
  completed one successfully; `lastBackupName`: the backup that successful run made;
* `lastFailureTime` and `lastFailureMessage`: when a run last failed, and why, with the error its Job
  reported (see [Backup and restore Jobs](#backup-and-restore-jobs));
* `retentionHosts` and `unprunedHosts`: the hosts of the cluster that a run has backed up, which
  retention prunes on, and the hosts that left them while retention was on, whose backups it
  prunes no more (see [Hosts that leave a schedule](#hosts-that-leave-a-schedule)).

Over all clusters, `status.lastSuccessfulTime` is the oldest of their last successes - by then every
cluster had been backed up - and `status.lastBackupName` the backup every cluster made last, the
one to [recover the whole CHI from](#recover-a-new-chi-from-a-backup). Both are empty while a
cluster has not completed a run yet - one that has just joined the schedule, or one whose CronJob
cannot be created (`NameConflict`, see [Backup and restore Jobs](#backup-and-restore-jobs)).
`lastBackupName` follows the clusters' own `lastBackupName`s:

* while the runs of a slot complete one after the other, it stays at the backup of the slot before,
  until every cluster has made the new one. A cluster that completed the new run may have pruned the
  older backup by then - with `retention.keepLast: 1`, or when the runs of another cluster outlast the
  schedule's period and miss slots (`concurrencyPolicy: Forbid`) while it goes on making newer ones;
* while the last run of a cluster failed, it is cleared as soon as another cluster completes a run
  newer than that cluster's last backup, which the [retention](#retention) of the other clusters is
  then going to prune.

So check that a backup exists on every cluster before you recover from it - a recovery from a
missing backup fails, before it changes anything - or recover each cluster from the backup of its
own entry, `status.clusters[].lastBackupName`, which its retention keeps, as that cluster's
`recovery.backupName` (see [Recover a new CHI from a backup](#recover-a-new-chi-from-a-backup)).

The `LastBackupSucceeded` condition tells whether the last finished runs succeeded, with the reason
of a failure otherwise (`JobFailed`, `DeadlineExceeded`, `JobNotStarted`). While a last run failed,
`Ready` is `False` with reason `LastBackupFailed`; the CronJobs keep running the schedule, and the
next successful run makes the schedule `Ready` again. A run whose backup
[holds no data](#backup-and-restore-jobs) succeeded, with the reason `CompletedWithoutData`, and
`Ready` stays `True` with the reason `LastBackupWithoutData` until a run backs up data again.

`kubectl get chbs` shows the reason of the `Ready` condition (`STATUS`) - whether the schedule runs,
why it does not, or that a last run failed or backed up no data - and the last success, `-o wide`
the last backup name too.

## ClickHouseRestore

Restores a backup into a CHI. See [`06-restore.yaml`](chb-examples/06-restore.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseRestore"
metadata:
  name: backup-demo-restore
spec:
  clickHouseInstallation: backup-demo-restored
  backupName: default-backup-demo-nightly-20261003-020000
```

| Field                    | Default            | Meaning                                                                                                                                   |
|--------------------------|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `clickHouseInstallation` | required           | CHI to restore into, in the same namespace. A fresh, empty CHI is recommended.                                                            |
| `cluster`                | all                | Restore only into this cluster. Empty means every cluster with backup enabled, each in a Job of its own.                                  |
| `backupName`             | required           | Backup to restore.                                                                                                                        |
| `sourceInstallation`     | the restored CHI   | CHI that made the backup, when another: the backup is read from the paths its name gives, see [Storage paths](#storage-paths). Immutable. |
| `sourceNamespace`        | the restored CHI's | Namespace of the CHI that made the backup, when another and the paths hold `{namespace}`. Immutable.                                      |
| `schemaOnly`             | `false`            | Restore table and database definitions only, no data.                                                                                     |
| `tables`                 | all                | Restore only the matching tables: a `clickhouse-backup` pattern as for a backup, e.g. `db.table`, `db.*`, comma-separated.                |
| `rbac`                   | `false`            | Also restore the [access entities](#access-entities) of the backup. Only with an empty `cluster`.                                         |
| `overwrite`              | `false`            | Allow restoring over existing tables, which are dropped first. See [Restore safety](#restore-safety).                                     |
| `validateTopology`       | `true`             | Refuse the restore unless every target shard holds the backup, and, from another CHI, a backup named as its base at the own paths.        |
| `activeDeadlineSeconds`  | `86400` (24h)      | How long each Job may run before it is stopped and the restore of its cluster fails.                                                      |

`tables` follows the rules of a backup's `tables`. The target CHI needs backup enabled and the same
backup storage configuration as the source, with the same cluster names and shard layout; a backup
of a CHI of another name, or of another namespace, is found by naming that CHI in
`sourceInstallation`, and its namespace in `sourceNamespace`, see
[Restoring into another CHI](#storage-paths).

On each cluster, the schema is restored on every replica, each from the copy of its shard, and the
data on one replica of each shard; replication then copies `Replicated*` tables to the other
replicas. The schema is restored replica by replica, not `ON CLUSTER`, to keep every table as the
backup describes it: restoring `ON CLUSTER`, `clickhouse-backup` re-points every `Distributed`
table that names another cluster - `all-sharded`, or another cluster of the CHI - at the cluster
it restores. `status.backupName` records the backup the Jobs restore, fixed when the Jobs are
created: editing `backupName` afterwards changes nothing.

**Overwrite.** With `overwrite: true` the restored tables replace the existing ones. A `tables`
pattern of the form `db.*`, and no `tables` at all, restore whole databases: each such database is
dropped, tables created in it after the backup included, and recreated; `db.table` drops only that
table. Tables the restore does not cover are left alone.

A replica dropping a `Replicated*` table on its own would fetch the old data back from another
replica that still has the table, and the restored data would land on top of it. So when a shard of
the cluster has more than one replica and the restore drops a `Replicated*` table the cluster holds,
the Job first drops and recreates the restored tables on the whole cluster at once, in one
`ON CLUSTER` pass from one host, which empties their replication; then it recreates them on every
replica from the backup's definitions - undoing what that pass re-pointed - and restores the data.
Otherwise - every shard has one replica, or no table the restore drops is replicated - there is no
replication to empty, and each host drops and recreates the tables on its own. On a cluster whose
shards have replicas, when the Job finds no replicated table among those the restore drops but
cannot tell the engine of every one of them, as a host or the backup lists a table it cannot read,
it runs the pass if the CHI configures ZooKeeper or Keeper for the cluster, and refuses the restore
otherwise; that is the only case the CHI's ZooKeeper configuration decides.

Before it changes anything, an overwrite restore also refuses, where they apply:

* **a table larger than `max_table_size_to_drop`** - 50 GB unless set - that a drop could not get
  past. ClickHouse drops such a table only when `clickhouse-backup` lifts the limit for it, which
  it does on its own host only: for each table it drops there, and for a database it drops as a
  whole, once, which lets the first such table of that database go. So, when the `ON CLUSTER` pass
  runs, the restore refuses such a table on any other host than the one the pass runs from; and it
  refuses two or more of them in one database on a host that drops that database as a whole itself:
  the host of the pass, or, without the pass, any host. ClickHouse would refuse that drop after the
  restore had dropped other tables, or databases, already;
* **a backup whose shards hold different tables**, of those `tables` matches, when the `ON CLUSTER`
  pass runs. The pass drops and recreates the tables of one shard's backup on every shard, so a
  table that only some shards hold would keep its old replicas on the others, or appear on shards
  that never had it. Narrow `tables` to the tables every shard holds, or drop the others by hand and
  restore them without `overwrite`;
* **a table the Job cannot read** in what a host or the backup lists, where the restore depends on
  it: whether it drops a replicated table, on a cluster the CHI configures no ZooKeeper or Keeper
  for, as above; and, when the `ON CLUSTER` pass runs, whether the shards' backups hold the same
  tables, and, while `max_table_size_to_drop` is set, the size of a table on another host than the
  one the pass runs from. A table whose name a host lists unreadably counts, for the checks of
  databases dropped as a whole, in every database of that host, unless its size is known and within
  the limit.

For tables over the limit, set `max_table_size_to_drop: 0`, no limit, for the time of the restore,
or drop such tables by hand first. The operator takes the limit from the `settings` of the CHI, and
of the cluster, its shards, replicas and hosts: on each host the most specific of the cluster,
shard, replica and host settings counts, and the smaller of that and the CHI's, and over the hosts
of the cluster the smallest applies, so `0` at one level lifts the limit only where no other level
sets one - set it to `0` wherever a limit is set. The operator does not see a limit set
elsewhere - in `files`, or in the image - nor ZooKeeper configured there rather than in the
`zookeeper` of the CHI or the cluster: configure both in these fields.

A database the backup holds without any table is dropped and recreated too, by a restore of whole
databases, but the Job cannot tell it is in the backup: `clickhouse-backup` lists the tables of a
backup, not its databases. The tables created in such a database since the backup count for none of
the checks above, so one of them above `max_table_size_to_drop` can stop the restore after it
changed something. Drop such a database by hand before the restore, or lift the limit.

Do not set `general.disable_environment_override` in a configuration file of the sidecar: the
sidecar then ignores the settings the operator gives it, the pinned ones among them (see
[What the operator runs](#what-the-operator-runs)), and runs the `ON CLUSTER` pass on its own host
only, so the old data of replicated tables comes back next to the restored data.

**A copy of a running CHI needs its own `zookeeper.root`** when both use the same ZooKeeper or
Keeper. A restored `Replicated*` table whose ZooKeeper path resolves to the same location as the
source's - a path built from the table's UUID, a path without `{installation}`, or one whose macros
resolve the same in both, as for a CHI of the same name in another namespace - joins the source's
replication instead of becoming an independent copy, and a `TRUNCATE`, `ALTER DELETE` or
`DROP PARTITION` on the copy then reaches the source.

### Access entities

A backup holds the access entities created with SQL - users, roles, grants, row policies, quotas
and settings profiles - which `clickhouse-backup` backs up by default. `rbac: true` restores them
too, after the schema and the data, on every host of the CHI, and ClickHouse reloads them live with
`SYSTEM RELOAD USERS`. `rbac` needs an empty `cluster`, as access entities belong to the whole CHI,
the more so in a `replicated` user directory, which every cluster using its Keeper path shares.
[Recovery](#recover-a-new-chi-from-a-backup) always restores them. A restore of access entities
holds the schedules, backups and verifications of every cluster of the CHI until it has finished,
see [One operation at a time per cluster](#one-operation-at-a-time-per-cluster).

Each entity of the backup is written back under its own id: one that still exists is set back to
what it was when the backup was made, and the other entities stay, so a user dropped since the
backup was made comes back. `clickhouse-backup` does not resolve conflicts by name, and its
`rbac_conflict_resolution` setting never applies: it looks for conflicts in tables such as
`system.users`, which the sidecar's ClickHouse user cannot read, and so finds none. An entity
created since the backup under the name of one in it - a user dropped and created anew, say - is
another entity of the same name, and ClickHouse keeps only one of the two, not necessarily the
backup's: drop such entities before you restore access entities into a CHI that has its own.

**ClickHouse 23.3 or newer is needed on every host**, as older versions lack `SYSTEM RELOAD USERS`.
The operator does not check the version: on an older ClickHouse the restore completes as usual,
with no warning, but ClickHouse loads the restored entities of a local user directory, the default,
only when it restarts. Restart it then, for example with `spec.restart: "RollingUpdate"`, before
clients rely on them.

A restore that leaves out access entities the backup holds - without `rbac`, or of one cluster -
completes with the reason `RBACNotRestored` and a warning event. Close that gap before clients use
the CHI: the users that exist only as SQL entities are missing, and so are the row policies that
limit what the others see. Restore the whole CHI with `rbac: true`, or restore them afterwards by
hand, in the sidecar of every host, while no backup or restore of its cluster runs:

```bash
kubectl exec chi-backup-demo-main-0-0-0 -c clickhouse-backup -- \
  clickhouse-backup restore_remote --rbac-only default-backup-demo-nightly-20261003-020000
```

## Which hosts are involved

The operator works out the hosts from the CHI as the operator itself built it, so templates,
explicit shard and replica lists and custom names are all accounted for. Only clusters with backup
enabled are considered, narrowed down to `spec.cluster` when set, and each of them is worked on by
a Job of its own. While the CHI cannot run an operation, no Job is created - a backup or restore
stays `Pending` with `Validated=False`, a schedule holds its CronJobs - and the condition's reason
says why:

* the CHI does not exist (`CHINotFound`) or has not completed its reconcile (`CHINotReady`) - a
  schedule validated before the reconcile keeps its CronJobs running on the hosts they had, and stays
  `Validated`, until the reconcile completes, but for the CronJobs a restore holds, see
  [One operation at a time per cluster](#one-operation-at-a-time-per-cluster), which resume once
  both the restore and the reconcile have completed;
* the CHI is stopped with `spec.stop`, so its hosts are down (`CHIStopped`) - its schedules hold
  their CronJobs until it is started again;
* the named cluster does not exist (`ClusterNotFound`), has backup disabled (`BackupDisabled`) or
  enabled without a storage (`NoBackupStorage`, see [Enabling backup](#enabling-backup)), or
  there is no host to work on (`NoTargetHosts`). A backup or a restore waits for a missing or
  disabled cluster only until the CHI has completed its reconcile; then it fails, holding nothing,
  so that a mistyped cluster name never starts a restore days later.

A backup, restore or schedule of every cluster leaves out the clusters with backup enabled without a
storage: a backup or restore warns about them once (`NoBackupStorage`), and a schedule holds
their CronJobs, suspended, until they have one, and names them in its `Validated` condition - with
the reason `NoBackupStorage`, or `CHIRecovered` for a recovered CHI, whose message tells both -
warning when that changes.

The operator checks again periodically, so fixing the CHI lets a waiting resource proceed. A
schedule whose own spec turns invalid - a cron expression or `timeZone` the API server refuses
included - holds its CronJobs the same way (`InvalidSpec`) until the spec is fixed. Once the target
is resolved, the hosts of each cluster, which `status.clusters[].hosts` records for a backup or
restore, are:

* **Backup:** one replica per shard (`FirstPerShard`) or every replica (`AllReplicas`).
* **Retention:** every host of the cluster the backup ran on, which the schedule lists in
  `status.clusters[].retentionHosts`.
* **Verification:** the first replica of each shard.
* **Restore:** the schema on every replica, the data on one replica per shard, access entities on
  every replica; with `overwrite`, one host first runs the `ON CLUSTER` pass when a shard has more
  than one replica and the restore drops a replicated table, see
  [ClickHouseRestore](#clickhouserestore). The overwrite guard checks every replica.
* **Path check:** the storage paths of every backup-enabled cluster of the CHI, see
  [Storage paths](#storage-paths).

## Backup and restore Jobs

Every backup, verification and restore runs as Kubernetes `Job`s in the CHI namespace, one per
cluster, owned by its resource. Jobs and CronJobs are labelled
`clickhouse.altinity.com/backup-cluster` with the cluster's name and
`clickhouse.altinity.com/backup-storage` with the [storage](#storages) the cluster keeps the backups
in, or a restore reads them from. A Job is named `<resource>-<cluster>-backup`, `-verify` or
`-restore`, and a CronJob of a schedule `<schedule>-<cluster>-backup`, shortened with a hash where
Kubernetes limits the length. As these names join resource and cluster names, two resources may need
the same one - backup `a-b` of cluster `c` and backup `a` of cluster `b-c` both need `a-b-c-backup`.
The one that finds the name taken fails on that cluster with the reason `NameConflict`, which names
the other owner, and a schedule leaves that cluster out while its other clusters run; rename one of
the two. A Job or CronJob of that name that nothing owns - created by hand, or left behind when its
resource was deleted with `--cascade=orphan` - is a `NameConflict` as well, whose message tells to
delete it: a schedule then creates its CronJob, while a backup or restore that failed on it has to
be created anew. One whose owner is deleted or being deleted, or that is being deleted itself, is on
its way out instead: the resource, or the schedule's cluster, waits until Kubernetes has removed it
(`WaitingForGarbageCollection`).

A Job runs `bash` (4.4 or newer) and `curl` from the backup image of its cluster and drives the
sidecars of that cluster through their REST API with the credentials of
`<chi-name>-auto-backup-secret`. It needs no access to the Kubernetes API, so no service account
token is mounted. Each Job makes a single attempt, bounded by `activeDeadlineSeconds`.

* **Pod settings follow the ClickHouse Pods.** The Job's Pod runs the image of the sidecar, with its
  pull policy, and takes the `imagePullSecrets`, the `serviceAccountName` and the pod-level
  `securityContext` of the pod template the first host of its cluster runs, so it meets the
  same pod-level rules as the ClickHouse Pods and can pull the backup image from the same private
  registry.
  The Job container itself always runs with `allowPrivilegeEscalation: false` and every capability
  dropped, as the restricted Pod Security Standard and the OpenShift `restricted-v2` SCC require; it
  does not set `readOnlyRootFilesystem`. Inherited `sysctls` are dropped, since the Job may land on
  another node. The user, group, `runAsNonRoot` and seccomp profile of the sidecar's
  `securityContext` - its container's own, or the copy of the ClickHouse container's - go on the Job
  container, where they take precedence over the pod's as they do in the sidecar, so the Job runs as
  the sidecar does - a root user excepted, as the Job needs no privilege. Nothing is invented:
  platforms that assign users, such as the OpenShift `restricted-v2` SCC, refuse a fixed one, and an
  SCC without seccomp profiles refuses any. So on plain Kubernetes a requirement of `runAsNonRoot`
  without a user needs the user named, in the `clickhouse-backup` container's `securityContext` or at
  the pod level of the pod template the hosts run, or the Job fails with `JobNotStarted`; a template
  that names no identity at all lets the Job run as the image's default user, root. The image needs
  `bash` 4.4 or later and `curl`, as the bundled one has.
* **Small, bounded resources.** The Job container requests `50m` CPU and `64Mi` memory, limited to
  `500m` and `512Mi`, so it counts against a `ResourceQuota` that requires limits. A `LimitRange`
  whose maximum is below these refuses the Job (`JobNotStarted`); the values are fixed. A resource
  that targets several clusters runs several such Jobs, which may run at the same time.
* **A Job whose Pod never starts fails.** When no Pod of the Job has started about 10 minutes after
  the Job was created - rejected by admission, an image that cannot be pulled, an exhausted quota -
  its cluster fails with `JobNotStarted` and the Job is stopped, so its Pod cannot start late next
  to another operation; the events of the Job and of its Pod, which is gone once the Job is stopped,
  show why (`kubectl get events`). A Pod that started once counts as started for good, so a
  running Job is never stopped because its Pod briefly lost readiness.
* **Paths are checked first.** Before it changes anything, a backup or restore Job checks the storage
  paths of every backup-enabled cluster of the CHI, see [Storage paths](#storage-paths).
* **Unreachable hosts fail the Job.** A Job does not wait out its deadline for a sidecar it cannot
  reach: when a host stays unreachable for about 10 minutes before the Job has changed anything, the
  Job fails with a message naming the host. Once it has changed something it keeps retrying for
  about an hour, then fails with a message that the operation is partially applied. A change that a
  proxy in front of the sidecar answers with HTTP 502 to 504, as a service mesh may, is never sent
  again, since it may have reached the sidecar: the step fails at once, as one that may have changed
  something - and with it the Job, except for retention and the cleanup of the copies the Job itself
  downloaded, which only warn.
* **A restarted sidecar is followed.** A sidecar that restarts during an upload or a download, the
  last step of a backup and of a verification, resumes it by itself, under a new action. The Job
  waits for that action and succeeds or fails with it. When the sidecar resumes nothing within about
  5 minutes, the Job looks where the operation puts the backup - in remote storage or, for a
  verification, on local disk - and takes the operation as completed when the backup is complete
  there. It does not look earlier: a sidecar that finds a listing in progress resumes nothing.
  Otherwise the Job fails with a message that the outcome is unknown and the upload or download may
  still complete: check the backup before relying on it or deleting it. A restore is never resumed:
  a sidecar restart during a restore, or between two of its steps on a host, fails the Job as
  partially applied rather than going on with an environment that may have changed.
* **Why a Job failed outlives the Job.** The Job script writes its final error to the termination
  message of its container; a container that fails without writing one leaves the end of its log
  instead. The operator copies that message, cut to a kilobyte that keeps its start and its end,
  into the cluster's entry in `status.clusters`, the condition that reports the failure (`Ready`,
  or `Verified` for a verification; `lastFailureMessage` for a schedule) and the warning event, so
  it is still there when the Job, its Pod and its log are gone.
* **A backup that holds no data is reported.** A backup of tables that hold no data, in the
  `partitions` it names if any, or of no table at all - a `tables` pattern that matches nothing, an
  installation recreated empty - succeeds. Once it is made, the Job lists it on every host it ran on
  and, when no host's copy holds data, locally or on an object disk, says so in its termination
  message. The operator then reports the backup completed with the reason `CompletedWithoutData`,
  in the `Ready` condition of a `ClickHouseBackup` or the `LastBackupSucceeded` condition of a
  schedule, and with a warning event of that reason that tells the likely cause and, for a schedule
  with [retention](#retention), that the backup does not count toward the backups with data it
  keeps, both as the Job was built. A `schemaOnly` backup never holds data and is not reported; nor
  is a backup when a host cannot be listed then, or lists its copy broken, or with a
  `clickhouse-backup` older than 2.6.6, which lists no sizes.

## Retention

`retention.keepLast: N` makes every successful run of a schedule delete that schedule's older
backups on the cluster it ran on, keeping the `N` most recent that hold data and, apart from them,
the `N` most recent that hold none (see below). Retention is a rule of the schedule's series - the
backups its runs name `<backupNamePrefix>-YYYYMMDD-HHMMSS` - not of any one backup, which is why a
`ClickHouseBackup` sets none, not even one that [runs the schedule](#run-a-schedule-now). As the
prefix names the series, it can be set once and is immutable after (see
[ClickHouseBackupSchedule](#clickhousebackupschedule)): retention would never prune the backups of
a former prefix. Retention is deliberately narrow:

* it only considers the backups of the series, so backups of other schedules and one-off backups are
  never touched - the backups that [run a schedule now](#run-a-schedule-now) included, and a one-off
  `backupName` of the series' form is refused. Give each schedule its own prefix. The default
  prefix, `<namespace>-<chi>-<schedule>`, differs between the schedules of one Kubernetes cluster,
  even across CHIs that share a storage path, unless their names joined with dashes coincide; a
  prefix set by hand must be unique among all schedules writing to that path, or each schedule's
  retention deletes the other's backups (see [Storage paths](#storage-paths));
* it works per cluster: each cluster's runs prune on every host of that cluster the backup ran on,
  since each shard (and, with `AllReplicas`, each replica) stores its own part of the backup, and on
  those only. A cluster whose runs keep failing does not stop the others from pruning, and a host
  that leaves them is pruned no more, see
  [Hosts that leave a schedule](#hosts-that-leave-a-schedule);
* it never deletes the backup the run has just made, nor a backup that a kept backup depends on as
  its incremental base;
* a backup that `clickhouse-backup` lists as broken - an upload that never completed, for example -
  does not count toward the `N` kept, so failed uploads never push restorable backups out. Broken
  backups newer than the `N`-th kept backup (with data or without, whichever is newer, see below)
  are left alone, as an upload still in progress looks broken too; older ones are deleted, unless
  another host's copy holds data (see the next bullet);
* a backup, with data or without, counts toward the `N` kept only if every host of the cluster
  that retention runs on lists it complete: a run that failed on one host leaves copies on the
  others that cannot be restored on their own. Such a copy with data - made before a host was
  added, for example - is kept until `N` backups with data that every host holds are; any other is
  kept and deleted like a broken backup. When a host cannot be listed, retention skips that run;
* a backup without data - of a `tables` pattern that matches nothing any more, or of an installation
  recreated empty - does not count toward the `N` kept either: retention keeps the `N` newest
  backups with data and, apart from them, the `N` newest without, so runs without data - every run
  of a `schemaOnly` schedule among them - never push out the backups that hold data, and a host that
  never holds data still prunes. Data on an object disk counts as data, and so does every backup of
  a `clickhouse-backup` older than 2.6.6, which does not list sizes. A run without data that is not
  `schemaOnly` is [reported](#backup-and-restore-jobs) on the schedule. A backup holds data when any
  host's copy of it does, so every host of the cluster keeps and deletes the same backups - an empty
  shard prunes its copies of the backups another shard filled only along with that shard.

`0`, the default, keeps every backup.

### Hosts that leave a schedule

Retention prunes on the hosts the schedule backs up when the run starts. A host that leaves them
takes its backups out of retention: a removed shard, a replica removed while the schedule
backs up `AllReplicas`, a cluster removed or with backup disabled, or one that `spec.cluster` or
`replicaSelection` no longer selects. No run reaches the host's sidecar any more, and when it kept
a storage path of its own, as the default path gives every shard, no other host prunes
the backups it uploaded there: they stay in the storage. The operator does not delete them, as
they may be all that is left of a removed shard's data, needed to restore the layout they were
made of.

The schedule records such hosts instead, per cluster. `status.clusters[].retentionHosts` lists the
hosts of the cluster that a run has backed up, as the Job of the run lists them: a host no run
reached - of a suspended schedule, or one added and removed again between two runs - holds no
backup of the schedule and is never recorded. With retention on, `status.clusters[].unprunedHosts`
lists the hosts that left `retentionHosts`, and an `UnprunedHosts` warning event names the hosts and
the backup name prefix when they leave. A host that rejoins, as a shard added back does, is pruned
again and drops out of `unprunedHosts`. The record is by host, so it also lists a host whose backups
another host still prunes on a shared storage path: the first replica of a shard removed while the
shard stays, for one, hands the shard's path to the next replica. A host that leaves while retention
is off is not recorded as unpruned, and retention turned on later prunes only the hosts the schedule
backs up by then. A cluster that leaves the schedule - removed, backup disabled - loses its CronJob,
and its entry in `status.clusters` stays as long as it lists unpruned hosts; one with backup enabled
without a storage keeps its CronJob, suspended, and its entry. A schedule whose
`spec.cluster` is removed, has backup disabled or lacks a storage holds its CronJob instead
(`ClusterNotFound`, `BackupDisabled`, `NoBackupStorage`) and prunes nothing until it runs again.

To clean up by hand, delete the backups you no longer need from the storage path the host used,
with its macros expanded, in the storage the cluster's entry names in `storage`, while no backup or
restore of its cluster runs. Run `clickhouse-backup` in the sidecar of a host still backed up that
uses that storage, with the path overridden for the command: `S3_PATH`, or its counterpart on
other storage types, such as `GCS_PATH` or `AZBLOB_PATH`. The sidecar of another storage needs
the bucket and credentials overridden as well. `delete remote` refuses a backup that another one is
based on.

A backup of tables on an object disk keeps their data apart, under the object disk path, and
`delete remote` deletes it there too. Without an override, that is the object disk path of the host
the command runs in, with that host's macros: for a backup that host still holds, the command
deletes the object disk data of the host's own copy. With object disks, override the object disk
path as well, with the removed host's macros expanded: `S3_OBJECT_DISK_PATH`, or its counterpart
such as `GCS_OBJECT_DISK_PATH` or `AZBLOB_OBJECT_DISK_PATH`. When hosts share one object disk path,
as the replicas of a shard do under `AllReplicas` unless it holds `{replica}`, the data there
belongs to every one of their copies of a backup: delete only the backups that the hosts still
backed up no longer list.

For shard `1` of cluster `main` of CHI `backup-demo` in namespace `default`, which the default paths
put at `backup/default/backup-demo/main/1` and `object_disks/default/backup-demo/main/1`:

```bash
kubectl exec chi-backup-demo-main-0-0-0 -c clickhouse-backup -- \
  clickhouse-backup --env S3_PATH=backup/default/backup-demo/main/1 list remote
# With object disks, add the object disk path of the shard:
# --env S3_OBJECT_DISK_PATH=object_disks/default/backup-demo/main/1
kubectl exec chi-backup-demo-main-0-0-0 -c clickhouse-backup -- \
  clickhouse-backup --env S3_PATH=backup/default/backup-demo/main/1 delete remote default-backup-demo-nightly-20261003-020000

# Once done, clear the record of the cluster - here the first entry of status.clusters, see
#   kubectl get chbs nightly -o jsonpath='{.status.clusters[*].name}'
# The hosts are not recorded again
kubectl patch chbs nightly --subresource=status --type=json \
  -p '[{"op": "remove", "path": "/status/clusters/0/unprunedHosts"}]'
```

## Incremental backups

`diffFromRemote: <existing backup>` on a `ClickHouseBackup` uploads only the data parts that are
not already in that backup. Each cluster's backup builds on that cluster's copy of the base, so the
base must exist on every cluster the backup targets. Restoring an incremental backup needs its whole
chain of bases in the storage. Retention never deletes a base that a kept backup still needs, but
deleting bases by other means (`BACKUPS_TO_KEEP_REMOTE`, a bucket lifecycle rule, manual removal)
breaks every backup built on them.

## Verification

`verify: true` on a `ClickHouseBackup` runs a second Job per cluster once the backup has completed
on every cluster; a backup that failed on any cluster is not verified. Each Job downloads the
backup from the storage on the first replica of each shard of its cluster and checks it, without
touching any table, so the result tells you the backup can actually be read back. With
`AllReplicas`, the copies uploaded by the other replicas are not downloaded. The download needs free
space on the data disk for one copy of the backup and is removed afterwards.
The outcome is the `Verified` condition (`Verified` or `VerificationFailed`).

## Restore safety

A restore is destructive, so each Job checks the hosts of its cluster **before** it changes
anything:

1. **Storage paths** of every backup-enabled cluster of the CHI, see [Storage paths](#storage-paths).
2. **Backup present everywhere** (`validateTopology: true`, the default): every target shard must
   hold the backup. This catches a backup taken from a different layout or a wrong storage path
   before anything is half-restored.
3. **Overwrite guard** (`overwrite: false`, the default): the restore is refused if any replica of
   the cluster already holds a table the restore would replace - a table of the backup that `tables`
   matches, or, as `db.*` restores a whole database, any table in such a database; other tables do
   not count. Without `tables`, every table outside the system databases counts, whether the backup
   holds it or not, and so does every table when the Job cannot make out a table of the backup or of
   a host. Every replica is checked, as the schema is restored on each, so the sidecar of every
   replica must be reachable. Set `overwrite: true` to drop and recreate the restored tables, see
   [ClickHouseRestore](#clickhouserestore). This guard is the only protection for existing data:
   `clickhouse-backup` recreates every restored table during the schema restore, whether or not
   `overwrite` is set.
4. **What an overwrite needs** (`overwrite: true`): no table above `max_table_size_to_drop` that a
   drop could not get past, and, where the restore drops the tables `ON CLUSTER`, the same tables in
   the backup of every shard, see [ClickHouseRestore](#clickhouserestore).

Only when every host passes does the restore start. The recommended path is restoring into a fresh,
empty CHI and switching clients over once it is verified.

Do not change the target CHI while a restore runs. A change that rolls its Pods restarts the
sidecars, which interrupts the restore on the restarted host and leaves the cluster partly restored;
the restore then has to be repeated, with `overwrite: true` since some tables already exist. Do not
change the Secrets the sidecars take their environment from either: a sidecar restarts for such a
change once it is idle, also while the restore works on other hosts, which leaves the restore
applied with the old values on some hosts and the new ones on others, or fails it as partially
applied (see [Secret changes](#secret-changes)).

## One operation at a time per cluster

Backups, scheduled runs and restores of the same cluster do not run concurrently, while those of
different clusters of a CHI do. The operator serializes the Jobs per cluster - a resource waiting
for its turn on a cluster reports `OperationInProgress` - and each Job additionally waits until the
sidecars it uses are idle, so overlapping runs queue instead of failing. A resource that targets
several clusters starts the Job of each cluster as soon as that cluster is free.

A restore also holds the schedules of the clusters it restores: every schedule's CronJob of such a
cluster stops starting runs (`SuspendedForRestore`) as soon as the restore has passed validation,
while the CronJobs of the other clusters keep running; a restore of the whole CHI holds them all,
and so does one that restores access entities, see below.
The restore records its validation first and creates its Jobs on a later pass, once every CronJob it
holds has been suspended for at least 15 seconds, long enough for the CronJob controller to have
seen it (`WaitingForSchedules` until then), so no scheduled backup starts in the middle of it: a
schedule that has not seen the validation yet could otherwise resume its CronJob, which then runs a
missed run at once. A scheduled run that started before the suspension is waited for
(`OperationInProgress`). The schedules resume when the restore completes - or, while their CHI
reconciles, once that reconcile completes.

A restore that reads the backup of another CHI of its namespace (`sourceInstallation`, see
[Restoring into another CHI](#storage-paths)) holds the schedules of that CHI's clusters it reads as
well, each until it finishes there: the retention of a run there could delete the backup it reads.
It waits for their CronJobs the same way, and for a scheduled run there that started before the
hold; the hold applies while that CHI reconciles too, which the restore does not wait for. While the
restore waits for its own CHI to exist, complete its reconcile or start again, it holds nothing
there, as that may never come, and holds them again before it starts. That CHI's runs missed
meanwhile are not all made up: a resumed CronJob starts the latest one it missed at once, unless
that is past the schedule's `startingDeadlineSeconds`, and skips the others, so restore from another
CHI outside its backup window. A restore of another namespace's backup (`sourceNamespace`) holds
nothing there - a restore of one namespace never stops the backups of another - so suspend the
schedules of that CHI for the restore, or restore a backup their retention keeps.

A restore that restores access entities - one with `rbac: true`, and every recovery restore -
changes them for the whole CHI, those kept in Keeper on every cluster at once, as it does the
user-defined functions kept there. So that no backup captures them half restored, such a restore
holds the schedules of every backup-enabled cluster of the CHI, whichever clusters it restores; it
starts the Job of each of its clusters only once the CronJobs of every backup-enabled cluster are
held that way and no other operation runs on any of them; and until it has finished, the backups and
verifications of the CHI wait for it, on every cluster (`OperationInProgress`). The recovery
restores of a CHI's clusters therefore run one after another.

A restore that fails after it changed something may have left a cluster partly restored, which must
not be backed up as if it were the CHI's data, possibly pushing good backups out of retention: it
keeps holding the schedules of its own CHI it held (`SuspendedForFailedRestore`, announced with a
warning event) until you delete the failed `ClickHouseRestore`, which the schedule's `Ready` message
names; those of the CHI whose backup it read resume as it fails - or, while that CHI reconciles,
once that reconcile completes - as nothing changed there. A restore whose Job failed before it
changed anything - the [overwrite guard](#restore-safety) refused it, `validateTopology` found the
backup missing, no sidecar could be reached - reports `JobFailedUnchanged` and holds nothing. A
restore whose Job never started (`JobNotStarted`) keeps holding: a Pod that started just as the Job
was stopped leaves no trace of what it ran, so once the events of the Job and its Pod show why the
Pod could not start, delete the restore. A CHI waiting to be [recovered from a
backup](#recover-a-new-chi-from-a-backup) holds the schedules of the clusters it recovers as well,
and a CHI recovered, or to be, from a backup whose name starts with its own `<namespace>-<chi>-`
holds every schedule of it that uses the default prefix, whatever its restores do
(`SharesSourcePrefix`, see [Storage paths](#storage-paths)).

## Recover a new CHI from a backup

Name the backup a new CHI starts from in its `spec.backup.recovery`:

```yaml
spec:
  backup:
    enabled: "yes"
    storages:
      - name: main
        secretRef:
          name: backup-demo-storage
    recovery:
      backupName: default-backup-demo-nightly-20261003-020000
      # The CHI that made the backup, when another one - its namespace too, when another and the
      # storage paths hold {namespace}: the backup lies under the storage paths of that CHI
      sourceInstallation: backup-demo
      # sourceNamespace: team-a
```

Once the CHI completes its first reconcile, the operator creates one `ClickHouseRestore` for each
backup-enabled cluster with a backup to recover from, named `<chi>-<cluster>-recovery` and owned by
the CHI, that restores the cluster from that backup, [access entities](#access-entities) included.
It creates each only while none of that name exists, so an operator restart never starts a second
one. When every recovery restore has completed, the operator stamps the
`clickhouse.altinity.com/recovered-from` annotation with the backup restored - or, when clusters
were restored from different backups, with `<cluster>:<backup>` pairs, separated by commas and
sorted by cluster. From then on `recovery` changes nothing. Until then the schedules of the clusters
being recovered are held, so nothing backs up the CHI before it holds its data; and as a recovery
restore restores access entities, while one runs nothing backs up any cluster of the CHI, and the
recovery restores run one after another, see [One operation at a time per
cluster](#one-operation-at-a-time-per-cluster). A recovery from the backup of another CHI of the
namespace (`sourceInstallation`) holds that CHI's schedules of the clusters each recovery restore
reads, too, from its validation until it ends: those production backups miss their runs meanwhile,
of which at most the latest starts once the restore ends, so recover outside that CHI's backup
window. When the backup's name starts with the CHI's own `<namespace>-<chi>-`, as a backup of a
disaster recovery copy's source does, every schedule of the CHI that uses the default prefix is held
besides, from the start and past the recovery, see [Storage paths](#storage-paths). Neither `recovery`
nor the annotation reaches the Pods, so setting them never restarts anything. See
[`07-recover-from-backup.yaml`](chb-examples/07-recover-from-backup.yaml).

Each cluster inherits `recovery` from `spec.backup`, as it does the other backup fields, and may
name another backup - one of a schedule of its own, for example, or the last backup of the cluster
by a schedule of all clusters, its `status.clusters[].lastBackupName`, while the schedule's
`lastBackupName` lags behind (see [ClickHouseBackupSchedule](#clickhousebackupschedule)) - or
`""`, for nothing to recover, as a cluster the backup does not hold needs:

```yaml
spec:
  backup:
    enabled: "yes"
    storages:
      - name: main
        secretRef:
          name: backup-demo-storage
    recovery:
      backupName: default-backup-demo-nightly-20261003-020000
  configuration:
    clusters:
      - name: main
      # Backed up by a schedule of its own
      - name: events
        backup:
          recovery:
            backupName: default-backup-demo-events-20261003-023000
      # New in this CHI, in no backup
      - name: scratch
        backup:
          recovery:
            backupName: ""
```

A cluster with backup disabled is not recovered. `recovery` is read from the CHI only, never from a
`ClickHouseInstallationTemplate`. A recovery restore never overwrites, and restores every table of
the backup: on a CHI that already holds any table outside the system databases, whether the backup
holds it or not - one an init script creates, say - the [overwrite guard](#restore-safety) refuses
it. Each recovered cluster needs the cluster name and shard layout it had in the source, and its
storage Secret must point at the same location and exist before the CHI is applied: the
restore starts right after the first reconcile, before the sidecars would pick up a Secret created
later.

The operator announces the recovery with events on the CHI, `JobCreated` when it creates a recovery
restore and `Completed` when it records the recovery. Problems are reported as warning events on
the CHI: a backup name that is not valid (`InvalidSpec`), a CHI that cannot run the restore
(`CHIStopped`, `BackupDisabled`, `NoTargetHosts`), a name conflict (`NameConflict`, see below), and a
failed restore, with its reason and the error its Job reported - a backup missing in the
storage, for example. A cluster that asks for a recovery with backup enabled but without a
storage holds the whole recovery, warning with `NoBackupStorage`, until it has one or is left out of
the recovery with `recovery.backupName: ""`: recovering the other clusters alone would record the
recovery, and that cluster would never be recovered. The schedules the recovery holds meanwhile name
the clusters it waits for.

A `ClickHouseRestore` of the name a recovery restore needs that restores another CHI or cluster -
the names join the CHI and cluster names, so CHI `a-b` with cluster `c` and CHI `a` with cluster
`b-c` both need `a-b-c-recovery` - or that the operator did not create, as one of your own of that
name, is a `NameConflict`. The operator leaves it alone, and the recovery of the cluster waits until
you delete it, once it has finished; the operator then creates its own. When it is the recovery
restore of another CHI, delete it only once that CHI has recorded its recovery, or no longer asks
for one: until then its operator creates it again. A restore of that name on
its way out - left by a deleted CHI of the same name, or being deleted - is waited for until
Kubernetes has removed it.

A recovery restore that failed before it changed anything (`JobFailedUnchanged`) releases the
schedules it held, which back up the CHI as it is - but for those the prefix guard holds
(`SharesSourcePrefix`, see [Storage paths](#storage-paths)), which stay held. One that failed after
changing something, or whose Job never started (`JobNotStarted`), keeps them all held. Either way
the CHI stays unrecorded:

* to try again, fix the cause - the backup name in `recovery`, the storage Secret (see
  [Secret changes](#secret-changes) on when a changed one takes effect), ... - and delete the failed
  `<chi>-<cluster>-recovery` restore: the operator creates a new one. A restore over a partly
  restored cluster is refused by the [overwrite guard](#restore-safety), so drop what the failed
  restore created first, and a local copy of the backup it left - from another CHI, its Job stopped
  or evicted - with `clickhouse-backup delete local <backup>` in the sidecar of the hosts named;
* to restore by hand instead, create a `ClickHouseRestore` that sets `overwrite: true` (and `rbac`,
  when it restores the whole CHI) and wait until it has completed; it holds the schedules of the
  clusters it restores meanwhile, as any restore does. Then record the recovery yourself by setting
  the `clickhouse.altinity.com/recovered-from` annotation to the name of the backup you restored -
  the operator takes any value as a recorded recovery - and delete the failed recovery restore. That
  releases the schedules the failed restore held, but for those the prefix guard holds
  (`SharesSourcePrefix`, see [Storage paths](#storage-paths)) until they get a `backupNamePrefix`;
  deleting the failed restore before recording the recovery makes the operator create it again;
* to give up, first make sure the CHI's schedules cannot prune the backups of the CHI the backup
  came from: where both share a storage path and their prefixes match, as the default ones of a CHI
  of the same name and namespace do, give the schedules a prefix of their own or the CHI a path of
  its own (see [Storage paths](#storage-paths)) - the next steps release them at once, and a missed
  run starts right away. Then set `recovery.backupName: ""` on the cluster, or remove `recovery` -
  from the CHI's manifest too, or the next apply of it starts the recovery anew - and delete the
  failed `ClickHouseRestore`: the schedules back up the CHI as it is, with no `CHIRecovered`
  warning.

When the source CHI keeps running on the same ZooKeeper or Keeper, the new CHI needs its own
`zookeeper.root` (see [ClickHouseRestore](#clickhouserestore)).

## Status, events and metrics

Every resource reports progress in `status`:

* `phase` (backups and restores): `Pending`, `Running`, `Completed` or `Failed`, over all their
  clusters - `Completed` once every cluster has completed, `Failed` once every cluster has finished
  and any of them failed, and `Pending` or `Running` until then;
* `clusters` (backups and restores): one entry per cluster, fixed when the resource is validated,
  with the cluster's `phase`, `jobName`, `hosts`, `storage` - the [storage](#storages) the Job keeps
  the backup in, or reads it from, as the Job was built - `startTime` and `completionTime`, and the
  `reason` and `message` of its outcome; for a backup with `verify: true`, `jobName` names the
  verification Job, `<resource>-<cluster>-verify`, once the backup has completed on every cluster;
* `cluster` (restores): the `spec.cluster` the restore was validated with, empty for a restore of
  every backup-enabled cluster; which schedules the restore holds, and whether it restores access
  entities, follow it, whatever the spec names later;
* `startTime`, `completionTime`, `durationSeconds`, and `backupName`: the backup made, or restored;
* schedules report per cluster in `clusters` - `cronJobName`, `storage`, `lastScheduleTime`,
  `lastSuccessfulTime`, `lastBackupName`, `lastFailureTime`, `lastFailureMessage`, `retentionHosts`
  and `unprunedHosts` - and over all clusters `lastSuccessfulTime` and `lastBackupName`, see
  [ClickHouseBackupSchedule](#clickhousebackupschedule);
* `conditions`:

| Condition             | Meaning                                                                                               |
|-----------------------|-------------------------------------------------------------------------------------------------------|
| `Validated`           | The spec and the target CHI passed validation.                                                        |
| `JobCreated`          | The Jobs (or the schedule's CronJobs) have been created.                                              |
| `Ready`               | The operation finished successfully on every cluster, or the schedule is active and its runs succeed. |
| `Verified`            | Backups with `verify: true`: the backup passed verification.                                          |
| `LastBackupSucceeded` | Schedules: the last finished runs succeeded.                                                          |

The condition `reason` says why, with the details in `message`:

| Reason                                               | Meaning                                                                                                                                                                                                                                                                                          |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Validated`                                          | The spec and the target CHI passed validation.                                                                                                                                                                                                                                                   |
| `CHIRecovered`                                       | Schedules: as `Validated`, for a CHI restored from a backup, which may share its storage path with the CHI the backup came from.                                                                                                                                                                 |
| `SharesSourcePrefix`                                 | Schedules: held, the CHI being recovered from a backup starting with `<namespace>-<chi>-`, until `backupNamePrefix` is set, see [Storage paths](#storage-paths).                                                                                                                                 |
| `InvalidSpec`                                        | The spec failed validation, or that of the schedule a backup [runs](#run-a-schedule-now).                                                                                                                                                                                                        |
| `WaitingForScheduledTime`                            | Backups: the backup waits for its `scheduledTime`, see [Back up at a later time](#back-up-at-a-later-time).                                                                                                                                                                                      |
| `StartingDeadlineExceeded`                           | Backups: the backup, or the Job of a cluster, did not start within `startingDeadlineSeconds` of the time it was due; the message tells what it waited for.                                                                                                                                       |
| `ScheduleNotFound`                                   | Backups that [run a schedule now](#run-a-schedule-now): the schedule does not exist (yet); the backup waits.                                                                                                                                                                                     |
| `CHINotFound`, `CHINotReady`, `CHIStopped`           | The CHI does not exist, has not completed its reconcile, or is stopped; the resource waits.                                                                                                                                                                                                      |
| `ClusterNotFound`, `BackupDisabled`, `NoTargetHosts` | The CHI cannot run the operation as configured, see [Which hosts are involved](#which-hosts-are-involved); for a backup that [runs a schedule](#run-a-schedule-now), also a `cluster` the schedule does not back up.                                                                             |
| `NoBackupStorage`                                    | The target cluster has backup enabled without a storage that has a `secretRef`, or `podTemplate`, which leaves it off, see [Enabling backup](#enabling-backup). Schedules of every cluster: as `Validated`, leaving such clusters out, their CronJobs suspended.                                 |
| `OperationInProgress`                                | Another operation of the cluster runs, or, for a backup or a verification, a restore of access entities, which waits for every cluster; the resource waits its turn; a restore also waits for a scheduled run of the CHI whose backup it reads.                                                  |
| `WaitingForGarbageCollection`                        | A Job or CronJob of the expected name belongs to a deleted resource, or is being deleted; the resource, or a schedule's cluster, waits until it is gone.                                                                                                                                         |
| `NameConflict`                                       | The Job, CronJob or recovery restore name is taken by another owner, or an unowned object; that cluster fails or is skipped, see [Jobs](#backup-and-restore-jobs).                                                                                                                               |
| `WaitingForSchedules`                                | A restore waits until every schedule it holds - of the clusters it restores, or of every cluster for access entities, and of the clusters of the CHI whose backup it reads - holds its runs.                                                                                                     |
| `JobCreated`                                         | The Jobs, or the schedule's CronJobs, have been created.                                                                                                                                                                                                                                         |
| `Completed`                                          | The operation finished successfully.                                                                                                                                                                                                                                                             |
| `CompletedWithoutData`                               | A backup finished successfully, but holds no data on any host, see [Backup and restore Jobs](#backup-and-restore-jobs).                                                                                                                                                                          |
| `RBACNotRestored`                                    | Restores: finished successfully, but left out access entities the backup holds, see [Access entities](#access-entities).                                                                                                                                                                         |
| `PartiallyCompleted`                                 | Backups and restores: failed on some clusters and completed on the others; `status.clusters` tells which.                                                                                                                                                                                        |
| `JobFailed`                                          | A Job failed; the message carries the error it reported.                                                                                                                                                                                                                                         |
| `JobFailedUnchanged`                                 | Restores: a Job failed before it changed anything; the message carries the error it reported. Holds no schedule.                                                                                                                                                                                 |
| `JobNotStarted`                                      | No Pod of a Job started within about 10 minutes; the events of the Job and its Pod say why. Restores: keeps holding its schedules.                                                                                                                                                               |
| `JobLost`                                            | A Job disappeared before it finished.                                                                                                                                                                                                                                                            |
| `DeadlineExceeded`                                   | A Job ran past `activeDeadlineSeconds`.                                                                                                                                                                                                                                                          |
| `Verifying`, `Verified`, `VerificationFailed`        | Backups with `verify: true`: verification is running, passed or failed.                                                                                                                                                                                                                          |
| `Scheduled`, `Suspended`, `SuspendedForRestore`      | Schedules: the CronJobs are active, suspended by `spec.suspend`, or held for a restore of their cluster or of access entities, or one that reads their backup, see above - or, once no restore holds them any longer, kept suspended until the CHI completes the reconcile it is in. |
| `SuspendedForFailedRestore`                          | Schedules: held by a failed restore, which may have left their cluster partly restored, until that restore is deleted.                                                                                                                                                                           |
| `LastBackupFailed`                                   | Schedules: the last finished run of a cluster failed; the CronJobs keep running the schedule.                                                                                                                                                                                                    |
| `LastBackupWithoutData`                              | Schedules: the last finished run succeeded, but its backup holds no data; the CronJobs keep running the schedule.                                                                                                                                                                                |

```bash
kubectl get chb,chbs,chr
kubectl describe chb backup-demo-once             # conditions and events
kubectl get chb backup-demo-once \
  -o jsonpath='{range .status.clusters[*]}{.name}{"\t"}{.phase}{"\t"}{.jobName}{"\n"}{end}'
kubectl logs job/<status.clusters[].jobName>      # what the Job of a cluster did
kubectl logs job/backup-demo-once-main-backup     # with verify: true, the backup Job of cluster main
```

The operator also emits Kubernetes Events on the resources when the operation of a cluster starts,
completes and fails, an `UnprunedHosts` warning on a schedule when hosts leave it while retention is
on, a warning event `CompletedWithoutData` for a backup that holds no data, which the metrics count
as completed, a warning event `RBACNotRestored` for a restore that left out access entities, a
warning event `SharesSourcePrefix` when a schedule is held for its prefix, and a warning event
`NoBackupStorage` for an operation of every cluster that leaves out a cluster with backup enabled
without a storage. It exports metrics on its existing metrics endpoint, labelled with
`namespace`, `chi` and `cluster` among them:
`clickhouse_operator_backups_started`, `clickhouse_operator_backups_completed`,
`clickhouse_operator_backups_failed`, `clickhouse_operator_restores_started`,
`clickhouse_operator_restores_completed`, `clickhouse_operator_restores_failed`,
`clickhouse_operator_backup_verifications_failed`, `clickhouse_operator_backup_duration_seconds`,
`clickhouse_operator_backup_last_success_timestamp` and
`clickhouse_operator_backup_last_failure_timestamp`. The two timestamps, the latest of any backup or
scheduled run of the cluster, are set again from the resources' status whenever the operator
reconciles them, so they are back after an operator restart; the counters start from zero. Alert on
the last success per cluster: one cluster that stopped backing up does not show in a timestamp of
another. The timestamps of a cluster that no longer backs up - removed from the CHI, or with backup
disabled - go away, and so do those of every cluster of a CHI that is deleted, once the operator
reconciles a backup resource of that CHI; nor does an operator restart bring them back, as the
operator sets them only for the clusters the CHI still backs up. An alert on their age therefore
does not fire for a cluster that is no longer backed up. A cluster with backup enabled without a
storage keeps them, as its spec still asks for backups: an alert on their age fires for it.

## Limitations and security notes

* **Experimental API.** Fields and behavior may change in a future release.
* **The sidecar REST API runs commands, for anyone with the CHI's credentials.** It is plain HTTP
  with basic auth, on port 7171 of every Pod and `ClusterIP` host Service of a backup-enabled
  cluster, inside the Kubernetes network. Besides backups and restores, it runs `clickhouse-backup`
  commands with overridden environment variables, the command `clickhouse-backup` runs to reload
  ClickHouse among them: whoever can call it can run commands in the sidecar, which holds the backup
  storage credentials and mounts the data volumes. One set of credentials, in
  `<chi-name>-auto-backup-secret`, serves every sidecar of every cluster of the CHI, so anyone who
  can read Secrets in the namespace, or capture the plain-HTTP traffic, can do so in all of them.
  Allow port 7171 only from the backup and restore Jobs, with a NetworkPolicy such as the one below.
  A NetworkPolicy that selects the ClickHouse Pods refuses every connection it does not allow, so it
  must keep all other ports of the CHI open as before; and anyone who can create Pods in the
  namespace can still give a Pod the label of the Jobs.

  ```yaml
  apiVersion: networking.k8s.io/v1
  kind: NetworkPolicy
  metadata:
    name: backup-demo-backup-api
  spec:
    podSelector:
      matchLabels:
        clickhouse.altinity.com/chi: backup-demo
    policyTypes: ["Ingress"]
    ingress:
      # The backup API: only from the backup and restore Jobs of this CHI
      - from:
          - podSelector:
              matchLabels:
                clickhouse.altinity.com/backup-chi: backup-demo
        ports:
          - port: 7171
      # Every other port the CHI serves, from wherever it was reached before
      - ports:
          - port: 8123   # HTTP
          - port: 9000   # native TCP
          - port: 9009   # interserver
  ```
* **No consistency across shards or clusters.** A backup is a set of per-shard backups of one
  name, taken one host after another, see [Installations and clusters](#installations-and-clusters).
* **Clusters that share data are not independent.** A `Distributed` table on one cluster over
  another, or over `all-sharded`, returns partial results or fails while the other cluster is
  restored, and returns the restored data afterwards. A `Replicated*` table or `Replicated`
  database whose Keeper path several clusters share - one without `{cluster}` - is backed up by
  each of them, so restoring the data of more than one of them can attach it twice; and an
  `overwrite` restore of one of them drops and refills it on the others too. Keep `{cluster}` in
  such paths unless the clusters are meant to share the table. When they are, restore its data
  through one cluster only: restore that cluster, then the others with `tables` naming their own
  tables, then their copy of the shared table with `schemaOnly`, which replication then fills. A
  recovery restores every cluster in full, so set `recovery.backupName: ""` on such other clusters
  and restore them this way.
* **Restore writes data on one replica per shard.** Non-replicated tables are therefore only restored
  on that replica: of an `AllReplicas` backup, the other replicas restore their schema from their own
  copies, and the data they uploaded is not restored, see [Storage paths](#storage-paths).
* **An overwrite restore has requirements of its own.** Tables larger than `max_table_size_to_drop`
  (50 GB by default) may need the limit lifted, at every level that sets it, and on a cluster with
  replicas whose replicated tables it drops, every shard's backup must hold the same tables; a
  database the backup holds without tables is dropped unchecked, see
  [ClickHouseRestore](#clickhouserestore).
* **Layout must match.** The target clusters need the names of the source clusters, and restoring
  into fewer shards than the backup has leaves the extra shards' data out; `validateTopology` only
  checks that every target shard holds the backup.
* **Access entities need ClickHouse 23.3.** Restoring them relies on `SYSTEM RELOAD USERS`; on an
  older ClickHouse the restore completes, but ClickHouse loads them only at its next restart, see
  [Access entities](#access-entities).
* **Several backup-enabled clusters need settings the operator can see.** In such a CHI, a Job
  refuses to run when the storage paths of its cluster cannot be checked - a configuration file of the
  sidecar, an `envFrom` of the sidecar's container, a macro the check does not know - and a configuration
  file built into the image goes unseen, see [Storage paths](#storage-paths).
* **Do not change a CHI while it is being restored.** A rolling update restarts the sidecars and
  leaves a partly applied restore, see [Restore safety](#restore-safety).
* **A Secret change restarts the sidecars.** Each sidecar restarts once it is idle, also while a
  backup or restore works on other hosts, which takes its Pod out of the CHI's Service for a moment;
  a key removed from a Secret is not noticed, see [Secret changes](#secret-changes).
* **Targets and a schedule's prefix are immutable on Kubernetes 1.25 and newer only.** Older
  versions, and the `v1beta1` bundle, accept an edit of `clickHouseInstallation` or `cluster`, and a
  change of a schedule's `backupNamePrefix`, which leaves the backups of the former series unpruned,
  as setting one on a schedule that has run does on any version. A backup or a restore keeps the
  cluster it recorded when it was validated, but an edit of `clickHouseInstallation` moves it, as a
  whole, to the other CHI; a schedule, which records no target, follows either edit, as if the
  clusters it no longer targets had left it (see
  [Hosts that leave a schedule](#hosts-that-leave-a-schedule)).
* **Hardening and FIPS settings do not extend to backup traffic.** `security.policy: Enforced` and the
  FIPS settings leave the sidecar REST API plain HTTP with basic auth, between the Jobs and the
  sidecars, and a sidecar of a host that serves TCP over TLS only connects to its ClickHouse over
  `localhost` without verifying the certificate, which is issued for the host's names.
* **FIPS image policy.** With `security.images.policy: FIPSRequired` in the operator configuration,
  the sidecar image of every host of a backup-enabled cluster, which the Jobs run as well, must carry
  `fips` in its tag like the ClickHouse image, otherwise the CHI is aborted with
  `[FIPSImagePolicyViolation]`. The default image is not a FIPS build; set the image of the
  sidecar's container in its pod template to a FIPS build of `clickhouse-backup`,
  such as `altinity/clickhouse-backup:<version>-fips`. See
  [security_hardening_fips.md](security_hardening_fips.md).
* **An orphaned Job can hold its cluster.** A Job whose Pod never started counts as running until the
  operator stops it. Should the stop be refused, or the backup, schedule or restore that owns it be
  deleted with `--cascade=orphan`, the Job holds every operation on its cluster until its own
  `activeDeadlineSeconds` passes, and a resource that needs its name fails on it with `NameConflict`;
  delete such a Job to release the cluster earlier.
* **Backups outlive their resources.** Deleting a `ClickHouseBackup` or a schedule does not
  delete anything in the storage; only schedule retention does.
* **Retention prunes only the hosts a schedule backs up.** Once a host leaves them - a removed shard,
  a replica removed under `AllReplicas`, a cluster removed or no longer backed up - nothing prunes
  the backups it kept on a path of its own, as they may be needed to restore the old layout.
  The schedule lists such hosts in `status.clusters[].unprunedHosts` and warns with an
  `UnprunedHosts` event; delete their backups by hand, see
  [Hosts that leave a schedule](#hosts-that-leave-a-schedule).
* **ClickHouse Keeper installations (`ClickHouseKeeperInstallation`) are not covered.**
