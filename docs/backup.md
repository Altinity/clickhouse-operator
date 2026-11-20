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
The operator runs it itself, one run per cluster, as the Altinity usual backup workflow does: it
sends the `clickhouse-backup` commands to the sidecars of that cluster through SQL, by the
[integration tables](#integration-tables) of each host's ClickHouse, and reports progress back in the
resource's `status`, cluster by cluster. No `Job`, `CronJob` or Pod is created for it; deleting the
resource of a completed backup deletes no backup in the storage, while deleting one that is unfinished
kills it and deletes what it left, see
[Deleting a backup, restore or schedule](#deleting-a-backup-restore-or-schedule). A new CHI can also start from a backup, see
[Recover a new CHI from a backup](#recover-a-new-chi-from-a-backup).

```
spec.backup / clusters[].backup ──► operator ──► clickhouse-backup sidecar in every Pod ──► backup storage
                                                              ▲
ClickHouseBackup         ──► run per cluster ─┐               │ system.backup_actions, system.backup_list
ClickHouseBackupSchedule ──► run per cluster ─┼── operator ──► SQL to the ClickHouse of every host
ClickHouseRestore        ──► run per cluster ─┘
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
* [`02-backup-once.yaml`](chb-examples/02-backup-once.yaml) - one-off `ClickHouseBackup`
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
  names, with a run of its own for each cluster. That run reaches only the hosts of its cluster,
  whose sidecars keep the backups in that cluster's backup storage. An
  operation running on a cluster makes the next one on that cluster wait, while the other clusters
  go on, and a restore of a cluster holds the schedules of that cluster only, whether it restores
  access entities or not. The resource reports the outcome of each cluster
  in `status.clusters`, besides one overall `phase`.

**One backup name for all clusters.** The runs of one backup share its name: a backup of the
whole CHI is stored as one `clickhouse-backup` backup per shard, all of that name, each cluster in
[storage paths](#storage-paths) of its own. A restore of that name restores every cluster, or the one
it names, from that cluster's paths. A cluster whose backup fails does not stop the others: the
backup then ends `Failed` with the reason `PartiallyCompleted`, and each cluster that completed
holds a complete backup that a restore of that cluster can use. The runs of a schedule share a
name the same way, so the backup to recover a whole CHI from is one that completed on every
cluster: the schedule names the last one in `status.lastBackupName`, see
[ClickHouseBackupSchedule](#clickhousebackupschedule) for when it lags behind.

**No consistency across shards or clusters.** A backup is not a snapshot of the CHI.
`clickhouse-backup` freezes one table after another on each host, a run starts the backup on every
host of its cluster at once - as the Altinity usual backup workflow has every shard make its own -
and the hosts finish when their data is uploaded, and the runs of different clusters go on
independently of each other. Data written in the meantime is in the copies of some shards and not in those of others.
A host whose backup fails leaves the others to finish theirs, which the run waits for. As in the
Altinity usual backup workflow, a shard is backed up once any of its replicas made the backup - with
`AllReplicas` too, whose other replicas then keep their copies - and the run fails, with the error of
every host that failed, only when a shard has no replica that made it; once the run cannot submit the
backup to a host, it submits it to no further host. `clickhouse-backup` deletes the local backup of a `create_remote`
whose create fails, but not when its upload fails: the local backup stays, and so does the part of
the remote backup it uploaded, listed as `broken`. The run resumes no upload, so on every host the
backup failed on it deletes the local backup, as the Altinity usual backup workflow does - before it
fails, or, when other replicas made the backup of its shard, with an `OrchestratorReplicaFailed`
warning. It tries once and never waits: a sidecar that runs another command - as one whose upload
outlived [`actionTimeout`](#timeouts) still does - or a delete that fails leaves the local backup,
with an `OrchestratorFailedBackupLeft` warning that tells how to delete it by hand
(`INSERT INTO system.backup_actions(command) VALUES ('delete local <backup>')` on that host), and the
run ends; `OrchestratorFailedBackupDeleted` tells one deleted. Nothing remote is deleted: an upload
still running may yet complete the backup, and with object disks a replica that failed may share
data with one that made it. What the failed upload left in the remote storage is left to the
[retention](#retention) of the schedule, which deletes an incomplete backup once a complete one is
newer and no `create_remote` or `upload` of it is in progress (`OrchestratorFailedBackupRemoteLeft`); delete that of a one-off backup by hand. The shared name ties the copies together for naming,
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
  restore of them - a recovery restore included - holds the schedules and backups of the clusters
  it restores until it has finished, see [Access entities](#access-entities);
* every schema restore recreates the user-defined functions of the backup on the hosts it restores:
  with functions kept in Keeper, a restore of one cluster sets them back for all clusters;
* a restore of a cluster that shares tables with another one, by replication or through
  `Distributed` tables, changes what the other cluster holds or reads, see
  [Limitations](#limitations-and-security-notes).

Users, profiles and quotas defined in the CHI's `configuration` come from its spec, and are not in
a backup. The state of the CHI as a whole also gates every cluster: while the CHI has not
completed its reconcile - `Aborted` because of any one of its clusters, for example - or is
stopped, no run starts on any of its clusters.

**A CHI with a single cluster** - nearly every CHI - needs none of this: leave `spec.cluster` empty,
and each operation runs once, on that cluster, which `status.clusters` lists alone; the default
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
    # The Secret of the clickhouse-backup configuration of the sidecar
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

| Field                    | Meaning                                                                                                                                                                                                                                                    |
|--------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `enabled`                | `"yes"` turns operator-managed backup on. Accepts the usual yes/no forms (`"yes"`, `"no"`, `true`, `false`, ...). Off by default. Requires `storageConfig`, see below.                                                                                        |
| `podTemplate`            | Pod template whose `clickhouse-backup` container the sidecar is built on, see [Customizing the sidecar](#customizing-the-sidecar). It customizes the container only: it names no storage. `""` on a cluster: the operator's own. |
| `storageConfig`          | `valueFrom.secretKeyRef` selects the `name` of a Secret in the CHI namespace and the `key` holding the `clickhouse-backup` storage configuration; see [Backup storage](#backup-storage). A non-empty name and key are needed to enable backup. The CRD requires `key` whenever `secretKeyRef` is present, even if `name: ""` clears an inherited Secret. `hotReload: true` and `optional: true` are not supported. |
| `preferredReplica`       | Index, from 0, of the replica of each shard a backup works on, as the `backupReplica` of the Altinity usual backup workflow; empty is 0, the first replica. A `FirstPerShard` backup runs on that replica's sidecar, or on the first replica's when the shard has fewer replicas or the preferred one's ClickHouse does not answer when the run starts (`OrchestratorReplicaFallback` event), see [Which hosts are involved](#which-hosts-are-involved). |
| `naming`                 | Template the backups of the cluster are named by, one name per shard, e.g. `shard{shardNum}-{YYYYMMDDHHIISS}` as the Altinity usual backup workflow names them; empty, the default, names a backup `<prefix>-YYYYMMDDHHMMSS` alike on every shard. See [Backup naming](#backup-naming). |
| `recovery`               | `backupName`: the backup a new CHI starts from, see [Recover a new CHI from a backup](#recover-a-new-chi-from-a-backup). `""` on a cluster: nothing to recover there. `sourceInstallation`, `sourceNamespace`: the CHI that made the backup, when another; `""` on a cluster: this CHI. |

Inheritance follows `spec.security`: a cluster inherits every field it leaves empty from
`spec.backup`, so a cluster-level value always wins, `storageConfig` included: a cluster may keep
its backups in a storage of its own. Everything else about the sidecar - its
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

**Backup needs a storage, or the reconcile is aborted.** A cluster with backup enabled, on the cluster
or inherited from `spec.backup`, needs a `storageConfig` whose Secret exists and holds a readable
configuration in its `key`, and the backup block has to validate: a `secretKeyRef` without a `key`, a `hotReload: true`
and an `optional: true` are refused, with a message naming the field. Otherwise the operator aborts the reconcile of the
whole CHI before it changes anything: its status turns `Aborted` with the reason
`BackupConfigInvalid` and a message naming the cluster and what is wrong, and a
`BackupStorageConfigInvalid` warning event goes to the CHI. `podTemplate` does not stand in for a
storage: it only customizes the `clickhouse-backup` container. Fix the spec or the Secret - the
operator reconciles the CHI again once the Secret can be used - or set `enabled: "no"` on a cluster
that inherited backup unasked, which never aborts anything. The operator looks at these fields only:
settings a mutating admission webhook injects do not count, and a configuration file a pod template
mounts at `/etc/clickhouse-backup` gives way to the one the operator generates there.

**Enabling or disabling backup rolls that cluster's Pods only.** Adding or removing the sidecar
changes the cluster's Pod template, so the operator performs its usual rolling update, host by host.
Changing `podTemplate` or the sidecar's container in its pod template rolls the Pods the same
way; `recovery` is no sidecar setting, and changing it restarts nothing. Changing the contents of the
storage Secret restarts nothing at all: the operator regenerates the configuration file of the
sidecars, which they read anew for every operation, see [Secret changes](#secret-changes). The other clusters of the CHI are not restarted: they only
reload their users configuration live when the operator adds or removes the backup user (see
below).

## What the operator runs

For every Pod of a cluster with backup enabled, the operator adds a container named
`clickhouse-backup` that runs `clickhouse-backup server`:

* **REST API on port 7171**, container port named `backup-api`, and the
  [integration tables](#integration-tables) over it in the ClickHouse of the Pod, through which the
  operator drives the sidecar: `api.create_integration_tables` is always on, the tables read
  `http://127.0.0.1:7171`. The port is also published on each host's Service, for tools that call the
  API directly; the operator does not use it. Only Services of type `ClusterIP` (the default, headless
  included) get the port: a host Service template of type `LoadBalancer`, `NodePort` or
  `ExternalName`, or a `ClusterIP` Service with `externalIPs`, does not, so the plain-HTTP API is
  never published outside the Kubernetes cluster. The operator warns about such hosts
  (`BackupAPIUnpublished` events). The API can run commands in the sidecar: restrict access to it, see
  [Limitations and security notes](#limitations-and-security-notes).
* **Credentials Secret `<chi-name>-auto-backup-secret`.** The operator creates it once per CHI with
  the CHI as owner, so it is deleted with the CHI. It holds the REST API basic-auth credentials
  (`API_USERNAME`, `API_PASSWORD`), random values the operator generates, the same for every cluster
  of the CHI, which the sidecars read through their configuration file, and the integration tables
  carry in their URL, see below. The ClickHouse credentials of the sidecar (`CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`) are
  yours to add to it, or not: the operator has no defaults of its own for them, see the backup user
  below.
* **Configuration file Secret `<chi-name>-<cluster-name>-backup-config`.** The operator generates
  one per backup-enabled cluster, owned by the CHI, from the [storage Secret](#backup-storage) of the
  cluster and the credentials above, and mounts its `config.yml` read-only into the sidecar alone, at
  `/etc/clickhouse-backup/config.yml`, where `clickhouse-backup` reads its configuration by default.
  `clickhouse-backup` reads the file anew for every operation, and Kubernetes keeps a mounted Secret up
  to date, so a changed storage Secret reaches a running sidecar without a restart, see
  [Secret changes](#secret-changes). Besides `config.yml`, the Secret holds the settings of the file
  that locate the storage, under the names of their environment variables, for the
  [storage path check](#storage-paths) of the operator. A sidecar that starts before the Secret exists
  does not serve its REST API, which would let in anybody without credentials, but waits for the
  file, leaving its Pod Ready.
* **Backup user, when you set one.** Without `CLICKHOUSE_USERNAME` and `CLICKHOUSE_PASSWORD` in the
  Secret above, the configuration file sets no ClickHouse credentials beyond what the storage
  settings may set, and `clickhouse-backup` connects with its own defaults - user `default`, empty
  password, from `localhost`. Set both, and the operator adds that user to the CHI, with the password
  from the Secret and access restricted to localhost (`127.0.0.1`, `::1`): it logs in only from
  inside its own Pod, the sidecar's. ClickHouse counts every address of the Pod as local, so a client
  in the same Pod gets in through the Pod IP or the host Service too, while no other Pod does. The
  operator reads the password from the Secret while it normalizes the CHI; if the Secret exists
  but cannot be read, the CHI is aborted with `[BackupSecretUnresolved]` rather than reconciled
  without the user. It stays aborted until the next reconcile with a readable Secret - after a
  spec edit, an operator restart, or auto-recovery when a Pod turns Ready. Without backup
  enabled, or with only one of the two set, a user of that name in the CHI is an ordinary user,
  and the sidecar connects with what is set; the latter is warned about on every reconcile with a
  `BackupCredentialsIncomplete` event on the CHI, naming the key that is set and the one that is not.
* **Local connection to ClickHouse.** The sidecar connects to `localhost` on the host's native TCP
  port, or on the secure TCP port with TLS when the host only exposes the secure one.
* **Schema restored `ON CLUSTER` only where the restore asks for it.** `RESTORE_SCHEMA_ON_CLUSTER`
  is set, empty, so that nothing else turns it on: a restore sets it, for its command alone, on the
  first replica of the first shard (see [ClickHouseRestore](#clickhouserestore)). The replicas of a
  restored `Replicated*` table share one Keeper path. This needs ZooKeeper or ClickHouse Keeper
  configured for the CHI, as replication does anyway.
* **Distributed tables pointed at the restored cluster.** `clickhouse.restore_distributed_cluster`
  is `{cluster}` in the configuration file, as the Altinity usual backup workflow sets it: a replica
  that restores its schema without `ON CLUSTER` - every one but the first replica of the first shard -
  points a `Distributed` table of the backup over a cluster its ClickHouse does not have, the cluster of
  the source CHI under another name, say, at the restored cluster, the one `{cluster}` expands to on
  the host. A `Distributed` table over a cluster the host has, or over a macro, is kept as it is. A
  value the storage settings set is kept, `""` turns it off, see [Backup storage](#backup-storage).
* **Tables restored under their own names.** `RESTORE_DATABASE_MAPPING` and `RESTORE_TABLE_MAPPING`
  are set, empty, so that a restore creates every table under the database and name it has in the
  backup, or in the database `databaseMapping` maps it to: a mapping of the sidecar would make every
  restore drop and replace other tables than it names.
* **Access entities reloaded live.** `CLICKHOUSE_RESTART_COMMAND` is set to
  `sql:SYSTEM RELOAD USERS; sql:SYSTEM RELOAD CONFIG`, as the Altinity usual backup workflow sets
  it, so ClickHouse picks up restored users, roles and grants, and configuration, without a restart;
  the `clickhouse-backup` default, `systemctl restart clickhouse-server`, cannot work in a
  sidecar. See [Access entities](#access-entities).
* **One operation at a time.** `API_ALLOW_PARALLEL` is set to `false`, so a sidecar runs one
  backup or restore command at a time and refuses another meanwhile, which the operator waits out.
* **Times in UTC.** `TZ` is set to `UTC`: `clickhouse-backup` writes the `start` and `finish` of a
  row of `system.backup_actions` in the local time of its container without a time zone
  ([Altinity/clickhouse-backup#1687](https://github.com/Altinity/clickhouse-backup/issues/1687)), and
  the operator compares them with `now('UTC')` of ClickHouse.
* **Default storage paths.** Where the storage settings set no path, the configuration file takes
  `backup/{namespace}/{installation}/{cluster}/{shard}`, and
  `object_disks/{namespace}/{installation}/{cluster}/{shard}` as the object disk path, or the paths the
  operator configuration sets, for every storage type, see [Storage paths](#storage-paths). A path set
  in the storage Secret or in the container's `env` is used as it is.
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
* **Remote retention is the schedules' alone.** `general.backups_to_keep_remote` is forced to `0`,
  and `general.rebase_during_delete` and `general.rebase_before_remove_old_remote` to `true`, in the
  configuration file and as the pinned variables `BACKUPS_TO_KEEP_REMOTE`, `REBASE_DURING_DELETE` and
  `REBASE_BEFORE_REMOVE_OLD_REMOTE`, over whatever the storage settings say: only the
  [retention](#retention) of the schedules deletes remote backups, and deleting a backup another one is
  incremental to rebases that one first.
The sidecar's configuration is assembled, from lowest to highest precedence, from: the
configuration file - the storage settings of the storage Secret, with the default paths filled in
and the credentials above over whatever the storage settings say of `api.username`,
`api.password`, `clickhouse.username` and `clickhouse.password` - then the environment of the
sidecar: the operator's own settings listed above, and the `env` of the sidecar's container in its
pod template. `clickhouse-backup` takes an environment variable over the setting of the file it
stands for, and the operator sets `general.disable_environment_override` to `false` in the file, so
that the storage settings cannot turn that off. A storage setting that collides with an operator
setting is therefore ignored. Thirteen settings are pinned: the operator always sets `API_LISTEN`
(`0.0.0.0:7171`), `API_SECURE` (`false`), `API_CREATE_INTEGRATION_TABLES` (`true`),
`API_INTEGRATION_TABLES_HOST` (`127.0.0.1`), `RESTORE_SCHEMA_ON_CLUSTER`, `RESTORE_DATABASE_MAPPING`,
`RESTORE_TABLE_MAPPING`, `API_ALLOW_PARALLEL`, `CLICKHOUSE_RESTART_COMMAND`, `BACKUPS_TO_KEEP_REMOTE`,
`REBASE_DURING_DELETE`, `REBASE_BEFORE_REMOVE_OLD_REMOTE` and `TZ` (`UTC`) as above, over the
container's own, and drops the container's `API_USERNAME`, `API_PASSWORD`, `CLICKHOUSE_USERNAME`,
`CLICKHOUSE_PASSWORD` and `CLICKHOUSE_BACKUP_CONFIG`, which would override the credentials of the
file or make `clickhouse-backup` read another file. The operator relies on all of them - the
integration tables of every host reach its sidecar at `127.0.0.1:7171` over plain HTTP with the
credentials of `<chi-name>-auto-backup-secret`, every restore `ON CLUSTER` would re-point `Distributed`
tables, a mapped restore would replace tables it does not name, two commands at once on one sidecar would share its environment,
restored access entities would not load, and the times of the commands would not compare with
`now('UTC')`. The default storage paths are no such setting: they
fill in a path left unset only.

A Kubernetes Service in the namespace sets `<SERVICE>_PORT=tcp://<ip>:<port>` in every container,
so a Service named `sftp` sets `SFTP_PORT`, which `clickhouse-backup` fails to parse, refusing its
whole configuration. The sidecar sets `CLICKHOUSE_PORT` itself, and starts the server without any
`*_PORT` variable that holds such a link; a port set in the storage settings or in the container's
`env` is kept.

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
    storageConfig:
      valueFrom:
        secretKeyRef:
          name: backup-demo-storage
          key: config.yml
  templates:
    podTemplates:
      - name: backup-sidecar
        spec:
          containers:
            - name: clickhouse-backup
              image: altinity/clickhouse-backup:2.8.1
              imagePullPolicy: IfNotPresent
              resources:
                requests:
                  cpu: 100m
                  memory: 256Mi
```

A `ClickHouseInstallationTemplate` may carry both `spec.backup.podTemplate` and the pod template it
names, to give every installation that uses it the same sidecar.

* **Yours:** `image` (over the default, `altinity/clickhouse-backup:stable`), `imagePullPolicy` (over the
  default, `Always`, which takes a moved `stable` tag on the next Pod start; pin a version and use
  `IfNotPresent` for reproducible Pods, as above), `env` (over the
  configuration file and the operator's defaults), `envFrom`, `resources`, `securityContext`,
  `volumeMounts` (of volumes the Pods have), `command`, `args`, `livenessProbe`.
  An `envFrom` of the container's own sets what no `env` entry sets, over the configuration file.
  Without a `securityContext` the sidecar copies the ClickHouse container's; without
  `resources` it has none, which makes a `Guaranteed` Pod `Burstable`, see
  [What the operator runs](#what-the-operator-runs).
* **Pinned:** the twelve settings above are set over the container's own.
* **Dropped:** the container's `env` entries `API_USERNAME`, `API_PASSWORD`, `CLICKHOUSE_USERNAME`,
  `CLICKHOUSE_PASSWORD` and `CLICKHOUSE_BACKUP_CONFIG`, and its mounts at `/etc/clickhouse-backup`,
  below or above it - as are such mounts of the ClickHouse container, which the sidecar otherwise
  shares: the operator's configuration file takes their place. A pod template volume named
  `clickhouse-backup-config` is replaced by the operator's.
* **Rejected:** a literal `API_SECURE` that turns TLS on: the integration tables speak plain HTTP, so the CHI is
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

Changing the container rolls the cluster's Pods.

A `command` or `args` of the sidecar's container needs the configuration file Secret to start - the
operator mounts it as mandatory then, as another command might serve the REST API without
credentials - and keeps the service links. It still reads the operator's configuration file, unless
it names another one with `-c` or `--config`, which hides the storage paths from the operator, as does an
`envFrom` of the container's own, see [Storage paths](#storage-paths).

A `clickhouse-backup` container in a pod template a cluster with backup off runs is that cluster's
own, run as written.

### Data volume

`clickhouse-backup` works on ClickHouse's files directly: ClickHouse freezes the data parts as
hard links inside its data directory and the sidecar uploads them from there, while restores
download into that directory for ClickHouse to attach. The sidecar therefore has to
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
in the `storageConfig.valueFrom.secretKeyRef` of the cluster, its own or inherited from `spec.backup`.
Its `key`, `config.yml` in the examples, holds the storage settings: a YAML mapping of the sections of the
`clickhouse-backup` configuration file - `general`, `s3`, `gcs`, `azblob`, ... - see the
[clickhouse-backup configuration reference](https://github.com/Altinity/clickhouse-backup#default-config)
for every option. The operator generates the configuration file of the sidecars from it, see
[What the operator runs](#what-the-operator-runs); other keys of the Secret are not read, and the `key` is yours to name. A minimal S3
or MinIO Secret:

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

Other storage types use the same scheme, for example `remote_storage: gcs` with a `gcs` section
holding `bucket`, `path` and `credentials_json`, or `remote_storage: azblob` with an `azblob` section
holding `account_name`, `account_key`, `container` and `path`. Each cluster takes the Secret of the
storage it uses, so clusters may keep their backups in different buckets, or with different
providers. Settings `clickhouse-backup` does not know are passed on as they are: a newer image may
take settings the operator knows nothing of.

Useful `clickhouse-backup` settings to consider in the storage settings:

* `general.backups_to_keep_remote` is forced to `0`, see above: it would prune every backup regardless
  of who made it. Use the `keep` of the schedule entries, see [retention](#retention), which only
  touches that schedule's backups.
* Compression, per storage: `s3.compression_format` (`tar`, `zstd`, `gzip`, ...),
  `gcs.compression_format`, `azblob.compression_format`. Not `lz4`: ClickHouse data is
  lz4-compressed already, so `clickhouse-backup` refuses that configuration, and every backup and
  restore of the cluster fails.
* Server-side encryption: `s3.sse` (e.g. `AES256` or `aws:kms`) with `s3.sse_kms_key_id`; see the
  clickhouse-backup reference for GCS and Azure.
* `clickhouse.restore_distributed_cluster` (`CLICKHOUSE_RESTORE_DISTRIBUTED_CLUSTER`) is `{cluster}`
  unless set here, see [What the operator runs](#what-the-operator-runs): a restore points a
  `Distributed` table of the backup over a cluster the restored host does not have at the restored
  cluster. Name another cluster of the CHI to point such tables there instead, or set `""` to keep
  them as the backup has them. Do not name the helper clusters `all-sharded`, `all-replicated` or
  `all-clusters`, which the operator generates over the hosts of every cluster: a `Distributed` table
  over one of them reads every cluster. The example sets the default explicitly:

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
      clickhouse:
        # The default: Distributed tables over a cluster the restored host lacks go over the restored cluster
        restore_distributed_cluster: "{cluster}"
      s3:
        bucket: clickhouse-backups
        access_key: "..."
        secret_key: "..."
  ```

`BACKUPS_TO_KEEP_LOCAL` is set by the operator as an environment variable (see above), so
`general.backups_to_keep_local` in the storage settings is ignored; set the variable in the `env` of
the sidecar's container in its pod template to keep local copies.

The reconcile of the CHI does not go ahead without a usable Secret, see
[Enabling backup](#enabling-backup). When the Secret does not exist - not created
yet, or misspelled in `storageConfig` - holds no such `key`, or an empty one, as a Secret of
environment-variable keys does, or holds storage settings that are no YAML mapping of sections - not
YAML at all, a section that is no mapping, a setting the operator reads that is a list - the operator
aborts the reconcile of the whole CHI before it creates or changes anything: its status turns
`Aborted` with the reason `BackupConfigInvalid` and a message naming the cluster, the Secret and what
is wrong, with a `BackupStorageConfigInvalid` warning event on the CHI. The operator does not watch
the Secret: it checks the storage Secrets of such a CHI once a minute and reconciles it again by
itself once they can be used. A Secret that goes bad after the configuration file was generated -
deleted while it is recreated, say - leaves the sidecars with the storage settings they have, with a
`BackupStorageSecretMissing` (or, for settings that cannot be read, `BackupStorageConfigInvalid`)
warning event, until the Secret is right again; a reconcile of the CHI meanwhile aborts as above.
Once the Secret is right, the sidecars pick it up by themselves, as they do any later change of it,
see [Secret changes](#secret-changes).

### Integration tables

`clickhouse-backup` creates tables in ClickHouse that show the backups and the operations of its
REST API: `system.backup_list` (the backups, local and remote, with their sizes),
`system.backup_actions` (the operations it runs and ran - `INSERT INTO system.backup_actions(command)
VALUES ('...')` runs one) and `system.backup_version`. They are tables of the `URL` engine that read
`http://127.0.0.1:7171/backup/...` - the REST API of the sidecar in the same Pod - with the API
credentials of the configuration file in the URL.

The operator drives every backup and restore through them, see
[How the operator runs an operation](#how-the-operator-runs-an-operation), so it always turns them on:
`api.create_integration_tables` is `true` and `api.integration_tables_host` is `127.0.0.1` in the
configuration file, and pinned as `API_CREATE_INTEGRATION_TABLES` and `API_INTEGRATION_TABLES_HOST`
in the sidecar, whatever the storage settings say. Every ClickHouse user who may read
`SHOW CREATE TABLE system.backup_list` sees the API credentials in the URL; the tables are also there
for you to read:

```sql
SELECT name, created, data_size, location, desc FROM system.backup_list ORDER BY created;
SELECT command, status, start, finish, error FROM system.backup_actions ORDER BY start DESC LIMIT 10;
```

The sidecar creates the tables when its server starts, or reloads on `POST /restart` or `SIGHUP`, and
only if ClickHouse answers then: a sidecar that started before ClickHouse accepted connections has no
tables until it restarts, and the operator cannot drive it meanwhile: a run that finds no
`system.backup_actions` on a host fails with that host's error.

### A storage per cluster

`storageConfig` is inherited like every other field, so a cluster that names a Secret of its own
keeps its backups in a storage of its own, and a CHI's Secret wins over the one of a
`ClickHouseInstallationTemplate` it uses:

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

* **To disable backup for one cluster**, set `backup.enabled: "no"` on that cluster. There is
  no need to override `storageConfig`.
* **To explicitly clear an inherited storage Secret**, set `secretKeyRef.name: ""` and include
  `secretKeyRef.key` (for example, `config.yml`). The CRD requires `key` whenever `secretKeyRef`
  is present, even when `name` is empty and the key will not be read. A cluster with backup enabled
  and no storage Secret cannot be reconciled, so set `enabled: "no"` as well:

  ```yaml
  - name: cold
    backup:
      enabled: "no"
      storageConfig:
        valueFrom:
          secretKeyRef:
            name: ""
            key: config.yml
  ```

### Storage paths

Every shard holds different data, but a backup has one name on all of them, so each shard of each
cluster keeps its part of a backup under a storage path of its own. `clickhouse-backup` expands the
path on each host with the ClickHouse macros the operator defines there: `{cluster}` (the cluster
name), `{shard}` (the shard name, by default its index in the cluster), `{replica}` (the host name),
`{all-sharded-shard}` (the index of the host in the CHI), `{installation}` (the CHI name) and
`{namespace}` (the namespace of the CHI). A macro of one of these
names that the CHI's own configuration defines takes precedence on the hosts, while the operator
checks the paths with its own values: do not redefine them.

**Default.** Without a path for the storage in use - `s3.path`, `gcs.path`, `azblob.path`, ... -
the configuration file takes `backup/{namespace}/{installation}/{cluster}/{shard}`, and, for tables
on object disks, `object_disks/{namespace}/{installation}/{cluster}/{shard}` as the object disk path
(`s3.object_disk_path`, `gcs.object_disk_path`, `azblob.object_disk_path`). A path you set, in the
storage settings or as an environment variable such as `S3_PATH` in the container's `env`, always
wins; one set empty is the root of the storage, one set null is unset. The namespace and the CHI name keep apart the backups of every CHI of one Kubernetes cluster
on a shared storage: with paths that expand alike for two CHIs, each lists, restores and deletes the
backups of the other by name, and two backups of one name would merge into one.

**Configured defaults.** The operator configuration sets the defaults, in the same macros:

```yaml
backup:
  storagePath: "eu1/backup/{namespace}/{installation}/{cluster}/{shard}"
  storageObjectDiskPath: "eu1/object_disks/{namespace}/{installation}/{cluster}/{shard}"
```

A fixed prefix of your own, as `eu1/` here, keeps apart the installations of several Kubernetes
clusters on one storage, CHIs of one name in one namespace among them. The operator writes the
defaults into the configuration files: changing them changes the paths of every backup-enabled
cluster within minutes, with no Pod restarted, and leaves the backups made under the old paths
where they are, where restores, retention and incremental backups no longer find them. Set both,
each with `{shard}`, and neither a prefix of the other.

**Checked before anything changes.** Every backup and restore run expands the paths of all
backup-enabled clusters of the CHI, not only of its own cluster, on every host with that host's
macros - as `clickhouse-backup` expands them there, the way its
`skip_restore_non_sharded_replicated_tables` expands the ZooKeeper paths of a table on every shard -
and refuses two writers of a backup of one name at one location of one storage (the same storage
type and bucket, container, endpoint or address). The writers are the shards of each cluster, and,
for an `AllReplicas` backup, every replica of its own cluster: they all make the backup at once.
The run fails without changing anything unless:

* **every shard of its cluster has a storage path of its own**, which no other shard uses, of its
  cluster or of another one on the same storage - but the shards of a cluster with a [naming
  template](#backup-naming), which names the backup of each apart, may share one path, which still no
  other cluster may use. Without `{shard}` the shards of a cluster would write over each other's part
  of a backup, and without `{cluster}` two clusters on one bucket would mix theirs: as they share the
  backup name, a restore of one cluster could restore the other's data. One cluster with one shard
  needs no macro at all, and clusters on different buckets need no `{cluster}`. A path that two other
  clusters share fails the runs of those clusters, not this one;
* **every shard has an object disk path of its own** the same way, when one is set. Deleting a
  backup - by retention, or by hand - deletes its object disk data under
  `<object disk path>/<backup name>`, so with a shared object disk path, deleting the backup of one
  shard, or of one cluster, would delete the object disk data of every other shard's copy of it;
* **the path has no `{replica}`** for a `FirstPerShard` backup: every replica of a shard restores
  its schema from its own path, where only the first replica's backup would be;
* **every replica has a storage path and an object disk path of its own** for an `AllReplicas`
  backup (`backup/{namespace}/{installation}/{cluster}/{shard}/{replica}` and
  `object_disks/{namespace}/{installation}/{cluster}/{shard}/{replica}`), as every replica then
  uploads its own copy under the same name - a naming template names the backups of the shards apart,
  never those of the replicas of a shard. `{replica}` in the object disk path alone would put the
  metadata of every replica's copy at one path; in the storage path alone, their object disk data,
  which deleting any copy deletes. `{all-sharded-shard}`, which differs on every host too, does as
  well as `{replica}`; a shard of one replica needs neither. Set the object disk path empty when no
  table is on an object disk: an empty one is not checked.

**Embedded backups.** With `clickhouse.use_embedded_backup_restore: true` - the only way
`clickhouse-backup` backs up the tables of CAS disks (`metadata_type: cas`) - ClickHouse makes the
backup with `BACKUP ... TO`. Without `clickhouse.embedded_backup_disk`, it writes the whole backup
under the object disk path, the root of the storage when that is empty: the check then compares the
object disk path even when it is empty, so an `AllReplicas` backup needs `{replica}` there. With a
backup disk, ClickHouse writes the backup to `Disk('<disk>', '<backup name>')` first, which the
operator cannot see into: when hosts make a backup of one name at once - the replicas of a shard
under `AllReplicas`, or the shards of a cluster without a naming template - the run warns that they
write over each other there if that disk is an object storage disk they share. The check reads
`use_embedded_backup_restore` and `embedded_backup_disk` as it reads the paths, and the
`CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE` of the [config overrides](#config-overrides) of the backup or
of any entry of the schedule.

**Aborted before it runs.** A `ClickHouseBackup` runs the check of each cluster before anything else,
before it waits for another operation: a refused cluster ends `Aborted`, reason `StoragePathConflict`,
the message telling which path is not unique, and which two writers share it, e.g.

```
storage path 's3://backups/backup/main/0' is not unique: replicas chi-demo-main-0-0 and
chi-demo-main-0-1 of shard 0 of cluster main both write a backup of one name there; give every
replica a storage path of its own, e.g. with {replica}
```

or, for two clusters sharing a path,

```
storage path 's3://backups/backup/0' is not unique: shard 0 of cluster hot and shard 0 of cluster
cold both write their backups there; give every shard of every cluster a storage path of its own,
e.g. with {cluster} and {shard}
```

with a `Warning` Event of that reason. A `ClickHouseBackupSchedule` runs the check of every cluster it
backs up on every reconcile: a refusal sets its `Validated` condition `False`, reason
`StoragePathConflict`, and the schedule `Aborted`, with a `ScheduleAborted` Warning Event, starting no
run until the settings change. What the check warns of - a macro it does not know, a cluster left out,
a backup disk - each run posts as an `OrchestratorStoragePathAdvice` Warning Event, and logs.

A restore takes the path as it is. Under `{replica}`, every replica restores its schema from its own
copy, and the first replica of each shard the data from its copy, so an `AllReplicas` backup can be
restored, while a backup of the first replica of each shard only cannot: the other replicas find no
copy at their paths. `validateTopology` refuses that before anything changes; without it, the run
only warns.

Settings left unset count as the defaults `clickhouse-backup` gives them, and paths are compared as
it cleans them, so `backup//{shard}/` and `backup/{shard}` are one path. To check, the operator
takes the settings that locate the storage (storage type, bucket, container, endpoint, address,
port and paths) of each cluster as its sidecars do: from the `env` of the sidecar's container, as its
pod template sets it - a value, or a key of a Secret or ConfigMap of the namespace, which the operator
reads - over the configuration file Secret of the cluster, which holds those settings of the file
under the names of their environment variables - `REMOTE_STORAGE`, `S3_BUCKET`, `S3_PATH`, ... - and
never its credentials.

**When the check cannot tell.** A path that holds a macro other than the six above, which the
operator does not define and cannot expand, such as one the CHI's own configuration defines, is left
out of the check, with a warning: make sure it differs on every host that makes a backup of one name
at once. The other path of the cluster is still checked. The paths of a cluster are unknown to the
check when:

* neither the container's `env` nor the storage settings set the storage type
  (`general.remote_storage`), or they name a storage type whose paths the check does not know, such
  as `custom`;
* the sidecar may take settings the operator cannot see: from a configuration file named on the
  command line of the sidecar's container in its pod template - a `command` or `args` with `-c` or
  `--config`, or any argument holding `--config`, as a shell wrapper passes it on; from an `envFrom`
  of that container; or from a setting that locates the storage but is taken from anything but a
  Secret or ConfigMap key, such as a `fieldRef`, or refers to another variable with `$(...)`. A mount
  at `/etc/clickhouse-backup` or `CLICKHOUSE_BACKUP_CONFIG` in the container's `env` no longer hides
  anything: the operator replaces the one and drops the other.

In a CHI with one backup-enabled cluster, the run then posts a warning and goes on. With several, the
runs of their clusters go on at the same time, and two of them writing one backup to one path would
leave a mix of both, listed as complete: a run that cannot tell the paths of its own cluster fails
without changing anything, and one that cannot tell those of another cluster leaves it out of the
check, with a warning - that cluster's own runs fail. Configure the sidecars of a CHI with several
backup-enabled clusters through the storage Secret and `env` only, without an `envFrom` or a
configuration file named on the command line.

A configuration file built into a custom `clickhouse-backup` image at
`/etc/clickhouse-backup/config.yml` is hidden by the operator's; one elsewhere is read only when the
command names it, see above.

**Restoring into another CHI.** The target CHI finds a backup by expanding its paths with its own
macros, so it needs the same cluster names and shard layout. A backup of a CHI of another name, or
of another namespace where the paths hold `{namespace}`, lies under the paths of that CHI: name it in
`sourceInstallation`, and its namespace in `sourceNamespace`, of the
[ClickHouseRestore](#clickhouserestore), or of `spec.backup.recovery` for a
[recovery](#recover-a-new-chi-from-a-backup). The restore then reads the backup from the paths those
macros give there, through clickhouse-backup commands that override both paths for that command
alone. The paths of the target must be known to the operator (see above): a configuration file named on
the command line of the sidecar fails such a restore before it changes anything, and paths that hold neither macro are read
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

The copies of the schema the preflight downloads are deleted before the restore downloads the
backup itself; a preflight that fails deletes them once, as well as it can, and tells one it leaves
with an `OrchestratorSourceCopyLeft` warning. A run that fails after the preflight deletes nothing it
downloaded (see [Restore](#clickhouserestore)): a later restore of the backup from the same paths then
refuses the hosts that hold a copy, until it is deleted with
`INSERT INTO system.backup_actions(command) VALUES ('delete local <backup>')` on each. The same
restore resumed after a restart of the operator takes a copy its status records it downloaded on a
host as its own, and resumes its download.

A CHI restored from a backup it reads from its own paths - a copy of the same name and namespace, or
one whose paths hold neither macro - then shares the storage path with the CHI it was restored from,
and so does every schedule of either. Their backups are told apart by name only:
[retention](#retention) prunes the schedule's series, the backups named
`<backupNamePrefix>-YYYYMMDDHHMMSS`, and a run refuses a name that is already in the
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
source's other schedules write to the same paths as well. They start no run, `Ready` is
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

### Backup naming

By default a backup has one name on every shard - `<backupNamePrefix>-YYYYMMDDHHMMSS` for a scheduled
one, the name of a `ClickHouseBackup` otherwise - and each shard keeps its part under a storage path
of its own. `backup.naming`, on the CHI and on a cluster, inherited like `preferredReplica`, names the backup of
every shard by a template instead, as the Altinity usual backup workflow does: `shard{shardNum}-{YYYYMMDDHHIISS}` names the backup made at
02:00:37 on 13 November 2026 `shard1-20261113020037` on the first shard and `shard2-20261113020037` on
the second. As the names differ by shard, the shards of the cluster may keep their backups at one
storage path, the layout of the Altinity usual backup workflow.

A template **requires** `{shard}` or `{shardNum}` - either, or both - each kept apart from a digit or
another placeholder by a literal that is no digit. Mind the bases: on a cluster of default shard
names `shard{shard}-...` names the first shard `shard0-...`, `shard{shardNum}-...` names it
`shard1-...`. A name made with `{shard}` is read back by the shard names of the cluster, so a restore
of the backup of [another CHI](#storage-paths) by `{shard}` needs the same shard names there;
`{shardNum}` needs the same shard order only.

| Placeholder | Value |
|---|---|
| `{shard}` | The shard name: the value of the `{shard}` macro the operator puts into `system.macros` of the host, the one the [storage path](#storage-paths) takes. It is the index of the shard in the cluster, **from 0** (`0`, `1`, ...), unless the shard has a `name` in the layout, e.g. `east`. |
| `{shardNum}` | The number of the shard in the cluster, **from 1**, its `shard_num` of `system.clusters`, in the order of the layout: the `shard<N>` of the Altinity usual backup workflow, whose names need `shard{shardNum}`. |
| `{cluster}`, `{chi}`, `{namespace}` | The names of the cluster, of the CHI and of its namespace; a restore of the backup of [another CHI](#storage-paths) takes those of `sourceInstallation` and `sourceNamespace`. |
| `{prefix}` | The `backupNamePrefix` of the schedule that makes the backup, or the name of a `ClickHouseBackup` made without one. |
| `YYYY` `MM` `DD` `HH` `II` `SS` | The time the backup is named after - year, month, day, hour, minutes, seconds - in braces, alone or several in one, e.g. `{YYYYMMDDHHIISS}` or `{YYYY}-{MM}-{DD}_{HH}{II}{SS}`. **All six are required**: retention and incremental backups order the backups by it. |

Literals are letters, digits, `.`, `_` and `-`. Empty `naming` leaves the names exactly as before. The
time is that of the slot of a scheduled run, in the schedule's `timeZone`; a backup made at once - a
`ClickHouseBackup`, of a [schedule](#run-a-schedule-now) or not - is named after the time it is due, in
UTC, with a `T` before the hour (`shard1-20261113T101500`), which keeps it out of the slots of the
schedule, as the `<prefix>-YYYYMMDDTHHMMSS` of the default naming does.

* **One name per backup.** A backup is told by the name of the backup of its first shard, which
  `status.backupName` of a `ClickHouseBackup`, `LAST-BACKUP` of a schedule and the
  recovered-from annotation show; a `ClickHouseRestore`, a `recovery` and `diffFromRemote` take it, or
  the name of the backup of any other shard, and restore on every host the backup of its own shard.
  When the template holds no `{prefix}` they also take the time alone, `20261113020037`, as the
  Altinity usual backup workflow tells a backup. The `backupName` of a `ClickHouseBackup` of such a cluster may only be such a time. One
  `ClickHouseBackup` of several clusters needs one template that holds no `{cluster}`; back up the
  clusters one by one otherwise.
* **One storage path for the shards.** The storage path check lets the shards of a cluster with a
  template share a path, e.g. `s3.path: backup/{namespace}/{installation}/{cluster}` without
  `{shard}`, and the object disk path likewise; it still refuses a path two clusters share. A template
  without `{shard}` and `{shardNum}` is refused, as the shards would overwrite each other's backups there. Each host
  lists, prunes and restores the backups of its own shard, whatever the others at the path.
* **A slot tolerance of 30 minutes.** The Altinity usual backup workflow names a scheduled backup
  after the time its run started, up to 30 minutes after the slot (its refresh rate times 1.5). With a
  template a backup belongs to the slot of an entry when its time is from the slot to 30 minutes after
  it, so retention counts the backups that workflow made for the entries of the same times.
* **Retention prunes the whole template.** Without `{prefix}` in the template, retention of a schedule
  takes every name the template makes on its cluster as the schedule's, as the cleanup of the Altinity
  usual backup workflow does: run one schedule per cluster.

Changing `naming` on a cluster with backups leaves the backups of the former naming where they are:
retention no longer finds them, and restores name them by the names they have.

#### Migrating from the Altinity usual backup workflow

1. Stop the backups of the cluster in that workflow (its schedule), so that two cleaners never prune
   one path.
2. Create the storage Secret with its storage settings and **its path, shared by the shards**: it
   keeps the backups of a cluster at `<environment path>/<cluster>`, or `<kubeNamespace>/<cluster>`
   when the environment sets no path, and the object disk data at `object_disks/<that path>`. Set
   `s3.path` (or `gcs.path`, ...) and the object disk path to those, without `{shard}`.
3. Set `backup.naming: "shard{shardNum}-{YYYYMMDDHHIISS}"` on the CHI or the cluster - `{shardNum}`,
   the `shard_num` from 1 that workflow names its backups by, not `{shard}`, the shard name from 0 - and
   `preferredReplica` to its `backupReplica`.
4. Create a `ClickHouseBackupSchedule` with its entries, the same `time`s, and `timeZone` set to the
   zone its backend runs in, as its tags are wall times there. Keep the `keep`s: retention then counts
   the backups it made, with the 30-minute tolerance, as it would have.
5. Copy the backups it keeps by hand - its "manual" backups and those pinned in its dialog of a
   schedule change - into `spec.retention.keepBackups` of the schedule, by their time
   (`- name: "20261001020000"`), with an `until` for those it keeps for a while only: the operator's
   retention does not know its records of them, nor did its schedule hold any change.
6. Check with `kubectl get chbs` and `clickhouse-backup list remote` on a sidecar that the first runs
   keep what you expect; restore a backup it made by its time (`backupName: "20261112021503"`).

### Secret changes

The sidecar reads its configuration file anew for every operation, from the configuration file
Secret the operator generates for its cluster out of the storage Secret and
`<chi-name>-auto-backup-secret`. The operator regenerates it on every reconcile of the CHI and, for
a CHI that has completed a reconcile and is not suspended, once a minute in between, as it does not
watch Secrets it did not create; Kubernetes then updates the mounted file, which takes about a minute
more. Creating the storage Secret after the Pods, editing it, or deleting and recreating it with
other contents therefore takes effect by itself:

* **Within a few minutes, without a restart.** Neither ClickHouse nor the sidecar restarts, and the
  Pod stays Ready. The generated Secret is updated only when its contents change.
* **Operation by operation.** An operation that runs keeps the settings it started with; the next
  one takes the new ones. A run starts its steps on the hosts of its cluster at different times, so a
  change while a backup or restore runs leaves it with the old settings on some hosts and the new ones on others:
  change the storage Secret only while no backup or restore runs.
* **A removed setting counts.** The file is generated from the Secret as it is, so a setting removed
  from it is gone from the file too. A deleted storage Secret, or one without the `key` of the `storageConfig` or with an
  empty one, leaves the storage settings of the file as they were, with a `BackupStorageSecretMissing`
  event: removing the Secret does not take the storage from the sidecars. Storage settings that
  cannot be read leave the file as it was, with a `BackupStorageConfigInvalid` event, see
  [Backup storage](#backup-storage). A reconcile of the CHI meanwhile aborts with the reason
  `BackupConfigInvalid`; the check once a minute in between warns once, when the storage Secret turns so.
* **Only the storage Secret and the credentials are followed.** An `env` entry of the sidecar's
  container, taken from another Secret or a ConfigMap, and the operator's own settings, are
  environment variables, which Kubernetes resolves when it starts the container: they change at the
  next restart of the sidecar only.

When `<chi-name>-auto-backup-secret` is deleted, the running sidecars keep their credentials. The
operator notices the deletion and reconciles the CHI, which creates the Secret anew, with new random
REST API credentials and no ClickHouse ones; so does any reconcile that finds the Secret missing or
lacking a REST API key, whatever started it, except for a suspended CHI, whose Secret is left as it is
until the CHI is resumed. The backup user, if there was one, is gone with the ClickHouse credentials:
add them again to the new Secret. The configuration files take the new credentials as above.

Editing the credentials in the Secret in place works too, but the two kinds take different paths:

* **ClickHouse credentials** (`CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD`) name and authenticate the
  backup user in ClickHouse, which only a reconcile of the CHI writes, through the users
  configuration. The operator therefore watches its backup Secret and reconciles the CHI when they
  change - set, edited or removed. The reconcile writes the users configuration first and the configuration files after it; a
  renamed user replaces the old one. The check once a minute keeps the ClickHouse credentials of the
  configuration files as they are until the users configuration has the ones of the Secret, so the
  operator never hands a sidecar a user, or a password, before it has handed it to ClickHouse - and a
  reconcile that wrote the users configuration but failed before the configuration files leaves them
  to it.
  Removed credentials work the same way: the files go on connecting as the backup user until the
  reconcile has removed it. Credentials of no user the operator manages - one of the two not set -
  are taken as they are. A storage Secret gone while the files keep their storage settings does not
  keep ClickHouse credentials removed from the backup Secret. A
  suspended CHI is not reconciled: its sidecars and ClickHouse keep the old credentials until it is
  resumed. The username must be a valid name of the users configuration (letters, digits, `_`, `.`
  and `-`, not starting with a digit, `.` or `-`), and neither `default` nor the operator's own user;
  any other name aborts the reconcile with reason `BackupSecretUnresolved`. A user of that name the CHI
  defines in `configuration.users` is taken over, its profile and quota aside.

  Within a Pod the order is not kept: Kubernetes updates the users configuration of ClickHouse and
  the configuration file of the sidecar, two volumes, each in its own time, up to about a minute or
  two apart. Until both have the change, the sidecar may connect with credentials ClickHouse no
  longer takes - the old user after a rename or the old password after a change - or does not take
  yet. `clickhouse-backup` retries a connection that fails without end, with the credentials it read
  when the operation started, so an operation started in that window waits until ClickHouse takes
  those credentials, and after a rename or a password change it never does: the operation hangs, and
  so does the run that drives it, until its `activeDeadlineSeconds` (24h by default) stops it. Change the ClickHouse credentials only while no
  backup or restore runs or is about to start by a schedule, and wait until every Pod has the change -
  the sidecar reads the new credentials from `/etc/clickhouse-backup/config.yml` and ClickHouse lets
  the user in with them - before starting one.
* **REST API credentials** (`API_USERNAME`, `API_PASSWORD`) concern the sidecar and the integration
  tables alone, and the configuration files follow them as above, without a reconcile.

`clickhouse-backup` checks the REST API credentials of a request against the configuration it last
loaded, so on its own it would refuse every request made with new ones. The sidecar therefore
restarts its server - the process, not the container - when the REST API credentials of its
configuration file change: the operator puts their fingerprint next to the file, and the sidecar
sends the server `SIGHUP` when it changes. The restart cancels the operations the sidecar runs at
that moment, and recreates the integration tables with the new credentials in their URL: change the
credentials only while no backup or restore runs. Until a sidecar has the new file, within a few
minutes, its integration tables call it with credentials it refuses. A sidecar started with a
command of its own is not restarted.

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
| `cluster`                 | all             | Back up only this cluster. Empty means every cluster with backup enabled, each in a run of its own; with `scheduleRef`, every cluster the schedule backs up. |
| `backupName`              | generated       | Backup name, the same on every cluster; generated from the resource name and the time it is due when empty, see `status.backupName`. Refused with `scheduleRef`, or if it ends like a schedule's, `-YYYYMMDDHHMMSS`.        |
| `scheduledTime`           | when created    | RFC 3339 time, with its offset, the backup is due at, see [Back up at a later time](#back-up-at-a-later-time).                                                                                                               |
| `startingDeadlineSeconds` |                 | How late, from the time it is due, the backup may start on a cluster; unset, it waits as long as it takes.                                                                                                                   |
| `diffFromRemote`          |                 | Make this an [incremental backup](#incremental-backups) on top of this existing backup.                                                                                                                                      |
| `configOverrides`         | none            | `clickhouse-backup` settings the backup is made with in place of the sidecar's, e.g. `["S3_STORAGE_CLASS=STANDARD_IA"]`, see [Config overrides](#config-overrides). Not with `scheduleRef`. |
| `schemaOnly`              | `false`         | Back up table and database definitions only, no data.                                                                                                                                                                        |
| `replicaSelection`        | `FirstPerShard` | `FirstPerShard` backs up one replica of each shard, which is complete for `Replicated*` tables. `AllReplicas` backs up every replica, needed when a cluster holds non-replicated tables whose data differs between replicas; a shard counts as backed up once any of its replicas made the backup. `AllReplicas` needs a storage path and an object disk path of its own on every replica, or the backup is `Aborted` (`StoragePathConflict`), see [Storage paths](#storage-paths). |
| `tables`                  | all             | `clickhouse-backup --tables` pattern, e.g. `db.table`, `db.*`, comma-separated.                                                                                                                                              |
| `partitions`              | all             | List of partition ids to back up.                                                                                                                                                                                            |
| `activeDeadlineSeconds`   | `86400` (24h)   | How long the run of each cluster may take, from its start, before it is stopped and the backup of that cluster fails. |
| `timeouts`                | see [Timeouts](#timeouts) | How the run waits for its commands on the sidecars. With `scheduleRef`, a field left unset is the schedule's. |

The spec is validated when the resource is created: backup names (`backupName`, `diffFromRemote`,
`backupNamePrefix`) start with a letter or digit, contain only letters, digits and `_ - .`, never two
dots in a row (`clickhouse-backup` would strip them and store the backup under another name), and are
at most 128 characters - a schedule's `backupNamePrefix` at most 112, leaving room for the timestamp.
A one-off backup's `backupName` must not end like the backups of a schedule, `-YYYYMMDDHHMMSS`: the
[retention](#retention) of a schedule of that prefix would take it for one of its own.
`tables` may only contain letters, digits and `_ . * ? , -` (up to 1024 characters), and each
partition id must match `^[A-Za-z0-9_-]{1,128}$`. The on/off fields of backups, schedules and
restores (`schemaOnly`, `suspend`, `overwrite`, `validateTopology`, `rbac`) take the yes/no
forms `spec.backup.enabled` takes - `true`, `false`, `"yes"`, `"no"`, `1`, `0` and the like - and any
other value, a number with a fraction, a list or an object included, is refused like an invalid name.
Here, as on schedules and restores, `clickHouseInstallation` and `cluster` cannot be changed
afterwards, nor can a backup's `scheduleRef` (`clickHouseInstallation, scheduleRef and cluster are
immutable; create a new resource`), which Kubernetes enforces from 1.25 on. Whatever the spec says
later, the operator works on the clusters it recorded in `status.clusters` when it validated the
resource.

A backup runs once. To back up again, create a new resource (or use a schedule). Deleting a
`ClickHouseBackup` stops its runs - a command a sidecar runs already goes on there - and deletes not
the backup in the storage.

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
```

* It takes the CHI, the clusters (unless `cluster` names one of them), the `backupNamePrefix` and
  every backup option of the schedule (`schemaOnly`, `replicaSelection`,
  `tables`, `partitions`, `activeDeadlineSeconds`) as they are when the backup is validated, and records
  them in `status.schedule`: an edit of the schedule afterwards changes nothing about it. None of these
  options can be set on the backup itself, nor can `backupName`; its own fields, `cluster` (one of
  the clusters the schedule backs up), `diffFromRemote`, `scheduledTime`,
  `startingDeadlineSeconds` and `timeouts`, keep their meaning. The schedule's `timeouts` are recorded
  too, and every field of them the backup's own `timeouts` leave unset applies.
* It is always named `<backupNamePrefix>-<UTC time it is due, as YYYYMMDDTHHMMSS>`, e.g.
  `default-backup-demo-nightly-20261012T101507`: a `backupName` is refused with `scheduleRef`
  (`a backup that runs a schedule is named with the schedule's backupNamePrefix: backupName is not
  set with scheduleRef`), so the name always tells the schedule the backup came from. The `T` keeps
  it out of the schedule's slots: creating such backups never pushes one of the schedule's out, and
  [retention](#retention) keeps it as long as it is no older than the oldest scheduled backup some
  entry keeps, then deletes it.
  The prefix also lets the [recovery prefix guard](#storage-paths) tell where it came from.
* It runs as a run of the schedule, with its entries and `timeZone`, which `status.schedule` records
  too: as the Altinity usual backup workflow makes a backup by hand, it is incremental, on the newest backup of the schedule, as the first
  entry with `incremental: true` says - full when that entry's chain is `keep` long, or when no entry
  is incremental - and [retention](#retention) runs after it as after a scheduled run, on the same
  hosts, skipped while a restore holds the schedule's runs of the cluster. Like a scheduled backup it
  is a base of the next incremental backup while it is the newest, as the Altinity usual backup
  workflow makes a backup incremental on the newest backup of the cluster whether made by hand or on
  schedule.
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
with `StartingDeadlineExceeded`, telling what it waited for, as does each cluster whose run has not
started by then. Without `scheduledTime` the deadline counts from the backup's creation, which
bounds a backup waiting for a CHI that never becomes ready. The operator checks the time a second
after a backup is due, so a deadline of a few seconds may fail a backup that is on time: give it a
minute or more. Edit neither field once the backup is validated: its clusters still waiting would
count from the edited values. Applied after its `scheduledTime`, as a GitOps tool may re-apply a
backup it keeps, a backup starts at once unless its deadline has passed: keep
`startingDeadlineSeconds` on backups kept in git.

## ClickHouseBackupSchedule

Recurring backups at several periods, each keeping its own number of backups, which the operator
runs on every cluster itself. See [`04-backup-schedule.yaml`](chb-examples/04-backup-schedule.yaml).

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseBackupSchedule"
metadata:
  name: nightly
spec:
  clickHouseInstallation: backup-demo
  schedules:
    - period: hour
      keep: 24
    - period: daily
      time: "02:30"
      keep: 7
    - period: weekly
      time: "02:30"
      day: 0
      keep: 4
```

| Field                                                                             | Default                               | Meaning                                                                                                                                                                                                                       |
|-----------------------------------------------------------------------------------|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `clickHouseInstallation`                                                          | required                              | CHI to back up, in the same namespace.                                                                                                                                                                                        |
| `cluster`                                                                         | all                                   | Back up only this cluster. Empty means every cluster with backup enabled, each in runs of its own. |
| `schedules`                                                                       | required                              | The periods the schedule backs up at, at least one, at most one entry per period, see below.                                                                                                                                  |
| `schedules[].period`                                                              |                                       | `minute`, `hour`, `6hours`, `daily`, `weekly` or `monthly`.                                                                                                                                                                   |
| `schedules[].time`                                                                | `00:00`                               | `HH:MM` in `timeZone`; required for `daily`, `weekly` and `monthly`. Every entry that sets a time sets the same one; `hour` takes only its minute, `6hours` its hour modulo 6 and its minute, `minute` none.                  |
| `schedules[].day`                                                                 |                                       | `weekly`: `0`..`6`, `0` being Sunday; `monthly`: `1`..`31`, a day past the end of a shorter month meaning its last day. Required for these two, refused for the others.                                                       |
| `schedules[].keep`                                                                | required                              | At least `1`: how many of the newest backups of the runs that match the entry [retention](#retention) keeps.                                                                                                                  |
| `schedules[].incremental`                                                         | `false`                               | At most one entry: its runs make [incremental backups](#incremental-backups) on the newest backup of the series while the chain is shorter than its `keep`.                                                                  |
| `schedules[].configOverrides`                                                     | none                                  | `clickhouse-backup` settings the runs of the entry make their backups with, e.g. `["S3_STORAGE_CLASS=GLACIER_IR"]` on a monthly entry: those of the heaviest entry of a slot apply. See [Config overrides](#config-overrides). |
| `timeZone`                                                                        | UTC                                   | IANA time zone the entries are read in, e.g. `Europe/Berlin`. Backup names carry the time of the slot in this zone. |
| `suspend`                                                                         | `false`                               | Stop starting new runs. A run already in progress continues.                                                                                                                                                                  |
| `startingDeadlineSeconds`                                                         |                                       | How late a run may still start after its slot, as one that waited for a run in progress, a restore or the operator: a slot missed by more is skipped. Runs never overlap. |
| `successfulRunsHistoryLimit`                                                      | `3`                                   | Successful finished runs `status.recentRuns` keeps, at least 1. |
| `failedRunsHistoryLimit`                                                          | `1`                                   | Failed finished runs `status.recentRuns` keeps, at least 1. |
| `schemaOnly`, `replicaSelection`, `tables`, `partitions`, `activeDeadlineSeconds` |                                       | As in [`ClickHouseBackup`](#clickhousebackup); `activeDeadlineSeconds` bounds the run of each cluster. |
| `timeouts`                                                                        | see [Timeouts](#timeouts)             | How every run, and a `ClickHouseBackup` that runs the schedule and leaves a field unset, waits for its commands on the sidecars. |
| `backupNamePrefix`                                                                | `<namespace>-<chi>-<schedule>`        | Backup names are `<prefix>-<YYYYMMDDHHMMSS>`, the time the run was due in `timeZone`, e.g. `default-backup-demo-nightly-20261003023000`. Also scopes [retention](#retention). Can be set once and is immutable after.         |
| `retention.onScheduleChange` | `Hold` | What [retention](#schedule-changes) does with the backups the schedule kept before a change of its entries or `timeZone`: `Hold` keeps them all until the change is released, `Prune` applies the change at once. |
| `retention.keepBackups` | none | Backups retention keeps beyond the entries, at most 512, each `{name, until}`: `name` the name, or with a [naming template](#backup-naming) the name of the backup of any shard or its time alone; `until`, optional, the RFC 3339 time the pin ends, see [Schedule changes](#schedule-changes). |
| `retention.releaseHeld` | none | The `changeID` of `status.retention.previous` whose held backups, and those of every earlier change, are released. |

**Runs at the slots of the finest entry.** The operator runs a schedule on every cluster it backs up
at the slots of its finest entry - every hour at :30 above. Validation makes every entry that sets a
time set the same one, so every slot of a coarser entry is a slot of the finest. Each run is one slot,
matched against the entries in `timeZone`, and counts for every entry it matches - the run of Sunday
02:30 is the weekly, the daily and the hourly one; the heaviest of them decides whether it is
incremental. A run lives in `status.active` while it runs, one per cluster at most, and in
`status.recentRuns` once it finished, newest first, as many as `successfulRunsHistoryLimit` and
`failedRunsHistoryLimit` keep: there is no `ClickHouseBackup`, `CronJob` or `Job` per scheduled run.
Runs never overlap: a slot that comes while the run of the cluster, or another operation of it, is in
progress waits for it, and only the latest slot missed runs, late, unless `startingDeadlineSeconds`
passed since it, which skips it with a `ScheduledRunSkipped` warning. A slot before the schedule was
created never runs. A late run still makes the backup of its slot, so the runs of one slot make one
backup name on every cluster. A cluster that joins - added to the CHI, backup enabled - runs from its
next slot once the CHI has completed its reconcile; a cluster that leaves runs no more (see
[Hosts that leave a schedule](#hosts-that-leave-a-schedule)).

**Daylight saving time.** A backup is named after the wall-clock time of its slot in `timeZone`, so
the two nights a year the clocks change are handled by that name:

* **Clocks go forward** (02:00 to 03:00 in `Europe/Berlin`, the last Sunday of March): a `daily`,
  `weekly` or `monthly` slot whose time does not exist that day - `02:30` - is the time the zone
  moves it to, `03:30`, and the backup is named after it, `...-20260329033000`. The slot is still the
  slot of its entries: the run is the weekly one on the Sunday, with that entry's `configOverrides`
  and its `incremental`, and [retention](#retention) keeps the backup as that of the entry's slot,
  never as the backup of no slot, which it would prune.
* **Clocks go back** (03:00 to 02:00, the last Sunday of October): a wall-clock time that happens
  twice is a slot once, its first occurrence. An `hour` or a `minute` entry runs no slot in the
  second 02:00 to 03:00, and a `daily` slot of a time in it - `02:30` - runs at its first 02:30, as
  two runs would make two backups of one name, which the second would refuse as it exists already.
  The skipped slots are not told: nothing was missed.

The default prefix joins the namespace, CHI and schedule names, shortened with a hash past 112
characters, so that schedules of the same name of different CHIs, or of CHIs of the same name in
different namespaces, get different prefixes; see [Storage paths](#storage-paths) for why that
matters. `backupNamePrefix` can be set once and is immutable after, which Kubernetes enforces from
1.25 on: [retention](#retention) knows the current prefix only. To change the prefix of a schedule
that has run, create a new schedule. To make a schedule's backup at once, see
[Run a schedule now](#run-a-schedule-now); `diffFromRemote` is only available on a
`ClickHouseBackup`.

**Phase.** `status.phase` tells, with `status.reason` and `status.message`, whether the schedule runs:

* `Active`: the operator runs it - also while a restore holds its runs, which the `Ready` condition tells
  (`SuspendedForRestore`);
* `Suspended`: `spec.suspend` is set;
* `Aborted`: it cannot run until somebody acts - an invalid spec (`InvalidSpec`), a missing CHI or
  cluster (`CHINotFound`, `ClusterNotFound`), backup disabled or without a storage (`BackupDisabled`,
  `NoBackupStorage`), a stopped CHI (`CHIStopped`), no ClickHouse credentials of the operator
  (`NoClickHouseAccess`, see [How the operator runs an operation](#how-the-operator-runs-an-operation)),
  a held default prefix (`SharesSourcePrefix`), storage paths two writers of its backups share
  (`StoragePathConflict`, see [Storage paths](#storage-paths)) - or the last scheduled run failed (`LastBackupFailed`),
  until a later run succeeds; the schedule keeps running in that last case only.

`status.nextScheduleTime` is the next slot of any entry, empty while the schedule is suspended or
aborted for another reason than a failed run. `status.entries[]` tells, per `period`, the
`lastScheduleTime` of the newest run whose slot matches the entry, and the `lastBackupName` of the
newest such run that completed.

The status also tells how the runs went, for each cluster in `status.clusters`:

* `configSecret`: the Secret of the `storageConfig` its runs keep their backups by, as of the last run - for a
  cluster the schedule backs up no more, the storage its unpruned backups lie in (see
  [Hosts that leave a schedule](#hosts-that-leave-a-schedule));
* `lastScheduleTime`: the slot of the last run that started; `lastSuccessfulTime`: when a run last
  completed successfully; `lastBackupName`: the backup that successful run made;
* `lastFailureTime` and `lastFailureMessage`: when a run last failed, and why, with the error the
  sidecars reported (see [How the operator runs an operation](#how-the-operator-runs-an-operation));
* `retentionHosts` and `unprunedHosts`: the hosts of the cluster that a run has backed up, which
  retention prunes on, and the hosts that left them while retention was on, whose backups it
  prunes no more (see [Hosts that leave a schedule](#hosts-that-leave-a-schedule)).

Over all clusters, `status.lastSuccessfulTime` is the oldest of their last successes - by then every
cluster had been backed up - and `status.lastBackupName` the backup every cluster made last, the
one to [recover the whole CHI from](#recover-a-new-chi-from-a-backup). Both are empty while a
cluster has not completed a run yet. While the runs of a slot complete one after the other,
`lastBackupName` stays at the backup of the slot before until every cluster has made the new one;
while the last run of a cluster failed, it is cleared as soon as another cluster completes a newer
run. Check that a backup exists on every cluster before you recover from it, or recover each
cluster from its own `status.clusters[].lastBackupName`.

The `LastBackupSucceeded` condition tells whether the last finished runs succeeded, with the reason
of a failure otherwise (`Failed`, `DeadlineExceeded`). While a last run failed,
`Ready` is `False` with reason `LastBackupFailed` and the phase is `Aborted`; the next successful run
makes the schedule `Active` and `Ready` again. A run whose backup
[holds no data](#how-the-operator-runs-an-operation) succeeded, with the reason `CompletedWithoutData`, and
`Ready` stays `True` with the reason `LastBackupWithoutData` until a run backs up data again.

**Events** on the schedule: `ScheduledBackupStarted` when a run starts on a cluster, and the warning
`ScheduledRunSkipped` for a slot missed past `startingDeadlineSeconds`; `ScheduleActive`, `ScheduleSuspended` and `ScheduleAborted` (a warning, with
the reason) on every change of the phase; `ScheduledBackupSucceeded` for every completed run, naming
its backup, and the warning `ScheduledBackupFailed` for every failed one, with the end of the error;
every run adds the events of its steps (see
[How the operator runs an operation](#how-the-operator-runs-an-operation)).

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
  backupName: default-backup-demo-nightly-20261003023000
```

| Field                    | Default            | Meaning                                                                                                                                   |
|--------------------------|--------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `clickHouseInstallation` | required           | CHI to restore into, in the same namespace. A fresh, empty CHI is recommended.                                                            |
| `cluster`                | all                | Restore only into this cluster. Empty means every cluster with backup enabled, each in a run of its own. |
| `backupName`             | required           | Backup to restore.                                                                                                                        |
| `sourceInstallation`     | the restored CHI   | CHI that made the backup, when another: the backup is read from the paths its name gives, see [Storage paths](#storage-paths). Immutable. |
| `sourceNamespace`        | the restored CHI's | Namespace of the CHI that made the backup, when another and the paths hold `{namespace}`. Immutable.                                      |
| `schemaOnly`             | `false`            | Restore table and database definitions only, no data.                                                                                     |
| `tables`                 | all                | Restore only the matching tables: a `clickhouse-backup` pattern as for a backup, e.g. `db.table`, `db.*`, comma-separated.                |
| `partitions`             | all                | Restore the data of only these partition ids (`--partitions`), e.g. `["202601", "202602"]`, into the existing tables, replacing those partitions; the schema is not restored. Not with `schemaOnly` or `rbac`. See [Restore filters](#restore-filters). |
| `skipTables`             | none               | Leave out the matching tables: a `clickhouse-backup` pattern, as `tables` (`CLICKHOUSE_SKIP_TABLES`).                                    |
| `skipTableEngines`       | none               | Leave out the tables of these engines, e.g. `["Kafka", "RabbitMQ"]` (`CLICKHOUSE_SKIP_TABLE_ENGINES`).                                    |
| `databaseMapping`        | none               | Restore the tables of a database of the backup into another one: `{source: target}` (`--restore-database-mapping`).                      |
| `rbac`                   | `false`            | Also restore the [access entities](#access-entities) of the backup. Only with an empty `cluster`.                                         |
| `overwrite`              | `false`            | Without `tables`, recreate the databases of the backup whole (`--rm`), dropping their tables the backup does not hold; with `tables`, or when false, only the tables of the backup are recreated. See [ClickHouseRestore](#clickhouserestore). |
| `validateTopology`       | `true`             | Refuse the restore unless every target shard holds the backup, and, from another CHI, a backup named as its base at the own paths.        |
| `replicatedCopyToDetached` | `true`           | The replicas of a shard but the first copy the data into the `detached` directories of their `Replicated*` tables before the first one restores it, so replication attaches it from there instead of fetching it; `false` downloads the schema alone on them. See below. |
| `resumeDownload`         | `true`             | A [rerun](#rerunning-with-spectaskid) resumes the downloads its earlier runs left, from another CHI too; `false` deletes them (`delete local <name>`, step `replaceLocal`) and downloads the backup again. The first run is the same either way. |
| `activeDeadlineSeconds`  | `86400` (24h)      | How long the run of each cluster may take, from its start, before it is stopped and the restore of that cluster fails. |
| `timeouts`               | see [Timeouts](#timeouts) | How the run waits for its commands on the sidecars. |

`tables` follows the rules of a backup's `tables`. The target CHI needs backup enabled and the same
backup storage configuration as the source, with the same cluster names and shard layout; a backup
of a CHI of another name, or of another namespace, is found by naming that CHI in
`sourceInstallation`, and its namespace in `sourceNamespace`, see
[Restoring into another CHI](#storage-paths).

On each cluster the restore runs as the Altinity usual backup workflow restores:

1. **Download.** Every host downloads the backup, all at once, hard-linking every file of it already
   on its disks (`download --hardlink-exists-files`) instead of downloading it again. With
   `replicatedCopyToDetached: false` the replicas of a shard but the first download the schema alone
   (`download --schema`). A host short of
   disk space fails the restore here, before anything changes; a host that already holds a local copy
   of the backup restores from that one. An incomplete local copy - one a download that failed or was
   stopped left, which `clickhouse-backup` lists as `broken` - is never deleted: the host downloads the
   backup again, which `clickhouse-backup` resumes where the copy ends (`use_resumable_state`, on by
   default).
2. **Schema.** Every replica restores the schema from its copy, one after another, the first replica of
   the first shard first and `ON CLUSTER` (`--env=RESTORE_SCHEMA_ON_CLUSTER={cluster}`), with the access
   entities when the restore restores them (`--rbac`). A restore of `partitions` has no schema step.
3. **Copy to detached** (step `copyToDetached`, with `replicatedCopyToDetached`, the default). The
   replicas of each shard but the first copy the data of the backup, all at once, into the `detached`
   directories of their `Replicated*MergeTree` tables (`restore --data=1 --ignore-dependencies
   --replicated-copy-to-detached`), attaching nothing; their tables of other engines take the data as
   a data restore does. A shard of one replica has no such step.
4. **Data.** One replica of each shard - the first - restores the data, all at once, once every copy
   to detached is done; replication brings `Replicated*` tables to the other replicas, which take the
   parts it tells them of from their `detached` directories when they copied them there, and fetch them
   from the first replica otherwise.
5. **Cleanup.** Once the restore succeeded, every host deletes its downloaded copy.

The replica of each shard that restores the data is chosen as the restore comes to the copy to
detached: a first replica that does not answer then leaves it to the next one, with an
`OrchestratorReplicaFallback` Event, and copies nothing - it takes the data by replication once it
answers. A first replica the restore of the data cannot be submitted to leaves it, likewise, to the
next replica of its shard that copied the data, which restores it over its copy (`clickhouse-backup`
renames the parts it copied there to `ignored_<part>` first). Without `replicatedCopyToDetached` the
first replica of each shard restores the data, with no fallback: no other replica downloaded it.
`status.clusters[].run.plan` records the replicas chosen (`dataHosts`, `copyHosts`), which a run the
operator resumes after a restart keeps. A part copied to `detached` that replication does not use - as
the parts of a backup made on every replica (`AllReplicas`) at a `{replica}` path, named as each
replica merged them - stays there once the restore deletes its local copy; `ALTER TABLE ... DROP
DETACHED PART` removes it.

A restore that fails, at any step, deletes nothing it downloaded: the local copies stay, to look into,
and for a restore of the backup again to resume their download. An `OrchestratorRestoreDownloadsKept`
warning names the hosts that keep one; delete a copy by hand with
`INSERT INTO system.backup_actions(command) VALUES ('delete local <backup>')` on its host, as a
copy takes as much disk as the data of the backup.

Every step takes the [filters](#restore-filters) of the restore; see
[How the operator runs an operation](#how-the-operator-runs-an-operation) for the commands. A restore
that reads the backup of [another CHI](#storage-paths) runs the same steps with the storage paths of
that CHI. Restoring the schema `ON CLUSTER`, `clickhouse-backup` re-points every `Distributed` table
that names another cluster - `all-sharded`, or another cluster of the CHI - at the cluster it
restores. `status.backupName` records the backup the runs restore, fixed when the restore is
validated: editing `backupName` afterwards changes nothing.

**Overwrite.** `clickhouse-backup` drops every table of the backup that exists before it recreates
it, whatever the restore says, and every restore command ignores dependencies
(`--ignore-dependencies`). `overwrite` decides only what happens to the databases:

* **`overwrite: true` without `tables`** recreates the databases of the backup whole (`restore
  --schema=1 --rm`): a table of such a database that the backup does not hold is dropped too;
* **`overwrite: false`, or `overwrite: true` with `tables`**, recreates the tables of the backup
  only, and leaves every other table alone: a restore of some tables never drops a database, so
  `--rm` is left out whenever `tables` is set.

The operator checks no existing table before the restore. `max_table_size_to_drop` need not be
changed for it: before every table or database it drops, `clickhouse-backup` creates the
`flags/force_drop_table` flag in the data directory of the ClickHouse of its Pod, which lets that
server drop a table of any size once. The flag is local to the host: the drop that the first replica
of the first shard runs `ON CLUSTER` (for a database other than `Replicated`) reaches the other hosts
without it, so they may still refuse a table larger than their `max_table_size_to_drop`, 50 GB unless
set.

**A copy of a running CHI needs its own `zookeeper.root`** when both use the same ZooKeeper or
Keeper. A restored `Replicated*` table whose ZooKeeper path resolves to the same location as the
source's - a path built from the table's UUID, a path without `{installation}`, or one whose macros
resolve the same in both, as for a CHI of the same name in another namespace - joins the source's
replication instead of becoming an independent copy, and a `TRUNCATE`, `ALTER DELETE` or
`DROP PARTITION` on the copy then reaches the source.

### Restore filters

As the restore of the Altinity usual backup workflow, a `ClickHouseRestore` can narrow what it restores
beyond `tables`:

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseRestore"
metadata:
  name: backup-demo-restore-events
spec:
  clickHouseInstallation: backup-demo-restored
  backupName: default-backup-demo-nightly-20261003023000
  tables: "analytics.*"
  # The data of two months only
  partitions: ["202609", "202610"]
  # Neither the scratch tables nor the queue consumers
  skipTables: "analytics.tmp_*"
  skipTableEngines: ["Kafka"]
  # Into a database of another name, next to the live one
  databaseMapping:
    analytics: analytics_restored
```

* `partitions` restores those partition ids into the tables as they are, as the Altinity usual backup workflow restores partitions:
  the download and the data restore are narrowed to them (`--partitions`), and the schema step is
  left out entirely, so nothing is dropped or recreated and `overwrite` does not apply. The tables must exist. On the first replica of each shard
  `clickhouse-backup` drops each of those partitions where a table holds it, which replication
  carries to the other replicas, and attaches the parts of the backup without checking the replicas
  first (`CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH=0`). As the
  access entities come with the schema, `partitions` takes neither `schemaOnly` nor `rbac`. An id
  is letters, digits, `_` and `-`, the form `system.parts` shows as `partition_id`.
* `skipTables` and `skipTableEngines` leave tables out of every step, through `CLICKHOUSE_SKIP_TABLES`
  - on top of the system databases `clickhouse-backup` leaves out anyway - and
  `CLICKHOUSE_SKIP_TABLE_ENGINES`.
* `databaseMapping` restores the schema and the data of each database it names into the database it
  maps it to (`--restore-database-mapping`), which overrides the empty `RESTORE_DATABASE_MAPPING` the
  sidecar pins: the tables of `analytics_restored` the backup holds are recreated, and `analytics` is
  left alone. A database may be mapped once, not to itself, not to a
  database another one is mapped to or that is mapped itself, and not to or from a system database.

Every value becomes a single token of a `clickhouse-backup` command, so each is checked strictly -
the patterns as `tables`, engines and databases of letters, digits, `_` (and `-` for databases) - and
a restore that fails the check ends `Aborted`.

### Access entities

A backup holds the access entities created with SQL - users, roles, grants, row policies, quotas
and settings profiles - which `clickhouse-backup` backs up by default. `rbac: true` restores them
too, with the schema of every host of the CHI (`restore --schema=1 --rbac`), before the data,
as the Altinity usual backup workflow restores them - existing access entities of the same names are replaced - and ClickHouse
reloads them live with `SYSTEM RELOAD USERS`. `rbac` needs an empty `cluster`, as access entities belong to the whole CHI,
the more so in a `replicated` user directory, which every cluster using its Keeper path shares.
[Recovery](#recover-a-new-chi-from-a-backup) always restores them. A restore of access entities
holds the schedules and backups of the clusters it restores until it has finished - of every cluster
for a restore of the whole CHI, of its own cluster for a recovery restore -,
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
  clickhouse-backup restore_remote --rbac-only default-backup-demo-nightly-20261003023000
```

## Which hosts are involved

The operator works out the hosts from the CHI as the operator itself built it, so templates,
explicit shard and replica lists and custom names are all accounted for. Only clusters with backup
enabled are considered, narrowed down to `spec.cluster` when set, and each of them is worked on by
a run of its own. While the CHI cannot run an operation, no run starts - a backup or restore stays
`Pending` with `Validated=False`, a schedule starts no run - and the condition's reason says why:

* the CHI does not exist (`CHINotFound`) or has not completed its reconcile (`CHINotReady`) - a
  schedule validated before the reconcile keeps running on the hosts it had, and stays
  `Validated`, until the reconcile completes, but for the clusters a restore holds, see
  [One operation at a time per cluster](#one-operation-at-a-time-per-cluster), which run again once
  both the restore and the reconcile have completed;
* the CHI is stopped with `spec.stop`, so its hosts are down (`CHIStopped`) - its schedules start
  no run until it is started again;
* the named cluster does not exist (`ClusterNotFound`), has backup disabled (`BackupDisabled`) or
  enabled without a storage (`NoBackupStorage`, see [Enabling backup](#enabling-backup)), or
  there is no host to work on (`NoTargetHosts`). A backup or a restore waits for a missing or
  disabled cluster only until the CHI has completed its reconcile; then it fails, holding nothing,
  so that a mistyped cluster name never starts a restore days later.

A backup, restore or schedule of every cluster leaves out the clusters with backup enabled without a
storage: a backup or restore warns about them once (`NoBackupStorage`), and a schedule starts
no run on them until they have one, and names them in its `Validated` condition - with
the reason `NoBackupStorage`, or `CHIRecovered` for a recovered CHI, whose message tells both -
warning when that changes.

The operator checks again periodically, so fixing the CHI lets a waiting resource proceed. A
schedule whose own spec turns invalid - a cron expression or `timeZone` the API server refuses
included - starts no run the same way (`InvalidSpec`) until the spec is fixed. Once the target
is resolved, the hosts of each cluster, which `status.clusters[].hosts` records for a backup or
restore, are:

* **Backup:** one replica per shard (`FirstPerShard`) or every replica (`AllReplicas`). The one replica
  is `backup.preferredReplica` of the cluster, the first replica (0) by default, as the Altinity usual backup
  workflow backs up its `backupReplica`: a lag-free replica, or one on a node with spare disk or network, can take the
  load of the backup off the replica that serves the queries. A shard with fewer replicas backs up
  its first one, and so does a shard whose preferred replica's ClickHouse does not answer through
  the sidecar when the run starts; the run tells it in a warning and an `OrchestratorReplicaFallback`
  event. Every replica of a shard keeps a `FirstPerShard` backup at the same storage path, without
  `{replica}`, so the choice changes neither where the backup is nor what restores it.
  `status.clusters[].hosts` records the preferred replicas while the run goes on, and once it has
  completed the replicas the backup was actually made on: a replica it fell back to takes the place
  of the preferred one.
* **Retention:** one host per shard of the cluster the backup ran on - the first replica, which
  lists the same backups as the preferred one -, or every replica with `AllReplicas`, which the
  schedule lists in `status.clusters[].retentionHosts`.
* **Restore:** the download on every replica, the schema on every replica - the first replica of the
  first shard `ON CLUSTER` -, access entities with it, the copy of the data to `detached` on the other
  replicas of each shard, and the data on one replica per shard, see
  [ClickHouseRestore](#clickhouserestore).
* **Path check:** the storage paths of every backup-enabled cluster of the CHI, see
  [Storage paths](#storage-paths).

## How the operator runs an operation

Every backup and restore, and every run of a schedule, runs in the operator itself, one
run per cluster, as the Altinity usual backup workflow runs them: no `Job`, `CronJob` or `Pod` is
created for it, and the operator needs no RBAC for any. A run drives the `clickhouse-backup` sidecars
of its cluster through SQL, by the [integration tables](#integration-tables) of the ClickHouse of every
host:

* **A command is a row.** The run reads the time of the host's ClickHouse, `now('UTC')`, then inserts
  the command, `INSERT INTO system.backup_actions (command) SELECT '<command>'`, and finds its row,
  the first row of that command in `system.backup_actions` that started then or later. It reads the
  row every 5 seconds until the command finishes, with the status `success`, or fails, with the
  `error` of the row. A command is waited for at most `timeouts.actionTimeout`, 12 hours by
  default (`Action Timeout`), the run at most its `activeDeadlineSeconds`, from its start. The insert
  sets `http_receive_timeout` and `http_send_timeout` of the URL engine to
  `timeouts.httpReceiveTimeout`, 4 hours by default: the sidecar answers a `delete local` or
  `delete remote` only once it is done.
* **A delete whose insert fails is settled by its row.** An insert of a delete that fails other than
  by a busy sidecar - cut by a timeout, a dropped connection - is not taken as failed: the run looks
  for the row of that delete, which started at the time read before the insert or later, for up to
  `timeouts.errorResponsesMax` reads, and settles the delete by its status as any other command; only
  a delete that left no row fails.
* **One command at a time per sidecar.** A sidecar that runs another command refuses one more
  (`API_ALLOW_PARALLEL` is pinned off): the run tries again once the sidecar is free, waiting 5
  seconds after the first refusal, twice as long after each next one, at most 5 minutes. The run waits
  in a goroutine of its own, never in a worker of a controller.
* **A lost command is settled.** A command without a row more than `timeouts.errorResponsesMax` reads
  in a row, 5 by default, was lost, as when the sidecar restarted: an upload or a download the sidecar resumes is waited for, a backup found
  complete where the command puts it - in the remote storage, or on local disk for a download -
  counts as made, and anything else fails the run: a restore is never resumed, as the environment of
  its next step may have changed.
* **Unreachable hosts fail the run.** A host whose ClickHouse does not answer is waited for, as its
  Pod may restart, for `timeouts.unreachableTimeout`, 10 minutes by default, from the first
  error in a row; then the run fails, naming the host. A read of the row of a command that fails -
  ClickHouse does not answer, the URL engine reports an HTTP error of the sidecar, or a response that
  is no valid JSON - is such an error, not a read without a row: it never counts against
  `errorResponsesMax`. The operator logs each with the error code of ClickHouse and the HTTP status
  code the URL engine reports, when the error tells them. A host that has no
  `system.backup_actions` - its sidecar started before its ClickHouse - fails the run before it
  changes anything there.
* **What a host holds** comes from `system.backup_list` - the backups, local and remote, with their
  sizes and the backup each is incremental from.
* **Paths are checked first.** Before it changes anything, a backup or restore run checks the storage
  paths of every backup-enabled cluster of the CHI, see [Storage paths](#storage-paths).

The commands are those of `clickhouse-backup`, as the Altinity usual backup workflow sends them:

| Step                                      | Command                                                                                        |
|-------------------------------------------|------------------------------------------------------------------------------------------------|
| backup                                    | `create_remote --rbac [--env=K=V ...] [--schema] [--tables=..] [--partitions=..] [--diff-from-remote=<base>] <name>` |
| restore: download, on every replica       | `download --hardlink-exists-files <name>`; with `replicatedCopyToDetached: false`, on the replicas but the first of each shard `download --schema <name>` |
| restore: schema, on every replica         | `restore --schema=1 [--rm] --ignore-dependencies [--rbac] <name>`, `--rm` with `overwrite` and no `tables`, on the first replica of the first shard with `--env=RESTORE_SCHEMA_ON_CLUSTER={cluster}` |
| restore: copyToDetached, on the other replicas of each shard | `restore --data=1 --ignore-dependencies --replicated-copy-to-detached <name>`, with `replicatedCopyToDetached` |
| restore: data, on one replica per shard   | `restore --data=1 --ignore-dependencies <name>`                                                                      |
| retention                                 | `delete remote <name>`                                                                         |
| a failed backup, a successful restore     | `delete local <name>`                                                                          |

A restore adds its [filters](#restore-filters) to each of its commands: `--tables=`,
`--partitions=` with `--env=CLICKHOUSE_CHECK_REPLICAS_BEFORE_ATTACH=0`, `--restore-database-mapping=`,
`--env=CLICKHOUSE_SKIP_TABLES=` and `--env=CLICKHOUSE_SKIP_TABLE_ENGINES=`.

**ClickHouse credentials.** The operator queries every host with the ClickHouse credentials of its own
configuration, `clickhouse.access` - a username and a password, or a Secret holding them - as it does
for metrics and schema propagation. It reaches each host by its name in the DNS domain of the namespace of the
CHI, `<host>.<namespace>.svc.cluster.local.`, or as the CHI's `namespaceDomainPattern` gives it, on the
port of `clickhouse.access`. Backup uses no default credentials: only a username and a password the
configuration sets itself, as `clickhouse.access.username` and `password` or in the Secret
`clickhouse.access.secret` names, enable it. The defaults the operator takes for metrics and schema
propagation otherwise, user `clickhouse_operator`, do not: until the configuration sets both, no
operation starts, and backups, restores and schedules wait with the reason `NoClickHouseAccess`. The
install bundle and the Helm chart set them in the Secret `clickhouse-operator`. The user needs to insert into `system.backup_actions` and read
`system.backup_actions`, `system.backup_list` and `system.tables` on every host.

**Status, and a restart of the operator.** `status.clusters[].run` of a backup or a restore, and
`status.active[]` of a schedule, record the run of each cluster: its `operation` and its `step`,
`mutated` once a command that changes something may have reached a sidecar, the `hosts` with the
backup each makes or restores and the base of an incremental one, every command the run inserted in
`actions`, with its `timeFrom` and the status the sidecar told, and the `plan` its preflight decided,
such as the hosts that hold a local copy of the backup to restore. The operator runs the backup controllers
leader-elected, by a `Lease` in its namespace, and the runs live in the leader: a new leader resumes
every run the status records running. An operator that loses the `Lease` - the API server did not
answer in time to renew it - stops its backup controllers and its runs, builds new ones and enters the
election again, waiting longer after each loss, up to a minute; it is not restarted, and the
ClickHouse controllers go on. Whichever operator leads next, that one or another, resumes the runs as
after a restart. It reads the rows of the commands the run recorded rather than
inserting them again, and finds a command the run inserted before the status recorded it by its row,
from 5 minutes before the run started on - but for a row that finished before the run started, which
an earlier operation left: a new run never takes a row it did not insert.

**Failure.** A run that fails tells why in `status.clusters[].message`, in the condition that
reports the failure (`Ready`, `lastFailureMessage` for a schedule) and
in a warning event, with the error the sidecars reported, cut to a kilobyte that keeps its start and
its end. A restore that failed before any command that changes something could reach a sidecar fails
with the reason `FailedUnchanged`, a run past its `activeDeadlineSeconds` with `DeadlineExceeded`, any
other with `Failed`. A host whose backup fails leaves the others to finish theirs, see
[Installations and clusters](#installations-and-clusters).

**A backup that holds no data is reported.** A backup of tables that hold no data, in the
`partitions` it names if any, or of no table at all - a `tables` pattern that matches nothing, an
installation recreated empty - succeeds. Once it is made, the run lists it on every host it ran on
and, when no host's copy holds data, locally or on an object disk, reports the backup completed with
the reason `CompletedWithoutData`, in the `Ready` condition of a `ClickHouseBackup` or the
`LastBackupSucceeded` condition of a schedule, and with a warning event of that reason that tells the
likely cause and, for a schedule with [retention](#retention), that the backup does not count toward
the backups with data it keeps. A `schemaOnly` backup never holds data and is not reported; nor is a
backup when a host cannot be listed then, or lists its copy broken, or with a `clickhouse-backup`
older than 2.6.6, which lists no sizes.

**Events.** Besides the events of its phases, the operator posts the steps of a run on its resource:
`OrchestratorCommand` and `OrchestratorCommandFinished` for every command,
`OrchestratorHostStepSucceeded`, `OrchestratorBackupStarted`, `OrchestratorBackupCompleted`,
`OrchestratorRestoreStarted`,
`OrchestratorRestoreCompleted`, `OrchestratorIncrementalBase`, `OrchestratorIncrementalFull`,
`OrchestratorConfigOverrides`, `OrchestratorReplicaFallback`, `OrchestratorRetentionDeleted`,
`OrchestratorRetentionHeld`, `OrchestratorFailedBackupDeleted`, `OrchestratorFailedBackupRemoteLeft`, and the warnings
`OrchestratorBackupFailed`, `OrchestratorHostFailed`, `OrchestratorReplicaFailed`, `OrchestratorNoData`, `OrchestratorRBACNotRestored`,
`OrchestratorRetentionFailed`, `OrchestratorRetentionSkipped`, `OrchestratorFailedBackupLeft`,
`OrchestratorRestoreDownloadsKept` and `OrchestratorSourceCopyLeft`.

### Timeouts

`spec.timeouts` of a `ClickHouseBackup`, a `ClickHouseBackupSchedule` and a `ClickHouseRestore` bound
how the operator waits for the commands it runs on the sidecars. Every field is optional; the timeouts
are Go durations, such as `"12h"`, `"90m"` or `"1h30m"`:

| Field                | Default | Bounds      | Description |
|----------------------|---------|-------------|-------------|
| `actionTimeout`      | `12h`   | `1s`..`720h` | How long the operator waits for one command on one host before it fails with `Action Timeout`. |
| `unreachableTimeout` | `10m`   | `1s`..`168h` | How long a host whose ClickHouse does not answer, or whose sidecar answers the URL engine with an error, is retried, from the first error in a row, before the host fails. |
| `errorResponsesMax`  | `5`     | `1`..`1000`  | How many reads in a row may find no row of a submitted command before it is taken as [lost](#how-the-operator-runs-an-operation); also how long the row of a delete whose insert failed is looked for. |
| `httpReceiveTimeout` | `4h`    | `1s`..`168h` | `http_receive_timeout` and `http_send_timeout` of every insert into `system.backup_actions`, in whole seconds: a synchronous `delete local` or `delete remote` that takes longer than the default of the URL engine is not cut. |

A field a resource leaves unset is taken, in this order, from:

1. the resource itself;
2. for a `ClickHouseBackup` with `scheduleRef`, the schedule's `timeouts`, as `status.schedule.timeouts`
   recorded them when the backup was validated; a run of a schedule takes the schedule's own;
3. `backup.timeouts` of the [operator configuration](operator_configuration.md#backup), same keys;
4. the built-in default above.

```yaml
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseBackupSchedule"
metadata:
  name: nightly
spec:
  clickHouseInstallation: backup-demo
  schedules:
    - period: daily
      time: "02:30"
      keep: 7
  timeouts:
    actionTimeout: 24h        # a slow upload may take a day
    unreachableTimeout: 30m   # ride out a rolling restart of the pods
    httpReceiveTimeout: 6h    # retention deletes from a slow remote storage
```

The API server refuses a value that is no Go duration; the operator refuses a resource with a value
out of its bounds as an invalid spec (`InvalidSpec`). A value out of its bounds in the operator
configuration is ignored, the built-in default standing for it.

## Retention

After every successful scheduled run, the run prunes the schedule's backups on the cluster it ran
on, newest first, as the cleanup of the Altinity usual backup workflow does:

* **Only the schedule's own backups count.** Retention looks at the backups named
  `<backupNamePrefix>-YYYYMMDDHHMMSS` - the scheduled ones, the time being their slot in `timeZone` -
  and `<backupNamePrefix>-YYYYMMDDTHHMMSS` - those that [run the schedule now](#run-a-schedule-now) -
  or, with a [naming template](#backup-naming), at every name the template makes of the shard of each
  host, with the schedule's prefix when it holds `{prefix}`, keyed by their time.
  Nothing else is ever touched: backups of other schedules, one-off backups, and any other name. Give
  each schedule its own prefix. The default prefix, `<namespace>-<chi>-<schedule>`, differs between
  the schedules of one Kubernetes cluster, even across CHIs that share a storage path, unless their
  names joined with dashes coincide; a prefix set by hand must be unique among all schedules writing to
  that path, or each schedule's retention deletes the other's backups (see [Storage paths](#storage-paths)).
  As the prefix names the series, it can be set once and is immutable after.
* **Every entry keeps its newest `keep` backups.** A scheduled backup counts for every entry whose
  slot its time is - the Sunday 02:30 backup of the example counts for the hourly, the daily and the
  weekly entry - and stays as long as any entry keeps it. The newest backup, and the one the run has
  just made, are always kept.
* **Run-now backups are kept while they are recent.** A backup that ran the schedule now stays as long
  as it is no older than the oldest scheduled backup some entry keeps, and goes after that.
* **Incomplete backups never count, and go once a complete one is newer.** A backup that is not
  listed complete on every host retention runs on - a run that failed on one host, a broken copy,
  a backup made before a host joined the cluster - never counts toward any `keep`, nor as the
  newest backup. An incomplete backup is either what a failure left or one still being written,
  which `system.backup_actions` tells apart: once it is older than the newest backup every host lists
  complete, and no host of its shard tells a `create_remote` or an `upload` of its name in progress,
  it is deleted, newest first among the others, on every host that holds it, as the Altinity usual
  backup workflow deletes every backup of a size, and so is one a host lists `broken`, whatever its
  size, as that is an upload that failed; a newer one, which may still be uploading, is left. When
  `system.backup_actions` of a host cannot be read, every incomplete backup is left that run, and a
  backup pinned by `keepBackups` is left as well (`OrchestratorRetentionIncompleteKept` event). A backup no host lists with a size above 0
  nor as broken, and one a host lists complete with size 0, is neither counted nor deleted: clean
  it up by hand. When a host cannot be listed, retention skips that run.
* **Incremental bases are rebased, not protected.** The operator forces
  `rebase_during_delete` and `rebase_before_remove_old_remote` on, so `clickhouse-backup` rebases a
  backup that another one is incremental to before it deletes it.
* **Per cluster, never during a restore.** Each cluster's runs prune on every host of that cluster the
  backup ran on - one per shard, or every replica with `AllReplicas`, as each keeps its own part of the
  backup - and on those only, see [Hosts that leave a schedule](#hosts-that-leave-a-schedule).
  Retention is skipped while a restore of the cluster has not finished, and a failed retention warns:
  it never fails the backup.

### Schedule changes

Retention keeps what the entries keep, so a change of the entries or of `timeZone` would make the next
run delete what the schedule kept until then: a shifted `time` matches no slot of the backups made
before, a removed weekly entry leaves its backups to the daily one, a lowered `keep` drops the
excess. The Altinity usual backup workflow asks first, in a dialog that lists what the change would
delete and lets you check what to keep. A schedule does the same declaratively, with `spec.retention`:

* **`onScheduleChange: Hold`, the default.** The operator records the entries and the zone before
  every change in `status.retention.previous`, under a `changeID` - the time of the change - and warns
  (`RetentionHeld` event). Every run then holds, of the backups made before each change, all those the
  schedule before it kept - the newest, the newest `keep` of every entry, the run-now backups in their
  window - whatever the entries are now, until each **expires** as the schedule before the change
  would have deleted it. The backups the schedule as it is would delete are listed in
  `status.clusters[].heldBackups` by the runs of each cluster, each with its `expires` (and an
  `OrchestratorRetentionHeld` event): that is the dialog of the Altinity usual backup workflow.
  Backups made after the change follow the entries as they are.
* **A held backup expires** at its time plus `keep` periods of the entry of the longest period
  that kept it, of the schedule before the change: a backup the weekly entry `keep: 4` kept expires
  4 weeks after it was made, one a monthly entry `keep: 2` kept 2 calendar months after; a run-now
  backup, or the newest one, by the entry of the longest period. When several changes hold a backup,
  the latest expiry wins. From then on retention treats it as any other backup - it deletes it unless
  the entries as they are or `keepBackups` keep it - and tells it (`OrchestratorRetentionHeldExpired`
  event). Each change records in `expires` when every backup it holds has expired, the latest `keep`
  periods of its entries after it; the operator then drops it from `status.retention.previous`
  (`RetentionHeldExpired` event), unless a pin without `until` may name a scheduled backup made
  before it, whose pin lasts as long as the hold.
* **`keepBackups`** pins backups, as the boxes checked in that dialog, and the manual backups of that
  workflow, do. Each pin is a `name` and an optional `until`, an RFC 3339 time: a pin with `until`
  keeps the backup until then, and the backup is treated as any other after; a pin without `until` of
  a backup an entry of a held change kept lasts as long as that hold, until the backup expires as
  above; a pin without `until` of any other backup - a run-now or a manual one - lasts as long as it
  is listed. Retention never keeps a backup for good without someone listing it so. A pin also keeps
  an incomplete backup.
* **`releaseHeld: <changeID>`** releases that change and every earlier one at once, before their
  backups expire: the next run deletes the backups none of the remaining changes, the entries and
  `keepBackups` keep. A later change is never released by an earlier `changeID`, so the field can stay
  in a manifest under GitOps.
* **`onScheduleChange: Prune`** holds nothing: the next run applies the change at once, as before.

```yaml
spec:
  schedules:
    - {period: daily, time: "03:00", keep: 7}   # was 02:00, with a weekly entry keep 4
  retention:
    keepBackups:
      - name: default-backup-demo-nightly-20261001020000
        until: "2027-01-01T00:00:00Z"          # kept until then
      - name: default-backup-demo-nightly-20261108020000   # held: kept until it expires, 2026-12-06T02:00:00Z
      - name: default-backup-demo-nightly-20261020T153000  # made by hand: kept while listed
    # Release the change before its backups expire:
    # releaseHeld: "2026-11-12T12:00:00Z"
status:
  retention:
    previous:
      - changeID: "2026-11-12T12:00:00Z"
        schedules:
          - {period: weekly, day: 0, time: "02:00", keep: 4}
          - {period: daily, time: "02:00", keep: 7}
        expires: "2026-12-10T12:00:00Z"
  clusters:
    - name: default
      heldBackups:
        - name: default-backup-demo-nightly-20261111020000
          expires: "2026-11-18T02:00:00Z"
        - name: default-backup-demo-nightly-20261101020000
          expires: "2026-11-29T02:00:00Z"
```

A change is a change of the period, the time, the day or the `keep` of an entry, of the entries, or
of `timeZone`; `incremental` is none. Up to 16 changes are kept; beyond that the oldest two are merged
into one that holds every backup made before the newer of them, until the later of their `expires`.
The operator starts recording on the first reconcile of a schedule: a change made before then is
not held. A backup [run now](#run-a-schedule-now) from the schedule
takes the pins and the held changes along, as its retention is that of a run of the schedule.

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
hosts of the cluster that a run has backed up: a host no run
reached - of a suspended schedule, or one added and removed again between two runs - holds no
backup of the schedule and is never recorded. With retention on, `status.clusters[].unprunedHosts`
lists the hosts that left `retentionHosts`, and an `UnprunedHosts` warning event names the hosts and
the backup name prefix when they leave. A host that rejoins, as a shard added back does, is pruned
again and drops out of `unprunedHosts`. The record is by host, so it also lists a host whose backups
another host still prunes on a shared storage path: the first replica of a shard removed while the
shard stays, for one, hands the shard's path to the next replica. A host that leaves while retention
is off is not recorded as unpruned, and retention turned on later prunes only the hosts the schedule
backs up by then. A cluster that leaves the schedule - removed, backup disabled - runs no more,
and its entry in `status.clusters` stays as long as it lists unpruned hosts; one with backup enabled
without a storage starts no run and keeps its entry. A schedule whose
`spec.cluster` is removed, has backup disabled or lacks a storage starts no run instead
(`ClusterNotFound`, `BackupDisabled`, `NoBackupStorage`) and prunes nothing until it runs again.

To clean up by hand, delete the backups you no longer need from the storage path the host used,
with its macros expanded, in the storage the `configSecret` of the cluster's entry configures, while no backup or
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
backed up no longer list. The [storage path check](#storage-paths) refuses such a shared object
disk path for an `AllReplicas` backup.

For shard `1` of cluster `main` of CHI `backup-demo` in namespace `default`, which the default paths
put at `backup/default/backup-demo/main/1` and `object_disks/default/backup-demo/main/1`:

```bash
kubectl exec chi-backup-demo-main-0-0-0 -c clickhouse-backup -- \
  clickhouse-backup --env S3_PATH=backup/default/backup-demo/main/1 list remote
# With object disks, add the object disk path of the shard:
# --env S3_OBJECT_DISK_PATH=object_disks/default/backup-demo/main/1
kubectl exec chi-backup-demo-main-0-0-0 -c clickhouse-backup -- \
  clickhouse-backup --env S3_PATH=backup/default/backup-demo/main/1 delete remote default-backup-demo-nightly-20261003023000

# Once done, clear the record of the cluster - here the first entry of status.clusters, see
#   kubectl get chbs nightly -o jsonpath='{.status.clusters[*].name}'
# The hosts are not recorded again
kubectl patch chbs nightly --subresource=status --type=json \
  -p '[{"op": "remove", "path": "/status/clusters/0/unprunedHosts"}]'
```

## Config overrides

The Altinity usual backup workflow makes a backup with `--env S3_STORAGE_CLASS=...` to keep, say, the monthly backups in a colder
storage class. `configOverrides` does the same, on each entry of a schedule and on a one-off
`ClickHouseBackup`:

```yaml
spec:
  clickHouseInstallation: prod
  schedules:
  - {period: daily,   time: "03:00", keep: 7, incremental: true}
  - {period: weekly,  time: "03:00", day: 0, keep: 4,  configOverrides: [S3_STORAGE_CLASS=STANDARD_IA]}
  - {period: monthly, time: "03:00", day: 1, keep: 12, configOverrides: [S3_STORAGE_CLASS=GLACIER_IR]}
# GCS: GCS_STORAGE_CLASS=NEARLINE
```

* **What an override is.** `KEY=VALUE`, the environment variable `clickhouse-backup` knows the
  setting by; a leading `--env=`, as that workflow writes it, is stripped. The key is upper-case letters,
  digits and `_`, the value letters, digits and `_ . , : / @ + = -`, so every override is one token of
  a command. At most 16, each key once.
* **Which apply.** A scheduled run takes the overrides of the heaviest entry of its slot, as it takes
  its `incremental`: the Sunday run of the example is weekly, `STANDARD_IA`, the 1st of the month
  monthly. A backup [run now](#run-a-schedule-now) from a schedule takes none, and `configOverrides`
  is not set with `scheduleRef`; a restore takes none, as the storage class matters only on upload.
* **How.** The backup runs as `create_remote ... --env=KEY=VALUE NAME` on every host, see
  [How the operator runs an operation](#how-the-operator-runs-an-operation).
* **Refused** - by the operator (`InvalidSpec`) and again by the run - are the settings the operator
  pins or sets in the sidecar (see [What the operator runs](#what-the-operator-runs)), every `API_*`
  setting, the credentials, those that tell where the backups are - `REMOTE_STORAGE`, bucket,
  endpoint, account, `*_PATH`, `*_OBJECT_DISK_PATH`, and the disk of embedded backups,
  `CLICKHOUSE_EMBEDDED_BACKUP_DISK` - which the listings, retention and restores of
  the backup, made with the sidecar's settings, would not find it by, and any setting of a secret
  (a key holding `PASSWORD`, `SECRET`, `ACCESS_KEY`, `TOKEN`, `CREDENTIAL`, `SAS`, ...): secrets belong
  in the configuration Secret.
* **A sidecar restart loses them.** A sidecar that restarts during the upload resumes it by itself,
  from what it keeps of the command, which leaves out the `--env` flags: the run follows the resumed
  upload, and the parts uploaded after the restart get the sidecar's settings.
* **Embedded backups.** `CLICKHOUSE_USE_EMBEDDED_BACKUP_RESTORE=true` is allowed: the
  [storage path check](#storage-paths) of a backup, or of a schedule, checks the paths as every one
  of its overrides would have the backup made.
* **Rebased parts.** Retention rebases a backup another one is incremental to before it deletes it,
  copying parts with the sidecar's settings: a rebased part takes the sidecar's storage class.
  Archive classes (`GLACIER`, `DEEP_ARCHIVE`) make listings, retention and restores read objects
  that `clickhouse-backup` must first restore; prefer instant-retrieval classes.

## Incremental backups

`diffFromRemote: <existing backup>` on a `ClickHouseBackup` uploads only the data parts that are
not already in that backup. Each cluster's backup builds on that cluster's copy of the base, so the
base must exist on every cluster the backup targets. On a schedule, the runs of the one entry with
`incremental: true` make incremental backups on the newest backup of the series as long as the chain
of incremental backups is shorter than that entry's `keep`; the run after makes a full backup again.
The series takes the backups [run now](#run-a-schedule-now) from the schedule too, ordered by the time
they are named after, as the Altinity usual backup workflow bases a backup on the newest backup
whether made by hand or on schedule.
Restoring an incremental backup needs its whole chain of bases in the storage. Retention rebases a
base that a kept backup still needs before it deletes it, but
deleting bases by other means (`BACKUPS_TO_KEEP_REMOTE`, a bucket lifecycle rule, manual removal)
breaks every backup built on them.

## Restore safety

A restore is destructive, so each run checks **before** it changes anything:

1. **Storage paths** of every backup-enabled cluster of the CHI, see [Storage paths](#storage-paths).
2. **Backup present everywhere** (`validateTopology: true`, the default): every target host must hold
   the backup. This catches a backup taken from a different layout or a wrong storage path before
   anything is half-restored.

It then downloads the backup to every host before it restores anything: a host that cannot hold it
fails the restore while the cluster is as it was. `clickhouse-backup` then drops the tables of the
backup that exist, and with `overwrite: true` and no `tables` the whole databases of the backup, see
[ClickHouseRestore](#clickhouserestore). The recommended path
is restoring into a fresh, empty CHI and switching clients over once it is checked.

Do not change the target CHI while a restore runs. A change that rolls its Pods restarts the
sidecars, which interrupts the restore on the restarted host and leaves the cluster partly restored;
the restore then has to be repeated. Do not
change the storage Secret either: the sidecars take a change with their next operation, also while
the restore works on other hosts, which leaves the restore applied with the old settings on some
hosts and the new ones on others (see [Secret changes](#secret-changes)).

## One operation at a time per cluster

Backups, scheduled runs and restores of the same cluster do not run concurrently, while those of
different clusters of a CHI do. The operator serializes the runs per cluster - a resource waiting
for its turn on a cluster reports `OperationInProgress` - and each run additionally waits until the
sidecars it uses are idle, so overlapping runs queue instead of failing. A resource that targets
several clusters starts the run of each cluster as soon as that cluster is free.

A restore also holds the schedules of the clusters it restores: every schedule stops starting runs
on such a cluster (`SuspendedForRestore`) as soon as the restore has passed validation, while it
keeps running on the other clusters; a restore of the whole CHI holds them all, and one that
restores access entities holds no more than its clusters, see below. The restore records its validation first and starts its runs
on a later pass (`WaitingForSchedules` until then), under the lock of each cluster the schedules
start their runs under, so no scheduled backup starts in the middle of it. A scheduled run that
started before the hold is waited for (`OperationInProgress`). The schedules resume when the restore completes - or, while their CHI
reconciles, once that reconcile completes.

A restore that reads the backup of another CHI of its namespace (`sourceInstallation`, see
[Restoring into another CHI](#storage-paths)) holds the schedules of that CHI's clusters it reads as
well, each until it finishes there: the retention of a run there could delete the backup it reads.
It waits for a scheduled run there that started before the hold the same way; the hold applies while that CHI reconciles too, which the restore does not wait for. While the
restore waits for its own CHI to exist, complete its reconcile or start again, it holds nothing
there, as that may never come, and holds them again before it starts. That CHI's runs missed
meanwhile are not all made up: a schedule held no more starts the latest slot it missed at once,
unless that is past its `startingDeadlineSeconds`, and skips the others, so restore from another
CHI outside its backup window. A restore of another namespace's backup (`sourceNamespace`) holds
nothing there - a restore of one namespace never stops the backups of another - so suspend the
schedules of that CHI for the restore, or restore a backup their retention keeps.

A restore that restores access entities - one with `rbac: true`, and every recovery restore -
restores them, and the user-defined functions, with the schema of every host of the clusters it
restores. So that no backup captures them half restored, until it has finished the backups of the
clusters it restores wait for it (`OperationInProgress`, naming the cluster and the restore) - of
every cluster for a restore of the whole CHI, of its own cluster for a recovery restore - while those
of the other clusters go on; its schedule holds are those of any restore, see above. Like any
restore, it starts the run of each of its clusters once no other operation runs on that cluster, so
the recovery restores of a CHI's clusters run at the same time. Access entities in a `replicated`
user directory that several clusters share through one Keeper path are not tracked across clusters:
a backup or restore of another cluster may read or write them while a recovery restore of one
cluster rewrites them.

A restore that fails after it changed something may have left a cluster partly restored, which must
not be backed up as if it were the CHI's data, possibly pushing good backups out of retention: it
keeps holding the schedules of its own CHI it held (`SuspendedForFailedRestore`, announced with a
warning event) until you delete the failed `ClickHouseRestore`, which the schedule's `Ready` message
names; those of the CHI whose backup it read resume as it fails - or, while that CHI reconciles,
once that reconcile completes - as nothing changed there. A restore whose run failed before it
changed anything - `validateTopology` found the backup missing, no ClickHouse answered, a download
failed - reports `FailedUnchanged` and holds nothing. A CHI waiting to be [recovered from a
backup](#recover-a-new-chi-from-a-backup) holds the schedules of the clusters it recovers as well,
and a CHI recovered, or to be, from a backup whose name starts with its own `<namespace>-<chi>-`
holds every schedule of it that uses the default prefix, whatever its restores do
(`SharesSourcePrefix`, see [Storage paths](#storage-paths)).

## Rerunning with spec.taskID

A `ClickHouseBackup` or a `ClickHouseRestore` runs once: once it finished - `Completed`, `Failed` or
`Aborted` - nothing runs it again. `spec.taskID`, as on a `ClickHouseInstallation`, asks for another run:
set it to a value the status does not record yet (`status.taskID`, `status.taskIDsStarted`,
`status.taskIDsCompleted`, newest first, 10 at most each). A value the status records does nothing.

```yaml
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseBackup
metadata:
  name: before-upgrade
spec:
  clickHouseInstallation: prod
  taskID: "2"   # any new value reruns the backup
```

`kubectl patch chb before-upgrade --type=merge -p '{"spec":{"taskID":"3"}}'` does the same in place.

* **A completed backup** makes a new backup, named anew at the time of the rerun
  (`status.rerunTime`, which the name, the naming template included, and `startingDeadlineSeconds`
  count from), on every cluster, validated again as a new backup is. The backup it made stays in the
  storage, which [retention](#retention) or a delete by hand removes; `status.previousBackupNames`
  records it. A backup named by `spec.backupName` cannot make a new backup under the same name: change
  `spec.backupName` together with `spec.taskID`, or the taskID is ignored with a `TaskIDIgnored` warning.
  The backup is unfinished again, so it gets its [finalizer](#deleting-a-backup-restore-or-schedule)
  back.
* **A backup that failed or was aborted** makes its backup again under its name, on the clusters it
  did not complete on; the clusters it completed on keep their backup. A failed run deletes the local
  copy it left, and the remote one an upload left broken stays, so before the rerun creates the backup
  on a host, it deletes the copies of it - local and remote - that an earlier run of this backup left
  there (`OrchestratorRerunReplaced`): clickhouse-backup makes no backup of a name its storage lists,
  and could resume none whose local copy and resumable state are gone. Only a copy an earlier run
  of this backup inserted a `create_remote` of on that host is deleted, as `status.previousRuns` records
  them; a copy of the name nothing of this backup made refuses the backup as ever. A delete waits for
  the sidecar to finish what it runs, an upload of the earlier run that outlived its action timeout
  included. A backup refused before it targeted any cluster is validated again.
* **A restore that failed or was aborted** runs again on the clusters it did not complete on,
  resuming the downloads it made there: a failed restore keeps what it downloaded, and
  `download --hardlink-exists-files` resumes it. `status.previousRuns` keeps the downloads of the
  earlier runs, so that a restore from [another installation](#storage-paths) does not refuse the local
  copy as one it did not download. With `validateTopology` such a restore checks, on a host that holds
  the local copy an earlier run of it downloaded, that copy in place of the copy of the schema it
  downloads elsewhere - which would be downloaded over it and deleted with it - and keeps it: an
  incomplete one, whose earlier run found the backup before it downloaded, is checked by its download,
  a complete one as any copy of the schema. A local copy nothing of this restore downloaded is refused
  as ever. `spec.resumeDownload: false` has the rerun start over instead, for a restore of the own
  backups as from another CHI: before its preflight it deletes on each host the local copy of the
  backup an earlier run downloaded (`delete local <name>`, step `replaceLocal`, an
  `OrchestratorRerunReplaced` event), complete or not, and downloads the backup again. The
  `OrchestratorHostStepSucceeded` event of a download tells whether it resumed the local copy of its
  host or replaced the one of an earlier run. **A completed restore** is not rerun: a restore again drops and
  recreates what it restored, which a new `ClickHouseRestore` asks for explicitly; the taskID is
  ignored with a `TaskIDIgnored` warning.
* **A schedule** (`ClickHouseBackupSchedule`) that is `Aborted` is reset: its `Validated` and `Ready`
  conditions and its phase are dropped, so that it is validated again at once and its phase told anew
  (`TaskIDReset`) - `Active` once the cause is fixed, `Aborted` again with a fresh `ScheduleAborted`
  warning otherwise. A schedule aborted as its last run failed stays so until a run succeeds. The reset
  runs no slot; a taskID of a schedule that is not aborted is only recorded.

A rerun is announced by a `TaskIDRerun` event. A taskID set while the backup or the restore has not
finished - pending or running - reruns nothing, then or later: it is recorded in `taskIDsCompleted` at
once with a `TaskIDIgnored` warning, as the outcome of the run it would follow is not known yet; set
another one once the resource finished. The taskID a resource is created with is the one of its first
run.

## Deleting a backup, restore or schedule

The operator sets a finalizer on every `ClickHouseBackup` that has not completed
(`finalizer.clickhousebackup.altinity.com`), every `ClickHouseRestore` that has not finished
(`finalizer.clickhouserestore.altinity.com`) and every `ClickHouseBackupSchedule`
(`finalizer.clickhousebackupschedule.altinity.com`), so that deleting one while it works settles what
it leaves before it goes. A completed backup that a new `spec.taskID` [reruns](#rerunning-with-spectaskid)
gets its finalizer again. Only the leading operator acts on a deletion, as on everything else; the
operator's role grants `update` and `patch` on the three kinds for that.

* **A backup that completed** keeps its remote backup, which belongs to the
  [retention](#retention) of its schedule or to you: the resource goes at once, with a
  `DeletionRemoteBackupKept` event. Delete the backup itself by hand with
  `INSERT INTO system.backup_actions(command) VALUES ('delete remote <backup>')`.
* **A backup that did not complete** - pending, running, failed or partly completed - is stopped and
  deleted. Its runs stop first (`DeletionStoppingRuns`); then, on every cluster it did not complete
  on and every host it sent a command to (`DeletionCleanup`), the operator kills the command of that
  backup the sidecar still runs - only one whose text names the backup, by
  `kill "<command>"`, never a command of anything else - and deletes the local copy of the backup and
  its remote copy, the latter once per shard, each only where the sidecar lists it
  (`OrchestratorDeletionKilled`, `OrchestratorDeletionDeleted`). The clusters it completed on keep
  their backup. `DeletionCleanedUp` tells it is done, and the resource goes. A backup that sent no
  command - one refused in its preflight, as for a backup of its name that the storage holds already
  - deletes nothing, above all not the backup that was there before it: `DeletionCleanedUp` tells
  that it sent no command to any sidecar. A run that was cut before it recorded its hosts is
  cleaned up on the hosts it inserted a command into.

  Every step is bounded by the [timeouts](#timeouts) of the backup: a host that does not answer is
  retried for `unreachableTimeout`, after which every host left is tried once; a kill or a delete is
  sent once, never to a sidecar busy with another command, and its row read `errorResponsesMax` times
  at most. What cannot be deleted that way is left, told in an `OrchestratorDeletionLeft` warning per
  host and a `DeletionLeftover` warning naming every leftover, with the SQL that deletes it by hand
  (`INSERT INTO system.backup_actions(command) VALUES ('delete local <backup>')` and
  `('delete remote <backup>')` on the host) - and the resource goes all the same: a deletion is never
  blocked for good. A CHI that is being deleted, its namespace say, has every host tried once, never
  waited for; one that is gone, stopped or without the cluster, or an operator without ClickHouse
  credentials, gives up at once, telling what is left the same way.
* **A restore** is never killed, nor does its deletion delete what it downloaded, as a failed restore
  keeps it: a cluster it has not started on never starts, and a run in progress is waited for, with a
  `DeletionWaitingForRun` event, until it finishes, its outcome recorded; then the resource goes
  (`DeletionRunFinished`).
* **A schedule** starts no run from the moment it is deleted; the runs in progress, retention
  included, are waited for the same way (`DeletionWaitingForRun`, then `DeletionRunFinished`), their
  outcomes recorded, and the resource goes. The backups the schedule made stay in the storage.

A finalizer needs the operator to remove it: with the operator uninstalled, or not watching the
namespace any longer, a resource it set one on stays `Terminating`. Remove the finalizer by hand
then, `kubectl patch chb <name> --type=merge -p '{"metadata":{"finalizers":null}}'` (`chr`, `chbs`
alike), after you settled what the resource leaves yourself.

## Recover a new CHI from a backup

Name the backup a new CHI starts from in its `spec.backup.recovery`:

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
restore restores access entities, while one runs nothing backs up its cluster; the recovery
restores of different clusters run at the same time, see [One operation at a time per
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
    storageConfig:
      valueFrom:
        secretKeyRef:
          name: backup-demo-storage
          key: config.yml
    recovery:
      backupName: default-backup-demo-nightly-20261003023000
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
the backup: `clickhouse-backup` drops and recreates a table of the backup the CHI already holds - one
an init script creates, say - and leaves every other table alone. Each recovered cluster needs the cluster name and shard layout it had in the source, and its
storage Secret must point at the same location and exist before the CHI is applied: the
restore starts right after the first reconcile, before the sidecars would pick up a Secret created
later.

The operator announces the recovery with events on the CHI, `Started` when it creates a recovery
restore and `Completed` when it records the recovery. Problems are reported as warning events on
the CHI: a backup name that is not valid (`InvalidSpec`), a CHI that cannot run the restore
(`CHIStopped`, `BackupDisabled`, `NoTargetHosts`), a name conflict (`NameConflict`, see below), and a
failed restore, with its reason and the error the sidecars reported - a backup missing in the
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

A recovery restore that failed before it changed anything (`FailedUnchanged`) releases the
schedules it held, which back up the CHI as it is - but for those the prefix guard holds
(`SharesSourcePrefix`, see [Storage paths](#storage-paths)), which stay held. One that failed after
changing something keeps them all held. Either way
the CHI stays unrecorded:

* to try again, fix the cause - the backup name in `recovery`, the storage Secret (see
  [Secret changes](#secret-changes) on when a changed one takes effect), ... - and delete the failed
  `<chi>-<cluster>-recovery` restore: the operator creates a new one. The new restore drops
  and recreates the tables of the backup the failed one left. The failed one keeps the local copies
  it downloaded, which the new one resumes the download of - but one from another CHI, which the new
  one refuses: delete those with `INSERT INTO system.backup_actions(command) VALUES ('delete local
  <backup>')` on the hosts the `OrchestratorRestoreDownloadsKept` warning names;
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

* `phase` (backups and restores): `Pending`, `Running`, `Completed`, `Failed` or `Aborted`, over all
  their clusters - `Completed` once every cluster has completed; `Failed` once every cluster has
  finished and the run of any of them failed, or it completed on some; `Aborted` once every cluster
  has finished and the operator stopped the operation on every one that did not complete - an invalid
  spec, the CHI or cluster missing or stopped, backup disabled, a starting deadline exceeded;
  `Pending` or `Running` until then. A cluster's own `phase` follows the same rule;
* `phase`, `reason`, `message`, `nextScheduleTime` and `entries` (schedules): `Active`, `Suspended` or
  `Aborted`, see [ClickHouseBackupSchedule](#clickhousebackupschedule);
* `clusters` (backups and restores): one entry per cluster, fixed when the resource is validated,
  with the cluster's `phase`, `hosts`, `configSecret` - the Secret of the `storageConfig` of the storage
  the run keeps the backup in, or reads it from - `startTime` and `completionTime`, the `reason` and
  `message` of its outcome, and `run`, the state of its run, see
  [How the operator runs an operation](#how-the-operator-runs-an-operation);
* `cluster` (restores): the `spec.cluster` the restore was validated with, empty for a restore of
  every backup-enabled cluster; which schedules the restore holds, and whether it restores access
  entities, follow it, whatever the spec names later;
* `startTime`, `completionTime`, `durationSeconds`, and `backupName`: the backup made, or restored;
* schedules report per cluster in `clusters` - `configSecret`, `lastScheduleTime`,
  `lastSuccessfulTime`, `lastBackupName`, `lastFailureTime`, `lastFailureMessage`, `retentionHosts`,
  `unprunedHosts` and `heldBackups` - their runs in `active` and `recentRuns`, and over all clusters
  `lastSuccessfulTime` and `lastBackupName`, see
  [ClickHouseBackupSchedule](#clickhousebackupschedule);
* `conditions`:

| Condition             | Meaning                                                                                               |
|-----------------------|-------------------------------------------------------------------------------------------------------|
| `Validated`           | The spec and the target CHI passed validation.                                                        |
| `Started`             | Backups and restores: the run of every cluster has started.                                           |
| `Ready`               | The operation finished successfully on every cluster, or the schedule is active and its runs succeed. |
| `LastBackupSucceeded` | Schedules: the last finished runs succeeded.                                                          |

The condition `reason` says why, with the details in `message`:

| Reason                                               | Meaning                                                                                                                                                                                                                                                                                          |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Validated`                                          | The spec and the target CHI passed validation.                                                                                                                                                                                                                                                   |
| `CHIRecovered`                                       | Schedules: as `Validated`, for a CHI restored from a backup, which may share its storage path with the CHI the backup came from.                                                                                                                                                                 |
| `SharesSourcePrefix`                                 | Schedules: held, the CHI being recovered from a backup starting with `<namespace>-<chi>-`, until `backupNamePrefix` is set, see [Storage paths](#storage-paths).                                                                                                                                 |
| `InvalidSpec`                                        | The spec failed validation, or that of the schedule a backup [runs](#run-a-schedule-now).                                                                                                                                                                                                        |
| `WaitingForScheduledTime`                            | Backups: the backup waits for its `scheduledTime`, see [Back up at a later time](#back-up-at-a-later-time).                                                                                                                                                                                      |
| `StartingDeadlineExceeded`                           | Backups: the backup, or the run of a cluster, did not start within `startingDeadlineSeconds` of the time it was due; the message tells what it waited for. |
| `ScheduleNotFound`                                   | Backups that [run a schedule now](#run-a-schedule-now): the schedule does not exist (yet); the backup waits.                                                                                                                                                                                     |
| `CHINotFound`, `CHINotReady`, `CHIStopped`           | The CHI does not exist, has not completed its reconcile, or is stopped; the resource waits. |
| `NoClickHouseAccess`                                 | The operator configuration sets no ClickHouse username and password of its own (`clickhouse.access`, or its Secret) to drive the sidecars with, the defaults not counting; the resource waits, see [How the operator runs an operation](#how-the-operator-runs-an-operation). |
| `ClusterNotFound`, `BackupDisabled`, `NoTargetHosts` | The CHI cannot run the operation as configured, see [Which hosts are involved](#which-hosts-are-involved); for a backup that [runs a schedule](#run-a-schedule-now), also a `cluster` the schedule does not back up.                                                                             |
| `NoBackupStorage`                                    | The target cluster has backup enabled without `storageConfig`, see [Enabling backup](#enabling-backup). Schedules of every cluster: as `Validated`, leaving such clusters out, which run no more. |
| `OperationInProgress`                                | Another operation of the cluster runs, or, for a backup, a restore of access entities of its cluster, or of every cluster, is unfinished; a restore of access entities waits for every cluster; the resource waits its turn; a restore also waits for a scheduled run of the CHI whose backup it reads.                                                  |
| `WaitingForGarbageCollection`                        | Recovery: a `ClickHouseRestore` of the name a recovery restore needs belongs to a deleted owner, or is being deleted; the recovery of the cluster waits until it is gone. |
| `NameConflict`                                       | Recovery: the name of a recovery restore is taken by another restore, see [Recover a new CHI from a backup](#recover-a-new-chi-from-a-backup). |
| `WaitingForSchedules`                                | A restore waits until every schedule it holds - of the clusters it restores, and of the clusters of the CHI whose backup it reads - holds its runs.                                                                                                     |
| `Started`                                            | The runs of the operation have started. |
| `Completed`                                          | The operation finished successfully.                                                                                                                                                                                                                                                             |
| `CompletedWithoutData`                               | A backup finished successfully, but holds no data on any host, see [How the operator runs an operation](#how-the-operator-runs-an-operation). |
| `RBACNotRestored`                                    | Restores: finished successfully, but left out access entities the backup holds, see [Access entities](#access-entities).                                                                                                                                                                         |
| `PartiallyCompleted`                                 | Backups and restores: failed on some clusters and completed on the others; `status.clusters` tells which.                                                                                                                                                                                        |
| `Failed`                                             | A run failed; the message carries the error the sidecars reported. |
| `FailedUnchanged`                                    | Restores: a run failed before it changed anything; the message carries the error. Holds no schedule. |
| `DeadlineExceeded`                                   | A run took longer than `activeDeadlineSeconds` from its start. |
| `Scheduled`, `Suspended`, `SuspendedForRestore`      | Schedules: the schedule runs, is suspended by `spec.suspend`, or is held for a restore of its cluster, or one that reads its backup, see above - or, once no restore holds it any longer, kept from running until the CHI completes the reconcile it is in. |
| `SuspendedForFailedRestore`                          | Schedules: held by a failed restore, which may have left their cluster partly restored, until that restore is deleted.                                                                                                                                                                           |
| `LastBackupFailed`                                   | Schedules: the last finished run of a cluster failed; the schedule keeps running. |
| `LastBackupWithoutData`                              | Schedules: the last finished run succeeded, but its backup holds no data; the schedule keeps running. |

```bash
kubectl get chb,chbs,chr
kubectl describe chb backup-demo-once             # conditions and events
kubectl get chb backup-demo-once \
  -o jsonpath='{range .status.clusters[*]}{.name}{"\t"}{.phase}{"\t"}{.run.step}{"\n"}{end}'
# The commands the run of a cluster sent, and how each ended
kubectl get chb backup-demo-once -o jsonpath='{.status.clusters[0].run.actions}'
kubectl get chbs nightly -o jsonpath='{.status.recentRuns}'
# On a host: what its sidecar ran
kubectl exec chi-backup-demo-main-0-0-0 -c clickhouse -- clickhouse-client -q \
  "SELECT command, status, start, finish, error FROM system.backup_actions ORDER BY start DESC LIMIT 10"
```

The operator also emits Kubernetes Events on the resources on every change of the phase of a backup
or restore - `BackupPending`, `BackupRunning`, `BackupCompleted`, `BackupFailed`, `BackupAborted`, and
`Restore...` alike, a warning for `Failed` and `Aborted`, naming the reason and the message of the
outcome, for a failed run the end of the error - and of a schedule (see
[ClickHouseBackupSchedule](#clickhousebackupschedule)), when the operation of a cluster starts,
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
`clickhouse_operator_backup_duration_seconds`,
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
  The operator does not call the port: the integration tables call the sidecar at `127.0.0.1` inside
  the Pod. Refuse port 7171 from everywhere else with a NetworkPolicy such as the one below. A
  NetworkPolicy that selects the ClickHouse Pods refuses every connection it does not allow, so it
  must keep all other ports of the CHI open as before. Every ClickHouse user who can read the
  definition of the integration tables, `SHOW CREATE TABLE system.backup_actions`, sees the API
  credentials, and every user who may insert into `system.backup_actions` runs commands in the
  sidecar: grant neither beyond the operator's user and the backup user.

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
      # The backup API, port 7171, is not listed: nothing outside the Pod reaches it.
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
* **The operator checks no existing table before a restore.** `clickhouse-backup` drops and recreates
  the tables of the backup that exist, `overwrite` without `tables` the whole databases of the backup.
  It lifts `max_table_size_to_drop` for each drop on its own host with the `flags/force_drop_table`
  flag; the hosts an `ON CLUSTER` drop reaches do not get the flag, see
  [ClickHouseRestore](#clickhouserestore).
* **Layout must match.** The target clusters need the names of the source clusters, and restoring
  into fewer shards than the backup has leaves the extra shards' data out; `validateTopology` only
  checks that every target shard holds the backup.
* **Access entities need ClickHouse 23.3.** Restoring them relies on `SYSTEM RELOAD USERS`; on an
  older ClickHouse the restore completes, but ClickHouse loads them only at its next restart, see
  [Access entities](#access-entities).
* **Several backup-enabled clusters need settings the operator can see.** In such a CHI, a run
  refuses to go on when the storage paths of its cluster cannot be checked - a configuration file named
  on the sidecar's command line, an `envFrom` of the sidecar's container, a macro the check does not
  know, see [Storage paths](#storage-paths).
* **Do not change a CHI while it is being restored.** A rolling update restarts the sidecars and
  leaves a partly applied restore, see [Restore safety](#restore-safety).
* **A storage Secret change reaches the sidecars operation by operation.** It takes a few minutes,
  and an operation running on other hosts meanwhile is left with the old settings on some hosts and
  the new ones on others, see [Secret changes](#secret-changes).
* **New REST API credentials need a sidecar restart.** After `<chi-name>-auto-backup-secret` is
  deleted and recreated, the sidecars keep accepting only the old credentials until they are
  restarted by hand, see [Secret changes](#secret-changes).
* **Targets and a schedule's prefix are immutable on Kubernetes 1.25 and newer only.** Older
  versions, and the `v1beta1` bundle, accept an edit of `clickHouseInstallation` or `cluster`, and a
  change of a schedule's `backupNamePrefix`, which leaves the backups of the former series unpruned,
  as setting one on a schedule that has run does on any version. A backup or a restore keeps the
  cluster it recorded when it was validated, but an edit of `clickHouseInstallation` moves it, as a
  whole, to the other CHI; a schedule, which records no target, follows either edit, as if the
  clusters it no longer targets had left it (see
  [Hosts that leave a schedule](#hosts-that-leave-a-schedule)).
* **Hardening and FIPS settings do not extend to backup traffic.** `security.policy: Enforced` and the
  FIPS settings leave the sidecar REST API plain HTTP with basic auth, between the integration
  tables and the sidecar of each Pod, and a sidecar of a host that serves TCP over TLS only connects to its ClickHouse over
  `localhost` without verifying the certificate, which is issued for the host's names.
* **FIPS image policy.** With `security.images.policy: FIPSRequired` in the operator configuration,
  the sidecar image of every host of a backup-enabled cluster must carry
  `fips` in its tag like the ClickHouse image, otherwise the CHI is aborted with
  `[FIPSImagePolicyViolation]`. The default image is not a FIPS build; set the image of the
  sidecar's container in its pod template to a FIPS build of `clickhouse-backup`,
  such as `altinity/clickhouse-backup:<version>-fips`. See
  [security_hardening_fips.md](security_hardening_fips.md).
* **A deleted operation leaves its command running.** Deleting a backup or restore, or stopping the
  operator, stops its run, not the command a sidecar runs already: that one goes on there, and the
  next operation on the host waits for it to end.
* **The operator needs ClickHouse credentials.** Backups and restores run with the operator's own
  `clickhouse.access` credentials, never the defaults; without a username and a password the
  configuration sets, inline or by its Secret, none starts (`NoClickHouseAccess`).
* **Backups outlive their resources.** Deleting a `ClickHouseBackup` or a schedule does not
  delete anything in the storage; only schedule retention does.
* **Retention prunes only the hosts a schedule backs up.** Once a host leaves them - a removed shard,
  a replica removed under `AllReplicas`, a cluster removed or no longer backed up - nothing prunes
  the backups it kept on a path of its own, as they may be needed to restore the old layout.
  The schedule lists such hosts in `status.clusters[].unprunedHosts` and warns with an
  `UnprunedHosts` event; delete their backups by hand, see
  [Hosts that leave a schedule](#hosts-that-leave-a-schedule).
* **ClickHouse Keeper installations (`ClickHouseKeeperInstallation`) are not covered.**
