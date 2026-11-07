// Copyright 2019 Altinity Ltd and/or its affiliates. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chbackup

import (
	_ "embed"
	"strconv"

	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
)

// Environment of the backup Job container. jobScript reads every input from these variables,
// so no user-provided value is ever part of the script text.
const (
	// envOperation selects what the Job does: operationBackup, operationVerify or operationRestore
	envOperation = "OPERATION"
	// envAPIPort is the port of the clickhouse-backup REST API on every target host
	envAPIPort = "API_PORT"
	// envAPIUsername and envAPIPassword are the REST API credentials, taken from the backup Secret
	envAPIUsername = config.ClickHouseBackupSecretKeyAPIUsername
	envAPIPassword = config.ClickHouseBackupSecretKeyAPIPassword
	// envPollInterval is the number of seconds between status polls, 5 when empty
	envPollInterval = "POLL_INTERVAL"
	// envTerminationMessagePath is the file the script writes why it failed to, or envNoDataMessage
	// when it succeeded with a backup that holds no data, or envRBACNotRestoredMessage when it succeeded
	// with a restore that left out the access entities of its backup: the termination message path of
	// its container
	envTerminationMessagePath = "TERMINATION_MESSAGE_PATH"
	// envUnchangedExitCode is the exit code the script fails with when it has changed nothing
	envUnchangedExitCode = "UNCHANGED_EXIT_CODE"

	// envCluster is the name of the cluster the Job targets, of which every host list holds hosts: a
	// single token, as the installation normalizes cluster names
	envCluster = "CLUSTER"
	// envBackupName is the backup to create, verify or restore
	envBackupName = "BACKUP_NAME"
	// envBackupNamePrefix names a scheduled backup "<prefix>-<UTC timestamp>" when envBackupName is empty,
	// and scopes retention to the backups named that way
	envBackupNamePrefix = "BACKUP_NAME_PREFIX"
	// envCronJobName is the name of the CronJob of a scheduled backup, which the names of its Jobs start with
	envCronJobName = "CRON_JOB_NAME"
	// envJobName is the name of the Job, from the Job name label of its pod: a scheduled run takes the
	// timestamp of its backup name from it
	envJobName = "JOB_NAME"
	// envBackupHosts lists the space-separated hosts create_remote runs on
	envBackupHosts = "BACKUP_HOSTS"
	// envRetentionHosts lists the space-separated hosts whose backups retention prunes
	envRetentionHosts = "RETENTION_HOSTS"
	// envSchemaOnly restricts a backup or a restore to the schema, "true" or "false"
	envSchemaOnly = "SCHEMA_ONLY"
	// envTables is the clickhouse-backup table pattern of a backup or a restore
	envTables = "TABLES"
	// envPartitions is the comma-separated list of partitions of a backup
	envPartitions = "PARTITIONS"
	// envDiffFromRemote names the remote base backup of an incremental backup
	envDiffFromRemote = "DIFF_FROM_REMOTE"
	// envKeepLast is how many of the most recent scheduled backups retention keeps of
	// those that hold data, and apart from them of those that hold none; 0 keeps all
	envKeepLast = "KEEP_LAST"
	// envNoDataMessage is the termination message the script leaves when the backup it made holds no
	// data on any host
	envNoDataMessage = "NO_DATA_MESSAGE"

	// envVerifyHosts lists the space-separated hosts a backup is downloaded to for verification
	envVerifyHosts = "VERIFY_HOSTS"
	// envSchemaHosts lists the space-separated hosts the schema is restored on: every replica, as each
	// restores it on its own, so that no ON CLUSTER DDL rewrites a table
	envSchemaHosts = "SCHEMA_HOSTS"
	// envDataHosts lists the space-separated hosts the data is restored on
	envDataHosts = "DATA_HOSTS"
	// envGuardHosts lists the space-separated hosts whose tables the overwrite guard checks: every
	// replica, as the schema restore drops the tables of the backup on each of them
	envGuardHosts = "GUARD_HOSTS"
	// envOverwrite allows a restore over existing tables, "true" or "false"
	envOverwrite = "OVERWRITE"
	// envReplicated tells whether the installation configures ZooKeeper for the cluster, "true" or "false",
	// empty taken as "true". An overwrite drops the tables of the backup ON CLUSTER whenever it drops a table
	// of a Replicated engine, as ZooKeeper may come from a configuration file the installation does not show:
	// this decides only when the engines of the tables cannot be told, failing an overwrite without ZooKeeper
	envReplicated = "REPLICATED"
	// envMaxTableSizeToDrop is the max_table_size_to_drop of the cluster in bytes, the smallest non-zero one of its
	// hosts, 0 for no limit. An overwrite fails before it changes anything when its drop would stop half way: at a
	// larger table on any host but the one that runs the drop ON CLUSTER, or at the second larger table of a database
	// it drops as a whole on a host that drops that database on its own
	envMaxTableSizeToDrop = "MAX_TABLE_SIZE_TO_DROP"
	// envValidateTopology requires the backup in the storage of every restored host, "true" or "false"
	envValidateTopology = "VALIDATE_TOPOLOGY"
	// envRestoreRBAC restores the access entities of the backup on every schema host once its schema and
	// data are restored, "true" or "false"
	envRestoreRBAC = "RESTORE_RBAC"
	// envRBACNotRestoredMessage is the termination message the script leaves when the restore it made
	// succeeded and did not restore the access entities the backup holds on any host
	envRBACNotRestoredMessage = "RBAC_NOT_RESTORED_MESSAGE"
	// envSourceInstallation and envSourceNamespace name the installation, and its namespace, whose storage
	// paths a restore reads the backup from: the values of the {installation} and {namespace} macros there.
	// Unset or empty, the restore reads the paths of its own installation for either
	envSourceInstallation = "SOURCE_INSTALLATION"
	envSourceNamespace    = "SOURCE_NAMESPACE"
)

// Environment of the storage path check, which the script runs before a backup or a restore changes
// anything: every shard of every backup-enabled cluster of the installation needs a storage path of
// its own. The settings of each cluster are named by its index: pathClusterEnv(index, name).
const (
	// envPathClusters lists the space-separated indexes, from 0, of every backup-enabled cluster of
	// the installation
	envPathClusters = "PATH_CLUSTERS"
	// envPathSelf is the index of the cluster the Job targets
	envPathSelf = "PATH_SELF"
	// envPathInstallation is the value of the {installation} macro: the name of the installation
	envPathInstallation = "PATH_INSTALLATION"
	// envPathNamespace is the value of the {namespace} macro: the namespace of the installation
	envPathNamespace = "PATH_NAMESPACE"
	// envPathReplicaSelection is the api.ReplicaSelection of a backup. A backup of every replica needs
	// the {replica} macro in the path, one of the first replica of each shard must not have it
	envPathReplicaSelection = "PATH_REPLICA_SELECTION"
	// envDefaultPath and envDefaultObjectDiskPath are the paths the configuration file of a sidecar takes when
	// the storage settings leave them unset; when absent, an unset path is the root of the storage. The
	// pathClusterEnv of either, when set, takes its place for that cluster
	envDefaultPath           = "DEFAULT_PATH"
	envDefaultObjectDiskPath = "DEFAULT_OBJECT_DISK_PATH"

	// pathEnvCluster is the pathClusterEnv of the cluster name, the value of its {cluster} macro
	pathEnvCluster = "CLUSTER"
	// pathEnvShards is the pathClusterEnv of the space-separated values of the {shard} macro of the
	// cluster, in shard order
	pathEnvShards = "SHARDS"
	// pathEnvUnverifiable is the pathClusterEnv of why the settings the operator gives of the cluster do not
	// have to be those its sidecar takes, a single line, empty when they have to as far as the operator can
	// tell. The check leaves such a cluster out, and a Job that targets it fails unless it is the only
	// backup-enabled cluster.
	pathEnvUnverifiable = "UNVERIFIABLE"
)

// pathEnvPrefix starts the name of every pathClusterEnv
const pathEnvPrefix = "PATH_"

// pathClusterEnv returns the name of the env var that holds the path check setting name of the cluster
// at index: pathEnvCluster, pathEnvShards, pathEnvUnverifiable, one of the default paths or one of
// PathCheckKeys.
func pathClusterEnv(index int, name string) string {
	return pathEnvPrefix + strconv.Itoa(index) + "_" + name
}

// PathCheckKeys are the clickhouse-backup settings the storage path check reads of every cluster, as
// pathClusterEnv(index, key): which type of storage it is, what tells one storage of its type from
// another, and its backup and object disk paths, each by the name of the env var clickhouse-backup knows it by,
// see backupconfig.Settings. None of them is a credential. A cluster takes each as its sidecar does: from an
// optional key of the configuration file Secret of the cluster, which holds the setting of the file, and which
// kubelet leaves unset when the file does not set it, overridden by the env of the sidecar container of its pod
// template; pathEnvUnverifiable tells when its sidecar may take them otherwise. An unset one counts with the
// default clickhouse-backup gives it, an unset path with the one the configuration file takes.
// The region of an S3 storage is none of them: a bucket name is global, the region only tells where
// the requests to it go and what they are signed for, and the AWS SDK of the sidecar takes it from its
// own environment when the setting is empty. A bucket of one name in another AWS partition then counts
// as the same, which refuses a path rather than let a shared one through.
var PathCheckKeys = []string{
	"REMOTE_STORAGE",
	"S3_BUCKET", "S3_ENDPOINT", "S3_PATH", "S3_OBJECT_DISK_PATH",
	"GCS_BUCKET", "GCS_ENDPOINT", "GCS_PATH", "GCS_OBJECT_DISK_PATH",
	"AZBLOB_ACCOUNT_NAME", "AZBLOB_CONTAINER", "AZBLOB_ENDPOINT_SUFFIX", "AZBLOB_PATH", "AZBLOB_OBJECT_DISK_PATH",
	"COS_URL", "COS_PATH", "COS_OBJECT_DISK_PATH",
	"FTP_ADDRESS", "FTP_PATH", "FTP_OBJECT_DISK_PATH",
	"SFTP_ADDRESS", "SFTP_PORT", "SFTP_PATH", "SFTP_OBJECT_DISK_PATH",
}

// UnchangedExitCode is the value of envUnchangedExitCode: the Job container exits with it when the
// script fails before any sidecar may have accepted a request that changes something.
const UnchangedExitCode = 3

// NoDataMessage is the value of envNoDataMessage: the termination message of a Job container that
// succeeded and made a backup that holds no data on any host.
const NoDataMessage = "the backup holds no data"

// RBACNotRestoredMessage is the value of envRBACNotRestoredMessage: the termination message of a Job
// container that succeeded and restored a backup without the access entities - users, roles, grants -
// it holds.
const RBACNotRestoredMessage = "the backup holds access entities that were not restored"

// Values of envOperation
const (
	operationBackup  = "backup"
	operationVerify  = "verify"
	operationRestore = "restore"
)

// jobScript is the bash script a backup Job runs as ["bash", "-c", jobScript]. It drives the
// clickhouse-backup sidecar REST API of the target hosts and fails the Job on any error.
//
//go:embed scripts/job.sh
var jobScript string
