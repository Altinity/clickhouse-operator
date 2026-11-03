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

package config

import api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

const (
	// CommonConfigDir specifies folder's name, where generated common XML files for ClickHouse would be placed
	CommonConfigDir = api.CommonConfigDirClickHouse

	// UsersConfigDir specifies folder's name, where generated users XML files for ClickHouse would be placed
	UsersConfigDir = api.UsersConfigDirClickHouse

	// HostConfigDir specifies folder's name, where generated host XML files for ClickHouse would be placed
	HostConfigDir = api.HostConfigDirClickHouse

	// TemplatesDir specifies folder's name where ClickHouseInstallationTemplates are located
	TemplatesDir = api.TemplatesDirClickHouse
)

const (
	DirPathConfigRoot = "/etc/clickhouse-server"

	// DirPathConfigCommon specifies full path to folder,
	// where generated common XML files for the following sections would be placed:
	// 1. remote servers
	// 2. operator-provided additional config files
	DirPathConfigCommon = DirPathConfigRoot + "/" + CommonConfigDir + "/"

	// DirPathConfigUsers specifies full path to folder, where generated users XML files would be placed
	// for the following sections:
	// 1. users
	// 2. quotas
	// 3. profiles
	// 4. operator-provided additional config files
	DirPathConfigUsers = DirPathConfigRoot + "/" + UsersConfigDir + "/"

	// DirPathConfigHost specifies full path to folder, where generated host XML files would be placed
	// for the following sections:
	// 1. macros
	// 2. zookeeper
	// 3. settings
	// 4. files
	// 5. operator-provided additional config files
	DirPathConfigHost = DirPathConfigRoot + "/" + HostConfigDir + "/"

	// DirPathSecretFilesConfig specifies full path to folder, where secrets are mounted
	DirPathSecretFilesConfig = DirPathConfigRoot + "/" + "secrets.d" + "/"

	// DirPathDataStorage specifies full path of data folder where ClickHouse would place its data storage
	DirPathDataStorage = "/var/lib/clickhouse"

	// DirPathLogStorage  specifies full path of data folder where ClickHouse would place its log files
	DirPathLogStorage = "/var/log/clickhouse-server"
)

const (
	// DefaultClickHouseDockerImage specifies default ClickHouse docker image to be used
	DefaultClickHouseDockerImage = "clickhouse/clickhouse-server:latest"

	// DefaultBusyBoxDockerImage specifies default BusyBox docker image to be used
	DefaultBusyBoxDockerImage = "busybox"

	// DefaultUbiDockerImage specifies default ubi docker image to be used
	DefaultUbiDockerImage = "registry.access.redhat.com/ubi8/ubi-minimal:latest"

	// Name of container within Pod with ClickHouse instance.
	// Pod may have other containers included, such as monitoring, logging

	// ClickHouseContainerName specifies name of the clickhouse container in the pod
	ClickHouseContainerName = "clickhouse"

	// ClickHouseLogContainerName specifies name of the logger container in the pod
	ClickHouseLogContainerName = "clickhouse-log"

	// ClickHouseBackupContainerName specifies name of the operator-managed clickhouse-backup sidecar in the pod
	ClickHouseBackupContainerName = "clickhouse-backup"
)

// ClickHouseSidecarContainerNames are the containers the operator runs next to ClickHouse in a pod, none of which is
// ever ClickHouse: a pod template that names no container ClickHouseContainerName runs ClickHouse in its first other one
var ClickHouseSidecarContainerNames = []string{
	ClickHouseLogContainerName,
	ClickHouseBackupContainerName,
}

const (
	// DefaultClickHouseBackupDockerImage specifies the clickhouse-backup image of a sidecar whose base container names
	// none, or that has no base container as backup.podTemplate is empty
	DefaultClickHouseBackupDockerImage = "altinity/clickhouse-backup:2.8.1"

	// ClickHouseBackupAPIPortName specifies name of the clickhouse-backup REST API port of the sidecar
	ClickHouseBackupAPIPortName = "backup-api"
	// ClickHouseBackupAPIPortNumber specifies number of the clickhouse-backup REST API port of the sidecar
	ClickHouseBackupAPIPortNumber = 7171

	// ClickHouseBackupSharedDataVolumeName specifies name of the volume the operator mounts at DirPathDataStorage
	// in both the ClickHouse container and the sidecar of a host that has no volume there
	ClickHouseBackupSharedDataVolumeName = "clickhouse-backup-shared-data"

	// ClickHouseBackupCredentialsVolumeName and ClickHouseBackupStorageVolumeName specify names of the volumes of
	// the backup Secret and of the storage Secret, which the sidecar alone mounts at DirPathClickHouseBackupCredentials
	// and DirPathClickHouseBackupStorage. kubelet keeps their files up to date, and the sidecar's liveness probe
	// compares them with the env vars the sidecar started with.
	ClickHouseBackupCredentialsVolumeName = "clickhouse-backup-credentials"
	ClickHouseBackupStorageVolumeName     = "clickhouse-backup-storage"
	DirPathClickHouseBackupCredentials    = "/var/run/secrets/clickhouse-backup/credentials"
	DirPathClickHouseBackupStorage        = "/var/run/secrets/clickhouse-backup/storage"

	// DirPathClickHouseBackupConfig is where clickhouse-backup reads its configuration file from, unless
	// ClickHouseBackupEnvConfig names another one
	DirPathClickHouseBackupConfig = "/etc/clickhouse-backup"

	// ClickHouseBackupUsername specifies the ClickHouse user the sidecar connects as. It is reachable
	// from localhost only, so nothing outside the pod can use it.
	ClickHouseBackupUsername = "clickhouse_backup"

	// Keys of the operator-generated backup Secret. They are named after the clickhouse-backup
	// environment variables they feed, so the sidecar can take the Secret as-is.
	ClickHouseBackupSecretKeyAPIUsername        = "API_USERNAME"
	ClickHouseBackupSecretKeyAPIPassword        = "API_PASSWORD"
	ClickHouseBackupSecretKeyClickHouseUsername = "CLICKHOUSE_USERNAME"
	ClickHouseBackupSecretKeyClickHousePassword = "CLICKHOUSE_PASSWORD"

	// ClickHouseBackupCommand starts the clickhouse-backup REST API server in the sidecar
	ClickHouseBackupCommand       = "/bin/clickhouse-backup"
	ClickHouseBackupCommandServer = "server"

	// Environment variables of the clickhouse-backup sidecar the operator sets besides the ones fed by the backup Secret
	ClickHouseBackupEnvAPIListen                = "API_LISTEN"
	ClickHouseBackupEnvAPISecure                = "API_SECURE"
	ClickHouseBackupEnvClickHouseHost           = "CLICKHOUSE_HOST"
	ClickHouseBackupEnvClickHousePort           = "CLICKHOUSE_PORT"
	ClickHouseBackupEnvClickHouseSecure         = "CLICKHOUSE_SECURE"
	ClickHouseBackupEnvClickHouseSkipVerify     = "CLICKHOUSE_SKIP_VERIFY"
	ClickHouseBackupEnvClickHouseRestartCommand = "CLICKHOUSE_RESTART_COMMAND"
	ClickHouseBackupEnvRestoreSchemaOnCluster   = "RESTORE_SCHEMA_ON_CLUSTER"
	ClickHouseBackupEnvBackupsToKeepLocal       = "BACKUPS_TO_KEEP_LOCAL"
	// ClickHouseBackupEnvAPIAllowParallel is kept off, as by default, also where the storage Secret or the
	// sidecar's base container turns it on: clickhouse-backup sets the env vars an operation overrides for the whole
	// process, which an operation running alongside would see, and the operations of two Jobs on one host would run
	// at once.
	ClickHouseBackupEnvAPIAllowParallel = "API_ALLOW_PARALLEL"
	// ClickHouseBackupEnvRestoreDatabaseMapping and ClickHouseBackupEnvRestoreTableMapping are kept empty: a mapping
	// makes clickhouse-backup restore into, and drop, other databases and tables than the backup names, which the
	// overwrite guard of a restore Job, comparing the names the backup holds, does not see.
	ClickHouseBackupEnvRestoreDatabaseMapping = "RESTORE_DATABASE_MAPPING"
	ClickHouseBackupEnvRestoreTableMapping    = "RESTORE_TABLE_MAPPING"
	// ClickHouseBackupEnvConfig names the configuration file clickhouse-backup reads instead of the one in
	// DirPathClickHouseBackupConfig
	ClickHouseBackupEnvConfig = "CLICKHOUSE_BACKUP_CONFIG"
	// ClickHouseBackupEnvAllowEmptyBackups lets a backup of a cluster without tables succeed. Without it every
	// scheduled backup of a freshly created cluster fails with "no tables for backup".
	ClickHouseBackupEnvAllowEmptyBackups = "ALLOW_EMPTY_BACKUPS"

	// ClickHouseBackupBackupsToKeepLocalDeleteAfterUpload makes clickhouse-backup delete the local copy of a
	// backup once it is uploaded. Its default keeps every local copy, and these fill up the data disk.
	ClickHouseBackupBackupsToKeepLocalDeleteAfterUpload = "-1"

	// ClickHouseBackupRestartCommandReloadUsers is what clickhouse-backup runs once it has restored access
	// objects, so that ClickHouse loads them. Its default restarts the server through systemctl, which a sidecar
	// does not have: the command fails, clickhouse-backup ignores the failure, and the restored users, roles and
	// grants stay unloaded. Reloading them needs ClickHouse 23.3 or later, and no restart.
	ClickHouseBackupRestartCommandReloadUsers = "sql:SYSTEM RELOAD USERS"

	// ClickHouseBackupClickHouseHost is where the sidecar reaches ClickHouse - both share the pod's network namespace
	ClickHouseBackupClickHouseHost = "localhost"
	// ClickHouseBackupAPIListenHost makes the REST API reachable through the host Service
	ClickHouseBackupAPIListenHost = "0.0.0.0"
)

// ClickHouseBackupUserNetworksIP lists the only addresses ClickHouseBackupUsername may connect from - the pod's loopback
var ClickHouseBackupUserNetworksIP = []string{"::1", "127.0.0.1"}

const (
	xmlTagYandex = "yandex"
)

const (
	configMacros        = "macros"
	configHostnamePorts = "hostname-ports"
	configProfiles      = "profiles"
	configQuotas        = "quotas"
	configRemoteServers = "remote_servers"
	configSettings      = "settings"
	configUsers         = "users"
	configZookeeper     = "zookeeper"
)

const (
	// ZkDefaultPort specifies Zookeeper default port
	ZkDefaultPort = 2181
)
