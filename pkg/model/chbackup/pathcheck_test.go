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
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// testPathNamespace is the namespace of the installation of the tests of the storage path check
const testPathNamespace = "demo-ns"

// Keys of the cases of the storage path check besides PathCheckKeys
const (
	pathEnvUnverifiable     = "UNVERIFIABLE"
	envDefaultPath          = "DEFAULT_PATH"
	envPathReplicaSelection = "PATH_REPLICA_SELECTION"
	envNamingTemplate       = "NAMING_TEMPLATE"
	// envConfigOverrides holds the config overrides of the runs, the lists of the runs apart by "|"
	envConfigOverrides = "CONFIG_OVERRIDES"
	operationRestore   = api.BackupRunOperationRestore
)

// pathCluster is a cluster of a case of the storage path check: settings are of PathCheckKeys, or UNVERIFIABLE; every
// shard has replicas hosts, 2 when unset
type pathCluster struct {
	name     string
	shards   string
	replicas int
	settings map[string]string
}

func storageSettings(storage string, settings ...string) map[string]string {
	values := map[string]string{"REMOTE_STORAGE": storage}
	for i := 0; i+1 < len(settings); i += 2 {
		values[settings[i]] = settings[i+1]
	}
	return values
}

// pathCheckOf returns the storage path check of an operation on the first of the clusters, of an installation demo
// of the namespace testPathNamespace whose sidecars take the default paths of the operator configuration
func pathCheckOf(operation string, env map[string]string, clusters ...pathCluster) PathCheck {
	check := PathCheck{
		Installation:          "demo",
		Namespace:             testPathNamespace,
		Operation:             operation,
		DefaultPath:           ptr.To(chop.Config().Backup.StoragePath),
		DefaultObjectDiskPath: ptr.To(chop.Config().Backup.StorageObjectDiskPath),
	}
	if operation == api.BackupRunOperationBackup {
		check.ReplicaSelection = api.ReplicaSelectionFirstPerShard
	}
	// A restore checks the backup on every host, by default
	check.ValidateTopology = true
	if selection, set := env[envPathReplicaSelection]; set {
		check.ReplicaSelection = api.ReplicaSelection(selection)
	}
	if def, set := env[envDefaultPath]; set {
		check.DefaultPath = ptr.To(def)
	}
	check.NamingTemplate = env[envNamingTemplate]
	if overrides, set := env[envConfigOverrides]; set {
		for _, run := range strings.Split(overrides, "|") {
			check.ConfigOverrides = append(check.ConfigOverrides, strings.Fields(run))
		}
	}
	for _, c := range clusters {
		cluster := PathCluster{Name: c.name, Shards: strings.Fields(c.shards), Settings: map[string]*string{}}
		replicas := c.replicas
		if replicas == 0 {
			replicas = 2
		}
		for i, shard := range cluster.Shards {
			for r := 0; r < replicas; r++ {
				// The host name of the pod of a host, chi-<installation>-<cluster>-<shard>-<replica>-0
				name := fmt.Sprintf("chi-demo-%s-%s-%d", c.name, shard, r)
				cluster.Hosts = append(cluster.Hosts, PathHost{Name: name, Shard: i, Macros: map[string]string{
					MacroReplica: name + "-0", MacroAllShardedShard: strconv.Itoa(len(check.Clusters)*100 + i*replicas + r)}})
			}
		}
		for key, value := range c.settings {
			if key == pathEnvUnverifiable {
				cluster.Unverifiable = value
				continue
			}
			cluster.Settings[key] = ptr.To(value)
		}
		check.Clusters = append(check.Clusters, cluster)
	}
	return check
}

// Before a backup or a restore changes anything, the operator checks that every shard of the target cluster has a
// storage path of its own: one that no other shard of any backup-enabled cluster of the installation shares in the same
// storage, as clickhouse-backup expands, trims and joins it, the default paths for unset ones included, the root of the
// storage too - and likewise for object disk data. A storage is told apart by its settings as clickhouse-backup takes
// them, the defaults it gives unset ones included. A backup of the first replica of each shard must not have {replica}
// in its path, as every replica restores its schema from its own path; a backup of every replica must have it. A path
// proven shared fails the operation; another cluster whose paths the check cannot know is left out of it with a
// warning. So is the target cluster, when it is the only backup-enabled one; beside others it fails instead.
func TestStoragePathCheck(t *testing.T) {
	s3 := func(settings ...string) map[string]string {
		return storageSettings("s3", append([]string{"S3_BUCKET", "backups"}, settings...)...)
	}
	for name, tc := range map[string]struct {
		clusters  []pathCluster
		env       map[string]string
		operation string
		// refused is the reason the Job fails for, empty when it runs
		refused string
		warned  string
		// log is what the check logs, when it warns of nothing, by default that every shard has a path of its own
		log string
	}{
		"the default paths": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3()}, {name: "b", shards: "0 1", settings: s3()}},
		},
		"one path for two clusters": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}},
			refused: "storage path 's3://backups/backup/0' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their " +
				"backups there; give every shard of every cluster a storage path of its own, e.g. with {cluster} and {shard}",
		},
		"one path in another bucket": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}", "S3_BUCKET", "other")}},
		},
		"one path behind another endpoint": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x", "S3_ENDPOINT", "https://minio:9000")}},
		},
		"an endpoint set empty and one unset": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x", "S3_ENDPOINT", "")}},
			refused:  "storage path 's3://backups/x' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"a path without the shard": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "backup/{cluster}")}},
			refused:  "storage path 's3://backups/backup/a' is not unique: shards 0 and 1 of cluster a both write a backup of one name there",
		},
		"paths that differ in slashes and blanks": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", " /backup/{shard}/")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}},
			refused:  "storage path 's3://backups/backup/0' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"an azblob endpoint suffix unset and one set to its default": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x", "AZBLOB_ENDPOINT_SUFFIX", "core.windows.net")},
			},
			refused: "storage path 'azblob://one/x' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"an sftp port unset and one set to its default": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("sftp", "SFTP_ADDRESS", "sftp", "SFTP_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("sftp", "SFTP_ADDRESS", "sftp", "SFTP_PORT", "22", "SFTP_PATH", "x")},
			},
			refused: "storage path 'sftp://sftp/x' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"paths that differ in empty and dot elements": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup//./{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x/../backup/{shard}/.")}},
			refused:  "storage path 's3://backups/backup/0' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"object disk paths that differ in empty elements": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od//{shard}")}, {name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/{shard}")}},
			refused:  "object disk path 's3://backups/od/0' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"absolute ftp paths that differ in a parent element": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("ftp", "FTP_ADDRESS", "ftp:21", "FTP_PATH", "/../backup/{shard}")},
				{name: "b", shards: "0", settings: storageSettings("ftp", "FTP_ADDRESS", "ftp:21", "FTP_PATH", "/backup/{shard}")},
			},
			refused: "storage path 'ftp://ftp:21/backup/0' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"an absolute and a relative ftp path": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("ftp", "FTP_ADDRESS", "ftp:21", "FTP_PATH", "/backup/{shard}/")},
				{name: "b", shards: "0", settings: storageSettings("ftp", "FTP_ADDRESS", "ftp:21", "FTP_PATH", "backup/{shard}")},
			},
		},
		"one installation macro expanded": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "{installation}/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "demo/0")}},
			refused:  "storage path 's3://backups/demo/0' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"one namespace macro expanded": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/{namespace}/{shard}")},
				{name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/"+testPathNamespace+"/0")},
			},
			refused: "object disk path 's3://backups/od/" + testPathNamespace + "/0' is not unique: shard 0 of cluster a and shard 0 of cluster b " +
				"both write their backups there",
		},
		"paths of other namespaces": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "{namespace}/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "other/0")}},
		},
		"a path set empty": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "")}, {name: "b", shards: "0", settings: s3("S3_PATH", "")}},
			refused:  "storage path 's3://backups/' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"unset paths without a default": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3()}, {name: "b", shards: "0", settings: s3()}},
			env:      map[string]string{envDefaultPath: ""},
			refused:  "storage path 's3://backups/' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"one object disk path for two clusters": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/{shard}")}, {name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/{shard}")}},
			refused:  "object disk path 's3://backups/od/0' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"object disk paths set empty": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "")}, {name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "")}},
		},
		// Unlike one that trims to nothing, an object disk path that cleans to the root is the root
		"object disk paths at the root": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "od/..")}, {name: "b", shards: "0", settings: s3("S3_OBJECT_DISK_PATH", "/./")}},
			refused:  "object disk path 's3://backups/' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		// A relative path keeps the parent elements above its start, which are keys of their own
		"a relative path above the root and the one it climbs to": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "../backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}},
		},
		"a relative path two levels above the root and the one it climbs to": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "../../backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "backup/{shard}")}},
		},
		"relative paths that climb above the root alike": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "../backup/{shard}")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x/../../backup/{shard}/")}},
			refused:  "storage path 's3://backups/../backup/0' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"the replica in a backup of the first replica of each shard": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}/{replica}")}},
			refused:  "the storage path 'backup/{shard}/{replica}' of cluster a holds {replica}",
		},
		// The replicas of a shard make a backup of one name at once, so every path is expanded on every host, as
		// clickhouse-backup expands it there
		"a backup of every replica without the replica": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3()}},
			env:      map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			refused: "storage path 's3://backups/backup/" + testPathNamespace + "/demo/a/0' is not unique: replicas chi-demo-a-0-0-0 and " +
				"chi-demo-a-0-1-0 of shard 0 of cluster a both write a backup of one name there; give every replica a storage path of " +
				"its own, e.g. with {replica}",
		},
		"a backup of every replica": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{replica}", "S3_OBJECT_DISK_PATH", "od/{replica}")},
				{name: "b", shards: "0", settings: s3("S3_PATH", "backup/{replica}", "S3_OBJECT_DISK_PATH", "od/{replica}")},
			},
			env: map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			log: "storage path check: every replica of cluster a has a path of its own",
		},
		"a backup of every replica of shards of one replica": {
			clusters: []pathCluster{{name: "a", shards: "0 1", replicas: 1, settings: s3()}},
			env:      map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			log:      "storage path check: every replica of cluster a has a path of its own",
		},
		// The metadata of the backups of the replicas would be at one path
		"a backup of every replica with the replica in the object disk path alone": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "backup/{shard}", "S3_OBJECT_DISK_PATH", "od/{shard}/{replica}")}},
			env:      map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			refused:  "storage path 's3://backups/backup/0' is not unique: replicas chi-demo-a-0-0-0 and chi-demo-a-0-1-0 of shard 0 of cluster a both write a backup of one name there",
		},
		// The object disk data of the backups of the replicas would be at one path, which deleting any of them deletes
		"a backup of every replica with the replica in the storage path alone": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}/{replica}")}},
			env:      map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			refused: "object disk path 's3://backups/object_disks/" + testPathNamespace + "/demo/a/0' is not unique: replicas " +
				"chi-demo-a-0-0-0 and chi-demo-a-0-1-0 of shard 0 of cluster a both write a backup of one name there",
		},
		"a backup of every replica without object disk data": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}/{replica}", "S3_OBJECT_DISK_PATH", "")}},
			env:      map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			log:      "storage path check: every replica of cluster a has a path of its own",
		},
		// A naming template names the backups of the shards apart, never those of the replicas of a shard
		"a backup of every replica with a naming template, the replica in no path": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "backup/{cluster}", "S3_OBJECT_DISK_PATH", "od/{cluster}")}},
			env: map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas),
				envNamingTemplate: api.BackupNamingShardTime},
			refused: "storage path 's3://backups/backup/a' is not unique: replicas chi-demo-a-0-0-0 and chi-demo-a-0-1-0 of shard 0 of cluster a both write a backup of one name there",
		},
		"a backup of every replica with a naming template, the shard in no path": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "backup/{cluster}/{replica}", "S3_OBJECT_DISK_PATH", "od/{replica}")}},
			env: map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas),
				envNamingTemplate: api.BackupNamingShardTime},
			log: "storage path check: every replica of cluster a has a path of its own",
		},
		"a backup of the first replica of each shard with a naming template, the shard in no path": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "backup/{cluster}", "S3_OBJECT_DISK_PATH", "od/{cluster}")}},
			env:      map[string]string{envNamingTemplate: api.BackupNamingShardTime},
			log: "storage path check: the shards of cluster a share a path, where the naming template '" + api.BackupNamingShardTime +
				"' names the backup of each apart, and no other cluster has it",
		},
		// ClickHouse writes an embedded backup without a backup disk whole under the object disk path, the root of the
		// storage when that is empty, as CAS disks are backed up
		"an embedded backup of every replica at the root of the storage": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{replica}", "S3_OBJECT_DISK_PATH", "",
				SettingUseEmbeddedBackupRestore, "true")}},
			env: map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			refused: "object disk path 's3://backups/' is not unique: replicas chi-demo-a-0-0-0 and chi-demo-a-0-1-0 of shard 0 of " +
				"cluster a both write a backup of one name there; give every replica an object disk path of its own, e.g. with " +
				"{replica}, the more so as embedded backups without a backup disk are written there whole",
		},
		"an embedded backup of every replica by a config override": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{replica}", "S3_OBJECT_DISK_PATH", "")}},
			env: map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas),
				envConfigOverrides: "S3_STORAGE_CLASS=GLACIER|--env=" + SettingUseEmbeddedBackupRestore + "=1"},
			refused: "object disk path 's3://backups/' is not unique: replicas chi-demo-a-0-0-0 and chi-demo-a-0-1-0 of shard 0 of cluster a both write a backup of one name there",
		},
		"an embedded backup of every replica at paths of their own": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{replica}", "S3_OBJECT_DISK_PATH", "od/{replica}",
				SettingUseEmbeddedBackupRestore, "true")}},
			env: map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			log: "storage path check: every replica of cluster a has a path of its own",
		},
		"an embedded backup of every replica on a backup disk": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{replica}", "S3_OBJECT_DISK_PATH", "od/{replica}",
				SettingUseEmbeddedBackupRestore, "true", SettingEmbeddedBackupDisk, "backups")}},
			env: map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			warned: "embedded backups of cluster a are made first on the backup disk backups, as BACKUP ... TO Disk('backups', " +
				"'<backup name>'), where the storage path check cannot see: 2 hosts make a backup of one name there at once",
		},
		"an embedded backup of the first replica of each shard named apart on a backup disk": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3(SettingUseEmbeddedBackupRestore, "true", SettingEmbeddedBackupDisk, "backups")}},
			env:      map[string]string{envNamingTemplate: api.BackupNamingShardTime},
		},
		"two other clusters that share a path": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3()},
				{name: "b", shards: "0", settings: s3("S3_PATH", "x")},
				{name: "c", shards: "0", settings: s3("S3_PATH", "x")},
			},
		},
		"azblob containers": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "two", "AZBLOB_PATH", "x")},
			},
		},
		"one azblob container": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("azblob", "AZBLOB_ACCOUNT_NAME", "acc", "AZBLOB_CONTAINER", "one", "AZBLOB_PATH", "x")},
			},
			refused: "storage path 'azblob://one/x' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"sftp servers on other ports": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: storageSettings("sftp", "SFTP_ADDRESS", "sftp", "SFTP_PORT", "22", "SFTP_PATH", "x")},
				{name: "b", shards: "0", settings: storageSettings("sftp", "SFTP_ADDRESS", "sftp", "SFTP_PORT", "2222", "SFTP_PATH", "x")},
			},
		},
		"another cluster that sets no storage": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: map[string]string{"S3_PATH": "x"}}},
			warned:   "cluster b is left out of the storage path check: its configuration does not set remote_storage",
		},
		"the target cluster that sets no storage": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: map[string]string{"S3_PATH": "x"}}},
			warned:   "the storage path check is skipped: its configuration does not set remote_storage",
		},
		"custom storage": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: storageSettings("custom")}},
			warned:   "the storage path check is skipped: it keeps its backups in custom storage",
		},
		"the target cluster that sets no storage, beside another cluster": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: map[string]string{"S3_PATH": "x"}}, {name: "b", shards: "0", settings: s3()}},
			refused:  "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: its configuration does not set remote_storage",
		},
		"custom storage, beside another cluster": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: storageSettings("custom")}, {name: "b", shards: "0", settings: s3()}},
			refused:  "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: it keeps its backups in custom storage",
		},
		// A macro the operator does not define skips the path that holds it, with a warning, never refuses it
		"a macro of unknown value in the target cluster's path, beside another cluster": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x/{layer}")}, {name: "b", shards: "0", settings: s3()}},
			warned:   "the storage path check skips the storage path 'x/{layer}' of cluster a: it holds {layer}, a macro the operator does not define",
		},
		"a restore of the target cluster that sets no storage, beside another cluster": {
			operation: operationRestore,
			clusters:  []pathCluster{{name: "a", shards: "0", settings: map[string]string{}}, {name: "b", shards: "0", settings: s3()}},
			refused:   "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation",
		},
		"the target cluster's settings unverifiable": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x", pathEnvUnverifiable, "its pod template takes env from envFrom")}},
			warned:   "the storage path check is skipped: the operator cannot tell the sidecar settings of cluster a: its pod template takes env from envFrom",
		},
		"the target cluster's settings unverifiable, beside another cluster": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3(pathEnvUnverifiable, "a volume is mounted at /etc/clickhouse-backup of its sidecar")},
				{name: "b", shards: "0", settings: s3("S3_PATH", "y")},
			},
			refused: "the storage path check cannot tell whether cluster a shares a storage path with another of the 2 backup-enabled clusters of the installation, which would mix their backups and restore one with the data of the other: the operator cannot tell the sidecar settings of cluster a: a volume is mounted at /etc/clickhouse-backup of its sidecar",
		},
		"another cluster's settings unverifiable": {
			clusters: []pathCluster{
				{name: "a", shards: "0", settings: s3("S3_PATH", "x")},
				{name: "b", shards: "0", settings: s3("S3_PATH", "x", pathEnvUnverifiable, "its hosts disagree on the default paths")},
			},
			warned: "cluster b is left out of the storage path check: the operator cannot tell the sidecar settings of cluster b: its hosts disagree on the default paths",
		},
		"a macro of unknown value in the target cluster's path": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "x/{layer}")}},
			warned:   "the storage path check skips the storage path 'x/{layer}' of cluster a: it holds {layer}",
		},
		"a macro of unknown value in another cluster's path": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "y", "S3_OBJECT_DISK_PATH", "{layer}")}},
			warned:   "cluster b is left out of the storage path check of its object disk path: its path '{layer}' holds {layer}",
		},
		// The other path of a cluster, without such a macro, is still checked
		"a macro of unknown value in the object disk path of another cluster that shares the storage path": {
			clusters: []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x", "S3_OBJECT_DISK_PATH", "{layer}")}},
			refused:  "storage path 's3://backups/x' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		"the macro of the index of the host in the installation": {
			clusters: []pathCluster{{name: "a", shards: "0 1", settings: s3("S3_PATH", "x/{all-sharded-shard}", "S3_OBJECT_DISK_PATH", "od/{all-sharded-shard}")}},
			env:      map[string]string{envPathReplicaSelection: string(api.ReplicaSelectionAllReplicas)},
			log:      "storage path check: every replica of cluster a has a path of its own",
		},
		"a restore from one path for two clusters": {
			operation: operationRestore,
			clusters:  []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "x")}, {name: "b", shards: "0", settings: s3("S3_PATH", "x")}},
			refused:   "storage path 's3://backups/x' is not unique: shard 0 of cluster a and shard 0 of cluster b both write their backups there",
		},
		// A backup of every replica is restored from the path of each replica
		"a restore from a path with the replica": {
			operation: operationRestore,
			clusters:  []pathCluster{{name: "a", shards: "0", settings: s3("S3_PATH", "backup/{shard}/{replica}")}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			operation := tc.operation
			if operation == "" {
				operation = api.BackupRunOperationBackup
			}
			result, err := pathCheckOf(operation, tc.env, tc.clusters...).Check()
			if tc.refused != "" {
				require.ErrorContains(t, err, tc.refused)
				return
			}
			require.NoError(t, err)
			advice := strings.Join(result.Advice, "\n")
			require.Contains(t, advice, tc.warned)
			if tc.warned == "" {
				require.Empty(t, advice)
				expected := tc.log
				if expected == "" {
					expected = "storage path check: every shard of cluster " + tc.clusters[0].name + " has a path of its own"
				}
				require.Equal(t, expected, result.Log)
			}
		})
	}
}
