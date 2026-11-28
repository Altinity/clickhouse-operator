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
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// A restore keeps to the cluster recorded when it was validated, whatever its spec names later.
func TestRestoresRBACFollowsThePinnedCluster(t *testing.T) {
	chr := testRestore("r1")
	chr.Spec.RBAC = types.NewStringBool(true)
	require.True(t, RestoresRBAC(chr), "a restore of every cluster restores access entities")
	chr.Spec.Cluster = "main"
	require.False(t, RestoresRBAC(chr), "a restore of one cluster does not")

	chr.Status.Clusters = []api.OperationClusterStatus{{Name: "main"}}
	chr.Status.Cluster = ""
	require.True(t, RestoresRBAC(chr), "validated as a restore of every cluster, it stays one")
	chr.Spec.Cluster, chr.Status.Cluster = "", "main"
	require.False(t, RestoresRBAC(chr), "validated as a restore of one cluster, it stays one")
}

// A recovery restores one cluster of a new installation, its access entities included.
func TestRecoveryRestore(t *testing.T) {
	chi := testInstallation()
	restore := BuildRecoveryRestore(chi, ClusterRecovery{Cluster: "cold", BackupName: "nightly-20260101-000000"})

	require.Equal(t, "demo-cold-recovery", restore.Name)
	require.Equal(t, chi.Namespace, restore.Namespace)
	require.Equal(t, "demo", restore.Spec.ClickHouseInstallation)
	require.Equal(t, "cold", restore.Spec.Cluster)
	require.Equal(t, "nightly-20260101-000000", restore.Spec.BackupName)
	require.True(t, restore.Spec.RBAC.IsTrue())
	require.NoError(t, restore.Spec.Validate())
}

// A recovery takes the installation that made its backup from the recovery it inherits, and hands it to the restore
// it creates.
func TestRecoveryRestoresFromItsSource(t *testing.T) {
	chi := newCHI("demo", &api.Cluster{Name: "hot", Backup: enabledBackup()}, &api.Cluster{Name: "cold", Backup: enabledBackup()})
	chi.Spec.Configuration.Clusters[1].Backup.Recovery = &api.BackupRecovery{SourceInstallation: types.NewString("cold-src")}
	chi.Spec.Backup = &api.ClusterBackup{Recovery: &api.BackupRecovery{
		BackupName:         types.NewString("nightly-20261004-020000"),
		SourceInstallation: types.NewString("prod"),
		SourceNamespace:    types.NewString("team-a"),
	}}
	_, stored := normalize(t, chi)

	recoveries := Recoveries(stored)
	require.Equal(t, []ClusterRecovery{
		{Cluster: "hot", BackupName: "nightly-20261004-020000", SourceInstallation: "prod", SourceNamespace: "team-a"},
		{Cluster: "cold", BackupName: "nightly-20261004-020000", SourceInstallation: "cold-src", SourceNamespace: "team-a"},
	}, recoveries)
	restore := BuildRecoveryRestore(stored, recoveries[1])
	require.Equal(t, "cold-src", restore.Spec.SourceInstallation)
	require.Equal(t, "team-a", restore.Spec.SourceNamespace)
	require.NoError(t, restore.Spec.Validate())
}

// Every backup-enabled cluster that inherits a non-empty backup name is recovered; a cluster that
// opts out with "", or has no sidecar to restore through, is not. A cluster that turns backup on without a
// storage is told apart, as its recovery waits for one.
func TestRecoveries(t *testing.T) {
	chi := newCHI("demo",
		&api.Cluster{Name: "hot", Backup: enabledBackup()},
		&api.Cluster{Name: "cold", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), Recovery: &api.BackupRecovery{BackupName: types.NewString("cold-20261001-030000")},
		}},
		&api.Cluster{Name: "scratch", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), Recovery: &api.BackupRecovery{BackupName: types.NewString("")},
		}},
		&api.Cluster{Name: "plain", Backup: &api.ClusterBackup{Enabled: types.NewStringBool(false)}},
		&api.Cluster{Name: "lacking", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), StorageConfig: api.NewStorageConfig("", ""),
		}},
	)
	chi.Spec.Backup = &api.ClusterBackup{
		StorageConfig: api.NewStorageConfig(testStorageSecret, "config.yml"),
		Recovery:      &api.BackupRecovery{BackupName: types.NewString("nightly-20261004-020000")},
	}
	_, stored := normalize(t, chi)

	require.Equal(t, []ClusterRecovery{
		{Cluster: "hot", BackupName: "nightly-20261004-020000"},
		{Cluster: "cold", BackupName: "cold-20261001-030000"},
	}, Recoveries(stored))
	require.Equal(t, []ClusterRecovery{{Cluster: "lacking", BackupName: "nightly-20261004-020000"}}, RecoveriesWithoutStorage(stored),
		"a cluster whose backup a missing storage leaves off is not recovered, and its recovery is told apart")
	require.Empty(t, Recoveries(nil))
}

// The record of a recovery names the backup, or, when the clusters were recovered from different
// backups, each cluster with its backup; either reads back as the backups it names.
func TestRecoveredFrom(t *testing.T) {
	same := []ClusterRecovery{{Cluster: "hot", BackupName: "nightly"}, {Cluster: "cold", BackupName: "nightly"}}
	require.Equal(t, "nightly", RecoveredFrom(same))
	require.Equal(t, []string{"nightly"}, RecoveredBackupNames(RecoveredFrom(same)))

	different := []ClusterRecovery{{Cluster: "hot", BackupName: "nightly"}, {Cluster: "cold", BackupName: "weekly"}}
	require.Equal(t, "cold:weekly,hot:nightly", RecoveredFrom(different), "sorted by cluster")
	require.Equal(t, []string{"weekly", "nightly"}, RecoveredBackupNames(RecoveredFrom(different)))

	require.Equal(t, []string{"by-hand"}, RecoveredBackupNames("by-hand"))
	require.Empty(t, RecoveredBackupNames(""))
}
