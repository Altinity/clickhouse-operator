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
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	toolsCache "k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/event"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func trim(t *testing.T, chi *api.ClickHouseInstallation) *api.ClickHouseInstallation {
	t.Helper()
	obj, err := TrimInstallation(chi)
	require.NoError(t, err)
	trimmed, ok := obj.(*api.ClickHouseInstallation)
	require.True(t, ok)
	return trimmed
}

// The backup cache holds installations trimmed to what the watches read. Everything the
// predicates, the mapping to schedules and the ownership of the recovery restores rely on must
// survive, the recoveries the spec names included; the rest of the spec, the normalized state and
// kubectl's copy of the spec must not.
func TestTrimInstallation(t *testing.T) {
	cold := backupCluster("cold", 1, true)
	cold.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString("cold-20261001-030000")}
	chi := installationFrom(t, withRecovery(installationSpec(backupCluster("main", 2, true), cold), recoveryBackup), api.StatusCompleted)
	chi.ResourceVersion = "42"
	chi.Generation = 3
	chi.Labels = map[string]string{"team": "db"}
	chi.Annotations = map[string]string{
		api.AnnotationRecoveredFrom:      recoveryBackup,
		core.LastAppliedConfigAnnotation: `{"spec":"the whole spec"}`,
	}
	chi.Finalizers = []string{"finalizer.clickhouseinstallation.altinity.com"}
	chi.ManagedFields = []meta.ManagedFieldsEntry{{Manager: "kubectl"}}
	chi.EnsureStatus().HostsCount = 4

	trimmed := trim(t, chi)
	require.Equal(t, chi.Name, trimmed.Name)
	require.Equal(t, chi.Namespace, trimmed.Namespace)
	require.Equal(t, chi.UID, trimmed.UID, "the recovery restores are owned by the installation")
	require.Equal(t, "42", trimmed.ResourceVersion, "the recovery is recorded with an optimistic lock")
	require.Equal(t, chi.Labels, trimmed.Labels)
	require.Equal(t, chi.Finalizers, trimmed.Finalizers)
	require.Equal(t, map[string]string{api.AnnotationRecoveredFrom: recoveryBackup}, trimmed.Annotations)
	require.Nil(t, trimmed.ManagedFields)
	require.Equal(t, &api.ClusterBackup{Recovery: &api.BackupRecovery{BackupName: types.NewString(recoveryBackup)}}, trimmed.Spec.Backup,
		"the recovery the installation level names, and nothing else of the backup settings")
	require.Equal(t, []*api.Cluster{{Name: "cold", Backup: &api.ClusterBackup{Recovery: cold.Backup.Recovery}}},
		trimmed.Spec.Configuration.Clusters, "the clusters that name a recovery of their own, with that alone")
	require.True(t, trimmed.RequestsBackupRecovery())
	require.Nil(t, trimmed.GetAncestorT())
	require.Equal(t, api.StatusCompleted, trimmed.Status.GetStatus())
	require.Zero(t, trimmed.Status.HostsCount)

	require.Equal(t, trimmed, trim(t, trimmed), "a cache transform may see an object twice")
	tombstone := toolsCache.DeletedFinalStateUnknown{Key: "ns/demo", Obj: chi}
	passed, err := TrimInstallation(tombstone)
	require.NoError(t, err)
	require.Equal(t, tombstone, passed)

	inProgress := trim(t, installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
	require.True(t, CHITopologyPredicate().Update(event.UpdateEvent{ObjectOld: inProgress, ObjectNew: trimmed}),
		"a trimmed installation still tells a completed reconcile")
	delete(trimmed.Annotations, api.AnnotationRecoveredFrom)
	require.True(t, RecoveryPredicate().Create(event.CreateEvent{Object: trimmed}), "a trimmed installation still tells a recovery")

	plain := trim(t, readyInstallation(t))
	require.Nil(t, plain.Spec.Backup, "an installation without recovery keeps no spec")
	require.Nil(t, plain.Spec.Configuration)
}

// A recovery may be named by a cluster alone, with none at the installation level: the installation
// asks for a recovery all the same, and the trimmed installation the cache holds still tells it. A
// cluster that opts out with "" asks for none.
func TestClusterOnlyRecovery(t *testing.T) {
	cold := backupCluster("cold", 1, true)
	cold.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString("cold-20261001-030000")}
	chi := installationFrom(t, installationSpec(backupCluster("main", 2, true), cold), api.StatusCompleted)
	require.Nil(t, chi.Spec.Backup.GetRecovery(), "fixture: no recovery at the installation level")
	require.True(t, chi.RequestsBackupRecovery())
	require.True(t, wantsRecovery(chi))
	require.True(t, RecoveryPredicate().Create(event.CreateEvent{Object: chi}))

	trimmed := trim(t, chi)
	require.Nil(t, trimmed.Spec.Backup)
	require.Equal(t, []*api.Cluster{{Name: "cold", Backup: &api.ClusterBackup{Recovery: cold.Backup.Recovery}}},
		trimmed.Spec.Configuration.Clusters)
	require.True(t, RecoveryPredicate().Create(event.CreateEvent{Object: trimmed}), "a trimmed installation still tells the recovery")

	cold.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString("")}
	optedOut := installationFrom(t, installationSpec(backupCluster("main", 2, true), cold), api.StatusCompleted)
	require.False(t, optedOut.RequestsBackupRecovery())
	require.False(t, RecoveryPredicate().Create(event.CreateEvent{Object: optedOut}))
	require.Nil(t, trim(t, optedOut).Spec.Configuration)
}
