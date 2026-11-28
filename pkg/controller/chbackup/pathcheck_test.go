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
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

// storageSettings returns the generated configuration file Secret of the cluster of the installation, holding the
// settings that locate its storage.
func storageSettings(t *testing.T, chi *api.ClickHouseInstallation, cluster string, settings map[string]string) *core.Secret {
	t.Helper()
	secret := &core.Secret{Data: map[string][]byte{}}
	for key, value := range settings {
		secret.Data[key] = []byte(value)
	}
	secret.Name = managers.NewNameManager(managers.NameManagerTypeClickHouse).Name(interfaces.NameBackupConfigSecret,
		chi.GetAncestorT(), cluster)
	secret.Namespace = testNamespace
	return secret
}

// replicaSharedPaths are storage settings whose storage path the replicas of a shard share, {replica} being in the
// object disk path alone: the metadata of the backups of the replicas would be at one path.
var replicaSharedPaths = map[string]string{
	"REMOTE_STORAGE": "s3", "S3_BUCKET": "backups", "S3_PATH": "backup/{cluster}/{shard}",
	"S3_OBJECT_DISK_PATH": "object_disks/{cluster}/{shard}/{replica}",
}

// A backup of every replica whose replicas would write the backup of one name at one storage path is aborted before
// anything is sent to a sidecar, telling why in its status and in a Warning Event.
func TestBackupOfEveryReplicaAtOneStoragePathIsAborted(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	chb.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	c := newClient(t, nil, chb, storageSettings(t, chi, "main", replicaSharedPaths))
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(chi))
	reconcileUntil(t, func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("b1")) }, func() bool {
		return getBackup(t, c, "b1").Status.Phase.IsTerminal()
	})
	got := getBackup(t, c, "b1")
	require.Equal(t, api.OperationPhaseAborted, got.Status.Phase)
	cluster := got.Status.Clusters[0]
	require.Equal(t, api.BackupReasonStoragePathConflict, cluster.Reason)
	require.Contains(t, cluster.Message, "storage path 's3://backups/backup/main/0' is not unique: replicas chi-demo-main-0-0 and "+
		"chi-demo-main-0-1 of shard 0 of cluster main both write a backup of one name there")
	require.Nil(t, cluster.Run, "no run started")
	require.Empty(t, sc.commands())
	events := eventMessagesOfType(t, c, api.BackupReasonStoragePathConflict, core.EventTypeWarning)
	require.Len(t, events, 1)
	require.Contains(t, events[0], "give every replica a storage path of its own, e.g. with {replica}")
}

// A backup of every replica at paths of their own runs, a macro the operator does not define in a path warned of, in
// an Event, not refused.
func TestBackupOfEveryReplicaAtPathsOfTheirOwnRuns(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	chb.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	c := newClient(t, nil, chb, storageSettings(t, chi, "main", map[string]string{
		"REMOTE_STORAGE": "s3", "S3_BUCKET": "backups", "S3_PATH": "backup/{cluster}/{shard}/{replica}",
		"S3_OBJECT_DISK_PATH": "object_disks/{layer}/{shard}/{replica}",
	}))
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(chi))
	reconcileUntil(t, func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("b1")) }, func() bool {
		return getBackup(t, c, "b1").Status.Phase.IsTerminal()
	})
	got := getBackup(t, c, "b1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase, got.Status.Clusters[0].Message)
	require.Len(t, sc.commands(), 2, "both replicas back up")
	advice := eventMessagesOfType(t, c, "OrchestratorStoragePathAdvice", core.EventTypeWarning)
	require.Len(t, advice, 1)
	require.Contains(t, advice[0], "the storage path check skips the object disk path 'object_disks/{layer}/{shard}/{replica}' of "+
		"cluster main: it holds {layer}")
}

// A schedule of a backup of every replica whose replicas would write the backup of one name at one storage path is
// aborted, and runs nothing, until the paths change.
func TestScheduleOfEveryReplicaAtOneStoragePathIsAborted(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	chbs := newScheduleCR("nightly")
	chbs.Spec.BackupNamePrefix = "nightly"
	chbs.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	chbs.CreationTimestamp = meta.NewTime(time.Date(2026, time.November, 12, 2, 0, 0, 0, time.UTC))
	secret := storageSettings(t, chi, "main", replicaSharedPaths)
	c := newClient(t, nil, chbs, secret)
	controller := NewScheduleController(c, c, newScheme(t), newFakeCR(chi))
	controller.clock = clocktesting.NewFakePassiveClock(time.Date(2026, time.November, 12, 3, 0, 5, 0, time.UTC))
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("nightly")) }
	_, err := reconcile()
	require.NoError(t, err)
	got := getSchedule(t, c, "nightly")
	require.Equal(t, api.SchedulePhaseAborted, got.Status.Phase)
	require.Equal(t, api.BackupReasonStoragePathConflict, got.Status.Reason)
	require.Contains(t, got.Status.Message, "storage path 's3://backups/backup/main/0' is not unique: replicas chi-demo-main-0-0 and chi-demo-main-0-1")
	validated := condition(t, got.Status.Conditions, api.BackupConditionValidated)
	require.Equal(t, meta.ConditionFalse, validated.Status)
	require.Equal(t, api.BackupReasonStoragePathConflict, validated.Reason)
	require.Empty(t, got.Status.Active, "the backup at the scheduled time does not run")
	require.Len(t, eventMessagesOfType(t, c, reasonScheduleAborted, core.EventTypeWarning), 1)
	require.Empty(t, sc.commands())

	// Paths of their own for every replica release it
	fixed := storageSettings(t, chi, "main", map[string]string{
		"REMOTE_STORAGE": "s3", "S3_BUCKET": "backups", "S3_PATH": "backup/{cluster}/{shard}/{replica}",
		"S3_OBJECT_DISK_PATH": "object_disks/{cluster}/{shard}/{replica}",
	})
	stored := &core.Secret{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(secret), stored))
	stored.Data = fixed.Data
	require.NoError(t, c.Update(context.Background(), stored))
	reconcileUntil(t, reconcile, func() bool { return len(getSchedule(t, c, "nightly").Status.RecentRuns) == 1 })
	got = getSchedule(t, c, "nightly")
	require.Equal(t, api.SchedulePhaseActive, got.Status.Phase, got.Status.Message)
	require.Equal(t, api.OperationPhaseCompleted, got.Status.RecentRuns[0].Phase, got.Status.RecentRuns[0].Message)
	require.Len(t, sc.commands(), 2, "both replicas back up")
}
