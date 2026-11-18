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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// withSidecars runs the runs of the test on the fake sidecars, with the ClickHouse credentials of the operator set.
func withSidecars(t *testing.T, sc *fakeSidecars) {
	t.Helper()
	saved, savedPoll := runs.newSidecars, runs.poll
	runs.newSidecars = func(*model.RunSpec) (sidecars, error) {
		if err := clickHouseAccess(); err != nil {
			return nil, err
		}
		return sc, nil
	}
	runs.poll = time.Millisecond
	runtime := &chop.Config().Runtime
	explicit := runtime.ClickHouseAccessExplicit
	runtime.ClickHouseAccessExplicit = ptr.To(true)
	t.Cleanup(func() {
		runs.newSidecars, runs.poll = saved, savedPoll
		runtime.ClickHouseAccessExplicit = explicit
		runs.mu.Lock()
		runs.runs = map[runKey]*run{}
		runs.mu.Unlock()
	})
}

// reconcileUntil reconciles until done tells the resource is where the test wants it, as the notifications of the
// runs would.
func reconcileUntil(t *testing.T, reconcile func() (ctrl.Result, error), done func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !done() {
		require.True(t, time.Now().Before(deadline), "the resource never got where the test waits for it")
		_, err := reconcile()
		require.NoError(t, err)
		time.Sleep(time.Millisecond)
	}
}

// A backup runs on every cluster it targets, its status recording each run, and completes once each completed.
func TestBackupRunsOnEveryCluster(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 2, true))
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	c := newClient(t, nil, chb)
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(chi))
	reconcileUntil(t, func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("b1")) }, func() bool {
		return getBackup(t, c, "b1").Status.Phase == api.OperationPhaseCompleted
	})
	got := getBackup(t, c, "b1")
	require.Len(t, got.Status.Clusters, 1)
	cluster := got.Status.Clusters[0]
	require.Equal(t, api.BackupReasonCompleted, cluster.Reason)
	require.NotNil(t, cluster.Run)
	require.Equal(t, api.BackupRunOperationBackup, cluster.Run.Operation)
	require.Len(t, cluster.Run.Actions, 2)
	require.Equal(t, []string{"chi-demo-main-0-0: create_remote --rbac manual", "chi-demo-main-1-0: create_remote --rbac manual"}, sc.commands())
	require.Equal(t, api.BackupReasonStarted, condition(t, got.Status.Conditions, api.BackupConditionStarted).Reason)
	require.Equal(t, api.BackupReasonCompleted, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	require.NotEmpty(t, eventMessages(t, c, "OrchestratorBackupCompleted"), "the run posts its Events on the backup")
}

// Without ClickHouse credentials its configuration sets itself the operator drives no sidecar, the default ones
// included: the backup waits, telling why.
func TestBackupWaitsForClickHouseAccess(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chop.Config().Runtime.ClickHouseAccessExplicit = ptr.To(false)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	c := newClient(t, nil, newBackupCR("b1"))
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(chi))
	for i := 0; i < 2; i++ {
		_, err := controller.Reconcile(context.Background(), request("b1"))
		require.NoError(t, err)
	}
	got := getBackup(t, c, "b1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Equal(t, api.BackupReasonNoClickHouseAccess, got.Status.Clusters[0].Reason)
	require.Empty(t, sc.commands())
}

// A restore runs once its validation holds the schedules of its clusters, and completes.
func TestRestoreRuns(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	for _, host := range []string{"chi-demo-main-0-0", "chi-demo-main-0-1"} {
		sc.hosts[host] = &fakeHost{remote: []backupRow{{Name: "nightly-20260101-000000", DataSize: 1, MetadataSize: 1}}}
	}
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, newScheme(t), newFakeCR(chi))
	reconcileUntil(t, func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("r1")) }, func() bool {
		return getRestore(t, c, "r1").Status.Phase.IsTerminal()
	})
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase, got.Status.Clusters[0].Message)
	require.Contains(t, sc.commands(), "chi-demo-main-0-0: restore --data=1 --ignore-dependencies nightly-20260101-000000")
	require.Contains(t, sc.commands(), "chi-demo-main-0-1: restore --schema=1 --ignore-dependencies nightly-20260101-000000")
}

// A backup waits while a restore of its cluster runs: two operations never drive one cluster at once.
func TestBackupWaitsForARestoreOfItsCluster(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	restore := newRestoreCR("r1")
	restore.Status.Phase = api.OperationPhaseRunning
	restore.Status.Clusters = []api.OperationClusterStatus{{Name: "main", Phase: api.OperationPhaseRunning,
		Run: &api.BackupRunStatus{Operation: api.BackupRunOperationRestore}}}
	c := newClient(t, nil, newBackupCR("b1"), restore)
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(chi))
	for i := 0; i < 2; i++ {
		_, err := controller.Reconcile(context.Background(), request("b1"))
		require.NoError(t, err)
	}
	got := getBackup(t, c, "b1")
	require.Equal(t, api.BackupReasonOperationInProgress, got.Status.Clusters[0].Reason)
	require.Empty(t, sc.commands())
}

// A schedule runs at the slots of its finest entry on every cluster it backs up, no resource made for a run: the run
// lives in status.active, then in status.recentRuns, and the status of its cluster tells the backup it made. A slot
// before the schedule was created never runs.
func TestScheduleRunsAtItsSlots(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	chbs := newScheduleCR("nightly")
	chbs.Spec.BackupNamePrefix = "nightly"
	chbs.CreationTimestamp = meta.NewTime(time.Date(2026, time.November, 12, 2, 0, 0, 0, time.UTC))
	c := newClient(t, nil, chbs)
	controller := NewScheduleController(c, c, newScheme(t), newFakeCR(chi))
	clock := clocktesting.NewFakePassiveClock(time.Date(2026, time.November, 12, 2, 30, 0, 0, time.UTC))
	controller.clock = clock
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("nightly")) }
	result, err := reconcile()
	require.NoError(t, err)
	require.Empty(t, getSchedule(t, c, "nightly").Status.Active, "the slot of 03:00 is not due yet")
	require.Equal(t, time.Date(2026, time.November, 12, 3, 0, 0, 0, time.UTC), getSchedule(t, c, "nightly").Status.NextScheduleTime.Time.UTC())
	require.Greater(t, result.RequeueAfter, time.Duration(0))

	clock.SetTime(time.Date(2026, time.November, 12, 3, 0, 5, 0, time.UTC))
	_, err = reconcile()
	require.NoError(t, err)
	active := getSchedule(t, c, "nightly").Status.Active
	require.Len(t, active, 1)
	require.Equal(t, "nightly-20261112030000", active[0].BackupName)
	reconcileUntil(t, reconcile, func() bool {
		return len(getSchedule(t, c, "nightly").Status.RecentRuns) == 1
	})
	got := getSchedule(t, c, "nightly")
	require.Empty(t, got.Status.Active)
	require.Equal(t, api.OperationPhaseCompleted, got.Status.RecentRuns[0].Phase, got.Status.RecentRuns[0].Message)
	require.Nil(t, got.Status.RecentRuns[0].Run, "a finished run keeps no state to resume")
	require.Equal(t, "nightly-20261112030000", got.Status.Clusters[0].LastBackupName)
	require.Equal(t, "nightly-20261112030000", got.Status.LastBackupName)
	require.Equal(t, []string{"chi-demo-main-0-0"}, got.Status.Clusters[0].RetentionHosts)
	require.Equal(t, api.BackupReasonCompleted, condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded).Reason)
	require.Equal(t, []string{"chi-demo-main-0-0: create_remote --rbac nightly-20261112030000"}, sc.commands())

	// The slot that ran never runs again; the next one does
	_, err = reconcile()
	require.NoError(t, err)
	require.Len(t, sc.commands(), 1)
	clock.SetTime(time.Date(2026, time.November, 13, 3, 0, 1, 0, time.UTC))
	reconcileUntil(t, reconcile, func() bool { return len(getSchedule(t, c, "nightly").Status.RecentRuns) == 2 })
	require.True(t, strings.HasSuffix(sc.commands()[1], "create_remote --rbac nightly-20261113030000"))
	recent := getSchedule(t, c, "nightly").Status.RecentRuns
	require.Equal(t, "nightly-20261113030000", recent[0].BackupName, "newest first")
}

// A schedule held by a restore of its cluster runs no slot; a slot missed by more than startingDeadlineSeconds is
// skipped.
func TestScheduleHoldsForARestoreAndSkipsLateSlots(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	chbs := newScheduleCR("nightly")
	chbs.CreationTimestamp = meta.NewTime(time.Date(2026, time.November, 1, 0, 0, 0, 0, time.UTC))
	restore := newRestoreCR("r1")
	restore.Status.Phase = api.OperationPhasePending
	restore.Status.Clusters = []api.OperationClusterStatus{{Name: "main", Phase: api.OperationPhasePending}}
	c := newClient(t, nil, chbs, restore)
	controller := NewScheduleController(c, c, newScheme(t), newFakeCR(chi))
	controller.clock = clocktesting.NewFakePassiveClock(time.Date(2026, time.November, 12, 3, 0, 5, 0, time.UTC))
	_, err := controller.Reconcile(context.Background(), request("nightly"))
	require.NoError(t, err)
	got := getSchedule(t, c, "nightly")
	require.Empty(t, got.Status.Active)
	require.Equal(t, api.BackupReasonSuspendedForRestore, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)

	require.NoError(t, c.Delete(context.Background(), restore))
	got.Spec.StartingDeadlineSeconds = ptrInt64(60)
	require.NoError(t, c.Update(context.Background(), got))
	controller.clock = clocktesting.NewFakePassiveClock(time.Date(2026, time.November, 12, 3, 5, 0, 0, time.UTC))
	_, err = controller.Reconcile(context.Background(), request("nightly"))
	require.NoError(t, err)
	got = getSchedule(t, c, "nightly")
	require.Empty(t, got.Status.Active, "the slot is past its starting deadline")
	require.Equal(t, time.Date(2026, time.November, 12, 3, 0, 0, 0, time.UTC), got.Status.Clusters[0].LastScheduleTime.Time.UTC())
	require.NotEmpty(t, eventMessages(t, c, reasonScheduledRunSkipped))
	require.Empty(t, sc.commands())
}

func ptrInt64(v int64) *int64 {
	return &v
}

var _ client.Object = &api.ClickHouseBackup{}
