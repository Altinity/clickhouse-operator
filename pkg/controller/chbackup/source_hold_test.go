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

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// sourceRestoreTarget is the installation the restores of these tests restore, from the backups of the test
// installation
const sourceRestoreTarget = "dr"

// sourceRestoreFixture returns an installation dr ready to be restored, the restore r1 of it from the backup of the
// test installation, which runs schedule s1, and the controllers of both.
func sourceRestoreFixture(t *testing.T, edit func(*api.ClickHouseRestore)) (*RestoreController, *ScheduleController, func(name string) *batch.CronJob, *fakeCR) {
	t.Helper()
	target := installationSpec(backupCluster("main", 2, true))
	target.Name, target.UID = sourceRestoreTarget, "dr-uid"
	cr := newFakeCR(readyInstallation(t), installationFrom(t, target, api.StatusCompleted))
	chr := newRestoreCR("r1")
	chr.Spec.ClickHouseInstallation = sourceRestoreTarget
	chr.Spec.SourceInstallation = testCHI
	if edit != nil {
		edit(chr)
	}
	c := newClient(t, nil, chr, newScheduleCR("s1"))
	restores := NewRestoreController(c, c, c.Scheme(), cr)
	schedules := NewScheduleController(c, c, c.Scheme(), cr)
	cronJob := func(name string) *batch.CronJob {
		cronJob := &batch.CronJob{}
		require.NoError(t, c.Get(context.Background(), key(name), cronJob))
		return cronJob
	}
	return restores, schedules, cronJob, cr
}

// The retention of a scheduled run of the installation a restore reads the backup of could delete that backup
// while the restore reads it: a validated restore holds the schedules of that installation, of its own namespace,
// as it holds those of the one it restores, and starts only once they are suspended for long enough.
func TestRestoreFromAnotherInstallationHoldsItsSchedules(t *testing.T) {
	restores, schedules, cronJob, _ := sourceRestoreFixture(t, nil)
	c := restores.Client
	reconcileSchedule := func() {
		_, err := schedules.Reconcile(context.Background(), request("s1"))
		require.NoError(t, err)
	}
	reconcileSchedule()
	require.False(t, ptr.Deref(cronJob("s1-main-backup").Spec.Suspend, true))

	validateRestore(t, c, restores, "r1")
	require.Equal(t, []reconcile.Request{request("s1")}, schedules.SchedulesForRestore(context.Background(), getRestore(t, c, "r1")),
		"a restore maps to the schedules of the installation it reads the backup of")
	reconcileSchedule()
	require.True(t, ptr.Deref(cronJob("s1-main-backup").Spec.Suspend, false))
	chbs := &api.ClickHouseBackupSchedule{}
	require.NoError(t, c.Get(context.Background(), key("s1"), chbs))
	ready := condition(t, chbs.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonSuspendedForRestore, ready.Reason)
	require.Contains(t, ready.Message, "ClickHouseRestore r1 restores ClickHouseInstallation dr from the backup nightly-20260101-000000 "+
		"of cluster main of ClickHouseInstallation demo")

	reconcileRestore(t, restores, "r1")
	waiting := condition(t, getRestore(t, c, "r1").Status.Conditions, api.BackupConditionJobCreated)
	require.Equal(t, api.BackupReasonWaitingForSchedules, waiting.Reason)
	require.Contains(t, waiting.Message, "ClickHouseInstallation demo", "the suspension of the schedules there is not old enough yet")
	require.Zero(t, jobCount(t, c))

	ageSuspensions(t, c)
	reconcileRestore(t, restores, "r1")
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r1").Status.Phase)

	finishJob(t, c, "r1-main-restore", batch.JobComplete, "")
	reconcileRestore(t, restores, "r1")
	require.Equal(t, api.OperationPhaseCompleted, getRestore(t, c, "r1").Status.Phase)
	reconcileSchedule()
	require.False(t, ptr.Deref(cronJob("s1-main-backup").Spec.Suspend, true), "a finished restore releases the schedules it read the backup of")
}

// A scheduled run of the installation whose backup a restore reads that started before the hold still runs its
// retention: the restore waits for it to finish as well.
func TestRestoreFromAnotherInstallationWaitsForARunThere(t *testing.T) {
	restores, schedules, _, _ := sourceRestoreFixture(t, nil)
	c := restores.Client
	run := jobOwnedBy(t, "s1-main-backup-29000000", newScheduleCR("s1"),
		model.Labels(api.ClickHouseBackupScheduleCRDResourceKind, "s1", testCHI, "main", testStorage))
	require.NoError(t, c.Create(context.Background(), run))
	_, err := schedules.Reconcile(context.Background(), request("s1"))
	require.NoError(t, err)
	validateRestore(t, c, restores, "r1")
	_, err = schedules.Reconcile(context.Background(), request("s1"))
	require.NoError(t, err)
	ageSuspensions(t, c)

	reconcileRestore(t, restores, "r1")
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Equal(t, api.BackupReasonOperationInProgress, got.Status.Clusters[0].Reason)
	require.Contains(t, got.Status.Clusters[0].Message, "ClickHouseInstallation demo")

	finishJob(t, c, "s1-main-backup-29000000", batch.JobComplete, "")
	reconcileRestore(t, restores, "r1")
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r1").Status.Phase)
}

// The installation whose backup a restore reads may be in the middle of a reconcile, or stuck there, which the
// restore does not wait for: its schedules are held all the same. A restore that waits for the installation it
// restores, which may never come to run it, holds them no longer.
func TestRestoreFromAnotherInstallationHoldsItsSchedulesWhateverTheyWaitFor(t *testing.T) {
	restores, schedules, cronJob, cr := sourceRestoreFixture(t, nil)
	c := restores.Client
	reconcileSchedule := func() {
		_, err := schedules.Reconcile(context.Background(), request("s1"))
		require.NoError(t, err)
	}
	reconcileSchedule()
	cr.set(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
	validateRestore(t, c, restores, "r1")
	reconcileSchedule()
	require.True(t, ptr.Deref(cronJob("s1-main-backup").Spec.Suspend, false), "held through the reconcile of the installation it backs up")
	chbs := &api.ClickHouseBackupSchedule{}
	require.NoError(t, c.Get(context.Background(), key("s1"), chbs))
	require.Equal(t, api.BackupReasonSuspendedForRestore, condition(t, chbs.Status.Conditions, api.BackupConditionReady).Reason,
		"the schedule tells the hold, not the reconcile")

	cr.set(readyInstallation(t))
	reconcileSchedule()
	require.True(t, ptr.Deref(cronJob("s1-main-backup").Spec.Suspend, false))
	target := installationSpec(backupCluster("main", 2, true))
	target.Name, target.UID = sourceRestoreTarget, "dr-uid"
	cr.set(installationFrom(t, target, api.StatusInProgress))
	reconcileRestore(t, restores, "r1")
	require.Equal(t, api.BackupReasonCHINotReady, getRestore(t, c, "r1").Status.Clusters[0].Reason)
	reconcileSchedule()
	require.False(t, ptr.Deref(cronJob("s1-main-backup").Spec.Suspend, true), "released while the restore waits for its own installation")
}

// A restore that waited for the installation it restores held the schedules of the one whose backup it reads no
// longer, and a schedule may still read that status once the installation is ready: the restore creates its Job only
// from a status that held them again, recorded by a reconcile before.
func TestRestoreFromAnotherInstallationHoldsItsSchedulesAgainBeforeItsJob(t *testing.T) {
	restores, schedules, cronJob, cr := sourceRestoreFixture(t, nil)
	c := restores.Client
	reconcileSchedule := func() {
		_, err := schedules.Reconcile(context.Background(), request("s1"))
		require.NoError(t, err)
	}
	reconcileSchedule()
	validateRestore(t, c, restores, "r1")
	reconcileSchedule()
	ageSuspensions(t, c)

	target := installationSpec(backupCluster("main", 2, true))
	target.Name, target.UID = sourceRestoreTarget, "dr-uid"
	cr.set(installationFrom(t, target, api.StatusInProgress))
	reconcileRestore(t, restores, "r1")
	require.Equal(t, api.BackupReasonCHINotReady, getRestore(t, c, "r1").Status.Clusters[0].Reason)

	cr.set(installationFrom(t, target, api.StatusCompleted))
	reconcileRestore(t, restores, "r1")
	got := getRestore(t, c, "r1")
	require.Zero(t, jobCount(t, c), "a schedule reading the status before could resume the CronJob under the Job")
	require.Equal(t, api.BackupReasonWaitingForSchedules, got.Status.Clusters[0].Reason)
	require.Contains(t, got.Status.Clusters[0].Message, "ClickHouseInstallation demo")

	reconcileSchedule()
	require.True(t, ptr.Deref(cronJob("s1-main-backup").Spec.Suspend, false))
	reconcileRestore(t, restores, "r1")
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r1").Status.Phase)
}

// A restore that ends while the installation whose backup it read is in the middle of a reconcile holds the schedules
// there no longer, while the reconcile keeps the CronJobs it held suspended until it completes: the schedule tells so,
// along with what holds it now, rather than the restore, or runs it does not make, and announces the resumption once
// the reconcile completes. A schedule its spec suspends tells that alone.
func TestRestoreFromAnotherInstallationEndsThroughAReconcile(t *testing.T) {
	const released = "CronJob s1-cold-backup stays suspended until ClickHouseInstallation demo completes its reconcile: " +
		"no restore holds it any longer"
	for name, tc := range map[string]struct {
		// specSuspended tells that the spec suspends the schedule
		specSuspended bool
		// restored tells that a restore of cluster hot of the installation itself holds the schedule there now
		restored bool
		reason   string
		messages []string
	}{
		"the restore ended": {reason: api.BackupReasonSuspendedForRestore, messages: []string{released}},
		"another restore holds it now": {restored: true, reason: api.BackupReasonSuspendedForRestore, messages: []string{
			"a restore of cluster hot of ClickHouseInstallation demo is in progress", released}},
		"the spec suspends it": {specSuspended: true, reason: api.BackupReasonSuspended},
	} {
		t.Run(name, func(t *testing.T) {
			target := installationSpec(backupCluster("cold", 1, true))
			target.Name, target.UID = sourceRestoreTarget, "dr-uid"
			cr := newFakeCR(twoClusters(t, api.StatusCompleted), installationFrom(t, target, api.StatusCompleted))
			chr := newRestoreCR("r1")
			chr.Spec.ClickHouseInstallation = sourceRestoreTarget
			chr.Spec.SourceInstallation = testCHI
			own := newRestoreCR("r2")
			own.Spec.Cluster = "hot"
			chbs := newScheduleCR("s1")
			if tc.specSuspended {
				chbs.Spec.Suspend = types.NewStringBool(true)
			}
			c := newClient(t, nil, chr, own, chbs)
			restores := NewRestoreController(c, c, c.Scheme(), cr)
			schedules := NewScheduleController(c, c, c.Scheme(), cr)
			reconcileSchedule := func() *api.ClickHouseBackupSchedule {
				_, err := schedules.Reconcile(context.Background(), request("s1"))
				require.NoError(t, err)
				chbs := &api.ClickHouseBackupSchedule{}
				require.NoError(t, c.Get(context.Background(), key("s1"), chbs))
				return chbs
			}
			suspended := func(name string) bool {
				cronJob := &batch.CronJob{}
				require.NoError(t, c.Get(context.Background(), key(name), cronJob))
				return ptr.Deref(cronJob.Spec.Suspend, false)
			}
			reconcileSchedule()
			validateRestore(t, c, restores, "r1")
			if tc.restored {
				validateRestore(t, c, restores, "r2")
			}
			reconcileSchedule()
			require.True(t, suspended("s1-cold-backup"))
			ageSuspensions(t, c)
			reconcileRestore(t, restores, "r1")
			require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r1").Status.Phase)

			cr.set(twoClusters(t, api.StatusInProgress))
			finishJob(t, c, "r1-cold-restore", batch.JobComplete, "")
			reconcileRestore(t, restores, "r1")
			require.Equal(t, api.OperationPhaseCompleted, getRestore(t, c, "r1").Status.Phase)
			ready := condition(t, reconcileSchedule().Status.Conditions, api.BackupConditionReady)
			require.Equal(t, meta.ConditionFalse, ready.Status)
			require.Equal(t, tc.reason, ready.Reason, ready.Message)
			for _, message := range tc.messages {
				require.Contains(t, ready.Message, message)
			}
			require.NotContains(t, ready.Message, "r1")
			require.True(t, suspended("s1-cold-backup"))
			require.Equal(t, tc.specSuspended || tc.restored, suspended("s1-hot-backup"))
			if tc.specSuspended || tc.restored {
				return
			}

			cr.set(twoClusters(t, api.StatusCompleted))
			require.Equal(t, api.BackupReasonScheduled, condition(t, reconcileSchedule().Status.Conditions, api.BackupConditionReady).Reason)
			require.False(t, suspended("s1-cold-backup"))
			require.Contains(t, eventMessages(t, c, api.BackupReasonScheduled), "scheduled backups resumed")
		})
	}
}

// A restore that reads the backup of an installation of another namespace holds nothing there: a restore of one
// namespace never stops the backups of another. Nor does a restore that failed, which changed nothing there, nor one
// that finished on the cluster whose backup it read while it runs on another.
func TestRestoreHoldsNoScheduleItDoesNotRead(t *testing.T) {
	for name, edit := range map[string]func(*api.ClickHouseRestore){
		"another namespace": func(chr *api.ClickHouseRestore) { chr.Spec.SourceNamespace = "elsewhere" },
		"a failed restore": func(chr *api.ClickHouseRestore) {
			chr.Status.Clusters = []api.OperationClusterStatus{{Name: "main", Phase: api.OperationPhaseFailed, JobName: "r1-main-restore"}}
			chr.Status.Phase = api.OperationPhaseFailed
		},
		"a restore finished on the cluster": func(chr *api.ClickHouseRestore) {
			chr.Status.Clusters = []api.OperationClusterStatus{
				{Name: "main", Phase: api.OperationPhaseCompleted, JobName: "r1-main-restore"},
				{Name: "second", Phase: api.OperationPhaseRunning, JobName: "r1-second-restore"},
			}
			chr.Status.Phase = api.OperationPhaseRunning
		},
	} {
		t.Run(name, func(t *testing.T) {
			_, schedules, cronJob, _ := sourceRestoreFixture(t, edit)
			_, err := schedules.Reconcile(context.Background(), request("s1"))
			require.NoError(t, err)
			require.False(t, ptr.Deref(cronJob("s1-main-backup").Spec.Suspend, true))
		})
	}
}
