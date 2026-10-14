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
	batch "k8s.io/api/batch/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// The backup to recover the installation from is one every cluster the schedule backs up holds. A
// cluster that joins the schedule holds none of the backups made before, so there is none until every
// cluster made one; while the runs of one slot complete one after the other the one before stays; a
// cluster whose runs keep failing stays behind while the others make newer backups and prune the one
// they all made, so there is none until it catches up.
func TestScheduleLastBackupNameIsOneEveryClusterHolds(t *testing.T) {
	name := func(due time.Time) string {
		return "ns-demo-s1-" + due.UTC().Format("20060102-150405")
	}
	slot := func(n int) time.Time {
		return firstRunDue.Add(time.Duration(n) * time.Hour)
	}
	completeRun := func(f *scheduleFixture, cronJob string, due time.Time) {
		run := f.cronJobRunAt(cronJob, due)
		finishJobAt(t, f.client, run.Name, batch.JobComplete, "", due.Add(time.Minute))
	}
	failRun := func(f *scheduleFixture, cronJob string, due time.Time) {
		run := f.cronJobRunAt(cronJob, due)
		finishJobAt(t, f.client, run.Name, batch.JobFailed, batch.JobReasonBackoffLimitExceeded, due.Add(2*time.Minute))
	}

	t.Run("a cluster joins", func(t *testing.T) {
		cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("hot", 2, true)))
		f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"))
		f.reconcile("s1")
		f.ranAt(slot(0))
		f.reconcile("s1")
		require.Equal(t, name(slot(0)), f.schedule("s1").Status.LastBackupName)

		cr.set(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, true)))
		f.reconcile("s1")
		require.Empty(t, f.schedule("s1").Status.LastBackupName, "cold holds no backup made before it joined")

		completeRun(f, "s1-hot-backup", slot(1))
		completeRun(f, "s1-cold-backup", slot(1))
		f.reconcile("s1")
		require.Equal(t, name(slot(1)), f.schedule("s1").Status.LastBackupName)
	})

	t.Run("a cluster keeps failing", func(t *testing.T) {
		f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newScheduleCR("s1"))
		f.reconcile("s1")
		f.ranAt(slot(0))
		f.reconcile("s1")
		require.Equal(t, name(slot(0)), f.schedule("s1").Status.LastBackupName)

		completeRun(f, "s1-hot-backup", slot(1))
		f.cronJobRunAt("s1-cold-backup", slot(1))
		f.reconcile("s1")
		require.Equal(t, name(slot(0)), f.schedule("s1").Status.LastBackupName, "cold is still running the slot hot completed")

		failRun(f, "s1-cold-backup", slot(2))
		completeRun(f, "s1-hot-backup", slot(2))
		f.reconcile("s1")
		require.Empty(t, f.schedule("s1").Status.LastBackupName, "the retention of hot prunes what cold made last")
		completeRun(f, "s1-hot-backup", slot(3))
		failRun(f, "s1-cold-backup", slot(3))
		f.reconcile("s1")
		require.Empty(t, f.schedule("s1").Status.LastBackupName)

		f.ranAt(slot(4))
		f.reconcile("s1")
		require.Equal(t, name(slot(4)), f.schedule("s1").Status.LastBackupName)
	})
}

// A recovered installation typically runs where the installation its backup came from runs, with the
// same namespace, name and storage paths. Any schedule of either that defaults its backup name
// prefix may run on a storage path a schedule of the other runs on: every such schedule of the recovered
// installation is held, whatever cluster it backs up, those the recovery leaves out included, and from
// the moment the recovery is asked for until the prefix is set.
func TestScheduleHoldsEveryDefaultPrefixOfARecoveredInstallation(t *testing.T) {
	const source = "ns-demo-nightly-20261004-020000"
	requireHeld := func(t *testing.T, f *scheduleFixture, schedule, cronJob string) {
		t.Helper()
		require.True(t, ptr.Deref(f.cronJob(cronJob).Spec.Suspend, false))
		ready := condition(t, f.schedule(schedule).Status.Conditions, api.BackupConditionReady)
		require.Equal(t, api.BackupReasonSharesSourcePrefix, ready.Reason)
		require.Contains(t, ready.Message, "ClickHouseInstallation demo is, or is to be, recovered from backup "+source)
		require.Contains(t, ready.Message, "backupTemplate.backupNamePrefix")
	}

	t.Run("a cluster the recovery leaves out", func(t *testing.T) {
		cold := backupCluster("cold", 1, true)
		cold.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString("")}
		spec := withRecovery(installationSpec(backupCluster("hot", 2, true), cold), source)
		recovering := installationFrom(t, spec, api.StatusCompleted)
		chbs := newScheduleCR("s1")
		chbs.Spec.Cluster = "cold"
		cr := newFakeCR(recovering)
		f := newScheduleFixture(t, cr, nil, chbs)

		f.reconcile("s1")
		requireHeld(t, f, "s1", "s1-cold-backup")

		cr.set(withAnnotations(recovering, map[string]string{api.AnnotationRecoveredFrom: "hot:" + source}))
		f.reconcile("s1")
		requireHeld(t, f, "s1", "s1-cold-backup")
	})

	t.Run("a schedule of another name", func(t *testing.T) {
		chi := withAnnotations(readyInstallation(t), map[string]string{api.AnnotationRecoveredFrom: source})
		f := newScheduleFixture(t, newFakeCR(chi), nil, newScheduleCR("hourly"))

		f.reconcile("hourly")
		requireHeld(t, f, "hourly", "hourly-main-backup")

		hourly := f.schedule("hourly")
		hourly.Spec.BackupTemplate.BackupNamePrefix = model.BackupNamePrefix(hourly)
		require.NoError(t, f.client.Update(context.Background(), hourly))
		f.reconcile("hourly")
		require.False(t, ptr.Deref(f.cronJob("hourly-main-backup").Spec.Suspend, false), "an explicit prefix, even the default one, releases it")
		require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("hourly")))
	})

	t.Run("a backup of no schedule of the installation", func(t *testing.T) {
		chi := withAnnotations(readyInstallation(t), map[string]string{api.AnnotationRecoveredFrom: "nightly-20261004-020000"})
		f := newScheduleFixture(t, newFakeCR(chi), nil, newScheduleCR("hourly"))
		f.reconcile("hourly")
		require.False(t, ptr.Deref(f.cronJob("hourly-main-backup").Spec.Suspend, false))
	})
}

// A restore waits until the suspension of the CronJobs it holds is as old as the CronJob controller
// needs to see it, which a record rounded to the second would cut short by up to a second.
func TestScheduleRecordsASuspensionToTheNanosecond(t *testing.T) {
	cronJob := &batch.CronJob{}
	recordSuspension(cronJob)
	require.Regexp(t, `\.[0-9]+Z$`, cronJob.Annotations[model.AnnotationCronJobSuspendedAt])
	suspended, recorded := suspendedAt(cronJob)
	require.True(t, recorded)
	require.WithinDuration(t, time.Now(), suspended, time.Second)

	cronJob.Spec.Suspend = ptr.To(true)
	cronJob.Annotations[model.AnnotationCronJobSuspendedAt] = time.Now().Add(-pendingRequeueInterval + 300*time.Millisecond).UTC().Format(time.RFC3339Nano)
	require.Less(t, suspendedFor(cronJob), pendingRequeueInterval)
}

// A record of a suspension that cannot be read is as good as none: a restore would wait on it forever.
// The schedule records the suspension anew, whether it applies its CronJob or holds it.
func TestScheduleRewritesASuspensionRecordItCannotRead(t *testing.T) {
	garble := func(t *testing.T, f *scheduleFixture) {
		t.Helper()
		cronJob := f.cronJob("s1-main-backup")
		require.True(t, ptr.Deref(cronJob.Spec.Suspend, false))
		cronJob.Annotations[model.AnnotationCronJobSuspendedAt] = "yesterday"
		require.NoError(t, f.client.Update(context.Background(), cronJob))
	}
	requireRecorded := func(t *testing.T, f *scheduleFixture) {
		t.Helper()
		suspended, recorded := suspendedAt(f.cronJob("s1-main-backup"))
		require.True(t, recorded)
		require.WithinDuration(t, time.Now(), suspended, time.Minute)
	}

	t.Run("applied", func(t *testing.T) {
		restore := newRestoreCR("r1")
		newRestoreOperation(restore).pin([]string{"main"})
		f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"), restore)
		f.reconcile("s1")
		garble(t, f)
		f.reconcile("s1")
		requireRecorded(t, f)
	})

	t.Run("held", func(t *testing.T) {
		f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"), failedRestore("r1", api.BackupReasonJobFailed))
		f.reconcile("s1")
		garble(t, f)
		invalid := f.schedule("s1")
		invalid.Spec.BackupTemplate.BackupNamePrefix = "-invalid"
		require.NoError(t, f.client.Update(context.Background(), invalid))
		f.reconcile("s1")
		requireRecorded(t, f)
	})
}

// While its installation reconciles, a schedule holds its CronJobs as they are and adds its clusters up
// from them and from its status. A cluster whose CronJob name another schedule holds runs no backup, has
// an entry and no CronJob, and keeps the schedule from naming a backup every cluster holds, as it does
// while the installation is ready.
func TestScheduleHeldWhileReconcilingCountsAClusterItCannotRun(t *testing.T) {
	cr := newFakeCR(collidingInstallation(t, backupCluster("d", 1, true)))
	other := newScheduleCR("a-b")
	other.Spec.Cluster = "c"
	f := newScheduleFixture(t, cr, nil, other, newScheduleCR("a"))
	f.reconcile("a-b")
	f.reconcile("a")
	for _, name := range []string{"a-c-backup", "a-d-backup"} {
		run := f.cronJobRunAt(name, firstRunDue)
		finishJobAt(t, f.client, run.Name, batch.JobComplete, "", firstRunDue.Add(time.Minute))
	}
	f.reconcile("a")
	require.Empty(t, f.schedule("a").Status.LastBackupName, "cluster b-c made no backup")

	cr.chis[key(testCHI).String()].Status.Status = api.StatusInProgress
	require.Equal(t, pendingRequeueInterval, f.reconcile("a").RequeueAfter, "the schedule waits for the installation")
	got := f.schedule("a")
	require.Empty(t, got.Status.LastBackupName, "cluster b-c still made no backup")
	require.Nil(t, got.Status.LastSuccessfulTime)
}

// A cluster that left the schedule keeps its entry for the hosts retention no longer prunes, without a
// CronJob: it counts neither while the installation is ready nor while it reconciles.
func TestScheduleHeldWhileReconcilingLeavesOutAClusterThatLeft(t *testing.T) {
	left := meta.NewTime(firstRunDue.Add(-24 * time.Hour))
	chbs := newScheduleCR("s1")
	chbs.Spec.Retention.KeepLast = ptr.To(int32(3))
	chbs.Status.Clusters = []api.ScheduleClusterStatus{{
		Name: "gone", CronJobName: "s1-gone-backup", LastSuccessfulTime: &left, LastBackupName: "ns-demo-s1-20260101-030000",
		UnprunedHosts: []string{"chi-demo-gone-0-0"},
	}}
	cr := newFakeCR(twoClusters(t, api.StatusCompleted))
	f := newScheduleFixture(t, cr, nil, chbs)
	f.reconcile("s1")
	f.ranAt(firstRunDue)
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, "", findScheduleCluster(got, "gone").CronJobName, "the cluster left, and runs the schedule on no CronJob")
	succeeded := got.Status.LastSuccessfulTime
	require.True(t, succeeded.After(left.Time))

	cr.chis[key(testCHI).String()].Status.Status = api.StatusInProgress
	require.Equal(t, pendingRequeueInterval, f.reconcile("s1").RequeueAfter, "the schedule waits for the installation")
	got = f.schedule("s1")
	require.True(t, succeeded.Equal(got.Status.LastSuccessfulTime), "the cluster that left does not count")
	require.Equal(t, "ns-demo-s1-20260102-030000", got.Status.LastBackupName)
}

// Through a reconcile a schedule tells a hold of its own cause first: the guard of a shared prefix and a hold that
// needs action stay what Ready tells, as when the schedule applies its CronJobs, while a restore hold found now
// replaces any other hold found before.
func TestScheduleTellsItsHoldThroughAReconcile(t *testing.T) {
	restore := &pending{reason: api.BackupReasonSuspendedForRestore, message: "restore"}
	before := &pending{reason: api.BackupReasonSuspendedForRestore, message: "before"}
	prefix := &pending{reason: api.BackupReasonSharesSourcePrefix}
	conflict := &pending{reason: api.BackupReasonNameConflict}
	failed := &pending{reason: api.BackupReasonSuspendedForFailedRestore}
	collected := &pending{reason: api.BackupReasonWaitingForGarbageCollection}
	for name, tc := range map[string]struct {
		kept, restored, want *pending
	}{
		"a shared prefix":        {kept: prefix, restored: restore, want: prefix},
		"a name conflict":        {kept: conflict, restored: restore, want: conflict},
		"a failed restore":       {kept: failed, restored: restore, want: failed},
		"a restore found before": {kept: before, restored: restore, want: restore},
		"a garbage collection":   {kept: collected, restored: restore, want: restore},
		"no hold before":         {restored: restore, want: restore},
		"no restore hold now":    {kept: before, want: before},
		"no hold":                {},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, heldThroughReconcile(tc.kept, tc.restored))
		})
	}
}
