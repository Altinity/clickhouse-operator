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
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

func reconcileRestore(t *testing.T, controller *RestoreController, name string) ctrl.Result {
	t.Helper()
	result, err := controller.Reconcile(context.Background(), request(name))
	require.NoError(t, err)
	return result
}

func getRestore(t *testing.T, c client.Client, name string) *api.ClickHouseRestore {
	t.Helper()
	chr := &api.ClickHouseRestore{}
	require.NoError(t, c.Get(context.Background(), key(name), chr))
	return chr
}

// validateRestore runs the reconcile that validates a restore its installation can run. It
// records the hold of the installation's schedules and creates no Job: the schedules may not have
// seen the hold yet.
func validateRestore(t *testing.T, c client.Client, controller *RestoreController, name string) {
	t.Helper()
	jobs := jobCount(t, c)
	require.Equal(t, pendingRequeueInterval, reconcileRestore(t, controller, name).RequeueAfter)
	got := getRestore(t, c, name)
	require.True(t, holdsSchedules(got))
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Equal(t, api.BackupReasonWaitingForSchedules, condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Reason)
	require.Equal(t, jobs, jobCount(t, c), "the reconcile that validates a restore creates no Job")
}

// startRestore validates a restore its installation can run, then runs the reconcile that creates
// its Job.
func startRestore(t *testing.T, c client.Client, controller *RestoreController, name string) {
	t.Helper()
	validateRestore(t, c, controller, name)
	reconcileRestore(t, controller, name)
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, name).Status.Phase)
}

func TestRestoreSkipsUnwatchedNamespaces(t *testing.T) {
	watchOnly(t, "elsewhere")
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))

	reconcileRestore(t, controller, "r1")
	require.Zero(t, jobCount(t, c))
	require.Empty(t, getRestore(t, c, "r1").Status.Phase)
}

func TestRestoreRejectsInvalidSpec(t *testing.T) {
	chr := newRestoreCR("r1")
	chr.Spec.BackupName = ""
	c := newClient(t, nil, chr)
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))

	require.Equal(t, ctrl.Result{}, reconcileRestore(t, controller, "r1"))
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Equal(t, api.BackupReasonInvalidSpec, condition(t, got.Status.Conditions, api.BackupConditionValidated).Reason)
	require.Zero(t, jobCount(t, c))
}

func TestRestoreRunsToCompletion(t *testing.T) {
	cr := newFakeCR(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), cr)

	require.NotZero(t, reconcileRestore(t, controller, "r1").RequeueAfter)
	require.Equal(t, api.BackupReasonCHINotReady, condition(t, getRestore(t, c, "r1").Status.Conditions, api.BackupConditionValidated).Reason)
	require.Zero(t, jobCount(t, c), "a restore waits for the installation to complete its reconcile")

	cr.set(readyInstallation(t))
	startRestore(t, c, controller, "r1")
	got := getRestore(t, c, "r1")
	require.Equal(t, "r1-main-restore", got.Status.Clusters[0].JobName)
	require.Equal(t, testStorage, got.Status.Clusters[0].Storage, "the storage the Job reads the backup from")
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key("r1-main-restore"), job))
	require.True(t, meta.IsControlledBy(job, got))
	require.Equal(t, model.Labels(api.ClickHouseRestoreCRDResourceKind, "r1", testCHI, "main", testStorage), job.Labels)

	cr.set(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
	finishJob(t, c, "r1-main-restore", batch.JobComplete, "")
	reconcileRestore(t, controller, "r1")
	got = getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase, "once its Job exists a restore is tracked whatever the installation does")
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionReady).Status)
	require.Equal(t, 1, eventCount(t, c, api.BackupReasonJobCreated))
	require.Equal(t, 1, eventCount(t, c, api.BackupReasonCompleted))
}

// Running a restore twice would drop data restored meanwhile, so a restore whose Job vanished is
// failed for good - never started again.
func TestRestoreIsNeverRerun(t *testing.T) {
	chr := newRestoreCR("r1")
	chr.Status.Phase = api.OperationPhaseRunning
	chr.Status.Clusters = runningOn("r1-main-restore")
	c := newClient(t, nil, chr)
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))

	reconcileRestore(t, controller, "r1")
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.BackupReasonJobLost, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)

	reconcileRestore(t, controller, "r1")
	require.Zero(t, jobCount(t, c), "a lost restore must never get a new Job")
}

func TestRestoreRefusesAStoppedInstallation(t *testing.T) {
	for name, chi := range stoppedInstallations(t) {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, nil, newRestoreCR("r1"))
			controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(chi))

			require.Equal(t, targetRequeueInterval, reconcileRestore(t, controller, "r1").RequeueAfter)
			require.Equal(t, api.BackupReasonCHIStopped, condition(t, getRestore(t, c, "r1").Status.Conditions, api.BackupConditionValidated).Reason)
			require.Zero(t, jobCount(t, c))
		})
	}
}

func TestRestoreFailsAJobThatNeverStarts(t *testing.T) {
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
	startRestore(t, c, controller, "r1")
	stallJob(t, c, "r1-main-restore", neverStarted)

	require.Equal(t, ctrl.Result{}, reconcileRestore(t, controller, "r1"))
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.BackupReasonJobNotStarted, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	requireStopped(t, c, "r1-main-restore")
}

// A CronJob fires on its own schedule: checking for running operations cannot stop it from
// starting a backup right after the check. A restore therefore starts only once every backup
// schedule of its cluster is suspended, which its validation makes them do, and has been for long
// enough for the CronJob controller, which acts on its own cache, to have seen the suspension.
func TestRestoreWaitsForTheSchedulesToBeSuspended(t *testing.T) {
	cr := newFakeCR(readyInstallation(t))
	other := newScheduleCR("other")
	other.Spec.ClickHouseInstallation = "another-chi"
	c := newClient(t, nil, newRestoreCR("r1"), newScheduleCR("s1"), other)
	restores := NewRestoreController(c, c, c.Scheme(), cr)
	schedules := NewScheduleController(c, c, c.Scheme(), cr)
	reconcileSchedule := func(name string) {
		_, err := schedules.Reconcile(context.Background(), request(name))
		require.NoError(t, err)
	}
	reconcileSchedule("s1")
	reconcileSchedule("other")

	validateRestore(t, c, restores, "r1")
	require.Equal(t, pendingRequeueInterval, reconcileRestore(t, restores, "r1").RequeueAfter)
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionValidated).Status)
	require.Equal(t, api.BackupReasonWaitingForSchedules, condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Reason)
	require.Zero(t, jobCount(t, c), "a recorded hold starts nothing while a CronJob still runs")

	require.Equal(t, []reconcile.Request{request("s1")}, schedules.SchedulesForRestore(context.Background(), got),
		"a restore maps to the schedules of its installation, and only those")
	reconcileSchedule("s1")
	cronJob := &batch.CronJob{}
	require.NoError(t, c.Get(context.Background(), key("s1-main-backup"), cronJob))
	require.True(t, ptr.Deref(cronJob.Spec.Suspend, false), "a validated restore holds the schedules of its installation")
	require.Contains(t, cronJob.Annotations, model.AnnotationCronJobSuspendedAt, "the suspension records when")

	require.Equal(t, pendingRequeueInterval, reconcileRestore(t, restores, "r1").RequeueAfter)
	require.Equal(t, api.BackupReasonWaitingForSchedules, condition(t, getRestore(t, c, "r1").Status.Conditions, api.BackupConditionJobCreated).Reason)
	require.Zero(t, jobCount(t, c), "the CronJob controller may still see the CronJob it suspended a moment ago active")

	ageSuspensions(t, c)
	reconcileRestore(t, restores, "r1")
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r1").Status.Phase)
	require.Equal(t, 1, jobCount(t, c))
}

// A schedule tells a hold from the restores its cache holds, which lag behind the reconcile of a
// restore, and a CronJob it resumes runs a missed run at once: the restore creates its Job only from
// a status that held the schedules already, which every schedule reading the restore afterwards
// sees. Once validated, a restore holds the schedules while its installation reconciles, so that the
// hold does not lapse and come back with every reconcile of the installation.
func TestRestoreStartsOnlyFromARecordedHold(t *testing.T) {
	cr := newFakeCR(readyInstallation(t))
	c := newClient(t, nil, newRestoreCR("r1"), newScheduleCR("s1"))
	restores := NewRestoreController(c, c, c.Scheme(), cr)
	schedules := NewScheduleController(c, c, c.Scheme(), cr)
	reconcileSchedule := func() {
		_, err := schedules.Reconcile(context.Background(), request("s1"))
		require.NoError(t, err)
	}
	suspended := func() bool {
		cronJob := &batch.CronJob{}
		require.NoError(t, c.Get(context.Background(), key("s1-main-backup"), cronJob))
		return ptr.Deref(cronJob.Spec.Suspend, false)
	}
	reconcileSchedule()
	validateRestore(t, c, restores, "r1")
	reconcileSchedule()
	require.True(t, suspended())

	cr.set(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
	reconcileRestore(t, restores, "r1")
	got := getRestore(t, c, "r1")
	require.True(t, holdsSchedules(got), "a validated restore waiting for its installation still holds the schedules")
	require.Equal(t, api.BackupReasonCHINotReady, condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Reason)
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionValidated).Status)
	reconcileSchedule()
	require.True(t, suspended(), "a schedule keeps its CronJob as it is while the installation reconciles")

	cr.set(readyInstallation(t))
	ageSuspensions(t, c)
	reconcileRestore(t, restores, "r1")
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r1").Status.Phase)
	require.Equal(t, 1, jobCount(t, c))
}

// A restore that cannot start, possibly forever with an invalid spec or a cluster the installation
// does not have, must not hold the schedules. A failed one holds them while a cluster it failed on
// may be left partly restored.
func TestRestoreHoldsSchedulesOnlyOnceValidated(t *testing.T) {
	validated := newRestoreCR("validated")
	newRestoreOperation(validated).pin([]string{"main"})
	running := newRestoreCR("running")
	running.Status.Phase = api.OperationPhaseRunning
	running.Status.Clusters = runningOn("running-main-restore")
	invalid := newRestoreCR("invalid")
	newRestoreOperation(invalid).wait(api.BackupConditionValidated, api.BackupReasonInvalidSpec, "invalid")
	refused := newRestoreCR("refused")
	newRestoreOperation(refused).refuse(api.BackupReasonClusterNotFound, "no such cluster")
	done := running.DeepCopy()
	done.Status.Phase = api.OperationPhaseCompleted
	done.Status.Clusters[0].Phase = api.OperationPhaseCompleted
	failed := running.DeepCopy()
	failed.Status.Phase = api.OperationPhaseFailed
	failed.Status.Clusters[0].Phase = api.OperationPhaseFailed
	failed.Status.Clusters[0].Reason = api.BackupReasonJobFailed
	unchanged := failed.DeepCopy()
	unchanged.Status.Clusters[0].Reason = api.BackupReasonJobFailedUnchanged
	gone := failed.DeepCopy()
	gone.Status.Clusters[0].JobName = ""
	gone.Status.Clusters[0].Reason = api.BackupReasonClusterNotFound

	require.True(t, holdsSchedules(validated))
	require.True(t, holdsSchedules(running))
	require.True(t, holdsSchedules(failed), "a failed restore may have left the installation partly restored")
	require.False(t, holdsSchedules(newRestoreCR("new")))
	require.False(t, holdsSchedules(invalid))
	require.False(t, holdsSchedules(refused), "a restore refused for its target touched nothing")
	require.False(t, holdsSchedules(done))
	require.False(t, holdsSchedules(unchanged), "a Job that failed before it changed anything left nothing behind")
	require.False(t, holdsSchedules(gone), "a cluster that left before its Job started was never touched")
}

// What a restore restores is fixed when its Job is created: an edit of the spec cannot change what
// runs, so the status records the backup the Job restores, and never follows the spec.
func TestRestoreRecordsTheBackupItRestores(t *testing.T) {
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
	startRestore(t, c, controller, "r1")
	require.Equal(t, "nightly-20260101-000000", getRestore(t, c, "r1").Status.BackupName)

	edited := getRestore(t, c, "r1")
	edited.Spec.BackupName = "edited-later"
	require.NoError(t, c.Update(context.Background(), edited))
	reconcileRestore(t, controller, "r1")
	require.Equal(t, "nightly-20260101-000000", getRestore(t, c, "r1").Status.BackupName)
	finishJob(t, c, "r1-main-restore", batch.JobComplete, "")
	reconcileRestore(t, controller, "r1")
	messages := eventMessages(t, c, api.BackupReasonCompleted)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], "nightly-20260101-000000")

	t.Run("a Job found after a lost status write", func(t *testing.T) {
		original := newRestoreCR("r2")
		topology, err := model.NewTopology(readyInstallation(t).GetAncestorT(), "")
		require.NoError(t, err)
		job := model.BuildRestoreJob(original, readyInstallation(t), topology)
		require.NoError(t, ctrl.SetControllerReference(original, job, newScheme(t)))
		edited := original.DeepCopy()
		edited.Spec.BackupName = "edited-later"
		c := newClient(t, nil, edited, job)
		controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))

		reconcileRestore(t, controller, "r2")
		require.Equal(t, "nightly-20260101-000000", getRestore(t, c, "r2").Status.BackupName)
	})
}

// A restore whose Job failed before it changed anything, as the Job tells by its exit code, left
// nothing behind and holds no schedule. Any other failed restore holds them until it is deleted,
// which its failure event says: one whose pods disagree, or are gone, may have changed something.
func TestRestoreHoldsSchedulesOnlyAfterAChange(t *testing.T) {
	const reported = "refusing to restore over existing tables (chi-demo-main-0-0:db1.t1); set overwrite to replace them"
	for name, tc := range map[string]struct {
		exitCodes []int32
		reason    string
		message   string
		holds     bool
	}{
		"failed before any change": {
			exitCodes: []int32{model.UnchangedExitCode},
			reason:    api.BackupReasonJobFailedUnchanged,
			message:   "Job r1-main-restore failed: " + reported,
		},
		"failed after a change": {
			exitCodes: []int32{1},
			reason:    api.BackupReasonJobFailed,
			message:   "Job r1-main-restore failed: " + reported,
			holds:     true,
		},
		"pods that disagree": {
			exitCodes: []int32{model.UnchangedExitCode, 1},
			reason:    api.BackupReasonJobFailed,
			message:   "Job r1-main-restore failed: " + reported,
			holds:     true,
		},
		"pods gone": {
			reason:  api.BackupReasonJobFailed,
			message: "Job r1-main-restore failed, see its logs",
			holds:   true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, nil, newRestoreCR("r1"))
			controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
			startRestore(t, c, controller, "r1")
			// A Job without a selector has no pods to read; one whose pods are gone lists none
			selectPodsOfJob(t, c, "r1-main-restore")
			for i, exitCode := range tc.exitCodes {
				failPodOfJobWithExitCode(t, c, "r1-main-restore", fmt.Sprintf("r1-main-restore-%d", i), reported, exitCode)
			}
			finishJob(t, c, "r1-main-restore", batch.JobFailed, batch.JobReasonBackoffLimitExceeded)

			reconcileRestore(t, controller, "r1")
			got := getRestore(t, c, "r1")
			require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
			ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
			require.Equal(t, tc.reason, ready.Reason)
			require.Equal(t, tc.message, ready.Message)
			require.Equal(t, tc.holds, holdsSchedules(got))
			messages := eventMessages(t, c, tc.reason)
			require.Len(t, messages, 1)
			if tc.holds {
				require.Contains(t, messages[0], "scheduled backups of ClickHouseInstallation demo stay suspended while this restore remains")
			} else {
				require.NotContains(t, messages[0], "stay suspended")
			}
		})
	}
}

func TestRestoreDeadlineExceeded(t *testing.T) {
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
	startRestore(t, c, controller, "r1")
	finishJob(t, c, "r1-main-restore", batch.JobFailed, batch.JobReasonDeadlineExceeded)

	reconcileRestore(t, controller, "r1")
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.BackupReasonDeadlineExceeded, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	require.Equal(t, 1, eventCount(t, c, api.BackupReasonDeadlineExceeded))
}

func TestRestoreNeverAdoptsAStaleJob(t *testing.T) {
	predecessor := newRestoreCR("r1")
	predecessor.UID = "predecessor-uid"
	stale := jobOwnedBy(t, "r1-main-restore", predecessor, model.Labels(api.ClickHouseRestoreCRDResourceKind, "r1", testCHI, "main", testStorage))
	c := newClient(t, nil, newRestoreCR("r1"), stale)
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))

	require.NotZero(t, reconcileRestore(t, controller, "r1").RequeueAfter)
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Empty(t, got.Status.Clusters[0].JobName)
}

// A restore drops what a running backup reads, so it waits for every other operation on the
// installation, scheduled backups included.
func TestRestoreWaitsForAnotherOperationOnTheInstallation(t *testing.T) {
	scheduled := jobOwnedBy(t, "nightly-main-backup-123", newScheduleCR("nightly"),
		model.Labels(api.ClickHouseBackupScheduleCRDResourceKind, "nightly", testCHI, "main", testStorage))
	c := newClient(t, nil, newRestoreCR("r1"), scheduled)
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
	validateRestore(t, c, controller, "r1")

	require.Equal(t, pendingRequeueInterval, reconcileRestore(t, controller, "r1").RequeueAfter)
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Equal(t, api.BackupReasonOperationInProgress, condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Reason)
	require.Equal(t, 1, jobCount(t, c))
}

func TestRestoreAnnouncesEachTransitionOnce(t *testing.T) {
	conflicts := 0
	c := newClient(t, conflictingStatusUpdates(&conflicts), newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
	validateRestore(t, c, controller, "r1")

	conflicts = 1
	require.True(t, reconcileRestore(t, controller, "r1").Requeue)
	require.Equal(t, 1, jobCount(t, c), "the status write lost is the one that records the created Job")
	require.Zero(t, eventCount(t, c, api.BackupReasonJobCreated))
	reconcileRestore(t, controller, "r1")
	reconcileRestore(t, controller, "r1")
	require.Equal(t, 1, eventCount(t, c, api.BackupReasonJobCreated))
	require.Equal(t, 1, jobCount(t, c), "the Job of the lost pass is reused, not doubled")
}

// A restore of every cluster runs a Job per backup-enabled cluster, each restoring the schema on every
// replica of its own cluster and the data on the first replica of each shard, all of one backup. It
// completes once every cluster completed.
func TestRestoreRunsAJobPerCluster(t *testing.T) {
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))
	validateRestore(t, c, controller, "r1")
	reconcileRestore(t, controller, "r1")

	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
	require.Equal(t, []string{"hot", "cold"}, []string{got.Status.Clusters[0].Name, got.Status.Clusters[1].Name})
	require.Equal(t, "chi-demo-hot-0-0 chi-demo-hot-0-1 chi-demo-hot-1-0 chi-demo-hot-1-1", jobEnv(t, c, "r1-hot-restore", "SCHEMA_HOSTS"))
	require.Equal(t, "chi-demo-hot-0-0 chi-demo-hot-1-0", jobEnv(t, c, "r1-hot-restore", "DATA_HOSTS"))
	require.Equal(t, "chi-demo-cold-0-0 chi-demo-cold-0-1", jobEnv(t, c, "r1-cold-restore", "SCHEMA_HOSTS"))
	require.Equal(t, "cold", jobEnv(t, c, "r1-cold-restore", "CLUSTER"))
	require.Equal(t, got.Status.Clusters[1].Hosts, []string{"chi-demo-cold-0-0", "chi-demo-cold-0-1"})

	finishJob(t, c, "r1-hot-restore", batch.JobComplete, "")
	reconcileRestore(t, controller, "r1")
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r1").Status.Phase)
	finishJob(t, c, "r1-cold-restore", batch.JobComplete, "")
	reconcileRestore(t, controller, "r1")
	got = getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
	require.Equal(t, api.BackupReasonCompleted, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	require.False(t, holdsSchedules(got))
}

// A restore that fails on one cluster after its Job may have changed something holds the schedules,
// of every cluster for a restore of every cluster, until it is deleted, whatever it did on the others.
func TestRestoreFailsOnOneClusterOnly(t *testing.T) {
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))
	startRestore(t, c, controller, "r1")
	finishJob(t, c, "r1-hot-restore", batch.JobComplete, "")
	finishJob(t, c, "r1-cold-restore", batch.JobFailed, batch.JobReasonBackoffLimitExceeded)

	reconcileRestore(t, controller, "r1")
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonPartiallyCompleted, ready.Reason)
	require.Equal(t, "completed on cluster hot; cluster cold: Job r1-cold-restore failed, see its logs", ready.Message)
	require.True(t, holdsSchedules(got))
	require.Empty(t, heldCluster(got), "a restore of every cluster holds every schedule")
	messages := eventMessagesOfType(t, c, api.BackupReasonPartiallyCompleted, core.EventTypeWarning)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], "scheduled backups of ClickHouseInstallation demo stay suspended while this restore remains")
}

// A restore of one cluster runs one Job, and holds the schedules of that cluster alone, by the cluster
// its status records.
func TestRestoreOfOneCluster(t *testing.T) {
	chr := newRestoreCR("r1")
	chr.Spec.Cluster = "cold"
	c := newClient(t, nil, chr)
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))
	startRestore(t, c, controller, "r1")

	got := getRestore(t, c, "r1")
	require.Len(t, got.Status.Clusters, 1)
	require.Equal(t, 1, jobCount(t, c))
	require.Equal(t, "cold", heldCluster(got))
	got.Spec.Cluster = "hot"
	require.Equal(t, "cold", heldCluster(got), "an edit of the spec does not move the hold")
}

// A one-off restore of a cluster the completed installation does not have, or does not back up, can
// never run: it fails for good, holding nothing.
func TestRestoreRefusesTargetsWithoutBackup(t *testing.T) {
	chr := newRestoreCR("r1")
	chr.Spec.Cluster = "plain"
	c := newClient(t, nil, chr)
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))

	require.Equal(t, ctrl.Result{}, reconcileRestore(t, controller, "r1"))
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.BackupReasonBackupDisabled, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	require.False(t, holdsSchedules(got))
	require.Zero(t, jobCount(t, c))
}

// A cluster that turns backup on without a storage has nothing to restore from: a restore of it is refused
// telling how to turn backup on there, and a restore of every cluster leaves it out, warning about it.
func TestRestoreOfAClusterWithoutStorage(t *testing.T) {
	lacking := lackingCluster("lacking", 1)
	cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 1, true), lacking))

	one := newRestoreCR("r1")
	one.Spec.Cluster = "lacking"
	c := newClient(t, nil, one)
	controller := NewRestoreController(c, c, c.Scheme(), cr)
	require.Equal(t, ctrl.Result{}, reconcileRestore(t, controller, "r1"))
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonNoBackupStorage, ready.Reason)
	require.Contains(t, ready.Message, api.BackupStorageHint)
	require.Zero(t, jobCount(t, c))

	c = newClient(t, nil, newRestoreCR("r2"))
	controller = NewRestoreController(c, c, c.Scheme(), cr)
	validateRestore(t, c, controller, "r2")
	got = getRestore(t, c, "r2")
	require.Len(t, got.Status.Clusters, 1)
	require.Equal(t, "main", got.Status.Clusters[0].Name)
	messages := eventMessagesOfType(t, c, api.BackupReasonNoBackupStorage, core.EventTypeWarning)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], "restore of backup nightly-20260101-000000 leaves out the clusters with backup enabled without a storage: lacking - ")
}

// Access entities belong to the whole installation: a restore that asks for them must target every
// cluster, and then restores them; a restore whose backup holds access entities it did not restore
// completes, and says so.
func TestRestoreRBAC(t *testing.T) {
	t.Run("of one cluster", func(t *testing.T) {
		chr := newRestoreCR("r1")
		chr.Spec.RBAC = types.NewStringBool(true)
		chr.Spec.Cluster = "cold"
		c := newClient(t, nil, chr)
		controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))

		require.Equal(t, ctrl.Result{}, reconcileRestore(t, controller, "r1"))
		validated := condition(t, getRestore(t, c, "r1").Status.Conditions, api.BackupConditionValidated)
		require.Equal(t, api.BackupReasonInvalidSpec, validated.Reason)
		require.Contains(t, validated.Message, "requires an empty cluster")
		require.False(t, holdsSchedules(getRestore(t, c, "r1")))
	})

	t.Run("of every cluster", func(t *testing.T) {
		chr := newRestoreCR("r1")
		chr.Spec.RBAC = types.NewStringBool(true)
		c := newClient(t, nil, chr)
		controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
		startRestore(t, c, controller, "r1")
		require.Equal(t, "true", jobEnv(t, c, "r1-main-restore", "RESTORE_RBAC"))
	})

	t.Run("left unrestored", func(t *testing.T) {
		c := newClient(t, nil, newRestoreCR("r1"))
		controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
		startRestore(t, c, controller, "r1")
		require.Equal(t, "false", jobEnv(t, c, "r1-main-restore", "RESTORE_RBAC"))
		succeedPodOfJob(t, c, "r1-main-restore", model.RBACNotRestoredMessage)
		finishJob(t, c, "r1-main-restore", batch.JobComplete, "")

		reconcileRestore(t, controller, "r1")
		got := getRestore(t, c, "r1")
		require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
		ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
		require.Equal(t, meta.ConditionTrue, ready.Status)
		require.Equal(t, api.BackupReasonRBACNotRestored, ready.Reason)
		require.Contains(t, ready.Message, "the backup holds access entities that were not restored")
		warnings := eventMessagesOfType(t, c, api.BackupReasonRBACNotRestored, core.EventTypeWarning)
		require.Len(t, warnings, 1)
		require.Zero(t, eventCount(t, c, api.BackupReasonCompleted))
	})
}
