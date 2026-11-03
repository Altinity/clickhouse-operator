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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

type scheduleFixture struct {
	t          *testing.T
	client     client.Client
	cr         *fakeCR
	controller *ScheduleController
}

func newScheduleFixture(t *testing.T, cr *fakeCR, funcs *interceptor.Funcs, objs ...client.Object) *scheduleFixture {
	t.Helper()
	c := newClient(t, funcs, objs...)
	return &scheduleFixture{t: t, client: c, cr: cr, controller: NewScheduleController(c, c, c.Scheme(), cr)}
}

func (f *scheduleFixture) reconcile(name string) ctrl.Result {
	f.t.Helper()
	result, err := f.controller.Reconcile(context.Background(), request(name))
	require.NoError(f.t, err)
	return result
}

func (f *scheduleFixture) schedule(name string) *api.ClickHouseBackupSchedule {
	f.t.Helper()
	chbs := &api.ClickHouseBackupSchedule{}
	require.NoError(f.t, f.client.Get(context.Background(), key(name), chbs))
	return chbs
}

func (f *scheduleFixture) cronJob(name string) *batch.CronJob {
	f.t.Helper()
	cronJob := &batch.CronJob{}
	require.NoError(f.t, f.client.Get(context.Background(), key(name), cronJob))
	return cronJob
}

func (f *scheduleFixture) cronJobEnv(name string) []string {
	f.t.Helper()
	return envValues(f.cronJob(name).Spec.JobTemplate.Spec.Template.Spec)
}

// scheduledJob returns a run of the schedule's CronJob, as the CronJob controller creates it.
func (f *scheduleFixture) scheduledJob(name string) *batch.Job {
	f.t.Helper()
	cronJob := f.cronJob("s1-main-backup")
	job := jobOwnedBy(f.t, name, cronJob, cronJob.Spec.JobTemplate.Labels)
	require.NoError(f.t, f.client.Create(context.Background(), job))
	return job
}

// scheduledRunAt returns the run of the schedule's CronJob of the main cluster for the slot due at the
// given time, as the CronJob controller creates it, with the Job name label the API server gives its pods.
func (f *scheduleFixture) scheduledRunAt(due time.Time) *batch.Job {
	f.t.Helper()
	return f.cronJobRunAt("s1-main-backup", due)
}

// cronJobRunAt returns the run of a CronJob for the slot due at the given time, as the CronJob
// controller creates it, with the Job name labels the API server gives its pods.
func (f *scheduleFixture) cronJobRunAt(name string, due time.Time) *batch.Job {
	f.t.Helper()
	cronJob := f.cronJob(name)
	job := jobOwnedBy(f.t, fmt.Sprintf("%s-%d", cronJob.Name, due.Unix()/60), cronJob, cronJob.Spec.JobTemplate.Labels)
	job.Spec = *cronJob.Spec.JobTemplate.Spec.DeepCopy()
	job.Spec.Template.Labels[model.LegacyJobNameLabel] = job.Name
	job.Spec.Template.Labels[batch.JobNameLabel] = job.Name
	require.NoError(f.t, f.client.Create(context.Background(), job))
	return job
}

// retentionHosts returns the hosts retention prunes on, of every cluster of the schedule in order.
func retentionHosts(chbs *api.ClickHouseBackupSchedule) []string {
	var hosts []string
	for _, cluster := range chbs.Status.Clusters {
		hosts = append(hosts, cluster.RetentionHosts...)
	}
	return hosts
}

// unprunedHosts returns the hosts retention no longer prunes on, of every cluster of the schedule in order.
func unprunedHosts(chbs *api.ClickHouseBackupSchedule) []string {
	var hosts []string
	for _, cluster := range chbs.Status.Clusters {
		hosts = append(hosts, cluster.UnprunedHosts...)
	}
	return hosts
}

func readyReason(t *testing.T, chbs *api.ClickHouseBackupSchedule) string {
	t.Helper()
	return condition(t, chbs.Status.Conditions, api.BackupConditionReady).Reason
}

func TestScheduleSkipsUnwatchedNamespaces(t *testing.T) {
	watchOnly(t, "elsewhere")
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))

	f.reconcile("s1")
	err := f.client.Get(context.Background(), key("s1-main-backup"), &batch.CronJob{})
	require.True(t, apiErrors.IsNotFound(err), "an unwatched namespace must never get a CronJob")
}

func TestScheduleRejectsInvalidSpec(t *testing.T) {
	chbs := newScheduleCR("s1")
	chbs.Spec.Schedule = ""
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs)

	require.Equal(t, ctrl.Result{}, f.reconcile("s1"))
	require.Equal(t, api.BackupReasonInvalidSpec, condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated).Reason)
	require.Equal(t, api.BackupReasonInvalidSpec, readyReason(t, f.schedule("s1")), "the schedule says why it does not run")
	err := f.client.Get(context.Background(), key("s1-main-backup"), &batch.CronJob{})
	require.True(t, apiErrors.IsNotFound(err))
}

// The CronJob runs the spec it was built from; an edit that fails validation must not leave it
// running the spec the user meant to replace.
func TestScheduleHoldsItsCronJobWhileTheSpecIsInvalid(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")

	chbs := f.schedule("s1")
	chbs.Spec.BackupNamePrefix = "-invalid"
	require.NoError(t, f.client.Update(context.Background(), chbs))
	require.Equal(t, ctrl.Result{}, f.reconcile("s1"), "only a change to the spec can fix it")
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
	require.Equal(t, api.BackupReasonInvalidSpec, condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated).Reason)
	require.Equal(t, api.BackupReasonInvalidSpec, readyReason(t, f.schedule("s1")))

	chbs = f.schedule("s1")
	chbs.Spec.BackupNamePrefix = "fixed"
	require.NoError(t, f.client.Update(context.Background(), chbs))
	f.reconcile("s1")
	require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
}

// The API server validates the cron expression, which the operator does not check: a schedule the
// CronJob refuses is an invalid spec like any other, and the CronJob of the last valid one is held
// instead of the reconcile failing over and over with the status still telling the old schedule. Nor
// is the schedule validated meanwhile, which would warn about it anew on every reconcile.
func TestScheduleHoldsItsCronJobWhenTheScheduleIsRefused(t *testing.T) {
	const refused = "61 * * * *"
	refuse := func(obj client.Object) error {
		if cronJob, ok := obj.(*batch.CronJob); ok && (cronJob.Spec.Schedule == refused) {
			return apiErrors.NewInvalid(schema.GroupKind{Group: batch.GroupName, Kind: "CronJob"}, obj.GetName(),
				field.ErrorList{field.Invalid(field.NewPath("spec", "schedule"), refused, "unparseable schedule")})
		}
		return nil
	}
	funcs := &interceptor.Funcs{
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := refuse(obj); err != nil {
				return err
			}
			return c.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := refuse(obj); err != nil {
				return err
			}
			return c.Update(ctx, obj, opts...)
		},
	}

	t.Run("an edit", func(t *testing.T) {
		f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), funcs, newScheduleCR("s1"))
		f.reconcile("s1")
		chbs := f.schedule("s1")
		chbs.Spec.Schedule = refused
		require.NoError(t, f.client.Update(context.Background(), chbs))

		require.Equal(t, ctrl.Result{}, f.reconcile("s1"), "only a change to the spec can fix it")
		require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false), "the CronJob runs the schedule the edit replaced")
		require.Equal(t, api.BackupReasonInvalidSpec, condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated).Reason)
		require.Equal(t, api.BackupReasonInvalidSpec, readyReason(t, f.schedule("s1")))
	})

	t.Run("a new schedule", func(t *testing.T) {
		chbs := newScheduleCR("s1")
		chbs.Spec.Schedule = refused
		f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), funcs, chbs)

		require.Equal(t, ctrl.Result{}, f.reconcile("s1"))
		require.Equal(t, api.BackupReasonInvalidSpec, condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated).Reason)
	})

	t.Run("a recovered installation", func(t *testing.T) {
		chbs := newScheduleCR("s1")
		chbs.Spec.Schedule = refused
		recovered := withAnnotations(readyInstallation(t), map[string]string{api.AnnotationRecoveredFrom: recoveryBackup})
		f := newScheduleFixture(t, newFakeCR(recovered), funcs, chbs)

		f.reconcile("s1")
		f.reconcile("s1")
		require.Equal(t, api.BackupReasonInvalidSpec, condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated).Reason)
		require.Empty(t, eventMessages(t, f.client, api.BackupReasonCHIRecovered))
	})
}

func TestScheduleCreatesItsCronJob(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))

	// A relisted watch can hide the completion of a reconcile that changed the hosts, so the
	// schedule rechecks them on its own as well
	require.Equal(t, ctrl.Result{RequeueAfter: topologyRecheckInterval}, f.reconcile("s1"))
	got := f.schedule("s1")
	cronJob := f.cronJob("s1-main-backup")
	require.True(t, meta.IsControlledBy(cronJob, got))
	require.False(t, ptr.Deref(cronJob.Spec.Suspend, true))
	require.Contains(t, f.cronJobEnv("s1-main-backup"), "chi-demo-main-0-0 chi-demo-main-1-0")
	require.Equal(t, "s1-main-backup", got.Status.Clusters[0].CronJobName)
	require.Equal(t, testStorage, got.Status.Clusters[0].Storage, "the storage the runs keep their backups in")
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionValidated).Status)
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, got))
}

// Scaling the installation changes the hosts every scheduled run must back up, so the CronJob is
// rebuilt when the installation completes the reconcile that applied the change.
func TestScheduleFollowsTheInstallationTopology(t *testing.T) {
	other := newScheduleCR("other")
	other.Spec.ClickHouseInstallation = "another-chi"
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"), other)
	f.reconcile("s1")
	before := f.cronJob("s1-main-backup").Annotations[model.AnnotationBackupObjectVersion]

	scaled := installation(t, api.StatusCompleted, backupCluster("main", 3, true))
	f.cr.set(scaled)
	require.Equal(t, []reconcile.Request{request("s1")}, f.controller.SchedulesForCHI(context.Background(), scaled),
		"an installation maps to the schedules that back it up, and only those")

	f.reconcile("s1")
	require.Contains(t, f.cronJobEnv("s1-main-backup"), "chi-demo-main-0-0 chi-demo-main-1-0 chi-demo-main-2-0")
	require.NotEqual(t, before, f.cronJob("s1-main-backup").Annotations[model.AnnotationBackupObjectVersion])

	inProgress := installation(t, api.StatusInProgress, backupCluster("main", 3, true))
	completed := installation(t, api.StatusCompleted, backupCluster("main", 3, true))
	predicate := CHITopologyPredicate()
	require.True(t, predicate.Update(event.UpdateEvent{ObjectOld: inProgress, ObjectNew: completed}),
		"a completed reconcile may have changed the topology")
	require.False(t, predicate.Update(event.UpdateEvent{ObjectOld: completed, ObjectNew: completed}),
		"status chatter of a completed installation changes nothing")
	require.False(t, predicate.Update(event.UpdateEvent{ObjectOld: completed, ObjectNew: inProgress}))
}

// While the installation reconciles its hosts are in flux; the CronJob keeps its last hosts until
// the reconcile completes rather than flapping with every intermediate state.
func TestScheduleKeepsItsCronJobWhileTheInstallationReconciles(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")

	f.cr.set(installation(t, api.StatusInProgress, backupCluster("main", 3, true)))
	require.NotZero(t, f.reconcile("s1").RequeueAfter)
	require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
	require.Contains(t, f.cronJobEnv("s1-main-backup"), "chi-demo-main-0-0 chi-demo-main-1-0")
}

// Backups of a cluster whose backup was switched off can only fail, so the CronJob is suspended,
// and resumed once backup is back - even though its own spec did not change meanwhile.
func TestScheduleHoldsItsCronJobWhileTheTargetIsInvalid(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")

	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 2, false)))
	f.reconcile("s1")
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
	require.Equal(t, api.BackupReasonBackupDisabled, readyReason(t, f.schedule("s1")))

	f.cr.set(readyInstallation(t))
	f.reconcile("s1")
	require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
}

func TestScheduleHoldsItsCronJobWhileTheInstallationIsStopped(t *testing.T) {
	for name, chi := range stoppedInstallations(t) {
		t.Run(name, func(t *testing.T) {
			f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
			f.reconcile("s1")

			f.cr.set(chi)
			f.reconcile("s1")
			require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
			require.Equal(t, api.BackupReasonCHIStopped, readyReason(t, f.schedule("s1")))
		})
	}
}

func TestScheduleSuspendsWhileARestoreRuns(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")

	restore := jobOwnedBy(t, "r1-restore", newRestoreCR("r1"), model.Labels(api.ClickHouseRestoreCRDResourceKind, "r1", testCHI, "main", testStorage))
	require.NoError(t, f.client.Create(context.Background(), restore))
	require.Equal(t, []reconcile.Request{request("s1")}, f.controller.SchedulesForJob(context.Background(), restore),
		"a restore Job holds the schedules of its installation")

	require.NotZero(t, f.reconcile("s1").RequeueAfter)
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
	require.Equal(t, api.BackupReasonSuspendedForRestore, readyReason(t, f.schedule("s1")))
	f.reconcile("s1")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonSuspendedForRestore))

	finishJob(t, f.client, "r1-restore", batch.JobComplete, "")
	require.Equal(t, ctrl.Result{RequeueAfter: topologyRecheckInterval}, f.reconcile("s1"))
	require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonScheduled), "the resumption is announced")
}

func TestScheduleSuspendedBySpecStaysSuspendedAfterARestore(t *testing.T) {
	chbs := newScheduleCR("s1")
	chbs.Spec.Suspend = types.NewStringBool(true)
	restore := jobOwnedBy(t, "r1-restore", newRestoreCR("r1"), model.Labels(api.ClickHouseRestoreCRDResourceKind, "r1", testCHI, "main", testStorage))
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs, restore)

	f.reconcile("s1")
	finishJob(t, f.client, "r1-restore", batch.JobComplete, "")
	f.reconcile("s1")
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
	require.Equal(t, api.BackupReasonSuspended, readyReason(t, f.schedule("s1")))
	require.Zero(t, eventCount(t, f.client, api.BackupReasonSuspended), "no scheduled backup resumes")
}

func TestScheduleReportsCronJobStatus(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")

	scheduled := meta.NewTime(time.Unix(1700000000, 0))
	succeeded := meta.NewTime(time.Unix(1700000600, 0))
	cronJob := f.cronJob("s1-main-backup")
	cronJob.Status.LastScheduleTime = &scheduled
	cronJob.Status.LastSuccessfulTime = &succeeded
	require.NoError(t, f.client.Status().Update(context.Background(), cronJob))

	f.reconcile("s1")
	got := f.schedule("s1")
	require.True(t, scheduled.Equal(got.Status.LastScheduleTime))
	require.True(t, succeeded.Equal(got.Status.LastSuccessfulTime))
}

// Each scheduled run is counted and announced exactly once, whatever the number of reconciles
// that see it, and however the race for recording it goes.
func TestScheduleReportsEachRunOnce(t *testing.T) {
	patchConflicts := 0
	funcs := &interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, isJob := obj.(*batch.Job); isJob && (patchConflicts > 0) {
				patchConflicts--
				return apiErrors.NewConflict(api.Resource("jobs"), obj.GetName(), nil)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), funcs, newScheduleCR("s1"))
	f.reconcile("s1")

	run := f.scheduledJob("s1-main-backup-1")
	require.Equal(t, []reconcile.Request{request("s1")}, f.controller.SchedulesForJob(context.Background(), run),
		"a scheduled run reports to its schedule")
	finishJob(t, f.client, "s1-main-backup-1", batch.JobComplete, "")

	patchConflicts = 1
	f.reconcile("s1")
	require.Zero(t, eventCount(t, f.client, api.BackupReasonCompleted), "a run whose record lost the race is not reported")

	f.reconcile("s1")
	f.reconcile("s1")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonCompleted))
	recorded := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("s1-main-backup-1"), recorded))
	require.Equal(t, string(api.OperationPhaseCompleted), recorded.Annotations[model.AnnotationScheduledJobObserved])

	f.scheduledJob("s1-main-backup-2")
	finishJob(t, f.client, "s1-main-backup-2", batch.JobFailed, batch.JobReasonDeadlineExceeded)
	f.reconcile("s1")
	f.reconcile("s1")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonDeadlineExceeded))
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonCompleted))
}

// A scheduled run whose pod never starts is reported failed once, and stopped, so the CronJob,
// which forbids concurrent runs, gets to start the next one.
func TestScheduleReportsARunThatNeverStarts(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")
	f.scheduledJob("s1-main-backup-1")
	f.reconcile("s1")
	stallJob(t, f.client, "s1-main-backup-1", neverStarted)

	f.reconcile("s1")
	f.reconcile("s1")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonJobNotStarted))
	requireStopped(t, f.client, "s1-main-backup-1")
	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("s1-main-backup-1"), job))
	require.Equal(t, string(api.OperationPhaseFailed), job.Annotations[model.AnnotationScheduledJobObserved])

	finishJob(t, f.client, "s1-main-backup-1", batch.JobFailed, batch.JobReasonDeadlineExceeded)
	f.reconcile("s1")
	require.Zero(t, eventCount(t, f.client, api.BackupReasonDeadlineExceeded), "the stop that ends the run is not a second failure")
}

// A scheduled run whose pod stopped being ready after it started is not a run that never started.
func TestScheduleKeepsARunWhosePodStoppedBeingReady(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")
	f.scheduledJob("s1-main-backup-1")
	setJobStatus(t, f.client, "s1-main-backup-1", runningReady)
	f.reconcile("s1")

	stallJob(t, f.client, "s1-main-backup-1", neverStarted)
	f.reconcile("s1")
	require.Zero(t, eventCount(t, f.client, api.BackupReasonJobNotStarted))
	requireRunning(t, f.client, "s1-main-backup-1")
}

// A cluster that refuses the stop of a scheduled run leaves the run to its own fate, as it does for
// a one-off operation: the run is reported failed once, and the schedule carries on.
func TestScheduleReportsARunThatCannotBeStopped(t *testing.T) {
	funcs := &interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			data, err := patch.Data(obj)
			require.NoError(t, err)
			if strings.Contains(string(data), model.AnnotationJobNotStarted) {
				return apiErrors.NewInvalid(batch.SchemeGroupVersion.WithKind("Job").GroupKind(), obj.GetName(), nil)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), funcs, newScheduleCR("s1"))
	f.reconcile("s1")
	f.scheduledJob("s1-main-backup-1")
	f.reconcile("s1")
	stallJob(t, f.client, "s1-main-backup-1", neverStarted)

	f.reconcile("s1")
	f.reconcile("s1")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonJobNotStarted))
}

// A pod that stays pending changes nothing on its Job, so no event would bring the schedule back
// to notice that the run never started: the schedule comes back by itself once the run's time
// to start is up.
func TestScheduleRechecksARunWaitingForItsPod(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	require.Equal(t, ctrl.Result{RequeueAfter: topologyRecheckInterval}, f.reconcile("s1"))
	f.scheduledJob("s1-main-backup-1")
	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("s1-main-backup-1"), job))
	job.CreationTimestamp = meta.NewTime(time.Now().Add(time.Minute - jobStartTimeout))
	require.NoError(t, f.client.Update(context.Background(), job))
	neverStarted(&job.Status)
	require.NoError(t, f.client.Status().Update(context.Background(), job))

	require.InDelta(t, float64(time.Minute+time.Second), float64(f.reconcile("s1").RequeueAfter), float64(5*time.Second))

	require.NoError(t, f.client.Get(context.Background(), key("s1-main-backup-1"), job))
	job.Status.Ready = ptr.To(int32(1))
	require.NoError(t, f.client.Status().Update(context.Background(), job))
	require.Equal(t, ctrl.Result{RequeueAfter: topologyRecheckInterval}, f.reconcile("s1"), "a run whose pod started needs no start-timeout recheck, only the topology one")
}

func TestScheduleNeverAdoptsAStaleCronJob(t *testing.T) {
	predecessor := newScheduleCR("s1")
	predecessor.UID = "predecessor-uid"
	stale := &batch.CronJob{}
	stale.Name = "s1-main-backup"
	stale.Namespace = testNamespace
	stale.Labels = model.Labels(api.ClickHouseBackupScheduleCRDResourceKind, "s1", testCHI, "main", testStorage)
	stale.Spec.Schedule = "* * * * *"
	require.NoError(t, ctrl.SetControllerReference(predecessor, stale, newScheme(t)))
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"), stale)

	require.NotZero(t, f.reconcile("s1").RequeueAfter)
	cronJob := f.cronJob("s1-main-backup")
	require.True(t, meta.IsControlledBy(cronJob, predecessor))
	require.Equal(t, "* * * * *", cronJob.Spec.Schedule, "a stale CronJob is never rewritten")
	require.Equal(t, api.BackupReasonWaitingForGarbageCollection, condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionJobCreated).Reason)
}

// failedRestore returns a restore whose Job of the main cluster failed with the given reason.
func failedRestore(name, reason string) *api.ClickHouseRestore {
	failed := newRestoreCR(name)
	failed.Status.Phase = api.OperationPhaseFailed
	failed.Status.Clusters = []api.OperationClusterStatus{{Name: "main", Phase: api.OperationPhaseFailed, JobName: name + "-main-restore", Reason: reason}}
	failed.Status.Conditions = []meta.Condition{{Type: api.BackupConditionReady, Status: meta.ConditionFalse, Reason: reason}}
	return failed
}

// A failed restore may have left the installation partly restored: a backup of it then would be a
// backup of a half-restored state, possibly pruning good ones. The schedules stay held until the
// failed restore is deleted, which the schedule warns about.
func TestScheduleHoldsWhileAFailedRestoreRemains(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"), failedRestore("r1", api.BackupReasonJobFailed))

	f.reconcile("s1")
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
	ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonSuspendedForFailedRestore, ready.Reason)
	require.Contains(t, ready.Message, "ClickHouseRestore r1 of ClickHouseInstallation demo failed and may have left cluster main partly restored")
	require.Contains(t, ready.Message, "scheduled backups stay suspended while that restore remains")
	warnings := eventMessagesOfType(t, f.client, api.BackupReasonSuspendedForFailedRestore, core.EventTypeWarning)
	require.Len(t, warnings, 1)
	require.Contains(t, warnings[0], "scheduled backups stay suspended while that restore remains")

	require.NoError(t, f.client.Delete(context.Background(), getRestore(t, f.client, "r1")))
	f.reconcile("s1")
	require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
}

// A failed restore deleted while the installation is in the middle of a reconcile holds the schedule no longer, while
// the reconcile keeps its CronJob suspended until it completes: the schedule tells so, as it tells any restore hold,
// without announcing a suspension anew, which only a restore that holds the CronJob now does.
func TestScheduleTellsAFailedRestoreDeletedThroughAReconcile(t *testing.T) {
	for name, restoring := range map[string]bool{"no restore holds it any longer": false, "a new restore holds it": true} {
		t.Run(name, func(t *testing.T) {
			cr := newFakeCR(readyInstallation(t))
			f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"), failedRestore("r1", api.BackupReasonJobFailed))
			f.reconcile("s1")
			require.Equal(t, api.BackupReasonSuspendedForFailedRestore, readyReason(t, f.schedule("s1")))

			cr.set(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
			require.NoError(t, f.client.Delete(context.Background(), getRestore(t, f.client, "r1")))
			if restoring {
				restore := newRestoreCR("r2")
				newRestoreOperation(restore).pin([]string{"main"})
				require.NoError(t, f.client.Create(context.Background(), restore))
			}
			f.reconcile("s1")
			ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
			require.Equal(t, api.BackupReasonSuspendedForRestore, ready.Reason)
			require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
			suspensions := eventMessages(t, f.client, api.BackupReasonSuspendedForRestore)
			if restoring {
				require.Equal(t, "a restore of cluster main of ClickHouseInstallation demo is in progress", ready.Message)
				require.Equal(t, []string{"scheduled backups suspended: " + ready.Message}, suspensions)
				return
			}
			require.Equal(t, "CronJob s1-main-backup stays suspended until ClickHouseInstallation demo completes its reconcile: "+
				"no restore holds it any longer", ready.Message)
			require.Empty(t, suspensions, "the schedule was announced suspended for the failed restore")

			cr.set(readyInstallation(t))
			f.reconcile("s1")
			require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
			require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
			require.Contains(t, eventMessages(t, f.client, api.BackupReasonScheduled), "scheduled backups resumed")
		})
	}
}

// failedRestoreOn returns a restore of one cluster that failed for reason.
func failedRestoreOn(name, cluster, reason string) *api.ClickHouseRestore {
	failed := failedRestore(name, reason)
	failed.Spec.Cluster, failed.Status.Cluster = cluster, cluster
	failed.Status.Clusters[0].Name, failed.Status.Clusters[0].JobName = cluster, model.RestoreJobName(failed, cluster)
	return failed
}

// A restore of another cluster validated while a failed restore deleted meanwhile leaves a CronJob suspended through a
// reconcile is announced, whichever of the two clusters comes first.
func TestScheduleAnnouncesANewRestoreAlongAReleasedCluster(t *testing.T) {
	const inProgress = "a restore of cluster %s of ClickHouseInstallation demo is in progress"
	const released = "CronJob s1-%s-backup stays suspended until ClickHouseInstallation demo completes its reconcile: no " +
		"restore holds it any longer"
	for name, tc := range map[string]struct {
		failed, restored string
		// message is the Ready message, the parts in the order of their clusters
		message string
	}{
		"the released cluster comes last": {failed: "hot", restored: "cold",
			message: fmt.Sprintf(inProgress+"; "+released, "cold", "hot")},
		"the released cluster comes first": {failed: "cold", restored: "hot",
			message: fmt.Sprintf(released+"; "+inProgress, "cold", "hot")},
	} {
		t.Run(name, func(t *testing.T) {
			cr := newFakeCR(twoClusters(t, api.StatusCompleted))
			f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"), failedRestoreOn("r1", tc.failed, api.BackupReasonJobFailed))
			f.reconcile("s1")
			require.Equal(t, api.BackupReasonSuspendedForFailedRestore, readyReason(t, f.schedule("s1")))
			require.False(t, ptr.Deref(f.cronJob("s1-"+tc.restored+"-backup").Spec.Suspend, true))

			cr.set(twoClusters(t, api.StatusInProgress))
			require.NoError(t, f.client.Delete(context.Background(), getRestore(t, f.client, "r1")))
			restore := newRestoreCR("r2")
			restore.Spec.Cluster = tc.restored
			newRestoreOperation(restore).pin([]string{tc.restored})
			require.NoError(t, f.client.Create(context.Background(), restore))
			f.reconcile("s1")
			ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
			require.Equal(t, api.BackupReasonSuspendedForRestore, ready.Reason)
			require.Equal(t, tc.message, ready.Message)
			require.True(t, ptr.Deref(f.cronJob("s1-cold-backup").Spec.Suspend, false))
			require.True(t, ptr.Deref(f.cronJob("s1-hot-backup").Spec.Suspend, false))
			require.Equal(t, []string{"scheduled backups suspended: " + ready.Message}, eventMessages(t, f.client, api.BackupReasonSuspendedForRestore))
		})
	}
}

// A failed restore of one cluster that restored access entities, as a recovery restore does, may have left
// those of every cluster partly restored, and holds the schedules of every cluster: the message on another
// cluster names the cluster the restore restored.
func TestScheduleHeldByAFailedRestoreOfAccessEntitiesNamesTheRestoredCluster(t *testing.T) {
	recovery := newRestoreCR("demo-hot-recovery")
	recovery.Spec.Cluster = "hot"
	recovery.Spec.RBAC = types.NewStringBool(true)
	require.NoError(t, ctrl.SetControllerReference(installationSpec(), recovery, newScheme(t)))
	newRestoreOperation(recovery).pin([]string{"hot"})
	recovery.Status.Phase = api.OperationPhaseFailed
	recovery.Status.Clusters[0].Phase = api.OperationPhaseFailed
	recovery.Status.Clusters[0].JobName = "demo-hot-recovery-hot-restore"
	recovery.Status.Clusters[0].Reason = api.BackupReasonJobFailed
	cold := newScheduleCR("s1")
	cold.Spec.Cluster = "cold"
	f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, cold, recovery)

	f.reconcile("s1")
	require.True(t, ptr.Deref(f.cronJob("s1-cold-backup").Spec.Suspend, false))
	ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonSuspendedForFailedRestore, ready.Reason)
	require.Equal(t, "ClickHouseRestore demo-hot-recovery of ClickHouseInstallation demo failed and may have left cluster hot, and the "+
		"access entities of every cluster, partly restored; scheduled backups stay suspended while that restore remains", ready.Message)
}

// A restore that failed before it changed anything left nothing behind to hold the schedules for.
func TestScheduleIgnoresARestoreThatFailedUnchanged(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"), failedRestore("r1", api.BackupReasonJobFailedUnchanged))

	f.reconcile("s1")
	require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
}

// An installation that waits to be restored from the backup its spec names is not backed up: the
// schedules stay held on a cluster until its recovery restore is validated, which then holds it as
// any restore does, or until the recovery is recorded, by the operator or by hand.
func TestScheduleHoldsWhileTheInstallationWaitsToBeRecovered(t *testing.T) {
	waiting := recoveryTarget(t, api.StatusCompleted, recoveryBackup)
	cr := newFakeCR(waiting)
	f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"))

	f.reconcile("s1")
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
	ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonSuspendedForRestore, ready.Reason)
	require.Equal(t, "cluster main of ClickHouseInstallation demo waits to be restored from backup "+recoveryBackup, ready.Message)

	recovery := newRestoreCR("demo-main-recovery")
	recovery.Spec.Cluster = "main"
	require.NoError(t, ctrl.SetControllerReference(waiting, recovery, newScheme(t)))
	require.NoError(t, f.client.Create(context.Background(), recovery))
	f.reconcile("s1")
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false), "the cluster stays held until its recovery restore is validated")

	given := withAnnotations(waiting, map[string]string{api.AnnotationRecoveredFrom: recoveryBackup})
	require.True(t, CHITopologyPredicate().Update(event.UpdateEvent{ObjectOld: waiting, ObjectNew: given}),
		"recording the recovery releases the schedules of the installation")
	require.NoError(t, f.client.Delete(context.Background(), recovery))
	cr.set(given)
	f.reconcile("s1")
	require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
}

// A schedule of every cluster does not count a cluster without a storage, whose CronJob it keeps suspended:
// nor does it while the installation reconciles, its other CronJobs running on - its Ready condition stays as the
// last completed reconcile left it, rather than turn on the last run of the cluster it holds.
func TestScheduleReconcileLeavesOutAClusterWithoutStorage(t *testing.T) {
	succeeded := meta.NewTime(time.Unix(1700000000, 0))
	failed := meta.NewTime(time.Unix(1700000100, 0))
	chbs := newScheduleCR("s1")
	chbs.Status.Clusters = []api.ScheduleClusterStatus{
		{Name: "hot", CronJobName: model.CronJobName(chbs, "hot"), LastSuccessfulTime: &succeeded, LastFailureTime: &failed, LastFailureMessage: "sidecar gone"},
		{Name: "cold", CronJobName: model.CronJobName(chbs, "cold"), LastSuccessfulTime: &succeeded},
	}
	cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, true)))
	f := newScheduleFixture(t, cr, nil, chbs)
	f.reconcile("s1")

	cr.set(installation(t, api.StatusCompleted, lackingCluster("hot", 2), backupCluster("cold", 1, true)))
	f.reconcile("s1")
	ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonScheduled, ready.Reason)

	cr.set(installation(t, api.StatusInProgress, lackingCluster("hot", 2), backupCluster("cold", 1, true)))
	f.reconcile("s1")
	got := f.schedule("s1")
	ready = condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonScheduled, ready.Reason)
	require.Equal(t, "CronJob s1-cold-backup runs the schedule", ready.Message)
	require.Equal(t, &succeeded, got.Status.LastSuccessfulTime)
	require.True(t, ptr.Deref(f.cronJob(model.CronJobName(chbs, "hot")).Spec.Suspend, false), "still held")
}

// A schedule held for its target keeps its CronJobs suspended through a reconcile of the installation, its Ready
// condition false, telling the reconcile: it never reports running CronJobs it does not run.
func TestScheduleHoldsItsCronJobsThroughAReconcile(t *testing.T) {
	for name, tc := range map[string]struct {
		cluster   string
		cronJobs  []string
		completed []*api.Cluster
		reason    string
	}{
		"its cluster turned off": {
			cluster:   "main",
			cronJobs:  []string{"s1-main-backup"},
			completed: []*api.Cluster{backupCluster("main", 2, false)},
			reason:    api.BackupReasonBackupDisabled,
		},
		"every cluster without a storage": {
			cronJobs:  []string{"s1-hot-backup", "s1-cold-backup"},
			completed: []*api.Cluster{lackingCluster("hot", 2), lackingCluster("cold", 1)},
			reason:    api.BackupReasonNoBackupStorage,
		},
	} {
		t.Run(name, func(t *testing.T) {
			chbs := newScheduleCR("s1")
			chbs.Spec.Cluster = tc.cluster
			cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 2, true), backupCluster("hot", 2, true), backupCluster("cold", 1, true)))
			f := newScheduleFixture(t, cr, nil, chbs)
			f.reconcile("s1")

			cr.set(installation(t, api.StatusCompleted, tc.completed...))
			f.reconcile("s1")
			require.Equal(t, tc.reason, readyReason(t, f.schedule("s1")))

			cr.set(installation(t, api.StatusInProgress, tc.completed...))
			f.reconcile("s1")
			ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
			require.Equal(t, meta.ConditionFalse, ready.Status)
			require.Equal(t, api.BackupReasonCHINotReady, ready.Reason)
			for _, cronJob := range tc.cronJobs {
				require.True(t, ptr.Deref(f.cronJob(cronJob).Spec.Suspend, false), cronJob)
			}
		})
	}
}

// An installation may complete a reconcile that leaves every cluster without a storage and start the next
// one before the schedule sees it: the CronJobs of those clusters, whose backups could only fail, are held through
// that reconcile already, and the Ready condition is false, telling the reconcile, rather than report the CronJobs
// of no cluster.
func TestScheduleReadyTellsTheReconcileWhileNoCronJobItCountsRuns(t *testing.T) {
	cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, true)))
	f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"))
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))

	cr.set(installation(t, api.StatusInProgress, lackingCluster("hot", 2), lackingCluster("cold", 1)))
	f.reconcile("s1")
	ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
	require.Equal(t, meta.ConditionFalse, ready.Status)
	require.Equal(t, api.BackupReasonCHINotReady, ready.Reason)
	for _, cronJob := range []string{"s1-hot-backup", "s1-cold-backup"} {
		require.True(t, ptr.Deref(f.cronJob(cronJob).Spec.Suspend, false), cronJob)
	}
}

// A hold whose cause is gone, a spec fixed while the installation reconciles above all, does not stay in the Ready
// condition until the reconcile completes: the condition tells the reconcile that holds the CronJobs meanwhile.
func TestScheduleReadyTellsTheReconcileRatherThanAHoldSinceGone(t *testing.T) {
	cr := newFakeCR(readyInstallation(t))
	f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"))
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))

	invalid := f.schedule("s1")
	invalid.Spec.BackupNamePrefix = "-invalid"
	require.NoError(t, f.client.Update(context.Background(), invalid))
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonInvalidSpec, readyReason(t, f.schedule("s1")))
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))

	cr.set(withStatus(readyInstallation(t), api.StatusInProgress))
	fixed := f.schedule("s1")
	fixed.Spec.BackupNamePrefix = ""
	require.NoError(t, f.client.Update(context.Background(), fixed))
	f.reconcile("s1")
	ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
	require.Equal(t, meta.ConditionFalse, ready.Status)
	require.Equal(t, api.BackupReasonCHINotReady, ready.Reason)
}

// A schedule its spec suspends stays reported as suspended through a reconcile of the installation: its CronJobs
// are suspended by the spec, not by the reconcile.
func TestScheduleSuspendedThroughAReconcile(t *testing.T) {
	chbs := newScheduleCR("s1")
	chbs.Spec.Suspend = types.NewStringBool(true)
	cr := newFakeCR(readyInstallation(t))
	f := newScheduleFixture(t, cr, nil, chbs)
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonSuspended, readyReason(t, f.schedule("s1")))

	cr.set(withStatus(readyInstallation(t), api.StatusInProgress))
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonSuspended, readyReason(t, f.schedule("s1")))
}

// A spec made invalid holds a schedule of every cluster, which counts the clusters it ran on before, never one
// without a storage: the installation tells which, as for any other hold.
func TestScheduleInvalidSpecLeavesOutAClusterWithoutStorage(t *testing.T) {
	older := meta.NewTime(time.Unix(1700000000, 0))
	newer := meta.NewTime(time.Unix(1700000500, 0))
	chbs := newScheduleCR("s1")
	chbs.Status.Clusters = []api.ScheduleClusterStatus{
		{Name: "hot", CronJobName: model.CronJobName(chbs, "hot"), LastSuccessfulTime: &newer},
		{Name: "cold", CronJobName: model.CronJobName(chbs, "cold"), LastSuccessfulTime: &older},
	}
	cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, true)))
	f := newScheduleFixture(t, cr, nil, chbs)
	f.reconcile("s1")
	cr.set(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), lackingCluster("cold", 1)))
	f.reconcile("s1")
	require.Equal(t, &newer, f.schedule("s1").Status.LastSuccessfulTime)

	invalid := f.schedule("s1")
	invalid.Spec.BackupNamePrefix = "-invalid"
	require.NoError(t, f.client.Update(context.Background(), invalid))
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonInvalidSpec, readyReason(t, f.schedule("s1")))
	require.Equal(t, &newer, f.schedule("s1").Status.LastSuccessfulTime, "the cluster without a storage stays uncounted")
}

// A stopped installation holds a schedule of every cluster, which counts the clusters it ran on before, never one
// without a storage: the last completed reconcile tells which.
func TestScheduleStoppedInstallationLeavesOutAClusterWithoutStorage(t *testing.T) {
	older := meta.NewTime(time.Unix(1700000000, 0))
	newer := meta.NewTime(time.Unix(1700000500, 0))
	chbs := newScheduleCR("s1")
	chbs.Status.Clusters = []api.ScheduleClusterStatus{
		{Name: "hot", CronJobName: model.CronJobName(chbs, "hot"), LastSuccessfulTime: &newer},
		{Name: "cold", CronJobName: model.CronJobName(chbs, "cold"), LastSuccessfulTime: &older},
	}
	cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, true)))
	f := newScheduleFixture(t, cr, nil, chbs)
	f.reconcile("s1")

	completed := installation(t, api.StatusCompleted, backupCluster("hot", 2, true), lackingCluster("cold", 1))
	cr.set(completed)
	f.reconcile("s1")
	require.Equal(t, &newer, f.schedule("s1").Status.LastSuccessfulTime)

	stopped := completed.DeepCopy()
	stopped.Spec.Stop = types.NewStringBool(true)
	cr.set(stopped)
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonCHIStopped, readyReason(t, f.schedule("s1")))
	require.Equal(t, &newer, f.schedule("s1").Status.LastSuccessfulTime, "the cluster without a storage stays uncounted")
}

// A recovery waits for every cluster it recovers to have a storage: the schedules it holds meanwhile tell
// which, as the cluster they back up waits for another one.
func TestScheduleHoldTellsTheRecoveryWaitsForStorage(t *testing.T) {
	spec := withRecovery(installationSpec(backupCluster("main", 2, true), lackingCluster("lacking", 1)), recoveryBackup)
	f := newScheduleFixture(t, newFakeCR(installationFrom(t, spec, api.StatusCompleted)), nil, newScheduleCR("s1"))

	f.reconcile("s1")
	require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
	ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonSuspendedForRestore, ready.Reason)
	require.Equal(t, "cluster main of ClickHouseInstallation demo waits to be restored from backup "+recoveryBackup+
		", once backup is enabled with a storage on lacking", ready.Message)
}

// A recovery restore that failed before it changed anything, a backup name mistyped above all, or that
// was refused for its target, left nothing to protect: the cluster it was to recover is backed up
// again, rather than held until somebody notices, which would leave a serving installation without
// backups.
func TestScheduleIgnoresARecoveryThatFailedUnchanged(t *testing.T) {
	refused := newRestoreCR("demo-main-recovery")
	newRestoreOperation(refused).refuse(api.BackupReasonBackupDisabled, "backup is not enabled")
	for name, recovery := range map[string]*api.ClickHouseRestore{
		"failed unchanged": failedRestore("demo-main-recovery", api.BackupReasonJobFailedUnchanged),
		"refused":          refused,
	} {
		t.Run(name, func(t *testing.T) {
			chi := recoveryTarget(t, api.StatusCompleted, recoveryBackup)
			recovery.Spec.Cluster = "main"
			require.NoError(t, ctrl.SetControllerReference(chi, recovery, newScheme(t)))
			f := newScheduleFixture(t, newFakeCR(chi), nil, newScheduleCR("s1"), recovery)

			f.reconcile("s1")
			require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))
			require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
		})
	}
}

// A schedule of every cluster runs on those whose backup is on, and leaves out a cluster that turns backup on
// without a storage, which it tells, and warns about, once: the cluster's spec asks for backups it would
// otherwise go without unnoticed.
func TestScheduleLeavesOutAClusterWithoutStorage(t *testing.T) {
	lacking := lackingCluster("lacking", 1)
	f := newScheduleFixture(t, newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 2, true), lacking)), nil,
		newScheduleCR("s1"))

	f.reconcile("s1")
	f.reconcile("s1")
	validated := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated)
	require.Equal(t, meta.ConditionTrue, validated.Status)
	require.Equal(t, api.BackupReasonNoBackupStorage, validated.Reason)
	require.Contains(t, validated.Message, "the schedule leaves out the clusters with backup enabled without a storage: lacking - ")
	require.Contains(t, validated.Message, api.BackupStorageHint)
	require.Len(t, eventMessages(t, f.client, api.BackupReasonNoBackupStorage), 1)
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
	require.False(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, true))

	t.Run("together with a recovered installation", func(t *testing.T) {
		recovered := installation(t, api.StatusCompleted, backupCluster("main", 2, true), lacking)
		recovered.Annotations = map[string]string{api.AnnotationRecoveredFrom: recoveryBackup}
		f := newScheduleFixture(t, newFakeCR(recovered), nil, newScheduleCR("s1"))
		f.reconcile("s1")
		validated := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated)
		require.Equal(t, api.BackupReasonCHIRecovered, validated.Reason)
		require.Contains(t, validated.Message, "restored from backup "+recoveryBackup)
		require.Contains(t, validated.Message, "the schedule leaves out the clusters with backup enabled without a storage: lacking - ")
	})
}

// A restored installation typically shares the storage path of the installation its backup
// came from. Retention there tells the backups of the two apart by their name prefix alone, which
// the schedule warns about, once.
func TestScheduleWarnsAboutARecoveredInstallation(t *testing.T) {
	recovered := readyInstallation(t)
	recovered.Annotations = map[string]string{api.AnnotationRecoveredFrom: recoveryBackup}
	f := newScheduleFixture(t, newFakeCR(recovered), nil, newScheduleCR("s1"))

	f.reconcile("s1")
	f.reconcile("s1")
	validated := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated)
	require.Equal(t, meta.ConditionTrue, validated.Status)
	require.Equal(t, api.BackupReasonCHIRecovered, validated.Reason)
	require.Contains(t, validated.Message, "restored from backup "+recoveryBackup)
	require.Contains(t, validated.Message, "ns-demo-s1-YYYYMMDD-HHMMSS", "the message names the series retention prunes")
	require.Len(t, eventMessages(t, f.client, api.BackupReasonCHIRecovered), 1)
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
	require.Contains(t, f.cronJobEnv("s1-main-backup"), "ns-demo-s1", "the prefix defaults to the namespace, installation and schedule names")

	t.Run("a schedule validated before the recovery", func(t *testing.T) {
		cr := newFakeCR(readyInstallation(t))
		f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"))
		f.reconcile("s1")
		require.Equal(t, api.BackupReasonValidated, condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated).Reason)

		cr.set(recovered)
		f.reconcile("s1")
		f.reconcile("s1")
		require.Equal(t, api.BackupReasonCHIRecovered, condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionValidated).Reason)
		require.Len(t, eventMessages(t, f.client, api.BackupReasonCHIRecovered), 1)
	})

	t.Run("a later reconcile of the installation", func(t *testing.T) {
		cr := newFakeCR(recovered)
		f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"))
		f.reconcile("s1")

		reconciling := installation(t, api.StatusInProgress, backupCluster("main", 3, true))
		reconciling.Annotations = recovered.Annotations
		cr.set(reconciling)
		f.reconcile("s1")
		cr.set(recovered)
		f.reconcile("s1")
		require.Len(t, eventMessages(t, f.client, api.BackupReasonCHIRecovered), 1)
	})
}

// Every finished run is recorded: the backup a successful one made, and why a failed one failed,
// which outlives its Job. A schedule whose last run failed is not reported ready.
func TestScheduleReportsTheLastRun(t *testing.T) {
	const reported = "backup ns-demo-s1-20260102-040000 failed on chi-demo-main-0-0: disk full"
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")
	due := time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC)

	first := f.scheduledRunAt(due)
	finishJobAt(t, f.client, first.Name, batch.JobComplete, "", due.Add(time.Minute))
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, "ns-demo-s1-20260102-030000", got.Status.LastBackupName, "the backup is named after the slot of its run")
	lastBackup := condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded)
	require.Equal(t, meta.ConditionTrue, lastBackup.Status)
	require.Equal(t, api.BackupReasonCompleted, lastBackup.Reason)
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, got))

	second := f.scheduledRunAt(due.Add(time.Hour))
	failPodOfJob(t, f.client, second.Name, reported)
	finishJobAt(t, f.client, second.Name, batch.JobFailed, batch.JobReasonBackoffLimitExceeded, due.Add(time.Hour+time.Minute))
	f.reconcile("s1")
	got = f.schedule("s1")
	require.Equal(t, "ns-demo-s1-20260102-030000", got.Status.LastBackupName, "the last backup made is the one of the last success")
	require.Equal(t, due.Add(time.Hour+time.Minute).Unix(), got.Status.LastFailureTime.Unix())
	require.Equal(t, "Job "+second.Name+" failed: "+reported, got.Status.LastFailureMessage)
	lastBackup = condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded)
	require.Equal(t, meta.ConditionFalse, lastBackup.Status)
	require.Equal(t, api.BackupReasonJobFailed, lastBackup.Reason)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, meta.ConditionFalse, ready.Status)
	require.Equal(t, api.BackupReasonLastBackupFailed, ready.Reason)
	require.Contains(t, ready.Message, reported)
	messages := eventMessages(t, f.client, api.BackupReasonJobFailed)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], reported)

	third := f.scheduledRunAt(due.Add(2 * time.Hour))
	finishJobAt(t, f.client, third.Name, batch.JobComplete, "", due.Add(2*time.Hour+time.Minute))
	f.reconcile("s1")
	f.reconcile("s1")
	got = f.schedule("s1")
	require.Equal(t, "ns-demo-s1-20260102-050000", got.Status.LastBackupName)
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded).Status)
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, got))
	require.Equal(t, "Job "+second.Name+" failed: "+reported, got.Status.LastFailureMessage, "the last failure stays on record")
}

// A scheduled run whose backup holds no data, as its Job tells, succeeded all the same: the schedule
// records the backup as its last one and stays ready, with reasons and a warning that tell why the
// backup restores no data and that retention does not count it. The schedule runs on through a
// restore like any other, and the next run that backs up data clears it.
func TestScheduleReportsARunWithoutData(t *testing.T) {
	const cause = `the tables pattern "db.*" matches no table, or none of the tables it matches holds data; ` +
		"it does not count toward the 7 backups with data that retention keeps"
	chbs := newScheduleCR("s1")
	chbs.Spec.Tables = "db.*"
	chbs.Spec.Retention.KeepLast = ptr.To(int32(7))
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs)
	f.reconcile("s1")
	due := time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC)

	empty := f.scheduledRunAt(due)
	succeedPodOfJob(t, f.client, empty.Name, model.NoDataMessage)
	finishJobAt(t, f.client, empty.Name, batch.JobComplete, "", due.Add(time.Minute))
	f.reconcile("s1")
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, "ns-demo-s1-20260102-030000", got.Status.LastBackupName)
	require.Equal(t, due.Add(time.Minute).Unix(), got.Status.LastSuccessfulTime.Unix())
	lastRun := condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded)
	require.Equal(t, meta.ConditionTrue, lastRun.Status, "a run without data succeeded")
	require.Equal(t, api.BackupReasonCompletedWithoutData, lastRun.Reason)
	require.Equal(t, "scheduled backup ns-demo-s1-20260102-030000 completed at 2026-01-02T03:01:00Z, but the backup holds no data: "+cause, lastRun.Message)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, meta.ConditionTrue, ready.Status, "the schedule runs on")
	require.Equal(t, api.BackupReasonLastBackupWithoutData, ready.Reason)
	require.Equal(t, "the last scheduled backup holds no data, CronJob s1-main-backup runs the schedule on: "+lastRun.Message, ready.Message)
	require.Equal(t, []string{
		"scheduled backup ns-demo-s1-20260102-030000 completed in Job " + empty.Name + ", but the backup holds no data: " + cause,
	}, eventMessagesOfType(t, f.client, api.BackupReasonCompletedWithoutData, core.EventTypeWarning), "the run is warned about once")
	require.Zero(t, eventCount(t, f.client, api.BackupReasonCompleted))
	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key(empty.Name), job))
	require.Contains(t, job.Annotations, model.AnnotationScheduledJobWithoutData)

	restore := jobOwnedBy(t, "r1-restore", newRestoreCR("r1"), model.Labels(api.ClickHouseRestoreCRDResourceKind, "r1", testCHI, "main", testStorage))
	require.NoError(t, f.client.Create(context.Background(), restore))
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonSuspendedForRestore, readyReason(t, f.schedule("s1")))
	finishJob(t, f.client, "r1-restore", batch.JobComplete, "")
	f.reconcile("s1")
	require.Equal(t, api.BackupReasonLastBackupWithoutData, readyReason(t, f.schedule("s1")))
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonLastBackupWithoutData), "the resumption is announced")

	filled := f.scheduledRunAt(due.Add(time.Hour))
	succeedPodOfJob(t, f.client, filled.Name, "")
	finishJobAt(t, f.client, filled.Name, batch.JobComplete, "", due.Add(time.Hour+time.Minute))
	f.reconcile("s1")
	got = f.schedule("s1")
	require.Equal(t, "ns-demo-s1-20260102-040000", got.Status.LastBackupName)
	require.Equal(t, api.BackupReasonCompleted, condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded).Reason)
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, got))
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonCompleted))
	require.Len(t, eventMessages(t, f.client, api.BackupReasonCompletedWithoutData), 1)
}

// The cause of a run without data tells the tables and partitions its Job backed up, and the
// retention it ran with, as the Job was built: an edit of the schedule after the run started does
// not rewrite it.
func TestScheduleTellsTheCauseOfARunWithoutDataAsItsJobRan(t *testing.T) {
	const cause = `the tables pattern "db.*" matches no table, or none of the tables it matches holds data in the partitions ` +
		`"202601,202602"; it does not count toward the 7 backups with data that retention keeps`
	chbs := newScheduleCR("s1")
	chbs.Spec.Tables = "db.*"
	chbs.Spec.Partitions = []string{"202601", "202602"}
	chbs.Spec.Retention.KeepLast = ptr.To(int32(7))
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs)
	f.reconcile("s1")
	run := f.scheduledRunAt(firstRunDue)
	succeedPodOfJob(t, f.client, run.Name, model.NoDataMessage)
	finishJobAt(t, f.client, run.Name, batch.JobComplete, "", firstRunDue.Add(time.Minute))

	edited := f.schedule("s1")
	edited.Spec.Tables = ""
	edited.Spec.Partitions = nil
	edited.Spec.Retention.KeepLast = ptr.To(int32(3))
	require.NoError(t, f.client.Update(context.Background(), edited))
	f.reconcile("s1")
	f.reconcile("s1")
	require.Equal(t, "scheduled backup ns-demo-s1-20260102-030000 completed at 2026-01-02T03:01:00Z, but the backup holds no data: "+cause,
		condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionLastBackupSucceeded).Message)
	require.Equal(t, []string{
		"scheduled backup ns-demo-s1-20260102-030000 completed in Job " + run.Name + ", but the backup holds no data: " + cause,
	}, eventMessages(t, f.client, api.BackupReasonCompletedWithoutData))
}

// A run without data marked reported on its Job while the status write recording it is lost to a
// conflict is recorded again as a run without data, as its Job tells, even once its pod is gone.
func TestScheduleRecordsARunWithoutDataWhoseStatusWriteWasLost(t *testing.T) {
	conflicts := 0
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), conflictingStatusUpdates(&conflicts), newScheduleCR("s1"))
	f.reconcile("s1")
	run := f.scheduledRunAt(time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC))
	succeedPodOfJob(t, f.client, run.Name, model.NoDataMessage)
	finishJob(t, f.client, run.Name, batch.JobComplete, "")

	conflicts = 1
	f.reconcile("s1")
	require.Nil(t, f.schedule("s1").Status.LastSuccessfulTime, "the status write was lost")
	pod := &core.Pod{}
	require.NoError(t, f.client.Get(context.Background(), key(run.Name+"-pod"), pod))
	require.NoError(t, f.client.Delete(context.Background(), pod))

	f.reconcile("s1")
	got := f.schedule("s1")
	require.NotNil(t, got.Status.LastSuccessfulTime)
	require.Equal(t, api.BackupReasonCompletedWithoutData, condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded).Reason)
	require.Equal(t, api.BackupReasonLastBackupWithoutData, readyReason(t, got))
	require.Len(t, eventMessages(t, f.client, api.BackupReasonCompletedWithoutData), 1)
}

// A run marked reported on its Job while the status write recording it is lost to a conflict is
// recorded by a later reconcile all the same, and announced once.
func TestScheduleRecordsARunWhoseStatusWriteWasLost(t *testing.T) {
	conflicts := 0
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), conflictingStatusUpdates(&conflicts), newScheduleCR("s1"))
	f.reconcile("s1")
	run := f.scheduledRunAt(time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC))
	failPodOfJob(t, f.client, run.Name, "disk full")
	finishJob(t, f.client, run.Name, batch.JobFailed, batch.JobReasonBackoffLimitExceeded)

	conflicts = 1
	f.reconcile("s1")
	require.Nil(t, f.schedule("s1").Status.LastFailureTime, "the status write was lost")
	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key(run.Name), job))
	require.Equal(t, string(api.OperationPhaseFailed), job.Annotations[model.AnnotationScheduledJobObserved])

	f.reconcile("s1")
	got := f.schedule("s1")
	require.NotNil(t, got.Status.LastFailureTime)
	require.Equal(t, "Job "+run.Name+" failed: disk full", got.Status.LastFailureMessage)
	require.Equal(t, api.BackupReasonLastBackupFailed, readyReason(t, got))
	require.Len(t, eventMessages(t, f.client, api.BackupReasonJobFailed), 1)
}

// A success the CronJob knows of, of a run whose Job went before the schedule saw it, is a later
// success than any recorded: it clears the backup name of an older run rather than keep it.
func TestScheduleTakesASuccessOnlyTheCronJobKnows(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")
	due := time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC)
	run := f.scheduledRunAt(due)
	finishJobAt(t, f.client, run.Name, batch.JobComplete, "", due.Add(time.Minute))
	f.reconcile("s1")
	require.Equal(t, "ns-demo-s1-20260102-030000", f.schedule("s1").Status.LastBackupName)

	unseen := meta.NewTime(due.Add(25 * time.Hour))
	cronJob := f.cronJob("s1-main-backup")
	cronJob.Status.LastSuccessfulTime = &unseen
	require.NoError(t, f.client.Status().Update(context.Background(), cronJob))
	f.reconcile("s1")
	got := f.schedule("s1")
	require.True(t, unseen.Equal(got.Status.LastSuccessfulTime))
	require.Empty(t, got.Status.LastBackupName)
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded).Status)
}

// The last-outcome gauges live in the operator's memory: every reconcile sets them again from the
// status, so that they are back after a restart, of the clusters the installation still backs up.
func TestScheduleOutcomeGaugesComeBackFromTheStatus(t *testing.T) {
	captured := captureOutcomes(t)
	succeeded, failed := meta.NewTime(time.Unix(1700000000, 0)), meta.NewTime(time.Unix(1700003600, 0))
	chbs := newScheduleCR("s1")
	chbs.Status.Clusters = []api.ScheduleClusterStatus{{Name: "main", CronJobName: "s1-main-backup", LastSuccessfulTime: &succeeded, LastFailureTime: &failed}}
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs)

	f.reconcile("s1")
	require.Equal(t, []outcome{{namespace: testNamespace, chi: testCHI, cluster: "main", at: succeeded.Unix()}}, captured.successes)
	require.Equal(t, []outcome{{namespace: testNamespace, chi: testCHI, cluster: "main", at: failed.Unix()}}, captured.failures)
}

// retainingSchedule returns a schedule with retention on.
func retainingSchedule(selection api.ReplicaSelection) *api.ClickHouseBackupSchedule {
	chbs := newScheduleCR("s1")
	chbs.Spec.Retention.KeepLast = ptr.To(int32(3))
	chbs.Spec.ReplicaSelection = selection
	return chbs
}

// firstRunDue is when the first scheduled run of a test is due.
var firstRunDue = time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC)

// ranAt has every CronJob of the schedule run once, for the slot due at the given time, as the CronJob
// is by then: the run backs up, and prunes on, the hosts the CronJob runs on.
func (f *scheduleFixture) ranAt(due time.Time) {
	f.t.Helper()
	cronJobs := &batch.CronJobList{}
	require.NoError(f.t, f.client.List(context.Background(), cronJobs, client.InNamespace(testNamespace)))
	for i := range cronJobs.Items {
		run := f.cronJobRunAt(cronJobs.Items[i].Name, due)
		finishJobAt(f.t, f.client, run.Name, batch.JobComplete, "", due.Add(time.Minute))
	}
}

// selectReplicas changes the replicas the schedule backs up.
func (f *scheduleFixture) selectReplicas(selection api.ReplicaSelection) {
	f.t.Helper()
	chbs := f.schedule("s1")
	chbs.Spec.ReplicaSelection = selection
	require.NoError(f.t, f.client.Update(context.Background(), chbs))
}

// singleReplica returns a cluster with one replica per shard.
func singleReplica(cluster *api.Cluster) *api.Cluster {
	cluster.Layout.ReplicasCount = 1
	return cluster
}

// No run reaches a host that left the hosts the schedule backs up, so retention prunes the storage
// path a run uploaded to no more. The operator leaves its backups alone, as they may be all that is
// left of a removed shard, and records the host instead, warning about it once.
func TestScheduleRecordsTheHostsRetentionNoLongerPrunes(t *testing.T) {
	tests := []struct {
		name     string
		schedule *api.ClickHouseBackupSchedule
		before   []*api.Cluster
		after    func(t *testing.T) *api.ClickHouseInstallation
		hosts    []string
		unpruned []string
	}{
		{
			name:     "a removed shard",
			schedule: retainingSchedule(""),
			before:   []*api.Cluster{backupCluster("main", 3, true)},
			after: func(t *testing.T) *api.ClickHouseInstallation {
				return installation(t, api.StatusCompleted, backupCluster("main", 2, true))
			},
			hosts:    []string{"chi-demo-main-0-0", "chi-demo-main-1-0"},
			unpruned: []string{"chi-demo-main-2-0"},
		},
		{
			name:     "a removed replica of a schedule that backs up every replica",
			schedule: retainingSchedule(api.ReplicaSelectionAllReplicas),
			before:   []*api.Cluster{backupCluster("main", 2, true)},
			after: func(t *testing.T) *api.ClickHouseInstallation {
				return installation(t, api.StatusCompleted, singleReplica(backupCluster("main", 2, true)))
			},
			hosts:    []string{"chi-demo-main-0-0", "chi-demo-main-1-0"},
			unpruned: []string{"chi-demo-main-0-1", "chi-demo-main-1-1"},
		},
		{
			name:     "a removed replica of a schedule that backs up one replica per shard",
			schedule: retainingSchedule(api.ReplicaSelectionFirstPerShard),
			before:   []*api.Cluster{backupCluster("main", 2, true)},
			after: func(t *testing.T) *api.ClickHouseInstallation {
				return installation(t, api.StatusCompleted, singleReplica(backupCluster("main", 2, true)))
			},
			hosts: []string{"chi-demo-main-0-0", "chi-demo-main-1-0"},
		},
		{
			name:     "a cluster with backup disabled",
			schedule: retainingSchedule(""),
			before:   []*api.Cluster{backupCluster("main", 1, true), backupCluster("second", 1, true)},
			after: func(t *testing.T) *api.ClickHouseInstallation {
				return installation(t, api.StatusCompleted, backupCluster("main", 1, true), backupCluster("second", 1, false))
			},
			hosts:    []string{"chi-demo-main-0-0"},
			unpruned: []string{"chi-demo-second-0-0"},
		},
		{
			name:     "a removed cluster",
			schedule: retainingSchedule(""),
			before:   []*api.Cluster{backupCluster("main", 1, true), backupCluster("second", 1, true)},
			after: func(t *testing.T) *api.ClickHouseInstallation {
				return installation(t, api.StatusCompleted, backupCluster("main", 1, true))
			},
			hosts:    []string{"chi-demo-main-0-0"},
			unpruned: []string{"chi-demo-second-0-0"},
		},
		{
			name: "a removed shard of a schedule without retention",
			schedule: func() *api.ClickHouseBackupSchedule {
				chbs := retainingSchedule("")
				chbs.Spec.Retention.KeepLast = nil
				return chbs
			}(),
			before: []*api.Cluster{backupCluster("main", 3, true)},
			after: func(t *testing.T) *api.ClickHouseInstallation {
				return installation(t, api.StatusCompleted, backupCluster("main", 2, true))
			},
			hosts: []string{"chi-demo-main-0-0", "chi-demo-main-1-0"},
		},
		{
			name:     "a shard being removed",
			schedule: retainingSchedule(""),
			before:   []*api.Cluster{backupCluster("main", 3, true)},
			after: func(t *testing.T) *api.ClickHouseInstallation {
				return installation(t, api.StatusInProgress, backupCluster("main", 2, true))
			},
			hosts: []string{"chi-demo-main-0-0", "chi-demo-main-1-0", "chi-demo-main-2-0"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newScheduleFixture(t, newFakeCR(installation(t, api.StatusCompleted, tt.before...)), nil, tt.schedule)
			f.reconcile("s1")
			f.ranAt(firstRunDue)
			f.reconcile("s1")
			require.Empty(t, unprunedHosts(f.schedule("s1")))

			f.cr.set(tt.after(t))
			f.reconcile("s1")
			f.reconcile("s1")
			got := f.schedule("s1")
			require.Equal(t, tt.hosts, retentionHosts(got))
			require.Equal(t, tt.unpruned, unprunedHosts(got))
			warnings := eventMessagesOfType(t, f.client, api.BackupReasonUnprunedHosts, core.EventTypeWarning)
			if len(tt.unpruned) == 0 {
				require.Empty(t, warnings)
				return
			}
			require.Len(t, warnings, 1, "a host that left is warned about once")
			require.Contains(t, warnings[0], strings.Join(tt.unpruned, ", "))
			require.Contains(t, warnings[0], "ns-demo-s1-YYYYMMDD-HHMMSS", "the warning names the series retention prunes")
		})
	}
}

// Only a run leaves backups behind: a host the CronJob ran on while no run reached it - of a
// suspended schedule, or one added and removed again between two runs - leaves none, and is not
// recorded.
func TestScheduleRecordsOnlyTheHostsARunBackedUp(t *testing.T) {
	suspended := retainingSchedule("")
	suspended.Spec.Suspend = types.NewStringBool(true)
	tests := []struct {
		name     string
		schedule *api.ClickHouseBackupSchedule
		change   func(t *testing.T, f *scheduleFixture)
		hosts    []string
	}{
		{
			name:     "a removed shard of a suspended schedule",
			schedule: suspended,
			change: func(t *testing.T, f *scheduleFixture) {
				f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 1, true)))
			},
		},
		{
			name:     "a shard added and removed again between two runs",
			schedule: retainingSchedule(""),
			change: func(t *testing.T, f *scheduleFixture) {
				f.ranAt(firstRunDue)
				f.reconcile("s1")
				f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 3, true)))
				f.reconcile("s1")
				f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 2, true)))
			},
			hosts: []string{"chi-demo-main-0-0", "chi-demo-main-1-0"},
		},
		{
			name:     "replicas selected and deselected again between two runs",
			schedule: retainingSchedule(api.ReplicaSelectionFirstPerShard),
			change: func(t *testing.T, f *scheduleFixture) {
				f.ranAt(firstRunDue)
				f.reconcile("s1")
				f.selectReplicas(api.ReplicaSelectionAllReplicas)
				f.reconcile("s1")
				f.selectReplicas(api.ReplicaSelectionFirstPerShard)
			},
			hosts: []string{"chi-demo-main-0-0", "chi-demo-main-1-0"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newScheduleFixture(t, newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 2, true))), nil, tt.schedule)
			f.reconcile("s1")
			tt.change(t, f)
			f.reconcile("s1")
			f.reconcile("s1")
			got := f.schedule("s1")
			require.Empty(t, unprunedHosts(got))
			require.Empty(t, eventMessages(t, f.client, api.BackupReasonUnprunedHosts))
			require.Equal(t, tt.hosts, retentionHosts(got))
		})
	}
}

// A run that starts while the installation reconciles backs up the hosts the CronJob runs on by
// then, among them a shard the reconcile removes: that shard is recorded once the reconcile completes.
func TestScheduleRecordsTheHostsOfARunDuringAReconcile(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 3, true))), nil, retainingSchedule(""))
	f.reconcile("s1")
	f.cr.set(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
	f.reconcile("s1")
	f.ranAt(firstRunDue)
	f.reconcile("s1")

	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 2, true)))
	f.reconcile("s1")
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, retentionHosts(got))
	require.Equal(t, []string{"chi-demo-main-2-0"}, unprunedHosts(got))
	require.Len(t, eventMessages(t, f.client, api.BackupReasonUnprunedHosts), 1)
}

// A host that rejoins the hosts the schedule backs up is pruned again, and one that leaves again is
// recorded again, as it still holds the backups of the runs before it first left.
func TestScheduleDropsAnUnprunedHostThatRejoins(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 3, true))), nil, retainingSchedule(""))
	f.reconcile("s1")
	f.ranAt(firstRunDue)
	f.reconcile("s1")
	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 1, true)))
	f.reconcile("s1")
	require.Equal(t, []string{"chi-demo-main-1-0", "chi-demo-main-2-0"}, unprunedHosts(f.schedule("s1")))

	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 2, true)))
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, retentionHosts(got))
	require.Equal(t, []string{"chi-demo-main-2-0"}, unprunedHosts(got))

	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 1, true)))
	f.reconcile("s1")
	require.Equal(t, []string{"chi-demo-main-2-0", "chi-demo-main-1-0"}, unprunedHosts(f.schedule("s1")))
	require.Len(t, eventMessages(t, f.client, api.BackupReasonUnprunedHosts), 2)
}

// The CronJob already runs without the hosts that left when the status write recording them is
// lost to a conflict; a later reconcile records them all the same, and warns once.
func TestScheduleRecordsUnprunedHostsWhoseStatusWriteWasLost(t *testing.T) {
	conflicts := 0
	f := newScheduleFixture(t, newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 3, true))),
		conflictingStatusUpdates(&conflicts), retainingSchedule(""))
	f.reconcile("s1")
	f.ranAt(firstRunDue)
	f.reconcile("s1")

	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 2, true)))
	conflicts = 1
	f.reconcile("s1")
	require.Contains(t, f.cronJobEnv("s1-main-backup"), "chi-demo-main-0-0 chi-demo-main-1-0")
	require.Empty(t, unprunedHosts(f.schedule("s1")), "the status write was lost")
	require.Empty(t, eventMessages(t, f.client, api.BackupReasonUnprunedHosts))

	f.reconcile("s1")
	f.reconcile("s1")
	require.Equal(t, []string{"chi-demo-main-2-0"}, unprunedHosts(f.schedule("s1")))
	require.Len(t, eventMessages(t, f.client, api.BackupReasonUnprunedHosts), 1)
}

// A host that rejoins and leaves again while the status write recording its return was lost is
// listed once among the unpruned hosts.
func TestScheduleListsAnUnprunedHostOnce(t *testing.T) {
	conflicts := 0
	f := newScheduleFixture(t, newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 3, true))),
		conflictingStatusUpdates(&conflicts), retainingSchedule(""))
	f.reconcile("s1")
	f.ranAt(firstRunDue)
	f.reconcile("s1")
	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 2, true)))
	f.reconcile("s1")
	require.Equal(t, []string{"chi-demo-main-2-0"}, unprunedHosts(f.schedule("s1")))

	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 3, true)))
	conflicts = 1
	f.reconcile("s1")
	require.Equal(t, []string{"chi-demo-main-2-0"}, unprunedHosts(f.schedule("s1")), "the status write was lost")

	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 2, true)))
	f.reconcile("s1")
	f.reconcile("s1")
	require.Equal(t, []string{"chi-demo-main-2-0"}, unprunedHosts(f.schedule("s1")))
}

// Once the backups a host left behind are cleaned up by hand, the host can be cleared from the
// status, and it is not recorded again.
func TestScheduleKeepsUnprunedHostsClearedByHand(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 3, true))), nil, retainingSchedule(""))
	f.reconcile("s1")
	f.ranAt(firstRunDue)
	f.reconcile("s1")
	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 2, true)))
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, []string{"chi-demo-main-2-0"}, unprunedHosts(got))

	for i := range got.Status.Clusters {
		got.Status.Clusters[i].UnprunedHosts = nil
	}
	require.NoError(t, f.client.Status().Update(context.Background(), got))
	f.reconcile("s1")
	f.reconcile("s1")
	require.Empty(t, unprunedHosts(f.schedule("s1")))
	require.Len(t, eventMessages(t, f.client, api.BackupReasonUnprunedHosts), 1)
}

// A schedule runs a CronJob per backup-enabled cluster, each driving the hosts of its own cluster.
// They share the cron schedule and the backup name prefix, so the runs of one slot make one backup
// name on every cluster.
func TestScheduleRunsACronJobPerCluster(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newScheduleCR("s1"))

	require.Equal(t, ctrl.Result{RequeueAfter: topologyRecheckInterval}, f.reconcile("s1"))
	hot, cold := f.cronJob("s1-hot-backup"), f.cronJob("s1-cold-backup")
	require.Equal(t, hot.Spec.Schedule, cold.Spec.Schedule)
	require.Contains(t, f.cronJobEnv("s1-hot-backup"), "chi-demo-hot-0-0 chi-demo-hot-1-0")
	require.Contains(t, f.cronJobEnv("s1-cold-backup"), "chi-demo-cold-0-0")
	require.NotContains(t, f.cronJobEnv("s1-cold-backup"), "chi-demo-hot-0-0 chi-demo-hot-1-0")
	require.Contains(t, f.cronJobEnv("s1-hot-backup"), "ns-demo-s1")
	require.Contains(t, f.cronJobEnv("s1-cold-backup"), "ns-demo-s1")
	err := f.client.Get(context.Background(), key("s1-plain-backup"), &batch.CronJob{})
	require.True(t, apiErrors.IsNotFound(err), "a cluster without backup gets no CronJob")

	got := f.schedule("s1")
	require.Equal(t, []string{"hot", "cold"}, []string{got.Status.Clusters[0].Name, got.Status.Clusters[1].Name})
	require.Equal(t, "s1-cold-backup", got.Status.Clusters[1].CronJobName)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonScheduled, ready.Reason)
	require.Equal(t, "CronJobs s1-hot-backup, s1-cold-backup run the schedule", ready.Message)
}

// The status of a schedule adds its clusters up: the backup to recover the installation from is the
// newest one every cluster made, and by the last success of the schedule every cluster had a backup.
// A run that failed on one cluster fails the last run of the schedule.
func TestScheduleAddsUpItsClusters(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newScheduleCR("s1"))
	f.reconcile("s1")
	due := time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC)

	f.ranAt(due)
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, "ns-demo-s1-20260102-030000", got.Status.LastBackupName)
	require.Equal(t, due.Add(time.Minute).Unix(), got.Status.LastSuccessfulTime.Unix())
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded).Status)

	hot := f.cronJobRunAt("s1-hot-backup", due.Add(time.Hour))
	finishJobAt(t, f.client, hot.Name, batch.JobComplete, "", due.Add(time.Hour+time.Minute))
	cold := f.cronJobRunAt("s1-cold-backup", due.Add(time.Hour))
	failPodOfJob(t, f.client, cold.Name, "disk full")
	finishJobAt(t, f.client, cold.Name, batch.JobFailed, batch.JobReasonBackoffLimitExceeded, due.Add(time.Hour+2*time.Minute))
	f.reconcile("s1")
	got = f.schedule("s1")
	require.Empty(t, got.Status.LastBackupName, "the retention of hot prunes the backup cold made last, while cold stays behind")
	require.Equal(t, "ns-demo-s1-20260102-040000", got.Status.Clusters[0].LastBackupName)
	require.Equal(t, due.Add(time.Minute).Unix(), got.Status.LastSuccessfulTime.Unix(), "the oldest of the last successes")
	require.Equal(t, due.Add(time.Hour+2*time.Minute).Unix(), got.Status.LastFailureTime.Unix())
	require.Equal(t, "Job "+cold.Name+" failed: disk full", got.Status.LastFailureMessage)
	lastRun := condition(t, got.Status.Conditions, api.BackupConditionLastBackupSucceeded)
	require.Equal(t, meta.ConditionFalse, lastRun.Status, "a failure on one cluster is a failed run")
	require.Equal(t, api.BackupReasonJobFailed, lastRun.Reason)
	require.Equal(t, "cluster cold: Job "+cold.Name+" failed: disk full", lastRun.Message)
	require.Equal(t, api.BackupReasonLastBackupFailed, readyReason(t, got))
	require.Contains(t, eventMessages(t, f.client, api.BackupReasonJobFailed)[0], "scheduled backup of cluster cold failed")
}

// A restore of one cluster holds the CronJob of that cluster alone; a restore of every cluster holds
// every CronJob of the installation.
func TestScheduleHoldsOnlyTheClustersARestoreRestores(t *testing.T) {
	suspended := func(f *scheduleFixture, name string) bool {
		return ptr.Deref(f.cronJob(name).Spec.Suspend, false)
	}
	t.Run("one cluster", func(t *testing.T) {
		restore := newRestoreCR("r1")
		restore.Spec.Cluster = "cold"
		newRestoreOperation(restore).pin([]string{"cold"})
		f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newScheduleCR("s1"), restore)

		f.reconcile("s1")
		require.True(t, suspended(f, "s1-cold-backup"))
		require.False(t, suspended(f, "s1-hot-backup"), "a restore of another cluster holds nothing here")
		ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
		require.Equal(t, api.BackupReasonSuspendedForRestore, ready.Reason)
		require.Equal(t, "a restore of cluster cold of ClickHouseInstallation demo is in progress", ready.Message)
	})
	t.Run("every cluster", func(t *testing.T) {
		restore := newRestoreCR("r1")
		newRestoreOperation(restore).pin([]string{"hot", "cold"})
		f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newScheduleCR("s1"), restore)

		f.reconcile("s1")
		require.True(t, suspended(f, "s1-cold-backup"))
		require.True(t, suspended(f, "s1-hot-backup"))
	})
	t.Run("the Job of a restore of one cluster", func(t *testing.T) {
		job := jobOwnedBy(t, "gone-hot-restore", newRestoreCR("gone"), model.Labels(api.ClickHouseRestoreCRDResourceKind, "gone", testCHI, "hot", testStorage))
		f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newScheduleCR("s1"), job)

		f.reconcile("s1")
		require.True(t, suspended(f, "s1-hot-backup"))
		require.False(t, suspended(f, "s1-cold-backup"))
	})
}

// The CronJob of a cluster the schedule no longer backs up is deleted. With retention on, the status
// of the cluster stays while its hosts hold backups retention no longer prunes; without, it goes.
func TestScheduleDropsTheCronJobOfALeavingCluster(t *testing.T) {
	for name, tc := range map[string]struct {
		keep     *int32
		unpruned []string
	}{
		"with retention":    {keep: ptr.To(int32(3)), unpruned: []string{"chi-demo-cold-0-0"}},
		"without retention": {},
	} {
		t.Run(name, func(t *testing.T) {
			chbs := newScheduleCR("s1")
			chbs.Spec.Retention.KeepLast = tc.keep
			cr := newFakeCR(twoClusters(t, api.StatusCompleted))
			f := newScheduleFixture(t, cr, nil, chbs)
			f.reconcile("s1")
			f.ranAt(firstRunDue)
			f.reconcile("s1")

			cr.set(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, false)))
			f.reconcile("s1")
			err := f.client.Get(context.Background(), key("s1-cold-backup"), &batch.CronJob{})
			require.True(t, apiErrors.IsNotFound(err), "the CronJob of a cluster that left is deleted")
			got := f.schedule("s1")
			require.Equal(t, tc.unpruned, unprunedHosts(got))
			if tc.unpruned == nil {
				require.Len(t, got.Status.Clusters, 1)
				return
			}
			require.Len(t, got.Status.Clusters, 2)
			require.Equal(t, "cold", got.Status.Clusters[1].Name)
			require.Empty(t, got.Status.Clusters[1].CronJobName)
			require.Equal(t, api.BackupReasonScheduled, readyReason(t, got))
			require.Equal(t, "CronJob s1-hot-backup runs the schedule", condition(t, got.Status.Conditions, api.BackupConditionReady).Message)
		})
	}
}

// An installation restored from a backup its schedule's default prefix names typically runs where
// the installation that made the backup runs a schedule of the same name, on the same storage
// path: their runs of one slot would overwrite each other's backups, and each would prune the
// other's. Such a schedule is held, and warned about once, until its prefix is set, even to the same
// value, which is an informed choice; a recovery about to happen holds it alike.
func TestScheduleHoldsWhileItSharesTheSourcePrefix(t *testing.T) {
	const source = "ns-demo-s1-20261004-020000"
	// A recovery that waits for a cluster to get a storage is about to happen all the same
	lacking := lackingCluster("lacking", 1)
	lacking.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString(source)}
	for name, chi := range map[string]*api.ClickHouseInstallation{
		"recovered":        withAnnotations(readyInstallation(t), map[string]string{api.AnnotationRecoveredFrom: "main:" + source}),
		"being recovered":  recoveryTarget(t, api.StatusCompleted, source),
		"recovered by one": withAnnotations(recoveryTarget(t, api.StatusCompleted, source), map[string]string{api.AnnotationRecoveredFrom: source}),
		"to be recovered once a cluster has a storage": installation(t, api.StatusCompleted, backupCluster("main", 2, true), lacking),
	} {
		t.Run(name, func(t *testing.T) {
			f := newScheduleFixture(t, newFakeCR(chi), nil, newScheduleCR("s1"))

			f.reconcile("s1")
			f.reconcile("s1")
			require.True(t, ptr.Deref(f.cronJob("s1-main-backup").Spec.Suspend, false))
			ready := condition(t, f.schedule("s1").Status.Conditions, api.BackupConditionReady)
			require.Equal(t, api.BackupReasonSharesSourcePrefix, ready.Reason)
			require.Contains(t, ready.Message, source)
			require.Contains(t, ready.Message, "spec.backupNamePrefix")
			require.Len(t, eventMessagesOfType(t, f.client, api.BackupReasonSharesSourcePrefix, core.EventTypeWarning), 1)

			chbs := f.schedule("s1")
			chbs.Spec.BackupNamePrefix = "ns-demo-s1"
			require.NoError(t, f.client.Update(context.Background(), chbs))
			f.reconcile("s1")
			require.NotEqual(t, api.BackupReasonSharesSourcePrefix, readyReason(t, f.schedule("s1")))
		})
	}

	t.Run("another installation", func(t *testing.T) {
		chi := withAnnotations(readyInstallation(t), map[string]string{api.AnnotationRecoveredFrom: "ns-demo2-s1-20261004-020000"})
		f := newScheduleFixture(t, newFakeCR(chi), nil, newScheduleCR("s1"))
		f.reconcile("s1")
		require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")), "a prefix is told by the dash that ends it")
	})
}

// The Ready condition follows the runs while the installation reconciles, as the CronJobs run on.
func TestScheduleReadyFollowsRunsWhileTheInstallationReconciles(t *testing.T) {
	cr := newFakeCR(readyInstallation(t))
	f := newScheduleFixture(t, cr, nil, newScheduleCR("s1"))
	f.reconcile("s1")
	cr.set(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))

	run := f.scheduledRunAt(firstRunDue)
	finishJobAt(t, f.client, run.Name, batch.JobFailed, batch.JobReasonBackoffLimitExceeded, firstRunDue.Add(time.Minute))
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, api.BackupReasonLastBackupFailed, readyReason(t, got))
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionValidated).Status)

	held := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"), failedRestore("r1", api.BackupReasonJobFailed))
	held.reconcile("s1")
	held.cr.set(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
	held.reconcile("s1")
	require.Equal(t, api.BackupReasonSuspendedForFailedRestore, readyReason(t, held.schedule("s1")),
		"a hold found before the reconcile stays until the reconcile completes")
}

// The operator records when it suspends a CronJob, which a restore waits on, and drops the record
// when it resumes the CronJob.
func TestScheduleRecordsWhenItSuspendsACronJob(t *testing.T) {
	restore := jobOwnedBy(t, "r1-main-restore", newRestoreCR("r1"), model.Labels(api.ClickHouseRestoreCRDResourceKind, "r1", testCHI, "main", testStorage))
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"), restore)

	f.reconcile("s1")
	suspendedAt := f.cronJob("s1-main-backup").Annotations[model.AnnotationCronJobSuspendedAt]
	require.NotEmpty(t, suspendedAt, "a CronJob created suspended records it")
	ageSuspensions(t, f.client)
	suspendedAt = f.cronJob("s1-main-backup").Annotations[model.AnnotationCronJobSuspendedAt]
	f.cr.set(installation(t, api.StatusCompleted, backupCluster("main", 3, true)))
	f.reconcile("s1")
	require.Contains(t, f.cronJobEnv("s1-main-backup"), "chi-demo-main-0-0 chi-demo-main-1-0 chi-demo-main-2-0", "the CronJob was updated")
	require.Equal(t, suspendedAt, f.cronJob("s1-main-backup").Annotations[model.AnnotationCronJobSuspendedAt],
		"the record is of the suspension, not of the last update of the CronJob")

	finishJob(t, f.client, "r1-main-restore", batch.JobComplete, "")
	f.reconcile("s1")
	require.NotContains(t, f.cronJob("s1-main-backup").Annotations, model.AnnotationCronJobSuspendedAt)

	invalid := f.schedule("s1")
	invalid.Spec.BackupNamePrefix = "-invalid"
	require.NoError(t, f.client.Update(context.Background(), invalid))
	f.reconcile("s1")
	require.Contains(t, f.cronJob("s1-main-backup").Annotations, model.AnnotationCronJobSuspendedAt, "a hold records it alike")
}

// A schedule reads the restores from a cache that may lag behind: one that sees no hold rechecks the
// restores past the cache before it creates or resumes a CronJob, and keeps it suspended for a restore
// validated meanwhile.
func TestScheduleRechecksTheRestoresPastItsCache(t *testing.T) {
	ctx := context.Background()
	validated := newRestoreCR("r1")
	newRestoreOperation(validated).pin([]string{"main"})
	live := newClient(t, nil, newScheduleCR("s1"), validated)
	lagging := interceptor.NewClient(live, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if restores, ok := list.(*api.ClickHouseRestoreList); ok {
				for i := range restores.Items {
					restores.Items[i].Status = api.ClickHouseRestoreStatus{}
				}
			}
			return nil
		},
	})
	schedule := NewScheduleController(lagging, live, live.Scheme(), newFakeCR(readyInstallation(t)))

	_, err := schedule.Reconcile(ctx, request("s1"))
	require.NoError(t, err)
	cronJob := &batch.CronJob{}
	require.NoError(t, live.Get(ctx, key("s1-main-backup"), cronJob))
	require.True(t, ptr.Deref(cronJob.Spec.Suspend, false), "the restore the cache does not show validated yet holds the CronJob")
}

// A failed restore r0 holds the schedules; the user deletes it and retries with r1. The schedule
// reconcile that the deletion triggers reads the restores before r1 is validated and is about to
// resume its CronJob; r1 is validated, and starts, meanwhile. A schedule resumes a CronJob only under
// the lock a restore starts under, after it checked the restores again past its cache, so the restore
// either sees the CronJob resumed and waits, or is seen and keeps the CronJob suspended: a restore Job
// never runs while a CronJob of its cluster is resumed, which would run a missed run at once.
func TestScheduleResumesNoCronJobARestoreStartsOn(t *testing.T) {
	ctx := context.Background()
	cr := newFakeCR(readyInstallation(t))
	base := newClient(t, nil, failedRestore("r0", api.BackupReasonJobFailed), newRestoreCR("r1"), newScheduleCR("s1"))
	restores := NewRestoreController(base, base, base.Scheme(), cr)

	plain := NewScheduleController(base, base, base.Scheme(), cr)
	_, err := plain.Reconcile(ctx, request("s1"))
	require.NoError(t, err)
	suspended := func() bool {
		cronJob := &batch.CronJob{}
		require.NoError(t, base.Get(ctx, key("s1-main-backup"), cronJob))
		return ptr.Deref(cronJob.Spec.Suspend, false)
	}
	require.True(t, suspended(), "the failed restore r0 holds the schedule")
	ageSuspensions(t, base)
	require.NoError(t, base.Delete(ctx, getRestore(t, base, "r0")))

	var once sync.Once
	restoreDone := make(chan struct{})
	funcs := interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if cronJob, ok := obj.(*batch.CronJob); ok && !ptr.Deref(cronJob.Spec.Suspend, false) {
				once.Do(func() {
					go func() {
						defer close(restoreDone)
						// the first reconcile validates r1 and records the hold, the second starts from it
						_, err := restores.Reconcile(ctx, request("r1"))
						require.NoError(t, err)
						_, err = restores.Reconcile(ctx, request("r1"))
						require.NoError(t, err)
					}()
					select {
					case <-restoreDone:
					case <-time.After(300 * time.Millisecond):
					}
				})
			}
			return c.Update(ctx, obj, opts...)
		},
	}
	schedule := NewScheduleController(interceptor.NewClient(base, funcs), base, base.Scheme(), cr)
	_, err = schedule.Reconcile(ctx, request("s1"))
	require.NoError(t, err)
	select {
	case <-restoreDone:
	case <-time.After(5 * time.Second):
		t.Fatal("restore reconciles did not finish")
	}

	err = base.Get(ctx, key("r1-main-restore"), &batch.Job{})
	restoreJobExists := err == nil
	require.False(t, restoreJobExists && !suspended(), "the restore Job runs while the schedule's CronJob is resumed")

	// The schedule then sees the recorded hold and suspends; the restore starts once the suspension is
	// old enough
	_, err = plain.Reconcile(ctx, request("s1"))
	require.NoError(t, err)
	require.True(t, suspended())
	ageSuspensions(t, base)
	_, err = restores.Reconcile(ctx, request("r1"))
	require.NoError(t, err)
	require.Equal(t, api.OperationPhaseRunning, getRestore(t, base, "r1").Status.Phase)
	require.NoError(t, base.Get(ctx, key("r1-main-restore"), &batch.Job{}))
}
