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
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"
	kubeSchema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	clickhouse_altinity_com "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// collidingInstallation returns an installation whose clusters c and b-c join with the resource names
// "a-b" and "a" to one name: "a-b" + "c" and "a" + "b-c" both give "a-b-c".
func collidingInstallation(t *testing.T, extra ...*api.Cluster) *api.ClickHouseInstallation {
	t.Helper()
	return installation(t, api.StatusCompleted, append([]*api.Cluster{backupCluster("c", 1, true), backupCluster("b-c", 1, true)}, extra...)...)
}

// clusterStatus returns the status of an operation on the named cluster.
func clusterStatus(t *testing.T, clusters []api.OperationClusterStatus, name string) api.OperationClusterStatus {
	t.Helper()
	for _, cluster := range clusters {
		if cluster.Name == name {
			return cluster
		}
	}
	require.Failf(t, "missing cluster", "%s", name)
	return api.OperationClusterStatus{}
}

// The Job a backup needs on a cluster may belong to another backup whose name and cluster join to the
// same Job name. No garbage collection ever removes it: waiting for one would hold the backup forever.
// The backup fails on that cluster, naming the other backup, and runs on its other clusters.
func TestBackupFailsOnAClusterWhoseJobNameAnotherBackupHolds(t *testing.T) {
	other := newBackupCR("a-b")
	other.Spec.Cluster = "c"
	f := newBackupFixture(t, newFakeCR(collidingInstallation(t)), nil, other, newBackupCR("a"))
	require.Equal(t, model.BackupJobName(other, "c"), model.BackupJobName(newBackupCR("a"), "b-c"))

	f.reconcile("a-b")
	finishJob(t, f.client, "a-b-c-backup", batch.JobComplete, "")
	f.reconcile("a-b")
	require.Equal(t, api.OperationPhaseCompleted, f.backup("a-b").Status.Phase)

	f.reconcile("a")
	got := f.backup("a")
	conflict := clusterStatus(t, got.Status.Clusters, "b-c")
	require.Equal(t, api.OperationPhaseFailed, conflict.Phase)
	require.Equal(t, api.BackupReasonNameConflict, conflict.Reason)
	require.Contains(t, conflict.Message, "ClickHouseBackup a-b on cluster c")
	require.Empty(t, conflict.JobName, "the Job is the other backup's")
	require.Len(t, eventMessagesOfType(t, f.client, api.BackupReasonNameConflict, core.EventTypeWarning), 1)

	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("a-b-c-backup"), job))
	require.True(t, meta.IsControlledBy(job, other), "the other backup keeps its Job")

	require.Equal(t, api.OperationPhaseRunning, clusterStatus(t, got.Status.Clusters, "c").Phase, "the other cluster backs up")
	finishJob(t, f.client, "a-c-backup", batch.JobComplete, "")
	f.reconcile("a")
	got = f.backup("a")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.BackupReasonPartiallyCompleted, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
}

// The verify Job of a cluster may belong to another backup alike: verification concludes there with the
// conflict, and the backup completes, as it does whatever verification concludes.
func TestBackupVerificationConcludesOnAJobNameAnotherBackupHolds(t *testing.T) {
	chb := newBackupCR("b")
	chb.Spec.Cluster = "m-x"
	chb.Spec.Verify = types.NewStringBool(true)
	other := newBackupCR("b-m")
	foreign := jobOwnedBy(t, model.VerifyJobName(chb, "m-x"), other, model.Labels(api.ClickHouseBackupCRDResourceKind, "b-m", testCHI, "x", testStorage))
	cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("m-x", 1, true), backupCluster("x", 1, true)))
	f := newBackupFixture(t, cr, nil, chb, other, foreign)

	f.reconcile("b")
	finishJob(t, f.client, "b-m-x-backup", batch.JobComplete, "")
	f.reconcile("b")
	f.reconcile("b")
	got := f.backup("b")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
	verified := condition(t, got.Status.Conditions, api.BackupConditionVerified)
	require.Equal(t, meta.ConditionFalse, verified.Status)
	require.Equal(t, api.BackupReasonNameConflict, verified.Reason)
	require.Contains(t, verified.Message, "ClickHouseBackup b-m on cluster x")
}

// A restore of every cluster whose Job name another restore holds on one cluster fails there without
// having changed anything, so that it ends once its other clusters are done and stops holding the
// schedules of the installation: waiting for the Job to go would hold them forever.
func TestRestoreOfEveryClusterEndsAndReleasesItsHoldOnAJobNameConflict(t *testing.T) {
	other := newRestoreCR("a-b")
	other.Spec.Cluster = "c"
	chr := newRestoreCR("a")
	c := newClient(t, nil, other, chr)
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(collidingInstallation(t)))

	startRestore(t, c, controller, "a-b")
	finishJob(t, c, "a-b-c-restore", batch.JobComplete, "")
	reconcileRestore(t, controller, "a-b")
	require.Equal(t, api.OperationPhaseCompleted, getRestore(t, c, "a-b").Status.Phase)

	reconcileRestore(t, controller, "a")
	got := getRestore(t, c, "a")
	conflict := clusterStatus(t, got.Status.Clusters, "b-c")
	require.Equal(t, api.OperationPhaseFailed, conflict.Phase)
	require.Equal(t, api.BackupReasonNameConflict, conflict.Reason)
	require.Contains(t, conflict.Message, "ClickHouseRestore a-b on cluster c")
	require.False(t, mayHaveChanged(conflict), "a Job of another restore changed nothing for this one")
	require.True(t, holdsSchedules(got), "the restore still runs on its other cluster")

	reconcileRestore(t, controller, "a")
	finishJob(t, c, "a-c-restore", batch.JobComplete, "")
	reconcileRestore(t, controller, "a")
	got = getRestore(t, c, "a")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.BackupReasonPartiallyCompleted, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	require.False(t, holdsSchedules(got), "a restore that ended without changing the conflicting cluster holds no schedule")
}

// A schedule whose CronJob name another schedule holds on one cluster runs no backup there, which it
// warns about once, and runs on its other clusters as ever: the CronJobs of those are created, and
// suspended for a restore of their cluster.
func TestScheduleHoldsOnlyTheClusterWhoseCronJobNameAnotherScheduleHolds(t *testing.T) {
	other := newScheduleCR("a-b")
	other.Spec.Cluster = "c"
	f := newScheduleFixture(t, newFakeCR(collidingInstallation(t, backupCluster("d", 1, true))), nil, other, newScheduleCR("a"))
	f.reconcile("a-b")

	require.Equal(t, targetRequeueInterval, f.reconcile("a").RequeueAfter)
	f.reconcile("a")
	got := f.schedule("a")
	for _, name := range []string{"a-c-backup", "a-d-backup"} {
		cronJob := f.cronJob(name)
		require.True(t, meta.IsControlledBy(cronJob, got), name)
		require.False(t, ptr.Deref(cronJob.Spec.Suspend, false), name)
	}
	require.True(t, meta.IsControlledBy(f.cronJob("a-b-c-backup"), other), "the other schedule keeps its CronJob")

	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonNameConflict, ready.Reason)
	require.Contains(t, ready.Message, "scheduled backups of cluster b-c cannot run")
	require.Contains(t, ready.Message, "ClickHouseBackupSchedule a-b on cluster c")
	require.Equal(t, api.BackupReasonNameConflict, condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Reason)
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonNameConflict), "the conflict is warned about once")
	require.NotNil(t, findScheduleCluster(got, "b-c"), "the cluster without a backup keeps the schedule from counting as backed up")
	require.Nil(t, got.Status.LastSuccessfulTime)

	restore := newRestoreCR("r1")
	restore.Spec.Cluster = "d"
	newRestoreOperation(restore).pin([]string{"d"})
	require.NoError(t, f.client.Create(context.Background(), restore))
	f.reconcile("a")
	require.True(t, ptr.Deref(f.cronJob("a-d-backup").Spec.Suspend, false), "a restore of another cluster still suspends its CronJob")
	require.False(t, ptr.Deref(f.cronJob("a-c-backup").Spec.Suspend, false))
	require.Equal(t, api.BackupReasonNameConflict, readyReason(t, f.schedule("a")), "the conflict needs somebody to act")
}

// A CronJob a deleted predecessor of the schedule left on one cluster holds that cluster until garbage
// collection removes it, and only that one: the other clusters run the schedule meanwhile.
func TestScheduleWaitsOutAStaleCronJobOnItsClusterOnly(t *testing.T) {
	predecessor := newScheduleCR("s1")
	predecessor.UID = "predecessor-uid"
	stale := &batch.CronJob{}
	stale.Name = "s1-hot-backup"
	stale.Namespace = testNamespace
	stale.Labels = model.Labels(api.ClickHouseBackupScheduleCRDResourceKind, "s1", testCHI, "hot", testStorage)
	stale.Spec.Schedule = "* * * * *"
	require.NoError(t, ctrl.SetControllerReference(predecessor, stale, newScheme(t)))
	f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newScheduleCR("s1"), stale)

	require.Equal(t, pendingRequeueInterval, f.reconcile("s1").RequeueAfter)
	got := f.schedule("s1")
	require.True(t, meta.IsControlledBy(f.cronJob("s1-cold-backup"), got), "the other cluster runs the schedule")
	require.True(t, meta.IsControlledBy(f.cronJob("s1-hot-backup"), predecessor))
	require.Equal(t, api.BackupReasonWaitingForGarbageCollection, readyReason(t, got))
	require.Zero(t, eventCount(t, f.client, api.BackupReasonNameConflict))

	require.NoError(t, f.client.Delete(context.Background(), stale))
	f.reconcile("s1")
	got = f.schedule("s1")
	require.True(t, meta.IsControlledBy(f.cronJob("s1-hot-backup"), got), "the CronJob is created once the predecessor's is gone")
	require.Equal(t, api.BackupReasonScheduled, readyReason(t, got))
}

// The restore that recovers a cluster may be named as the recovery of a cluster of another installation:
// "demo" + "x-y" and "demo-x" + "y". It is no leftover of a predecessor and never goes away by itself, so
// the recovery warns, once, that it cannot start, and creates nothing.
func TestRecoveryWarnsOfARestoreNameAnotherInstallationHolds(t *testing.T) {
	chi := withRecovery(installationSpec(backupCluster("x-y", 1, true)), recoveryBackup)
	otherCHI := installationSpec(backupCluster("y", 1, true))
	otherCHI.Name = "demo-x"
	otherCHI.UID = "other-chi-uid"
	foreign := newRestoreCR(model.RecoveryRestoreName(chi, "x-y"))
	foreign.Spec.ClickHouseInstallation = otherCHI.Name
	foreign.Spec.Cluster = "y"
	require.NoError(t, ctrl.SetControllerReference(otherCHI, foreign, newScheme(t)))
	require.Equal(t, model.RecoveryRestoreName(otherCHI, "y"), foreign.Name)
	f := newRecoveryFixture(t, newFakeCR(installationFrom(t, chi, api.StatusCompleted)), nil, chi, otherCHI, foreign)

	require.Equal(t, targetRequeueInterval, f.reconcile().RequeueAfter)
	require.Len(t, eventMessagesOfType(t, f.client, api.BackupReasonNameConflict, core.EventTypeWarning), 1, "the conflict is warned about at once")
	f.reconcile()
	f.reconcile()
	require.Equal(t, 1, f.restoreCount(), "no restore is created")
	require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom])
	messages := eventMessagesOfType(t, f.client, api.BackupReasonNameConflict, core.EventTypeWarning)
	require.Len(t, messages, 1, "the conflict is warned about once")
	require.Contains(t, messages[0], "unable to recover cluster x-y")
	require.Contains(t, messages[0], "is taken by a restore of cluster y of ClickHouseInstallation demo-x: delete that restore once "+
		"ClickHouseInstallation demo-x has recorded its recovery, or no longer asks for one, as it creates the restore again until then")
}

// A Job of the name a backup needs that the backup does not control either goes away by itself or stays.
// Garbage collection removes one whose controller is gone, was replaced by another resource of its name,
// or is being deleted, and one being deleted itself: the backup waits for it. Nothing removes one another
// live resource controls, or one no resource controls, as a deletion that orphaned its dependents leaves
// behind: the backup fails on that cluster, telling what to do, rather than wait for good.
func TestBackupTellsAJobThatGoesAwayFromOneThatStays(t *testing.T) {
	now := meta.Now()
	for name, tc := range map[string]struct {
		// controller controls the Job of cluster b-c of backup "a", nil for none; others are the
		// resources the API server holds besides backup "a"
		controller *api.ClickHouseBackup
		others     []client.Object
		deleting   bool
		reason     string
		message    string
	}{
		"its controller is gone": {
			controller: newBackupCR("a-b"),
			reason:     api.BackupReasonWaitingForGarbageCollection,
		},
		"its controller was replaced by another of its name": {
			controller: newBackupCR("a-b"),
			others: []client.Object{func() client.Object {
				successor := newBackupCR("a-b")
				successor.UID = "successor-uid"
				return successor
			}()},
			reason: api.BackupReasonWaitingForGarbageCollection,
		},
		"its controller is being deleted": {
			controller: newBackupCR("a-b"),
			others: []client.Object{func() client.Object {
				deleting := newBackupCR("a-b")
				deleting.DeletionTimestamp = &now
				deleting.Finalizers = []string{"foreground"}
				return deleting
			}()},
			reason: api.BackupReasonWaitingForGarbageCollection,
		},
		"it is being deleted itself": {
			deleting: true,
			reason:   api.BackupReasonWaitingForGarbageCollection,
		},
		"its controller lives": {
			controller: newBackupCR("a-b"),
			others:     []client.Object{newBackupCR("a-b")},
			reason:     api.BackupReasonNameConflict,
			message:    "is taken by ClickHouseBackup a-b on cluster c of ClickHouseInstallation demo",
		},
		"no resource controls it": {
			reason:  api.BackupReasonNameConflict,
			message: "the Job name a-b-c-backup is taken by a Job no resource controls, which no garbage collection removes: delete it",
		},
	} {
		t.Run(name, func(t *testing.T) {
			foreign := &batch.Job{}
			foreign.Name = "a-b-c-backup"
			foreign.Namespace = testNamespace
			foreign.Labels = model.Labels(api.ClickHouseBackupCRDResourceKind, "a-b", testCHI, "c", testStorage)
			if tc.controller != nil {
				require.NoError(t, ctrl.SetControllerReference(tc.controller, foreign, newScheme(t)))
			}
			if tc.deleting {
				foreign.DeletionTimestamp = &now
				foreign.Finalizers = []string{"foreground"}
			}
			f := newBackupFixture(t, newFakeCR(collidingInstallation(t)), nil, append(tc.others, newBackupCR("a"), foreign)...)

			f.reconcile("a")
			got := clusterStatus(t, f.backup("a").Status.Clusters, "b-c")
			require.Equal(t, tc.reason, got.Reason, got.Message)
			if tc.reason == api.BackupReasonWaitingForGarbageCollection {
				require.Equal(t, api.OperationPhasePending, got.Phase, "the backup waits for the Job to go")
				require.Zero(t, eventCount(t, f.client, api.BackupReasonNameConflict))
				return
			}
			require.Equal(t, api.OperationPhaseFailed, got.Phase)
			require.Contains(t, got.Message, tc.message)
		})
	}
}

// The controller of a Job of the name a backup needs is read past the cache, by its kind. One the operator
// may not read, or of a kind the API server does not serve, may live on, and nothing tells that the Job goes
// away: the backup fails on that cluster rather than wait for good. A read that fails otherwise may succeed
// later: the reconcile is retried, and fails nothing meanwhile.
func TestBackupTellsAJobOfAControllerItCannotRead(t *testing.T) {
	for name, tc := range map[string]struct {
		err error
		// reason is the one the backup reports on the cluster, "" for a reconcile that is retried
		reason string
	}{
		"the operator may not read it": {
			err: apiErrors.NewForbidden(kubeSchema.GroupResource{Group: clickhouse_altinity_com.APIGroupName, Resource: "clickhousebackups"},
				"a-b", errors.New("forbidden by RBAC")),
			reason: api.BackupReasonNameConflict,
		},
		"the API server does not serve its kind": {
			err:    &apiMeta.NoKindMatchError{GroupKind: kubeSchema.GroupKind{Group: clickhouse_altinity_com.APIGroupName, Kind: api.ClickHouseBackupCRDResourceKind}},
			reason: api.BackupReasonNameConflict,
		},
		"reading it fails for a while": {
			err: errors.New("connection refused"),
		},
	} {
		t.Run(name, func(t *testing.T) {
			foreign := &batch.Job{}
			foreign.Name = "a-b-c-backup"
			foreign.Namespace = testNamespace
			foreign.Labels = model.Labels(api.ClickHouseBackupCRDResourceKind, "a-b", testCHI, "c", testStorage)
			require.NoError(t, ctrl.SetControllerReference(newBackupCR("a-b"), foreign, newScheme(t)))
			unreadable := &interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, controller := obj.(*meta.PartialObjectMetadata); controller {
					return tc.err
				}
				return c.Get(ctx, key, obj, opts...)
			}}
			f := newBackupFixture(t, newFakeCR(collidingInstallation(t)), unreadable, newBackupCR("a"), foreign)

			_, err := f.controller.Reconcile(context.Background(), request("a"))
			if tc.reason == "" {
				require.ErrorIs(t, err, tc.err)
				for _, cluster := range f.backup("a").Status.Clusters {
					require.NotEqual(t, api.OperationPhaseFailed, cluster.Phase, "%s: %s", cluster.Name, cluster.Message)
				}
				return
			}
			require.NoError(t, err)
			got := clusterStatus(t, f.backup("a").Status.Clusters, "b-c")
			require.Equal(t, api.OperationPhaseFailed, got.Phase)
			require.Equal(t, tc.reason, got.Reason, got.Message)
		})
	}
}

// A backup of every cluster whose Job name on one cluster a deleted backup's Job, awaiting garbage
// collection, holds waits there and backs up that cluster once the Job is gone.
func TestBackupRunsOnceTheJobOfADeletedBackupIsGone(t *testing.T) {
	other := newBackupCR("a-b")
	other.Spec.Cluster = "c"
	f := newBackupFixture(t, newFakeCR(collidingInstallation(t)), nil, other)
	f.reconcile("a-b")
	require.NoError(t, f.client.Delete(context.Background(), f.backup("a-b")))
	require.NoError(t, f.client.Create(context.Background(), newBackupCR("a")))

	f.reconcile("a")
	got := clusterStatus(t, f.backup("a").Status.Clusters, "b-c")
	require.Equal(t, api.OperationPhasePending, got.Phase)
	require.Equal(t, api.BackupReasonWaitingForGarbageCollection, got.Reason)

	require.NoError(t, f.client.Delete(context.Background(), &batch.Job{ObjectMeta: meta.ObjectMeta{Name: "a-b-c-backup", Namespace: testNamespace}}))
	f.reconcile("a")
	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("a-b-c-backup"), job))
	require.True(t, meta.IsControlledBy(job, f.backup("a")))
	require.Equal(t, "b-c", jobEnv(t, f.client, "a-b-c-backup", "CLUSTER"))
}

// newCachedScheduleFixture returns a schedule fixture whose controller reads as the cache of the operator does, which
// holds only the CronJobs its selector selects.
func newCachedScheduleFixture(t *testing.T, cr *fakeCR, objs ...client.Object) *scheduleFixture {
	t.Helper()
	cached := interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		if err := c.Get(ctx, key, obj, opts...); err != nil {
			return err
		}
		if _, ok := obj.(*batch.CronJob); ok && !model.JobSelector().Matches(k8sLabels.Set(obj.GetLabels())) {
			return apiErrors.NewNotFound(batch.Resource("cronjobs"), key.Name)
		}
		return nil
	}}
	c := newClient(t, nil, objs...)
	return &scheduleFixture{t: t, client: c, cr: cr, controller: NewScheduleController(interceptor.NewClient(c, cached), c, c.Scheme(), cr)}
}

// editCronJob edits a CronJob as a user would.
func editCronJob(t *testing.T, c client.Client, name string, edit func(cronJob *batch.CronJob)) {
	t.Helper()
	cronJob := &batch.CronJob{}
	require.NoError(t, c.Get(context.Background(), key(name), cronJob))
	edit(cronJob)
	require.NoError(t, c.Update(context.Background(), cronJob))
}

// A CronJob of a schedule whose labels, or those of the Jobs it starts, a user stripped or changed escapes, with its
// Jobs, a restore that waits for it to be suspended or checks for a backup running, and the cache once its kind label
// went too: the schedule finds it by its name, past the cache, and labels it again, keeping the labels the user added
// to it, through later changes of its spec as well.
func TestScheduleLabelsAgainACronJobWhoseLabelsWereEdited(t *testing.T) {
	for name, edit := range map[string]func(cronJob *batch.CronJob){
		"every label stripped":       func(cronJob *batch.CronJob) { cronJob.Labels = map[string]string{} },
		"the cluster label stripped": func(cronJob *batch.CronJob) { delete(cronJob.Labels, model.LabelBackupCluster) },
		"the cluster label changed to another cluster": func(cronJob *batch.CronJob) {
			cronJob.Labels[model.LabelBackupCluster] = "cold"
		},
		"the labels of its Jobs stripped": func(cronJob *batch.CronJob) { cronJob.Spec.JobTemplate.Labels = nil },
	} {
		t.Run(name, func(t *testing.T) {
			f := newCachedScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), newScheduleCR("s1"))
			f.reconcile("s1")
			stamped := f.cronJob("s1-hot-backup")
			editCronJob(t, f.client, "s1-hot-backup", func(cronJob *batch.CronJob) {
				edit(cronJob)
				cronJob.Labels["team"] = "dba"
			})
			labels := maps.Clone(stamped.Labels)
			labels["team"] = "dba"

			f.reconcile("s1")
			cronJob := f.cronJob("s1-hot-backup")
			require.Equal(t, labels, cronJob.Labels)
			require.Equal(t, stamped.Spec.JobTemplate.Labels, cronJob.Spec.JobTemplate.Labels)
			require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))

			chbs := f.schedule("s1")
			chbs.Spec.Schedule = "0 4 * * *"
			require.NoError(t, f.client.Update(context.Background(), chbs))
			f.reconcile("s1")
			cronJob = f.cronJob("s1-hot-backup")
			require.Equal(t, "0 4 * * *", cronJob.Spec.Schedule)
			require.Equal(t, labels, cronJob.Labels)
		})
	}
}

// A CronJob whose cluster label a user stripped, or changed to name another cluster, tells its cluster by the labels of
// the Jobs it starts, or by the status of the schedule when a user edited those too, until the schedule applies its
// CronJobs and labels it again: a hold suspends it, and it goes with its cluster when that leaves the schedule, while
// the CronJob of the cluster the label names stays. The label of the CronJob, those of its Jobs and the status each
// tell it on their own: a user may edit the labels of the Jobs alone, and a status write may be lost.
func TestScheduleTellsTheClusterOfACronJobWhoseClusterLabelChanged(t *testing.T) {
	for name, tc := range map[string]struct {
		// label is the cluster label the user leaves, none when empty
		label string
		// jobs tells that the user edits the cluster label of the Jobs the CronJob starts too, jobsOnly that the user
		// edits only that one, leaving the label of the CronJob
		jobs, jobsOnly bool
		// unrecorded tells that the status of the schedule records no cluster
		unrecorded bool
		// invalid tells that the spec turns invalid, otherwise cluster hot leaves the schedule
		invalid bool
	}{
		"stripped, the spec turns invalid":                              {invalid: true},
		"stripped, the cluster leaves":                                  {},
		"changed, the spec turns invalid":                               {label: "cold", invalid: true},
		"changed, the cluster leaves":                                   {label: "cold"},
		"stripped from its Jobs too, the spec turns invalid":            {jobs: true, invalid: true},
		"stripped from its Jobs too, the cluster leaves":                {jobs: true},
		"changed on its Jobs too, the spec turns invalid":               {label: "cold", jobs: true, invalid: true},
		"changed on its Jobs too, the cluster leaves":                   {label: "cold", jobs: true},
		"stripped, unrecorded, the spec turns invalid":                  {unrecorded: true, invalid: true},
		"stripped, unrecorded, the cluster leaves":                      {unrecorded: true},
		"changed on its Jobs alone, unrecorded, the spec turns invalid": {label: "cold", jobsOnly: true, unrecorded: true, invalid: true},
	} {
		t.Run(name, func(t *testing.T) {
			cr := newFakeCR(twoClusters(t, api.StatusCompleted))
			f := newCachedScheduleFixture(t, cr, newScheduleCR("s1"))
			f.reconcile("s1")
			editCronJob(t, f.client, "s1-hot-backup", func(cronJob *batch.CronJob) {
				var labeled []map[string]string
				if !tc.jobsOnly {
					labeled = append(labeled, cronJob.Labels)
				}
				if tc.jobs || tc.jobsOnly {
					labeled = append(labeled, cronJob.Spec.JobTemplate.Labels)
				}
				for _, labels := range labeled {
					delete(labels, model.LabelBackupCluster)
					if tc.label != "" {
						labels[model.LabelBackupCluster] = tc.label
					}
				}
			})
			if tc.unrecorded {
				chbs := f.schedule("s1")
				chbs.Status.Clusters = nil
				require.NoError(t, f.client.Status().Update(context.Background(), chbs))
			}

			if tc.invalid {
				chbs := f.schedule("s1")
				chbs.Spec.Schedule = ""
				require.NoError(t, f.client.Update(context.Background(), chbs))
				f.reconcile("s1")
				require.True(t, ptr.Deref(f.cronJob("s1-hot-backup").Spec.Suspend, false))
				require.True(t, ptr.Deref(f.cronJob("s1-cold-backup").Spec.Suspend, false))
				return
			}
			cr.set(installation(t, api.StatusCompleted, backupCluster("cold", 1, true)))
			f.reconcile("s1")
			err := f.client.Get(context.Background(), key("s1-hot-backup"), &batch.CronJob{})
			require.True(t, apiErrors.IsNotFound(err), "the CronJob of the cluster that left stays: %v", err)
			require.True(t, meta.IsControlledBy(f.cronJob("s1-cold-backup"), f.schedule("s1")))
		})
	}
}

// The CronJob a schedule needs on a cluster may be one a deletion that orphaned its dependents left behind,
// stamped as the schedule stamps its own, or one made by hand without the labels the operator's cache selects,
// which only a read past the cache finds: no garbage collection removes it, so the schedule tells to delete it
// rather than wait for good, and runs on the cluster once it is gone.
func TestScheduleTellsToDeleteACronJobNoResourceControls(t *testing.T) {
	for name, labels := range map[string]map[string]string{
		"stamped as the schedule stamps its own": model.Labels(api.ClickHouseBackupScheduleCRDResourceKind, "s1", testCHI, "hot", testStorage),
		"made by hand":                           nil,
	} {
		t.Run(name, func(t *testing.T) {
			orphan := &batch.CronJob{}
			orphan.Name = "s1-hot-backup"
			orphan.Namespace = testNamespace
			orphan.Labels = labels
			orphan.Spec.Schedule = "* * * * *"
			f := newCachedScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), newScheduleCR("s1"), orphan)

			require.Equal(t, targetRequeueInterval, f.reconcile("s1").RequeueAfter)
			got := f.schedule("s1")
			ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
			require.Equal(t, api.BackupReasonNameConflict, ready.Reason)
			require.Contains(t, ready.Message, "scheduled backups of cluster hot cannot run: the CronJob name s1-hot-backup is taken by a "+
				"CronJob no resource controls, which no garbage collection removes: delete it")
			require.True(t, meta.IsControlledBy(f.cronJob("s1-cold-backup"), got), "the other cluster runs the schedule")

			require.NoError(t, f.client.Delete(context.Background(), orphan))
			f.reconcile("s1")
			require.True(t, meta.IsControlledBy(f.cronJob("s1-hot-backup"), f.schedule("s1")))
			require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")))
		})
	}
}

// The restore a cluster is recovered by may be named as a restore the user created by hand, which no
// resource controls and no garbage collection removes: the recovery warns, once, to delete it once it has
// finished, rather than wait for it to go, and recovers the cluster once it is gone.
func TestRecoveryWarnsOfARestoreNoResourceControls(t *testing.T) {
	user := newRestoreCR("demo-main-recovery")
	user.Spec.Cluster = "main"
	user.Status.Phase = api.OperationPhaseCompleted
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusCompleted, recoveryBackup)), nil,
		recoveringInstallation(recoveryBackup, nil), user)

	require.Equal(t, targetRequeueInterval, f.reconcile().RequeueAfter)
	require.Len(t, eventMessagesOfType(t, f.client, api.BackupReasonNameConflict, core.EventTypeWarning), 1, "the conflict is warned about at once")
	f.reconcile()
	f.reconcile()
	require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom], "a restore of the user records no recovery")
	messages := eventMessagesOfType(t, f.client, api.BackupReasonNameConflict, core.EventTypeWarning)
	require.Len(t, messages, 1, "the conflict is warned about once")
	require.Contains(t, messages[0], "unable to recover cluster main from backup "+recoveryBackup+": the ClickHouseRestore name "+
		"demo-main-recovery is taken by a restore of cluster main of ClickHouseInstallation demo that no resource controls: "+
		"delete that restore once it has finished")

	require.NoError(t, f.client.Delete(context.Background(), user))
	f.reconcile()
	require.True(t, meta.IsControlledBy(f.restore("demo-main-recovery"), f.chi()), "the recovery starts once the other restore is gone")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonNameConflict))
	f.controller.conflicts.Range(func(key, _ any) bool {
		require.Failf(t, "a conflict that is gone is kept", "%v", key)
		return true
	})
}

// A name conflict needs somebody to act, which the Ready condition tells over a hold of another cluster
// for a restore, whichever comes first among the clusters.
func TestScheduleNameConflictWinsOverARestoreHoldOfAnEarlierCluster(t *testing.T) {
	other := newScheduleCR("a-b")
	other.Spec.Cluster = "c"
	f := newScheduleFixture(t, newFakeCR(collidingInstallation(t, backupCluster("d", 1, true))), nil, other, newScheduleCR("a"))
	f.reconcile("a-b")
	f.reconcile("a")

	restore := newRestoreCR("r1")
	restore.Spec.Cluster = "c"
	newRestoreOperation(restore).pin([]string{"c"})
	require.NoError(t, f.client.Create(context.Background(), restore))
	f.reconcile("a")
	require.True(t, ptr.Deref(f.cronJob("a-c-backup").Spec.Suspend, false), "the restore holds cluster c, which comes before b-c")
	require.Equal(t, api.BackupReasonNameConflict, readyReason(t, f.schedule("a")))
}

// A schedule whose name is longer than a label value holds is told from its deleted predecessor of the same
// name all the same: the CronJob the predecessor left awaits garbage collection.
func TestLongNamedScheduleWaitsOutItsPredecessor(t *testing.T) {
	name := strings.Repeat("s", 70)
	predecessor := newScheduleCR(name)
	predecessor.UID = "predecessor-uid"
	stale := &batch.CronJob{}
	stale.Name = model.CronJobName(predecessor, "hot")
	stale.Namespace = testNamespace
	stale.Labels = model.Labels(api.ClickHouseBackupScheduleCRDResourceKind, name, testCHI, "hot", testStorage)
	stale.Spec.Schedule = "* * * * *"
	require.NoError(t, ctrl.SetControllerReference(predecessor, stale, newScheme(t)))
	f := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newScheduleCR(name), stale)

	f.reconcile(name)
	require.Equal(t, api.BackupReasonWaitingForGarbageCollection, readyReason(t, f.schedule(name)))
	require.Zero(t, eventCount(t, f.client, api.BackupReasonNameConflict))
}
