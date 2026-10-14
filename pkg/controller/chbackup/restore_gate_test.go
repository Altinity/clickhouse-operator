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
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// A restore of access entities restores them, with the user-defined functions kept in Keeper, for the
// whole installation: a scheduled backup still running on another cluster, started before the restore
// suspended its CronJob, would capture them half restored. Each Job of such a restore starts only once
// no cluster the restore restores runs an operation but the restore's own; any other restore starts on
// a cluster as soon as that cluster is free.
func TestRestoreOfAccessEntitiesWaitsForEveryCluster(t *testing.T) {
	for name, rbac := range map[string]bool{"with rbac": true, "without rbac": false} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			chr := newRestoreCR("r1")
			chr.Spec.RBAC = types.NewStringBool(rbac)
			cr := newFakeCR(twoClusters(t, api.StatusCompleted))
			c := newClient(t, nil, chr, newScheduleCR("s1"))
			restores := NewRestoreController(c, c, c.Scheme(), cr)
			schedules := NewScheduleController(c, c, c.Scheme(), cr)
			_, err := schedules.Reconcile(ctx, request("s1"))
			require.NoError(t, err)

			cronJob := &batch.CronJob{}
			require.NoError(t, c.Get(ctx, key("s1-cold-backup"), cronJob))
			scheduled := jobOwnedBy(t, "s1-cold-backup-29470000", cronJob, cronJob.Spec.JobTemplate.Labels)
			require.NoError(t, c.Create(ctx, scheduled))

			validateRestore(t, c, restores, "r1")
			_, err = schedules.Reconcile(ctx, request("s1"))
			require.NoError(t, err)
			ageSuspensions(t, c)
			reconcileRestore(t, restores, "r1")

			hot := getRestore(t, c, "r1").Status.Clusters[0]
			require.Equal(t, "hot", hot.Name)
			hotCreated := !apiErrors.IsNotFound(c.Get(ctx, key("r1-hot-restore"), &batch.Job{}))
			coldCreated := !apiErrors.IsNotFound(c.Get(ctx, key("r1-cold-restore"), &batch.Job{}))
			require.False(t, coldCreated, "the scheduled backup runs on cold")
			if !rbac {
				require.True(t, hotCreated, "a restore of no access entities starts on a cluster that is free")
				return
			}
			require.False(t, hotCreated, "the access entities of every cluster wait for the scheduled backup of cold")
			require.Equal(t, api.BackupReasonOperationInProgress, hot.Reason)
			require.Equal(t, operationInProgressMessage(testCHI, "cold"), hot.Message)

			finishJob(t, c, scheduled.Name, batch.JobComplete, "")
			reconcileRestore(t, restores, "r1")
			require.NoError(t, c.Get(ctx, key("r1-hot-restore"), &batch.Job{}))
			require.NoError(t, c.Get(ctx, key("r1-cold-restore"), &batch.Job{}), "the restore's own Job on hot holds nothing")
			require.Equal(t, "true", jobEnv(t, c, "r1-hot-restore", "RESTORE_RBAC"))
		})
	}
}

// The clusters an operation runs on are pinned when it is validated. Where the API server does not
// refuse an edit of spec.cluster, the edit must neither fail the pinned clusters as clusters the
// installation lacks nor move what a restore holds or whether it restores access entities.
func TestOperationsKeepToThePinnedCluster(t *testing.T) {
	t.Run("backup", func(t *testing.T) {
		chb := newBackupCR("b1")
		chb.Spec.Cluster = "cold"
		newBackupOperation(chb).pin([]string{"cold"})
		chb.Spec.Cluster = "gone"
		f := newBackupFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, chb)

		f.reconcile("b1")
		require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase)
		require.Equal(t, "cold", jobEnv(t, f.client, "b1-cold-backup", "CLUSTER"))
	})

	t.Run("verification", func(t *testing.T) {
		chb := newBackupCR("b1")
		chb.Spec.Cluster = "cold"
		chb.Spec.Verify = types.NewStringBool(true)
		op := newBackupOperation(chb)
		op.pin([]string{"cold"})
		chb.Status.BackupName = "b1-backup"
		chb.Status.Clusters[0].Phase = api.OperationPhaseCompleted
		chb.Status.Clusters[0].JobName = "b1-cold-backup"
		chb.Status.Clusters[0].Reason = api.BackupReasonCompleted
		completed := meta.Now()
		chb.Status.Clusters[0].CompletionTime = &completed
		op.setCondition(api.BackupConditionReady, meta.ConditionTrue, api.BackupReasonCompleted, "Job b1-cold-backup completed")
		chb.Spec.Cluster = "gone"
		f := newBackupFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, chb)

		f.reconcile("b1")
		require.Equal(t, "cold", jobEnv(t, f.client, "b1-cold-verify", "CLUSTER"))
		require.Equal(t, api.BackupReasonVerifying, f.backup("b1").Status.Clusters[0].Reason)
	})

	t.Run("restore", func(t *testing.T) {
		chr := newRestoreCR("r1")
		chr.Spec.Cluster = "cold"
		chr.Spec.RBAC = types.NewStringBool(true)
		c := newClient(t, nil, chr)
		controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))
		require.Equal(t, ctrl.Result{}, reconcileRestore(t, controller, "r1"))
		require.Equal(t, api.BackupReasonInvalidSpec, condition(t, getRestore(t, c, "r1").Status.Conditions, api.BackupConditionValidated).Reason)

		edited := getRestore(t, c, "r1")
		edited.Spec.RBAC = types.NewStringBool(false)
		require.NoError(t, c.Update(context.Background(), edited))
		validateRestore(t, c, controller, "r1")
		got := getRestore(t, c, "r1")
		require.Equal(t, "cold", got.Status.Cluster, "validation records the cluster the spec names")

		got.Spec.Cluster = ""
		got.Spec.RBAC = types.NewStringBool(true)
		require.NoError(t, c.Update(context.Background(), got))
		got = getRestore(t, c, "r1")
		require.Equal(t, "cold", heldCluster(got), "an edit of the spec does not widen the hold")
		reconcileRestore(t, controller, "r1")
		require.Equal(t, "cold", jobEnv(t, c, "r1-cold-restore", "CLUSTER"))
		require.Equal(t, "false", jobEnv(t, c, "r1-cold-restore", "RESTORE_RBAC"), "a restore of one cluster restores no access entities")
		require.Equal(t, 1, jobCount(t, c))
	})
}

// A restore of access entities waits for the schedules of every backup-enabled cluster to be suspended for
// as long as the CronJob controller takes to see it, those of the cluster each of its Jobs runs on as much
// as those of the others: a CronJob of another cluster not seen suspended may still start a backup there.
func TestRestoreOfAccessEntitiesWaitsForTheSuspensionOfEveryCluster(t *testing.T) {
	ctx := context.Background()
	chr := newRestoreCR("r1")
	chr.Spec.RBAC = types.NewStringBool(true)
	c := newClient(t, nil, chr, newScheduleCR("s1"))
	restores := NewRestoreController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))
	schedules := NewScheduleController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))
	_, err := schedules.Reconcile(ctx, request("s1"))
	require.NoError(t, err)
	validateRestore(t, c, restores, "r1")
	_, err = schedules.Reconcile(ctx, request("s1"))
	require.NoError(t, err)

	hot := &batch.CronJob{}
	require.NoError(t, c.Get(ctx, key("s1-hot-backup"), hot))
	hot.Annotations[model.AnnotationCronJobSuspendedAt] = time.Now().Add(-2 * pendingRequeueInterval).UTC().Format(time.RFC3339Nano)
	require.NoError(t, c.Update(ctx, hot))
	reconcileRestore(t, restores, "r1")
	require.True(t, apiErrors.IsNotFound(c.Get(ctx, key("r1-hot-restore"), &batch.Job{})), "cold's CronJob was suspended just now")
	waiting := clusterStatus(t, getRestore(t, c, "r1").Status.Clusters, "hot")
	require.Equal(t, api.BackupReasonWaitingForSchedules, waiting.Reason)
	require.Equal(t, schedulesNotSuspended(testCHI, "cold").message, waiting.Message)

	ageSuspensions(t, c)
	reconcileRestore(t, restores, "r1")
	require.NoError(t, c.Get(ctx, key("r1-hot-restore"), &batch.Job{}))
}

// The start of a Job of a restore of access entities reads the state of every backup-enabled cluster, so
// it runs under the lock of each of them: an operation another reconcile starts on any of them meanwhile
// is either seen, or sees the Job.
func TestRestoreOfAccessEntitiesStartsUnderTheLockOfEveryCluster(t *testing.T) {
	ctx := context.Background()
	chr := newRestoreCR("r1")
	chr.Spec.RBAC = types.NewStringBool(true)
	c := newClient(t, nil, chr)
	restores := NewRestoreController(c, c, c.Scheme(), newFakeCR(twoClusters(t, api.StatusCompleted)))
	validateRestore(t, c, restores, "r1")

	unlock := lockOperations(testNamespace, testCHI, "cold")
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = restores.Reconcile(ctx, request("r1"))
	}()
	time.Sleep(500 * time.Millisecond)
	hotCreated := !apiErrors.IsNotFound(c.Get(ctx, key("r1-hot-restore"), &batch.Job{}))
	unlock()
	<-done
	require.False(t, hotCreated, "hot's Job was created while cold's lock was held")
	require.NoError(t, c.Get(ctx, key("r1-hot-restore"), &batch.Job{}))
}

// A restore of access entities restores them for the whole installation, one cluster after the other: no
// backup Job, nor verify Job, of any of its clusters starts until it has finished, so that none captures
// them half restored, while one restores a cluster that is done and another still runs above all. A
// restore of no access entities keeps only its own clusters busy.
func TestBackupWaitsForARestoreOfAccessEntities(t *testing.T) {
	for name, rbac := range map[string]bool{"with rbac": true, "without rbac": false} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			chr := newRestoreCR("r1")
			chr.Spec.RBAC = types.NewStringBool(rbac)
			cr := newFakeCR(twoClusters(t, api.StatusCompleted))
			backup := newBackupCR("b1")
			backup.Spec.Cluster = "cold"
			verified := newBackupCR("v1")
			verified.Spec.Cluster = "cold"
			verified.Spec.Verify = types.NewStringBool(true)
			op := newBackupOperation(verified)
			op.pin([]string{"cold"})
			verified.Status.BackupName = "v1-backup"
			completed := meta.Now()
			verified.Status.Clusters[0].Phase = api.OperationPhaseCompleted
			verified.Status.Clusters[0].JobName = "v1-cold-backup"
			verified.Status.Clusters[0].Reason = api.BackupReasonCompleted
			verified.Status.Clusters[0].CompletionTime = &completed
			op.setCondition(api.BackupConditionReady, meta.ConditionTrue, api.BackupReasonCompleted, "Job v1-cold-backup completed")
			c := newClient(t, nil, chr, backup, verified)
			restores := NewRestoreController(c, c, c.Scheme(), cr)
			backups := NewBackupController(c, c, c.Scheme(), cr)
			startRestore(t, c, restores, "r1")
			require.NoError(t, c.Get(ctx, key("r1-hot-restore"), &batch.Job{}))
			finishJob(t, c, "r1-cold-restore", batch.JobComplete, "")
			reconcileRestore(t, restores, "r1")
			require.Equal(t, api.OperationPhaseRunning, getRestore(t, c, "r1").Status.Phase, "the restore still runs on hot")

			// The verify Job runs on the cluster of the backup Job, once that one is done
			created := func(name, job string) bool {
				_, err := backups.Reconcile(ctx, request(name))
				require.NoError(t, err)
				if apiErrors.IsNotFound(c.Get(ctx, key(job), &batch.Job{})) {
					return false
				}
				finishJob(t, c, job, batch.JobComplete, "")
				return true
			}
			backupCreated := created("b1", "b1-cold-backup")
			verifyCreated := created("v1", "v1-cold-verify")
			if !rbac {
				require.True(t, backupCreated, "a restore of no access entities keeps only its own clusters busy")
				require.True(t, verifyCreated)
				return
			}
			require.False(t, backupCreated, "a backup of cold captures the access entities the restore restores on hot")
			require.False(t, verifyCreated)
			got := &api.ClickHouseBackup{}
			require.NoError(t, c.Get(ctx, key("b1"), got))
			require.Equal(t, api.BackupReasonOperationInProgress, got.Status.Clusters[0].Reason)
			require.Equal(t, accessRestoreInProgressMessage(testCHI, "r1"), got.Status.Clusters[0].Message)
			require.NoError(t, c.Get(ctx, key("v1"), got))
			require.Equal(t, api.BackupReasonOperationInProgress, got.Status.Clusters[0].Reason)

			finishJob(t, c, "r1-hot-restore", batch.JobComplete, "")
			reconcileRestore(t, restores, "r1")
			require.Equal(t, api.OperationPhaseCompleted, getRestore(t, c, "r1").Status.Phase)
			require.True(t, created("b1", "b1-cold-backup"))
			require.True(t, created("v1", "v1-cold-verify"))
		})
	}
}

// Only a restore of the access entities of the installation itself, validated and unfinished, holds its
// backups: one not validated yet starts no Job until it is, and then sees the backup Job running, and one of
// another installation restores the access entities of that one.
func TestBackupIgnoresARestoreOfAccessEntitiesThatHoldsNothing(t *testing.T) {
	for name, tc := range map[string]struct {
		chi       string
		validated bool
	}{
		"of the installation, not validated yet": {chi: testCHI},
		"of another installation":                {chi: "other", validated: true},
	} {
		t.Run(name, func(t *testing.T) {
			chr := newRestoreCR("r1")
			chr.Spec.ClickHouseInstallation = tc.chi
			chr.Spec.RBAC = types.NewStringBool(true)
			chr.Status.Phase = api.OperationPhasePending
			if tc.validated {
				newRestoreOperation(chr).pin([]string{"hot", "cold"})
				chr.Status.Phase = api.OperationPhaseRunning
			}
			backup := newBackupCR("b1")
			backup.Spec.Cluster = "cold"
			f := newBackupFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, backup, chr)

			f.reconcile("b1")
			require.NoError(t, f.client.Get(context.Background(), key("b1-cold-backup"), &batch.Job{}), "%+v", f.backup("b1").Status.Clusters)
		})
	}
}

// A recovery restores the access entities of the installation from a backup of one cluster: it holds the
// schedules of every backup-enabled cluster, those of a cluster the recovery leaves out, with a backup name
// prefix of their own, included, and each of its Jobs waits for every one of them to be suspended for long
// enough and free of any other operation.
func TestRecoveryOfAccessEntitiesHoldsEveryCluster(t *testing.T) {
	ctx := context.Background()
	cold := backupCluster("cold", 1, true)
	cold.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString("")}
	spec := withRecovery(installationSpec(backupCluster("main", 2, true), cold), recoveryBackup)
	cr := newFakeCR(installationFrom(t, spec, api.StatusCompleted))
	archive := newScheduleCR("archive")
	archive.Spec.Cluster = "cold"
	archive.Spec.BackupTemplate.BackupNamePrefix = "archive"
	c := newClient(t, nil, spec, archive)
	schedules := NewScheduleController(c, c, c.Scheme(), cr)
	restores := NewRestoreController(c, c, c.Scheme(), cr)
	recoveries := NewRecoveryController(c, c, c.Scheme(), cr)
	reconcileSchedule := func() {
		_, err := schedules.Reconcile(ctx, request("archive"))
		require.NoError(t, err)
	}

	reconcileSchedule()
	cronJob := &batch.CronJob{}
	require.NoError(t, c.Get(ctx, key("archive-cold-backup"), cronJob))
	require.False(t, ptr.Deref(cronJob.Spec.Suspend, false), "a cluster the recovery leaves out runs its schedule until the recovery starts")
	scheduled := jobOwnedBy(t, "archive-cold-backup-29470000", cronJob, cronJob.Spec.JobTemplate.Labels)
	require.NoError(t, c.Create(ctx, scheduled))

	_, err := recoveries.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	validateRestore(t, c, restores, "demo-main-recovery")
	reconcileSchedule()
	require.NoError(t, c.Get(ctx, key("archive-cold-backup"), cronJob))
	require.True(t, ptr.Deref(cronJob.Spec.Suspend, false), "the recovery restores the access entities of cold as well")
	held := &api.ClickHouseBackupSchedule{}
	require.NoError(t, c.Get(ctx, key("archive"), held))
	require.Equal(t, api.BackupReasonSuspendedForRestore, readyReason(t, held))
	require.Equal(t, "a restore of cluster main of ClickHouseInstallation demo, which restores the access entities of every cluster, "+
		"is in progress", condition(t, held.Status.Conditions, api.BackupConditionReady).Message, "the message names the cluster restored")

	ageSuspensions(t, c)
	reconcileRestore(t, restores, "demo-main-recovery")
	require.True(t, apiErrors.IsNotFound(c.Get(ctx, key("demo-main-recovery-main-restore"), &batch.Job{})),
		"the recovery waits for the scheduled backup that runs on cold")
	waiting := getRestore(t, c, "demo-main-recovery").Status.Clusters[0]
	require.Equal(t, api.BackupReasonOperationInProgress, waiting.Reason)
	require.Equal(t, operationInProgressMessage(testCHI, "cold"), waiting.Message)

	finishJob(t, c, scheduled.Name, batch.JobComplete, "")
	reconcileRestore(t, restores, "demo-main-recovery")
	require.NoError(t, c.Get(ctx, key("demo-main-recovery-main-restore"), &batch.Job{}))
	require.Equal(t, "true", jobEnv(t, c, "demo-main-recovery-main-restore", "RESTORE_RBAC"))
}
