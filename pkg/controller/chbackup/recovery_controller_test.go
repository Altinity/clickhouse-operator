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

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

const recoveryBackup = "nightly-20260101-000000"

type recoveryFixture struct {
	t *testing.T
	// client is a WithWatch so that the cold cache of a restarted operator can be put in front of it.
	client     client.WithWatch
	cr         *fakeCR
	controller *RecoveryController
}

func newRecoveryFixture(t *testing.T, cr *fakeCR, funcs *interceptor.Funcs, objs ...client.Object) *recoveryFixture {
	t.Helper()
	c := newClient(t, funcs, objs...)
	return &recoveryFixture{t: t, client: c, cr: cr, controller: NewRecoveryController(c, c, c.Scheme(), cr)}
}

func (f *recoveryFixture) reconcile() ctrl.Result {
	f.t.Helper()
	result, err := f.controller.Reconcile(context.Background(), request(testCHI))
	require.NoError(f.t, err)
	return result
}

func (f *recoveryFixture) chi() *api.ClickHouseInstallation {
	f.t.Helper()
	chi := &api.ClickHouseInstallation{}
	require.NoError(f.t, f.client.Get(context.Background(), key(testCHI), chi))
	return chi
}

func (f *recoveryFixture) restore(name string) *api.ClickHouseRestore {
	f.t.Helper()
	return getRestore(f.t, f.client, name)
}

func (f *recoveryFixture) restoreCount() int {
	f.t.Helper()
	restores := &api.ClickHouseRestoreList{}
	require.NoError(f.t, f.client.List(context.Background(), restores, client.InNamespace(testNamespace)))
	return len(restores.Items)
}

// setRestorePhase ends a restore with the given phase, as the restore controller does.
func (f *recoveryFixture) setRestorePhase(name string, phase api.OperationPhase) {
	f.t.Helper()
	restore := f.restore(name)
	restore.Status.Phase = phase
	restore.Status.BackupName = restore.Spec.BackupName
	restore.Status.Clusters = []api.OperationClusterStatus{{Name: restore.Spec.Cluster, Phase: phase, JobName: name + "-restore"}}
	op := newRestoreOperation(restore)
	switch phase {
	case api.OperationPhaseCompleted:
		op.setCondition(api.BackupConditionReady, meta.ConditionTrue, api.BackupReasonCompleted, "Job "+name+"-restore completed")
	case api.OperationPhaseFailed:
		restore.Status.Clusters[0].Reason = api.BackupReasonJobFailed
		op.setCondition(api.BackupConditionReady, meta.ConditionFalse, api.BackupReasonJobFailed,
			"Job "+name+"-restore failed: preflight failed: backup "+recoveryBackup+" is missing in the storage of chi-demo-main-0-0")
	}
	require.NoError(f.t, f.client.Status().Update(context.Background(), restore))
}

// withRecovery returns chi asking to recover every backup-enabled cluster from backupName.
func withRecovery(chi *api.ClickHouseInstallation, backupName string) *api.ClickHouseInstallation {
	chi = chi.DeepCopy()
	if chi.Spec.Backup == nil {
		chi.Spec.Backup = &api.ClusterBackup{}
	}
	chi.Spec.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString(backupName)}
	return chi
}

// recoveringInstallation returns the installation as the user created it, with no status, asking to
// recover its main cluster from backupName, and the annotations given.
func recoveringInstallation(backupName string, annotations map[string]string) *api.ClickHouseInstallation {
	chi := withRecovery(installationSpec(backupCluster("main", 2, true)), backupName)
	chi.Annotations = annotations
	return chi
}

// recoveryTarget returns the installation recoveringInstallation creates, with the given status and
// the normalized state of its last completed reconcile.
func recoveryTarget(t *testing.T, status, backupName string) *api.ClickHouseInstallation {
	t.Helper()
	return installationFrom(t, recoveringInstallation(backupName, nil), status)
}

// withStatus sets the status of an installation, as its reconcile does.
func withStatus(chi *api.ClickHouseInstallation, status string) *api.ClickHouseInstallation {
	chi = chi.DeepCopy()
	chi.EnsureStatus().Status = status
	return chi
}

// Only installations whose spec names a backup to recover from and that have not been recovered are
// queued, and of their updates only those that may start the recovery: a completed reconcile, which
// every change of the spec ends with, and a change of the record of the recovery.
func TestRecoveryPredicate(t *testing.T) {
	asks := recoveringInstallation(recoveryBackup, nil)
	done := recoveringInstallation(recoveryBackup, map[string]string{api.AnnotationRecoveredFrom: recoveryBackup})
	none := installationSpec(backupCluster("main", 2, true))
	optedOut := recoveringInstallation("", nil)
	predicate := RecoveryPredicate()

	require.True(t, predicate.Create(event.CreateEvent{Object: asks}))
	require.True(t, predicate.Create(event.CreateEvent{Object: trim(t, asks)}), "the cache keeps what tells a recovery")
	require.False(t, predicate.Create(event.CreateEvent{Object: none}), "only installations asking for recovery")
	require.False(t, predicate.Create(event.CreateEvent{Object: optedOut}), "an empty backup name asks for nothing")
	require.False(t, predicate.Create(event.CreateEvent{Object: done}), "a recovered installation is never recovered again")
	require.True(t, predicate.Update(event.UpdateEvent{ObjectOld: done, ObjectNew: asks}), "removing the record of a recovery recovers again")
	require.False(t, predicate.Update(event.UpdateEvent{ObjectOld: asks, ObjectNew: done}))
	require.False(t, predicate.Delete(event.DeleteEvent{Object: asks}))

	inProgress := withStatus(asks, api.StatusInProgress)
	completed := withStatus(asks, api.StatusCompleted)
	require.True(t, predicate.Update(event.UpdateEvent{ObjectOld: inProgress, ObjectNew: completed}),
		"the recovery waits for a completed reconcile")
	require.False(t, predicate.Update(event.UpdateEvent{ObjectOld: completed, ObjectNew: completed}),
		"status chatter would repeat every warning about the recovery")

	watchOnly(t, "elsewhere")
	require.False(t, predicate.Create(event.CreateEvent{Object: asks}), "only watched namespaces")
}

// The recovery is recorded once its restore has completed, not when it is created: a restore that
// fails has not recovered anything. The restore itself is created once, restores the cluster with
// its users, roles and grants, and goes with the installation. Its start and completion are
// announced with the reasons every other operation announces them with.
func TestRecoveryRunsOnce(t *testing.T) {
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusCompleted, recoveryBackup)), nil,
		recoveringInstallation(recoveryBackup, nil))

	require.Equal(t, ctrl.Result{}, f.reconcile())

	restore := f.restore("demo-main-recovery")
	chi := f.chi()
	require.True(t, meta.IsControlledBy(restore, chi), "the restore goes with its installation")
	require.Equal(t, recoveryBackup, restore.Spec.BackupName)
	require.Equal(t, testCHI, restore.Spec.ClickHouseInstallation)
	require.Equal(t, "main", restore.Spec.Cluster)
	require.True(t, restore.Spec.RBAC.IsTrue(), "a recovery restores access entities too")
	require.Empty(t, chi.Annotations[api.AnnotationRecoveredFrom], "nothing is recovered before the restore completes")
	require.Equal(t, []string{"recovery of cluster main from backup " + recoveryBackup + " started in ClickHouseRestore demo-main-recovery"},
		eventMessages(t, f.client, api.BackupReasonJobCreated))

	f.reconcile()
	require.Equal(t, 1, f.restoreCount())
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonJobCreated))

	f.setRestorePhase("demo-main-recovery", api.OperationPhaseCompleted)
	require.Equal(t, ctrl.Result{}, f.reconcile())
	require.Equal(t, recoveryBackup, f.chi().Annotations[api.AnnotationRecoveredFrom])
	require.Equal(t, []string{"recovery from backup " + recoveryBackup + " completed"}, eventMessages(t, f.client, api.BackupReasonCompleted))

	f.cr.set(withAnnotations(f.cr.chis[key(testCHI).String()], f.chi().Annotations))
	f.reconcile()
	require.Equal(t, 1, f.restoreCount())
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonCompleted))
}

// withAnnotations returns chi with the given annotations.
func withAnnotations(chi *api.ClickHouseInstallation, annotations map[string]string) *api.ClickHouseInstallation {
	chi = chi.DeepCopy()
	chi.Annotations = annotations
	return chi
}

// Every backup-enabled cluster is recovered by a restore of its own, from the backup it inherits or
// names itself; a cluster that opts out with an empty name, and one without backup, are not. The
// recovery is recorded once every restore completed, naming the backup of every cluster.
func TestRecoveryOfEveryCluster(t *testing.T) {
	cold := backupCluster("cold", 1, true)
	cold.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString("cold-20261001-030000")}
	scratch := backupCluster("scratch", 1, true)
	scratch.Backup.Recovery = &api.BackupRecovery{BackupName: types.NewString("")}
	spec := withRecovery(installationSpec(backupCluster("hot", 2, true), cold, scratch, backupCluster("plain", 1, false)), recoveryBackup)
	f := newRecoveryFixture(t, newFakeCR(installationFrom(t, spec, api.StatusCompleted)), nil, spec)

	f.reconcile()
	require.Equal(t, 2, f.restoreCount())
	require.Equal(t, recoveryBackup, f.restore("demo-hot-recovery").Spec.BackupName)
	require.Equal(t, "hot", f.restore("demo-hot-recovery").Spec.Cluster)
	require.Equal(t, "cold-20261001-030000", f.restore("demo-cold-recovery").Spec.BackupName)
	require.Equal(t, "cold", f.restore("demo-cold-recovery").Spec.Cluster)

	f.setRestorePhase("demo-hot-recovery", api.OperationPhaseCompleted)
	f.reconcile()
	require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom], "a cluster is still being recovered")

	f.setRestorePhase("demo-cold-recovery", api.OperationPhaseCompleted)
	f.reconcile()
	require.Equal(t, "cold:cold-20261001-030000,hot:"+recoveryBackup, f.chi().Annotations[api.AnnotationRecoveredFrom])
}

// The restore that exists is the record of the recovery: an operator restarted while it runs, its
// cache empty, never creates a second one.
func TestRecoveryNeverRunsTwiceAfterARestart(t *testing.T) {
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusCompleted, recoveryBackup)), nil,
		recoveringInstallation(recoveryBackup, nil))
	f.reconcile()
	f.setRestorePhase("demo-main-recovery", api.OperationPhaseRunning)

	cold := interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, isRestore := obj.(*api.ClickHouseRestore); isRestore {
				return apiErrors.NewNotFound(api.Resource("clickhouserestores"), key.Name)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}
	f.controller = NewRecoveryController(interceptor.NewClient(f.client, cold), f.client, f.client.Scheme(), f.cr)
	f.reconcile()
	require.Equal(t, 1, f.restoreCount())
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonJobCreated))
	require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom])
}

// A failed recovery is warned about on the installation, with the reason of the restore, and not
// recorded. The restore may have changed something, access entities of every cluster included, so it
// holds the schedules of every cluster. Deleting the failed restore recovers the cluster again.
func TestRecoveryWarnsAboutAFailedRestore(t *testing.T) {
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusCompleted, recoveryBackup)), nil,
		recoveringInstallation(recoveryBackup, nil))
	f.reconcile()
	f.setRestorePhase("demo-main-recovery", api.OperationPhaseFailed)

	require.Equal(t, ctrl.Result{}, f.reconcile())
	require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom])
	messages := eventMessages(t, f.client, api.BackupReasonJobFailed)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], "recovery of cluster main from backup "+recoveryBackup+" failed")
	require.Contains(t, messages[0], "is missing in the storage")
	require.Contains(t, messages[0], "stay suspended until that ClickHouseRestore is deleted", "a restore that may have changed something holds the schedules")
	require.Contains(t, messages[0], "Scheduled backups of ClickHouseInstallation demo stay suspended",
		"a recovery restores access entities, which belong to every cluster")
	require.Contains(t, messages[0], "deleting that ClickHouseRestore starts the recovery of the cluster anew")
	events := &core.EventList{}
	require.NoError(t, f.client.List(context.Background(), events, client.InNamespace(testNamespace)))
	for _, event := range events.Items {
		if event.Reason == api.BackupReasonJobFailed {
			require.Equal(t, api.ClickHouseInstallationCRDResourceKind, event.InvolvedObject.Kind)
			require.Equal(t, testCHI, event.InvolvedObject.Name)
		}
	}

	require.NoError(t, f.client.Delete(context.Background(), f.restore("demo-main-recovery")))
	f.reconcile()
	require.Equal(t, 1, f.restoreCount())
	require.Empty(t, f.restore("demo-main-recovery").Status.Phase, "a new restore recovers again")
	require.Equal(t, 2, eventCount(t, f.client, api.BackupReasonJobCreated))
}

// A recovery restore that failed before it changed anything, a backup name mistyped above all, holds
// no schedule: neither does it itself, nor does the cluster waiting to be recovered, as its restore
// exists. The schedules that default their backup name prefix stay held for it, which the warning tells.
func TestRecoveryWarnsAboutARestoreThatChangedNothing(t *testing.T) {
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusCompleted, recoveryBackup)), nil,
		recoveringInstallation(recoveryBackup, nil))
	f.reconcile()
	f.setRestorePhase("demo-main-recovery", api.OperationPhaseFailed)
	restore := f.restore("demo-main-recovery")
	restore.Status.Clusters[0].Reason = api.BackupReasonJobFailedUnchanged
	op := newRestoreOperation(restore)
	op.setCondition(api.BackupConditionReady, meta.ConditionFalse, api.BackupReasonJobFailedUnchanged, "Job demo-main-recovery-restore failed: refusing")
	require.NoError(t, f.client.Status().Update(context.Background(), restore))

	f.reconcile()
	messages := eventMessages(t, f.client, api.BackupReasonJobFailedUnchanged)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], "Scheduled backups of the cluster run on, except those of schedules that leave their backup "+
		"name prefix to default, which stay held as SharesSourcePrefix tells")
	require.NotContains(t, messages[0], "stay suspended")
}

// A recovery that cannot start is warned about on the installation rather than only logged.
func TestRecoveryWarnsWhenItCannotStart(t *testing.T) {
	for name, tc := range map[string]struct {
		chi     *api.ClickHouseInstallation
		reason  string
		message string
	}{
		"invalid backup name": {
			chi:     recoveringInstallation("not a valid name", nil),
			reason:  api.BackupReasonInvalidSpec,
			message: "unable to recover cluster main from backup not a valid name: ",
		},
		"backup disabled": {
			chi:     withRecovery(installationSpec(backupCluster("main", 1, false)), recoveryBackup),
			reason:  api.BackupReasonBackupDisabled,
			message: "unable to recover: spec.backup.recovery names a backup, but no backup-enabled cluster inherits one",
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newRecoveryFixture(t, newFakeCR(installationFrom(t, tc.chi, api.StatusCompleted)), nil, tc.chi)

			require.Equal(t, ctrl.Result{}, f.reconcile())
			require.Zero(t, f.restoreCount())
			require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom])
			messages := eventMessagesOfType(t, f.client, tc.reason, core.EventTypeWarning)
			require.Len(t, messages, 1)
			require.True(t, strings.HasPrefix(messages[0], tc.message), messages[0])
		})
	}
}

// A cluster that asks for a recovery while it turns backup on without a storage has no sidecar to restore
// through. Recovering the other clusters alone would record the recovery once they completed, and the cluster
// would never be recovered after it gets a storage: nothing is recovered, and nothing recorded, until it
// has one.
func TestRecoveryWaitsForStorage(t *testing.T) {
	lacking := backupCluster("lacking", 1, true)
	lacking.Backup.Storage = types.NewString("")
	spec := withRecovery(installationSpec(backupCluster("hot", 2, true), lacking), recoveryBackup)
	f := newRecoveryFixture(t, newFakeCR(installationFrom(t, spec, api.StatusCompleted)), nil, spec)

	require.Equal(t, ctrl.Result{}, f.reconcile())
	require.Zero(t, f.restoreCount())
	require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom])
	messages := eventMessagesOfType(t, f.client, api.BackupReasonNoBackupStorage, core.EventTypeWarning)
	require.Len(t, messages, 1)
	require.True(t, strings.HasPrefix(messages[0], "unable to recover: backup is enabled without a storage on the clusters to recover lacking - "), messages[0])
	require.Contains(t, messages[0], api.BackupStorageHint)
	require.Contains(t, messages[0], `leave them out of the recovery with backup.recovery.backupName ""`)
}

// The recovery restores drive the sidecars, so they wait for the installation to complete its
// reconcile.
func TestRecoveryWaitsForTheInstallation(t *testing.T) {
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusInProgress, recoveryBackup)), nil,
		recoveringInstallation(recoveryBackup, nil))

	require.NotZero(t, f.reconcile().RequeueAfter)
	require.Zero(t, f.restoreCount())
}

// A restore of the same name the installation does not own is a leftover of a deleted
// predecessor. Recording the recovery against it would skip the real one.
func TestRecoveryWaitsOutAStaleRestore(t *testing.T) {
	predecessor := recoveringInstallation(recoveryBackup, nil)
	predecessor.UID = "predecessor-uid"
	stale := newRestoreCR("demo-main-recovery")
	stale.Spec.Cluster = "main"
	require.NoError(t, ctrl.SetControllerReference(predecessor, stale, newScheme(t)))
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusCompleted, recoveryBackup)), nil,
		recoveringInstallation(recoveryBackup, nil), stale)

	require.Equal(t, pendingRequeueInterval, f.reconcile().RequeueAfter)
	require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom])
	require.Zero(t, eventCount(t, f.client, api.BackupReasonNameConflict), "a leftover of a predecessor is no conflict")
}

// The restore an installation owns is the recovery of its cluster, whichever pass created it: no
// second one is created, and the recovery is recorded once it completes, with the backup it restored.
func TestRecoveryPicksUpItsOwnRestore(t *testing.T) {
	chi := recoveringInstallation(recoveryBackup, nil)
	own := newRestoreCR("demo-main-recovery")
	own.Spec.Cluster = "main"
	own.Spec.BackupName = "restored-20260101-000000"
	require.NoError(t, ctrl.SetControllerReference(chi, own, newScheme(t)))
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusCompleted, recoveryBackup)), nil, chi, own)

	require.Equal(t, ctrl.Result{}, f.reconcile())
	require.Equal(t, 1, f.restoreCount())
	require.Empty(t, f.chi().Annotations[api.AnnotationRecoveredFrom])

	f.setRestorePhase("demo-main-recovery", api.OperationPhaseCompleted)
	f.reconcile()
	require.Equal(t, own.Spec.BackupName, f.chi().Annotations[api.AnnotationRecoveredFrom])
}

func TestRecoverySkipsUnwatchedNamespaces(t *testing.T) {
	watchOnly(t, "elsewhere")
	f := newRecoveryFixture(t, newFakeCR(recoveryTarget(t, api.StatusCompleted, recoveryBackup)), nil,
		recoveringInstallation(recoveryBackup, nil))

	f.reconcile()
	require.Zero(t, f.restoreCount())
}
