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

	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// RecoveryController restores the clusters of a new installation from the backups its
// spec.backup.recovery names, once the installation has completed its first reconcile. It creates a
// ClickHouseRestore of each cluster to recover, owned by the installation, while none exists, and
// records the recovery in an annotation once every one of them has completed. A failed recovery
// restore stays until it is deleted, which starts the recovery of its cluster anew unless the
// recovery was given up or recorded by hand in the meantime; a cluster waiting to be recovered holds
// its backup schedules, and so does a failed restore that may have changed something. It never
// touches the installation's spec.
type RecoveryController struct {
	*reconciler
	// conflicts holds, by recoveryKey, the name conflict of the recovery restore of a cluster warned about,
	// as its conflictKey, while it lasts
	conflicts sync.Map
}

// recoveryKey identifies the recovery of a cluster of an installation.
type recoveryKey struct {
	chi     kubeTypes.NamespacedName
	cluster string
}

// conflictKey identifies a name conflict of the recovery restore of an installation with another restore.
type conflictKey struct {
	chi     kubeTypes.UID
	restore kubeTypes.UID
}

// NewRecoveryController creates a RecoveryController with its request-independent
// collaborators initialized once, as controller-runtime shares it across all reconcile goroutines.
func NewRecoveryController(
	c client.Client,
	apiReader client.Reader,
	scheme *apiMachinery.Scheme,
	cr interfaces.IKubeCR,
) *RecoveryController {
	return &RecoveryController{
		reconciler: newReconciler(c, apiReader, scheme, cr, api.ClickHouseInstallationCRDResourceKind),
	}
}

// Reconcile starts the recovery of every cluster of an installation that asks for one, and records
// the recovery once all of them have completed. Problems are announced as warning events on the
// installation.
func (c *RecoveryController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if c.skip(ctx, req.Namespace) {
		return ctrl.Result{}, nil
	}

	cached := &api.ClickHouseInstallation{}
	if err := c.Get(ctx, req.NamespacedName, cached); err != nil {
		if apiErrors.IsNotFound(err) {
			c.forgetConflicts(req.NamespacedName)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !wantsRecovery(cached) {
		c.forgetConflicts(req.NamespacedName)
		return ctrl.Result{}, nil
	}
	chi, wait, err := c.readInstallation(ctx, req.Namespace, req.Name, true)
	switch {
	case err != nil:
		return ctrl.Result{}, err
	case (wait != nil) && wait.waitsForInstallation():
		return ctrl.Result{RequeueAfter: wait.requeue}, nil
	case wait != nil:
		c.warning(cached, a.EventActionCreate, wait.reason, "unable to recover: %s", wait.message)()
		return ctrl.Result{}, nil
	}
	if !wantsRecovery(chi) {
		return ctrl.Result{}, nil
	}
	// The recovery is recorded once every cluster that asks for one has been recovered: one of them that lacks a
	// storage would never be recovered after that, so nothing is recovered until it has one
	if held := model.RecoveryClusters(model.RecoveriesWithoutStorage(chi.GetAncestorT())); len(held) > 0 {
		c.warning(chi, a.EventActionCreate, api.BackupReasonNoBackupStorage,
			"unable to recover: backup is enabled without a storage on the clusters to recover %s - %s, or leave "+
				"them out of the recovery with backup.recovery.backupName \"\"", strings.Join(held, ", "), api.BackupStorageHint)()
		return ctrl.Result{}, nil
	}
	recoveries := model.Recoveries(chi.GetAncestorT())
	if len(recoveries) == 0 {
		c.warning(chi, a.EventActionCreate, api.BackupReasonBackupDisabled,
			"unable to recover: spec.backup.recovery names a backup, but no backup-enabled cluster inherits one")()
		return ctrl.Result{}, nil
	}

	var result ctrl.Result
	completed := 0
	for i := range recoveries {
		restore, clusterResult, err := c.recoverCluster(ctx, chi, &recoveries[i])
		if err != nil {
			return ctrl.Result{}, err
		}
		result = shorterResult(result, clusterResult)
		if (restore != nil) && (restore.Status.Phase == api.OperationPhaseCompleted) {
			completed++
		}
	}
	if completed < len(recoveries) {
		return result, nil
	}
	return c.recovered(ctx, cached, recoveries)
}

// recoverCluster creates the restore that recovers a cluster while none exists, and returns the one
// that does. A failed restore is warned about. The backup a restore restores is recorded in the
// recovery, as it may differ from the one the spec names by now.
func (c *RecoveryController) recoverCluster(
	ctx context.Context,
	chi *api.ClickHouseInstallation,
	recovery *model.ClusterRecovery,
) (*api.ClickHouseRestore, ctrl.Result, error) {
	restore := &api.ClickHouseRestore{}
	key := kubeTypes.NamespacedName{Namespace: chi.GetNamespace(), Name: model.RecoveryRestoreName(chi, recovery.Cluster)}
	err := c.getConfirmed(ctx, key, restore)
	if (err == nil) && !meta.IsControlledBy(restore, chi) {
		// A leftover of a deleted predecessor of the same name awaits garbage collection: recording the
		// recovery against it would skip the real one
		stale, err := c.awaitsGarbageCollection(ctx, chi, restore)
		switch {
		case err != nil:
			return nil, ctrl.Result{}, err
		case stale:
			c.conflicts.Delete(conflictRecoveryKey(chi, recovery))
			log.V(1).M(c.meta(chi)).F().Info("%s", staleOwnerMessage(api.ClickHouseRestoreCRDResourceKind, restore.GetName()))
			return nil, ctrl.Result{RequeueAfter: pendingRequeueInterval}, nil
		}
		c.nameConflict(chi, recovery, restore)
		return nil, ctrl.Result{RequeueAfter: targetRequeueInterval}, nil
	}
	c.conflicts.Delete(conflictRecoveryKey(chi, recovery))
	switch {
	case apiErrors.IsNotFound(err):
		result, err := c.startRecovery(ctx, chi, *recovery)
		return nil, result, err
	case err != nil:
		return nil, ctrl.Result{}, err
	}

	recovery.BackupName = restoredBackupName(restore)
	if restore.Status.Phase == api.OperationPhaseFailed {
		c.recoveryFailed(chi, restore)
	}
	return restore, ctrl.Result{}, nil
}

// nameConflict warns, once per conflict, that the restore a cluster is recovered by cannot be created: a
// restore of its name stays, unlike a leftover of a deleted predecessor, whether another installation
// controls it, whose name joined with its cluster gives the same name, or none does. The name follows
// from the installation and the cluster, so only that restore can make way: once it has finished, or, of
// another installation that recovers by it, once that installation no longer creates it again. The
// installation has no status to remember the warning by, and the recovery is retried until the conflict
// is gone.
func (c *RecoveryController) nameConflict(chi *api.ClickHouseInstallation, recovery *model.ClusterRecovery, restore *api.ClickHouseRestore) {
	owner := fmt.Sprintf("a restore of %s %s", api.ClickHouseInstallationCRDResourceKind, restore.Spec.ClickHouseInstallation)
	if restore.Spec.Cluster != "" {
		owner = fmt.Sprintf("a restore of cluster %s of %s %s", restore.Spec.Cluster, api.ClickHouseInstallationCRDResourceKind,
			restore.Spec.ClickHouseInstallation)
	}
	advice := "delete that restore once it has finished"
	switch {
	case meta.GetControllerOf(restore) == nil:
		owner += " that no resource controls"
	case model.IsRecoveryRestore(restore):
		advice = fmt.Sprintf("delete that restore once %s %s has recorded its recovery, or no longer asks for one, as it creates "+
			"the restore again until then", api.ClickHouseInstallationCRDResourceKind, restore.Spec.ClickHouseInstallation)
	}
	message := fmt.Sprintf("the %s name %s is taken by %s: %s", api.ClickHouseRestoreCRDResourceKind, restore.GetName(), owner, advice)
	conflict := conflictKey{chi: chi.GetUID(), restore: restore.GetUID()}
	if warned, _ := c.conflicts.Swap(conflictRecoveryKey(chi, recovery), conflict); warned == conflict {
		log.V(1).M(c.meta(chi)).F().Info("unable to recover cluster %s: %s", recovery.Cluster, message)
		return
	}
	c.warning(chi, a.EventActionCreate, api.BackupReasonNameConflict, "unable to recover cluster %s from backup %s: %s",
		recovery.Cluster, recovery.BackupName, message)()
}

// conflictRecoveryKey returns the key the name conflict of the recovery restore of a cluster is kept by.
func conflictRecoveryKey(chi *api.ClickHouseInstallation, recovery *model.ClusterRecovery) recoveryKey {
	return recoveryKey{chi: kubeTypes.NamespacedName{Namespace: chi.GetNamespace(), Name: chi.GetName()}, cluster: recovery.Cluster}
}

// forgetConflicts drops the name conflicts kept of an installation that is gone, or no longer recovers.
func (c *RecoveryController) forgetConflicts(chi kubeTypes.NamespacedName) {
	c.conflicts.Range(func(key, _ any) bool {
		if key.(recoveryKey).chi == chi {
			c.conflicts.Delete(key)
		}
		return true
	})
}

// startRecovery creates the restore that recovers a cluster once the installation runs the sidecars
// of the cluster, which the restore drives.
func (c *RecoveryController) startRecovery(ctx context.Context, chi *api.ClickHouseInstallation, recovery model.ClusterRecovery) (ctrl.Result, error) {
	restore := model.BuildRecoveryRestore(chi, recovery)
	if err := restore.Spec.Validate(); err != nil {
		c.warning(chi, a.EventActionCreate, api.BackupReasonInvalidSpec, "unable to recover cluster %s from backup %s: %v",
			recovery.Cluster, recovery.BackupName, err)()
		return ctrl.Result{}, nil
	}
	if err := ctrl.SetControllerReference(chi, restore, c.scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := c.Create(ctx, restore); err != nil {
		if apiErrors.IsAlreadyExists(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	c.info(chi, a.EventActionCreate, api.BackupReasonJobCreated,
		"recovery of cluster %s from backup %s started in ClickHouseRestore %s", recovery.Cluster, recovery.BackupName, restore.GetName())()
	return ctrl.Result{}, nil
}

// recovered records on the installation that every recovery restore has completed, so that it is
// never recovered again. The record goes on the installation as the cache holds it: a merge patch
// carries the annotation alone, and the version the cache read it at.
func (c *RecoveryController) recovered(ctx context.Context, chi *api.ClickHouseInstallation, recoveries []model.ClusterRecovery) (ctrl.Result, error) {
	recoveredFrom := model.RecoveredFrom(recoveries)
	base := chi.DeepCopy()
	meta.SetMetaDataAnnotation(&chi.ObjectMeta, api.AnnotationRecoveredFrom, recoveredFrom)
	if err := c.Patch(ctx, chi, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		if apiErrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	c.info(chi, a.EventActionReconcile, api.BackupReasonCompleted, "recovery from backup %s completed", recoveredFrom)()
	return ctrl.Result{}, nil
}

// recoveryFailed warns that the recovery restore of a cluster failed, and how to go on: a restore that
// may have changed something holds the backup schedules it holds until it is deleted, which starts the
// recovery of the cluster anew, while one that failed before it changed anything holds nothing, but for
// the schedules sharesSourcePrefix holds as long as the installation recovers from the backup.
func (c *RecoveryController) recoveryFailed(chi *api.ClickHouseInstallation, restore *api.ClickHouseRestore) {
	reason, message := api.BackupReasonJobFailed, ""
	if ready := apiMeta.FindStatusCondition(restore.Status.Conditions, api.BackupConditionReady); ready != nil {
		reason, message = ready.Reason, ready.Message
	}
	schedules := fmt.Sprintf("Scheduled backups of the cluster run on, except those of schedules that leave their backup name "+
		"prefix to default, which stay held as %s tells", api.BackupReasonSharesSourcePrefix)
	if holdsSchedules(restore) {
		schedules = fmt.Sprintf("Scheduled backups of %s stay suspended until that ClickHouseRestore is deleted", restoredScope(restore))
	}
	c.warning(chi, a.EventActionReconcile, reason,
		"recovery of cluster %s from backup %s failed in ClickHouseRestore %s: %s. %s; deleting that ClickHouseRestore starts "+
			"the recovery of the cluster anew, unless the %s annotation records a recovery or spec.backup.recovery no longer "+
			"names a backup for the cluster",
		restore.Spec.Cluster, restoredBackupName(restore), restore.GetName(), message, schedules, api.AnnotationRecoveredFrom)()
}
