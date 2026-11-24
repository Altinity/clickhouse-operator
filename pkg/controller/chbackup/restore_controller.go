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
	"cmp"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chbackup/metrics"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

type RestoreController struct {
	*reconciler
}

// NewRestoreController creates a RestoreController with its request-independent collaborators
// initialized once, as controller-runtime shares it across all reconcile goroutines.
func NewRestoreController(
	c client.Client,
	apiReader client.Reader,
	scheme *apiMachinery.Scheme,
	cr interfaces.IKubeCR,
) *RestoreController {
	return &RestoreController{
		reconciler: newReconciler(c, apiReader, scheme, cr, api.ClickHouseRestoreCRDResourceKind),
	}
}

func (c *RestoreController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if c.skip(ctx, req.Namespace) {
		return ctrl.Result{}, nil
	}

	cur := &api.ClickHouseRestore{}
	if err := c.Get(ctx, req.NamespacedName, cur); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if deleting(cur) {
		return c.finalize(ctx, cur)
	}
	reruns := (cur.Status.Phase != api.OperationPhaseCompleted) && (newTaskID(cur.Spec.TaskID, &cur.Status.BackupTaskStatus) != "")
	if cur.Status.Phase.IsTerminal() && (newTaskID(cur.Spec.TaskID, &cur.Status.BackupTaskStatus) == "") {
		return ctrl.Result{}, nil
	}
	if !cur.Status.Phase.IsTerminal() || reruns {
		if err := c.addFinalizer(ctx, cur, restoreFinalizer); err != nil {
			return ignoreConflict(err)
		}
	}

	next := cur.DeepCopy()
	var out announcements
	c.applyTaskID(next, &out)
	if next.Status.Phase.IsTerminal() {
		return c.commitStatus(ctx, next, !apiEquality.Semantic.DeepEqual(cur.Status, next.Status), ctrl.Result{}, out)
	}
	result, err := c.reconcile(ctx, next, holdsSchedules(cur), &out)
	if err != nil {
		return ctrl.Result{}, err
	}
	if next.Status.Phase.IsTerminal() {
		next.Status.Complete()
	}
	c.announcePhase(next, cur.Status.Phase, next.Status.Phase, next.Status.Conditions, &out)
	return c.commitStatus(ctx, next, !apiEquality.Semantic.DeepEqual(cur.Status, next.Status), result, out)
}

func (c *RestoreController) reconcile(ctx context.Context, chr *api.ClickHouseRestore, held bool, out *announcements) (ctrl.Result, error) {
	op := newRestoreOperation(chr)
	if !op.validated() {
		if err := validateRestoreSpec(chr); err != nil {
			op.wait(api.BackupConditionValidated, api.BackupReasonInvalidSpec, err.Error())
			return ctrl.Result{}, nil
		}
		target, wait, err := c.resolveTarget(ctx, chr.GetNamespace(), chr.Spec.ClickHouseInstallation, chr.Spec.Cluster, true)
		switch {
		case err != nil:
			return ctrl.Result{}, err
		case (wait != nil) && wait.refusesOneOff():
			op.refuse(wait.reason, wait.message)
			out.add(c.warning(chr, a.EventActionReconcile, wait.reason, "restore of backup %s refused: %s", chr.Spec.BackupName, wait.message))
			return ctrl.Result{}, nil
		case wait != nil:
			op.wait(api.BackupConditionValidated, wait.reason, wait.message)
			return ctrl.Result{RequeueAfter: wait.requeue}, nil
		}
		// Runs start only after a reconcile sees the schedule hold recorded.
		op.pin(target.topology.ClusterNames())
		if leftOut := leftOutWithoutStorage(target, chr.Spec.Cluster); leftOut != "" {
			out.add(c.warning(chr, a.EventActionReconcile, api.BackupReasonNoBackupStorage, "restore of backup %s %s",
				chr.Spec.BackupName, leftOut))
		}
	}

	// Pin the backup name now, so a later spec edit cannot make clusters restore different backups.
	if chr.Status.BackupName == "" {
		chr.Status.BackupName = chr.Spec.BackupName
	}
	targets := c.newTargetResolver(ctx, chr.GetNamespace(), chr.Spec.ClickHouseInstallation, true)
	var result ctrl.Result
	for i := range chr.Status.Clusters {
		cluster := &chr.Status.Clusters[i]
		if cluster.Phase.IsTerminal() {
			continue
		}
		clusterResult, err := c.reconcileCluster(ctx, chr, cluster, held, targets, out)
		if err != nil {
			return ctrl.Result{}, err
		}
		result = shorterResult(result, clusterResult)
	}

	outcome := op.aggregate()
	switch {
	case !outcome.finished:
		return result, nil
	case !outcome.succeeded():
		reason, message := failureOf(outcome)
		op.setCondition(api.BackupConditionReady, meta.ConditionFalse, reason, message)
		if reason == api.BackupReasonPartiallyCompleted {
			if holdsSchedules(chr) {
				message = fmt.Sprintf("%s; scheduled backups of %s stay suspended while this restore remains", message, restoredScope(chr))
			}
			out.add(c.warning(chr, a.EventActionReconcile, reason, "restore of backup %s %s", restoredBackupName(chr), message))
		}
		return ctrl.Result{}, nil
	}
	c.succeeded(op, outcome)
	chr.Status.Phase = api.OperationPhaseCompleted
	return ctrl.Result{}, nil
}

func (c *RestoreController) reconcileCluster(
	ctx context.Context,
	chr *api.ClickHouseRestore,
	cluster *api.OperationClusterStatus,
	held bool,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	key := runKeyOf(chr, api.ClickHouseRestoreCRDResourceKind, cluster.Name, api.BackupRunOperationRestore)
	if cluster.Run == nil {
		return c.start(ctx, chr, cluster, key, held, targets, out)
	}
	run, err := c.resumeRun(chr, chr.Spec.ClickHouseInstallation, key, cluster, func() (*model.RunSpec, error) {
		return c.restoreSpec(ctx, chr, cluster.Name, targets)
	})
	switch {
	case errors.Is(err, errNoClickHouseAccess) || errors.Is(err, errTargetGone):
		failOn(cluster, api.BackupReasonFailed, fmt.Sprintf("the restore cannot resume: %v", err))
		out.add(c.failed(chr, cluster))
		return ctrl.Result{}, nil
	case err != nil:
		return ctrl.Result{}, err
	}
	outcome, finished := observeOn(cluster, run)
	if !finished {
		return ctrl.Result{RequeueAfter: runningRequeueInterval}, nil
	}
	out.add(func() { runs.forget(key) })
	if outcome.failed {
		failOn(cluster, outcomeReason(outcome, api.BackupRunOperationRestore), outcome.message)
		out.add(c.failed(chr, cluster))
		return ctrl.Result{}, nil
	}
	reason, message := api.BackupReasonCompleted, "the restore completed"
	if outcome.rbacNotRestored {
		reason, message = api.BackupReasonRBACNotRestored, "the restore completed, but "+rbacNotRestoredCause
	}
	if completeOn(cluster, reason, message) {
		out.add(c.completed(chr, cluster))
	}
	return ctrl.Result{}, nil
}

func (c *RestoreController) restoreSpec(ctx context.Context, chr *api.ClickHouseRestore, cluster string, targets *targetResolver) (*model.RunSpec, error) {
	target, wait, err := targets.forCluster(cluster)
	switch {
	case err != nil:
		return nil, err
	case wait != nil:
		return nil, fmt.Errorf("%w: %s", errTargetGone, wait.message)
	}
	spec := model.RestoreRunSpec(chr, target.topology)
	spec.PreviousActions = previousActions(chr.Status.PreviousRuns, cluster)
	if err := spec.CheckPaths(target.topology, "", c.settingResolver(ctx, chr.GetNamespace())); err != nil {
		return nil, err
	}
	return spec, nil
}

// The cluster lock keeps out a schedule that has not seen the hold yet.
func (c *RestoreController) start(
	ctx context.Context,
	chr *api.ClickHouseRestore,
	cluster *api.OperationClusterStatus,
	key runKey,
	held bool,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	_, wait, err := targets.forCluster(cluster.Name)
	switch {
	case err != nil:
		return ctrl.Result{}, err
	case (wait != nil) && wait.refusesOneOff():
		failOn(cluster, wait.reason, wait.message)
		out.add(c.failed(chr, cluster))
		return ctrl.Result{}, nil
	case wait != nil:
		waitOn(cluster, wait.reason, wait.message)
		return ctrl.Result{RequeueAfter: wait.requeue}, nil
	}
	if !held {
		waiting := schedulesNotHeld(chr.Spec.ClickHouseInstallation, cluster.Name)
		waitOn(cluster, waiting.reason, waiting.message)
		return ctrl.Result{RequeueAfter: waiting.requeue}, nil
	}

	var spec *model.RunSpec
	gate := c.runGate(chr, cluster.Name)
	run, wait, err := c.startRun(ctx, chr, chr.Spec.ClickHouseInstallation, key, gate, func() (*model.RunSpec, error) {
		var err error
		spec, err = c.restoreSpec(ctx, chr, cluster.Name, targets)
		return spec, err
	})
	switch {
	case err != nil:
		return ctrl.Result{}, err
	case wait != nil:
		waitOn(cluster, wait.reason, wait.message)
		return ctrl.Result{RequeueAfter: wait.requeue}, nil
	}
	startOn(cluster, run, spec.SchemaHosts, spec.ConfigSecret, c.clock.Now())
	c.run(chr, cluster, out)
	return ctrl.Result{RequeueAfter: runningRequeueInterval}, nil
}

// The restore waits for a scheduled backup on the source cluster, because its retention could delete the backup.
// Access entities shared between clusters through a replicated user directory in Keeper are not tracked.
func (c *RestoreController) runGate(chr *api.ClickHouseRestore, cluster string) *startGate {
	namespace, source := chr.GetNamespace(), chr.LocalSource()
	if source == "" {
		return nil
	}
	return &startGate{sources: operationLockKeys(namespace, source, cluster), check: func(ctx context.Context) (*pending, error) {
		running, err := c.scheduledRunActive(ctx, namespace, source, cluster)
		if (err != nil) || !running {
			return nil, err
		}
		return &pending{
			reason:  api.BackupReasonOperationInProgress,
			message: operationInProgressMessage(source, cluster),
			requeue: pendingRequeueInterval,
		}, nil
	}}
}

func schedulesNotHeld(chiName, cluster string) *pending {
	return &pending{
		reason:  api.BackupReasonWaitingForSchedules,
		message: fmt.Sprintf("waiting for the backup schedules of cluster %s of ClickHouseInstallation %s to be held", cluster, chiName),
		requeue: time.Second,
	}
}

// validateRestoreSpec checks the spec of a restore, and that it restores access entities only where it
// may: they belong to the whole installation.
func validateRestoreSpec(chr *api.ClickHouseRestore) error {
	if err := chr.Spec.Validate(); err != nil {
		return err
	}
	if chr.Spec.RBAC.IsTrue() && !model.RestoresRBAC(chr) {
		return fmt.Errorf("%w: rbac restores the access entities of the whole installation, so it requires an empty cluster", api.ErrInvalidBackupSpec)
	}
	return nil
}

// A failed restore can leave a cluster partly restored, so it holds the schedules until it is deleted.
func holdsSchedules(chr *api.ClickHouseRestore) bool {
	switch {
	case len(chr.Status.Clusters) == 0:
		return false
	case chr.Status.Phase == api.OperationPhaseCompleted:
		return false
	case failedPhase(chr.Status.Phase):
		for _, cluster := range chr.Status.Clusters {
			if mayHaveChanged(cluster) {
				return true
			}
		}
		return false
	}
	return true
}

func mayHaveChanged(cluster api.OperationClusterStatus) bool {
	return failedPhase(cluster.Phase) && (cluster.Run != nil) && (cluster.Reason != api.BackupReasonFailedUnchanged)
}

func heldCluster(chr *api.ClickHouseRestore) string {
	if (chr.TargetCluster() == "") || (len(chr.Status.Clusters) != 1) {
		return ""
	}
	return chr.Status.Clusters[0].Name
}

func restoresAccessEntitiesNow(chr *api.ClickHouseRestore, cluster string) bool {
	if !model.RestoresRBAC(chr) || (len(chr.Status.Clusters) == 0) || chr.Status.Phase.IsTerminal() {
		return false
	}
	held := heldCluster(chr)
	return (held == "") || (held == cluster)
}

// A backup waits for an access-entity restore of its cluster, or it could capture half-restored access entities.
// It reads restores from the API server, so it cannot miss a just-validated restore.
func (r *reconciler) accessRestoreGate(namespace, chiName, cluster string) *startGate {
	return &startGate{check: func(ctx context.Context) (*pending, error) {
		restores := &api.ClickHouseRestoreList{}
		if err := r.apiReader.List(ctx, restores, client.InNamespace(namespace)); err != nil {
			return nil, err
		}
		for i := range restores.Items {
			restore := &restores.Items[i]
			if (restore.Spec.ClickHouseInstallation == chiName) && restoresAccessEntitiesNow(restore, cluster) {
				return &pending{
					reason:  api.BackupReasonOperationInProgress,
					message: accessRestoreInProgressMessage(chiName, cluster, restore.GetName()),
					requeue: pendingRequeueInterval,
				}, nil
			}
		}
		return nil, nil
	}}
}

// restoredScope names what a restore restores in a message.
func restoredScope(chr *api.ClickHouseRestore) string {
	if cluster := heldCluster(chr); cluster != "" {
		return fmt.Sprintf("cluster %s of ClickHouseInstallation %s", cluster, chr.Spec.ClickHouseInstallation)
	}
	return "ClickHouseInstallation " + chr.Spec.ClickHouseInstallation
}

func (c *RestoreController) run(chr *api.ClickHouseRestore, cluster *api.OperationClusterStatus, out *announcements) {
	namespace, chiName, clusterName := chr.GetNamespace(), chr.Spec.ClickHouseInstallation, cluster.Name
	out.add(c.info(chr, a.EventActionCreate, api.BackupReasonStarted, "restore of backup %s on cluster %s started",
		restoredBackupName(chr), clusterName))
	out.add(func() { metrics.RestoreStarted(context.Background(), namespace, chiName, clusterName) })
}

const rbacNotRestoredCause = "the backup holds access entities that were not restored: users, roles and grants are restored by a " +
	"restore of every cluster with rbac set"

func (c *RestoreController) completed(chr *api.ClickHouseRestore, cluster *api.OperationClusterStatus) func() {
	namespace, chiName, clusterName := chr.GetNamespace(), chr.Spec.ClickHouseInstallation, cluster.Name
	duration := int64(0)
	if (cluster.StartTime != nil) && (cluster.CompletionTime != nil) {
		duration = int64(cluster.CompletionTime.Sub(cluster.StartTime.Time).Seconds())
	}
	announce := c.info(chr, a.EventActionReconcile, api.BackupReasonCompleted, "restore of backup %s on cluster %s completed in %ds",
		restoredBackupName(chr), clusterName, duration)
	if cluster.Reason == api.BackupReasonRBACNotRestored {
		announce = c.warning(chr, a.EventActionReconcile, api.BackupReasonRBACNotRestored, "restore of backup %s on cluster %s completed in %ds: %s",
			restoredBackupName(chr), clusterName, duration, cluster.Message)
	}
	return func() {
		announce()
		metrics.RestoreCompleted(context.Background(), namespace, chiName, clusterName)
	}
}

func (c *RestoreController) failed(chr *api.ClickHouseRestore, cluster *api.OperationClusterStatus) func() {
	namespace, chiName, clusterName := chr.GetNamespace(), chr.Spec.ClickHouseInstallation, cluster.Name
	message := cluster.Message
	if mayHaveChanged(*cluster) {
		message = fmt.Sprintf("%s; scheduled backups of %s stay suspended while this restore remains", message, restoredScope(chr))
	}
	announce := c.warning(chr, a.EventActionReconcile, cluster.Reason, "restore of backup %s on cluster %s failed: %s",
		restoredBackupName(chr), clusterName, message)
	return func() {
		announce()
		metrics.RestoreFailed(context.Background(), namespace, chiName, clusterName)
	}
}

// succeeded records the completion of the restore on every cluster. A restore that left access
// entities unrestored on a cluster says so.
func (c *RestoreController) succeeded(op *operation, outcome operationOutcome) {
	reason := api.BackupReasonCompleted
	var messages []string
	for _, cluster := range outcome.completed {
		if cluster.Reason == api.BackupReasonRBACNotRestored {
			reason = api.BackupReasonRBACNotRestored
		}
		messages = append(messages, clusterMessage(len(outcome.completed), cluster.Name, cluster.Message))
	}
	op.setCondition(api.BackupConditionReady, meta.ConditionTrue, reason, strings.Join(messages, "; "))
}

func restoredBackupName(chr *api.ClickHouseRestore) string {
	return cmp.Or(chr.Status.BackupName, chr.Spec.BackupName)
}
