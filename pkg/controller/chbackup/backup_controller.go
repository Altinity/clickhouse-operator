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
	"slices"
	"strings"
	"time"

	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
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

type BackupController struct {
	*reconciler
}

// NewBackupController creates a BackupController with its request-independent collaborators
// initialized once, as controller-runtime shares it across all reconcile goroutines.
func NewBackupController(
	c client.Client,
	apiReader client.Reader,
	scheme *apiMachinery.Scheme,
	cr interfaces.IKubeCR,
) *BackupController {
	return &BackupController{
		reconciler: newReconciler(c, apiReader, scheme, cr, api.ClickHouseBackupCRDResourceKind),
	}
}

func (c *BackupController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if c.skip(ctx, req.Namespace) {
		return ctrl.Result{}, nil
	}

	cur := &api.ClickHouseBackup{}
	if err := c.Get(ctx, req.NamespacedName, cur); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if deleting(cur) {
		result, err := c.finalize(ctx, cur)
		if err != nil {
			return ignoreConflict(err)
		}
		return result, nil
	}
	if err := c.addFinalizer(ctx, cur, backupFinalizer); err != nil {
		return ignoreConflict(err)
	}

	next := cur.DeepCopy()
	var out announcements
	c.applyTaskID(next, &out)
	if next.Status.Phase.IsTerminal() {
		if result, err := c.commitStatus(ctx, next, !apiEquality.Semantic.DeepEqual(cur.Status, next.Status), ctrl.Result{}, out); (err != nil) || result.Requeue {
			return result, err
		}
		return ctrl.Result{}, c.reportFinishedBackup(ctx, cur)
	}
	reportBackupOutcome(cur, everyCluster)

	result, err := c.reconcile(ctx, next, &out)
	if err != nil {
		return ctrl.Result{}, err
	}
	if next.Status.Phase.IsTerminal() {
		next.Status.Complete()
	}
	c.announcePhase(next, cur.Status.Phase, next.Status.Phase, next.Status.Conditions, &out)
	return c.commitStatus(ctx, next, !apiEquality.Semantic.DeepEqual(cur.Status, next.Status), result, out)
}

func (c *BackupController) reconcile(ctx context.Context, chb *api.ClickHouseBackup, out *announcements) (ctrl.Result, error) {
	op := newBackupOperation(chb)
	if !op.validated() {
		result, validated, err := c.validate(ctx, chb, op, out)
		if !validated || (err != nil) {
			return result, err
		}
	}

	// Pin the name now, so a later spec edit cannot give the clusters different names.
	if chb.Status.BackupName == "" {
		chb.Status.BackupName = model.ResolveBackupName(chb)
	}
	targets := c.newTargetResolver(ctx, chb.GetNamespace(), chb.TargetInstallation(), true)
	var result ctrl.Result
	for i := range chb.Status.Clusters {
		cluster := &chb.Status.Clusters[i]
		if cluster.Phase.IsTerminal() {
			continue
		}
		clusterResult, err := c.reconcileCluster(ctx, chb, cluster, targets, out)
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
			out.add(c.warning(chb, a.EventActionReconcile, reason, "backup %s %s", chb.Status.BackupName, message))
		}
		return ctrl.Result{}, nil
	}
	c.succeeded(chb, op, outcome)
	chb.Status.Phase = api.OperationPhaseCompleted
	return ctrl.Result{}, nil
}

func (c *BackupController) validate(ctx context.Context, chb *api.ClickHouseBackup, op *operation, out *announcements) (ctrl.Result, bool, error) {
	now := c.clock.Now()
	if missed := missedStartingDeadline(chb, now, validatedMessage(chb)); missed != nil {
		op.refuse(missed.reason, missed.message)
		out.add(c.refused(chb, missed))
		return ctrl.Result{}, false, nil
	}
	// Only a change to the spec can fix it, or the starting deadline end the wait
	if err := chb.Spec.Validate(); err != nil {
		op.wait(api.BackupConditionValidated, api.BackupReasonInvalidSpec, err.Error())
		return ctrl.Result{RequeueAfter: untilStartingDeadline(chb, now, 0)}, false, nil
	}
	if due := chb.DueTime(); now.Before(due) {
		op.wait(api.BackupConditionValidated, api.BackupReasonWaitingForScheduledTime,
			fmt.Sprintf("the backup is scheduled for %s", due.UTC().Format(time.RFC3339)))
		return ctrl.Result{RequeueAfter: min(due.Sub(now)+dueRecheckDelay, dueRecheckCap)}, false, nil
	}

	wait, err := c.recordSchedule(ctx, chb)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	var target *backupTarget
	if wait == nil {
		target, wait, err = c.resolveTarget(ctx, chb.GetNamespace(), chb.TargetInstallation(), chb.TargetCluster(), true)
		if err != nil {
			return ctrl.Result{}, false, err
		}
	}
	switch {
	case (wait != nil) && wait.refusesOneOff():
		op.refuse(wait.reason, wait.message)
		out.add(c.refused(chb, wait))
		return ctrl.Result{}, false, nil
	case wait != nil:
		op.wait(api.BackupConditionValidated, wait.reason, wait.message)
		return ctrl.Result{RequeueAfter: untilStartingDeadline(chb, now, wait.requeue)}, false, nil
	}
	name, err := templateBackupName(chb, target.topology)
	if err != nil {
		op.wait(api.BackupConditionValidated, api.BackupReasonInvalidSpec, err.Error())
		return ctrl.Result{RequeueAfter: untilStartingDeadline(chb, now, 0)}, false, nil
	}
	// A failed run's name can be reused, because the rerun replaces what it left.
	if made := cmp.Or(chb.Status.BackupName, name, model.ResolveBackupName(chb)); slices.Contains(chb.Status.PreviousBackupNames, made) {
		refusal := &pending{reason: api.BackupReasonInvalidSpec, message: fmt.Sprintf("backup %s was made by this ClickHouseBackup "+
			"before spec.taskID reran it, and stays in the storage: set another spec.backupName with another spec.taskID", made)}
		op.refuse(refusal.reason, refusal.message)
		out.add(c.refused(chb, refusal))
		return ctrl.Result{}, false, nil
	}
	if (chb.Status.BackupName == "") && (name != "") {
		chb.Status.BackupName = name
	}
	op.pin(target.topology.ClusterNames())
	retainRequestedClusters(chb.GetNamespace(), chb.TargetInstallation(), target.chi)
	if leftOut := leftOutWithoutStorage(target, chb.TargetCluster()); leftOut != "" {
		out.add(c.warning(chb, a.EventActionReconcile, api.BackupReasonNoBackupStorage, "backup %s", leftOut))
	}
	return ctrl.Result{}, true, nil
}

// All clusters must share one template without {cluster}, because one backup has one name.
func templateBackupName(chb *api.ClickHouseBackup, topology *model.Topology) (string, error) {
	namings := topology.ClusterNamings()
	templates := map[string]bool{}
	for _, template := range namings {
		templates[template] = true
	}
	naming := topology.BackupNaming()
	if (len(templates) == 1) && templates[""] {
		return "", nil
	}
	if (len(templates) > 1) || ((len(namings) > 1) && api.BackupNamingHolds(naming.Template, api.BackupNamingCluster)) {
		return "", fmt.Errorf("the clusters %s name their backups by naming templates that differ between them, or hold {cluster}: "+
			"back up each of them in a ClickHouseBackup of its own (spec.cluster)", strings.Join(topology.ClusterNames(), ", "))
	}
	return model.ResolveTemplateBackupName(chb, naming)
}

// recordSchedule records what a backup that runs a schedule takes from it, the schedule as it is now: until the
// backup is validated, every reconcile records it anew. A schedule that does not exist yet, as when both are
// applied at once, or whose spec is invalid is waited for; a cluster the backup names that the schedule does not
// back up refuses it, as neither can change.
func (c *BackupController) recordSchedule(ctx context.Context, chb *api.ClickHouseBackup) (*pending, error) {
	name := chb.Spec.GetScheduleName()
	if name == "" {
		return nil, nil
	}
	chbs := &api.ClickHouseBackupSchedule{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: chb.GetNamespace(), Name: name}, chbs); err != nil {
		if apiErrors.IsNotFound(err) {
			return &pending{
				reason:  api.BackupReasonScheduleNotFound,
				message: fmt.Sprintf("ClickHouseBackupSchedule %s not found", name),
				requeue: pendingRequeueInterval,
			}, nil
		}
		return nil, err
	}
	if err := chbs.Spec.Validate(); err != nil {
		return &pending{
			reason:  api.BackupReasonInvalidSpec,
			message: fmt.Sprintf("ClickHouseBackupSchedule %s: %s", name, err),
			requeue: pendingRequeueInterval,
		}, nil
	}
	cluster := cmp.Or(chb.Spec.Cluster, chbs.Spec.Cluster)
	if (chbs.Spec.Cluster != "") && (cluster != chbs.Spec.Cluster) {
		return &pending{
			reason:  api.BackupReasonClusterNotFound,
			message: fmt.Sprintf("ClickHouseBackupSchedule %s backs up cluster %s only, not %s", name, chbs.Spec.Cluster, cluster),
		}, nil
	}
	chb.Status.Schedule = &api.BackupScheduleRecord{
		Name:                   name,
		Generation:             chbs.GetGeneration(),
		ClickHouseInstallation: chbs.Spec.ClickHouseInstallation,
		Cluster:                cluster,
		BackupNamePrefix:       model.BackupNamePrefix(chbs),
		Schedules:              chbs.Spec.DeepCopy().Schedules,
		Time:                   chbs.Spec.Time,
		TimeZone:               chbs.Spec.TimeZone,
		KeepBackups:            slices.Clone(chbs.Spec.Retention.GetKeepBackups()),
		PreviousSchedules:      chbs.Status.Retention.DeepCopy().GetPrevious(),
		Timeouts:               chbs.Spec.Timeouts.DeepCopy(),
		BackupOptions:          *chbs.Spec.BackupOptions.DeepCopy(),
	}
	return nil, nil
}

// validatedMessage returns what the Validated condition of a backup tells, empty without one.
func validatedMessage(chb *api.ClickHouseBackup) string {
	if validated := apiMeta.FindStatusCondition(chb.Status.Conditions, api.BackupConditionValidated); validated != nil {
		return validated.Message
	}
	return ""
}

// missedStartingDeadline tells why the backup fails where it has not started by its starting deadline, nil while it
// may still start: with what it waited for then, the last message given.
func missedStartingDeadline(chb *api.ClickHouseBackup, now time.Time, waited string) *pending {
	deadline, bounded := chb.StartingDeadline()
	if !bounded || !now.After(deadline) {
		return nil
	}
	message := fmt.Sprintf("not started by %s, %ds after its scheduled time", deadline.UTC().Format(time.RFC3339),
		*chb.Spec.StartingDeadlineSeconds)
	if waited != "" {
		message += ": " + waited
	}
	return &pending{reason: api.BackupReasonStartingDeadlineExceeded, message: message}
}

// untilStartingDeadline returns when a backup that waits should be rechecked: after delay, never for none, or just
// past its starting deadline when that comes first.
func untilStartingDeadline(chb *api.ClickHouseBackup, now time.Time, delay time.Duration) time.Duration {
	if deadline, bounded := chb.StartingDeadline(); bounded {
		return shorterDelay(delay, max(deadline.Sub(now), 0)+dueRecheckDelay)
	}
	return delay
}

// reconcileCluster drives the backup on one of its clusters.
func (c *BackupController) reconcileCluster(
	ctx context.Context,
	chb *api.ClickHouseBackup,
	cluster *api.OperationClusterStatus,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	key := runKeyOf(chb, api.ClickHouseBackupCRDResourceKind, cluster.Name, api.BackupRunOperationBackup)
	if cluster.Run == nil {
		return c.start(ctx, chb, cluster, key, targets, out)
	}
	run, err := c.resumeRun(chb, chb.TargetInstallation(), key, cluster, func() (*model.RunSpec, error) {
		return c.backupSpec(ctx, chb, cluster.Name, targets)
	})
	switch {
	case errors.Is(err, errNoClickHouseAccess) || errors.Is(err, errTargetGone):
		failOn(cluster, api.BackupReasonFailed, fmt.Sprintf("the backup cannot resume: %v", err))
		out.add(c.failed(chb, cluster))
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
		failOn(cluster, outcomeReason(outcome, api.BackupRunOperationBackup), outcome.message)
		out.add(c.failed(chb, cluster))
		return ctrl.Result{}, nil
	}
	c.complete(chb, cluster, outcome, out)
	return ctrl.Result{}, nil
}

var errTargetGone = errors.New("the installation or the cluster is gone")

func (c *BackupController) backupSpec(ctx context.Context, chb *api.ClickHouseBackup, cluster string, targets *targetResolver) (*model.RunSpec, error) {
	target, wait, err := targets.forCluster(cluster)
	switch {
	case err != nil:
		return nil, err
	case wait != nil:
		return nil, fmt.Errorf("%w: %s", errTargetGone, wait.message)
	}
	spec := model.BackupRunSpec(chb, target.topology)
	spec.PreviousActions = previousActions(chb.Status.PreviousRuns, cluster)
	if err := spec.CheckPaths(target.topology, chb.GetBackupOptions().GetReplicaSelection(), c.settingResolver(ctx, chb.GetNamespace())); err != nil {
		return nil, err
	}
	return spec, nil
}

func refusedPaths(spec *model.RunSpec) string {
	if spec.PathCheck == nil {
		return ""
	}
	if _, err := spec.PathCheck.Check(); err != nil {
		return err.Error()
	}
	return ""
}

func (c *BackupController) start(
	ctx context.Context,
	chb *api.ClickHouseBackup,
	cluster *api.OperationClusterStatus,
	key runKey,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	now := c.clock.Now()
	if missed := missedStartingDeadline(chb, now, cluster.Message); missed != nil {
		failOn(cluster, missed.reason, missed.message)
		out.add(c.failed(chb, cluster))
		return ctrl.Result{}, nil
	}
	target, wait, err := targets.forCluster(cluster.Name)
	switch {
	case err != nil:
		return ctrl.Result{}, err
	case (wait != nil) && wait.refusesOneOff():
		failOn(cluster, wait.reason, wait.message)
		out.add(c.failed(chb, cluster))
		return ctrl.Result{}, nil
	case wait != nil:
		waitOn(cluster, wait.reason, wait.message)
		return ctrl.Result{RequeueAfter: untilStartingDeadline(chb, now, wait.requeue)}, nil
	}

	retainRequestedClusters(chb.GetNamespace(), chb.TargetInstallation(), target.chi)
	checked, err := c.backupSpec(ctx, chb, cluster.Name, targets)
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal := refusedPaths(checked); refusal != "" {
		failOn(cluster, api.BackupReasonStoragePathConflict, refusal)
		out.add(c.failed(chb, cluster))
		return ctrl.Result{}, nil
	}
	restoreInProgress := false
	if chb.Spec.ScheduleRef != nil {
		schedules := &ScheduleController{c.reconciler}
		holder := &api.ClickHouseBackupSchedule{}
		holder.Namespace = chb.GetNamespace()
		holder.Spec.ClickHouseInstallation = chb.TargetInstallation()
		holds, err := schedules.restoreHolds(ctx, c.Client, holder, target.chi, []string{cluster.Name})
		if err != nil {
			return ctrl.Result{}, err
		}
		restoreInProgress = holds[cluster.Name] != nil
	}
	var spec *model.RunSpec
	gate := c.accessRestoreGate(chb.GetNamespace(), chb.TargetInstallation(), cluster.Name)
	run, wait, err := c.startRun(ctx, chb, chb.TargetInstallation(), key, gate, func() (*model.RunSpec, error) {
		var err error
		if spec, err = c.backupSpec(ctx, chb, cluster.Name, targets); err != nil {
			return nil, err
		}
		spec.RestoreInProgress = restoreInProgress
		return spec, nil
	})
	switch {
	case err != nil:
		return ctrl.Result{}, err
	case wait != nil:
		waitOn(cluster, wait.reason, wait.message)
		return ctrl.Result{RequeueAfter: untilStartingDeadline(chb, now, wait.requeue)}, nil
	}
	startOn(cluster, run, spec.BackupHosts, spec.ConfigSecret, now)
	c.run(chb, cluster, out)
	return ctrl.Result{RequeueAfter: runningRequeueInterval}, nil
}

func (c *BackupController) run(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, out *announcements) {
	namespace, chiName, clusterName := chb.GetNamespace(), chb.TargetInstallation(), cluster.Name
	out.add(c.info(chb, a.EventActionCreate, api.BackupReasonStarted, "backup %s of cluster %s started", chb.Status.BackupName, clusterName))
	out.add(func() { metrics.BackupStarted(context.Background(), namespace, chiName, clusterName) })
}

func (c *BackupController) complete(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, outcome runOutcome, out *announcements) {
	if len(outcome.hostsUsed) > 0 {
		cluster.Hosts = outcome.hostsUsed
	}
	reason, message := api.BackupReasonCompleted, "the backup completed"
	if outcome.noData != "" {
		reason, message = api.BackupReasonCompletedWithoutData, fmt.Sprintf("the backup completed, but holds no data: %s", outcome.noData)
	}
	if completeOn(cluster, reason, message) {
		out.add(c.completed(chb, cluster, outcome.noData))
	}
	if outcome.stored != nil {
		chb.Status.Backups = api.ReplaceStoredBackups(chb.Status.Backups, cluster.Name, outcome.stored)
		chb.Status.BackupsListTime = cluster.CompletionTime.DeepCopy()
	}
}

// completed announces the completion of the backup on a cluster, as a warning when noData tells why
// the backup holds no data there.
func (c *BackupController) completed(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, noData string) func() {
	namespace, chiName, clusterName := chb.GetNamespace(), chb.TargetInstallation(), cluster.Name
	completion := *cluster.CompletionTime
	duration := time.Duration(0)
	if cluster.StartTime != nil {
		duration = completion.Sub(cluster.StartTime.Time)
	}
	announce := c.info(chb, a.EventActionReconcile, api.BackupReasonCompleted, "backup %s of cluster %s completed in %ds",
		chb.Status.BackupName, clusterName, int64(duration.Seconds()))
	if noData != "" {
		announce = c.warning(chb, a.EventActionReconcile, api.BackupReasonCompletedWithoutData,
			"backup %s of cluster %s completed in %ds, but holds no data: %s", chb.Status.BackupName, clusterName, int64(duration.Seconds()), noData)
	}
	return func() {
		announce()
		metrics.BackupCompleted(context.Background(), namespace, chiName, clusterName, duration, completion.Time)
	}
}

// failed announces the failure of the backup on a cluster.
func (c *BackupController) failed(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus) func() {
	namespace, chiName, clusterName := chb.GetNamespace(), chb.TargetInstallation(), cluster.Name
	announce := c.warning(chb, a.EventActionReconcile, cluster.Reason, "backup %s of cluster %s failed: %s",
		chb.Status.BackupName, clusterName, cluster.Message)
	failed := cluster.CompletionTime.Time
	return func() {
		announce()
		metrics.BackupFailed(context.Background(), namespace, chiName, clusterName, failed)
	}
}

// refused announces a backup refused before it targeted any cluster.
func (c *BackupController) refused(chb *api.ClickHouseBackup, wait *pending) func() {
	return c.warning(chb, a.EventActionReconcile, wait.reason, "backup refused: %s", wait.message)
}

// succeeded records the completion of the backup on every cluster. It holds no data when it holds
// none on any of them.
func (c *BackupController) succeeded(chb *api.ClickHouseBackup, op *operation, outcome operationOutcome) {
	var withoutData, messages []string
	for _, cluster := range outcome.completed {
		if cluster.Reason == api.BackupReasonCompletedWithoutData {
			withoutData = append(withoutData, cluster.Name)
		}
		messages = append(messages, clusterMessage(len(outcome.completed), cluster.Name, cluster.Message))
	}
	reason := api.BackupReasonCompleted
	if len(withoutData) == len(outcome.completed) {
		reason = api.BackupReasonCompletedWithoutData
	}
	op.setCondition(api.BackupConditionReady, meta.ConditionTrue, reason, strings.Join(messages, "; "))
}

// shorterResult returns the result that comes back sooner.
func shorterResult(x, y ctrl.Result) ctrl.Result {
	if x.Requeue || y.Requeue {
		return ctrl.Result{Requeue: true}
	}
	return ctrl.Result{RequeueAfter: shorterDelay(x.RequeueAfter, y.RequeueAfter)}
}
