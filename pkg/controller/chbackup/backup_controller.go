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

// BackupController reconciles a ClickHouseBackup: it runs the backup on every cluster it targets, driving the
// clickhouse-backup sidecars of the cluster through SQL.
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

// Reconcile drives a ClickHouseBackup through its phases. A finished backup is never touched
// again, but for its deletion, see finalize, and a new spec.taskID, which reruns it, see applyTaskID.
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
	// A backup deleted unfinished is killed and deleted before it goes, see finalize
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

	// Every cluster makes the backup under one name: the one the spec resolves to now, whatever it names later
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

// validate resolves the clusters the backup targets once it is due, its spec is valid and its target can run it,
// and reports whether it did: until then the backup waits, and a backup its starting deadline passes before is
// refused, as is one whose target can never run it. A backup that runs a schedule records what it takes from
// the schedule first, and the target it names. The spec matters only for starting the runs: once they run, an
// edit cannot change them.
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
			fmt.Sprintf("the backup is due at %s", due.UTC().Format(time.RFC3339)))
		return ctrl.Result{RequeueAfter: min(due.Sub(now)+dueRecheckDelay, dueRecheckCap)}, false, nil
	}

	wait, err := c.recordSchedule(ctx, chb)
	if err != nil {
		return ctrl.Result{}, false, err
	}
	var target *target
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
	// A naming template names the backup by its time, which the name of the backup of the first shard tells
	name, err := templateBackupName(chb, target.topology)
	if err != nil {
		op.wait(api.BackupConditionValidated, api.BackupReasonInvalidSpec, err.Error())
		return ctrl.Result{RequeueAfter: untilStartingDeadline(chb, now, 0)}, false, nil
	}
	// A backup a rerun of a completed one would make again under the name of one it made is refused: the rerun of a
	// backup that failed replaces what it left under its name
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

// templateBackupName returns the name of a backup of clusters with a naming template, "" for clusters without one: the
// name the template gives the backup of the first shard, which tells the backup of every shard. One backup of several
// clusters has one name, so their template has to be one, and to name the backups of every cluster alike, without
// {cluster}.
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
	message := fmt.Sprintf("not started by %s, %ds after the backup was due", deadline.UTC().Format(time.RFC3339),
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

// errTargetGone tells a run that cannot resume as its installation or its cluster is gone.
var errTargetGone = errors.New("the installation or the cluster is gone")

// backupSpec builds the spec of the run of the backup on a cluster.
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
	if spec.PathCheck, err = target.topology.PathCheck(api.BackupRunOperationBackup, chb.GetBackupOptions().GetReplicaSelection(),
		c.settingResolver(ctx, chb.GetNamespace())); err != nil {
		return nil, err
	}
	spec.PathCheck.ConfigOverrides = spec.PathCheckOverrides()
	return spec, nil
}

// refusedPaths runs the storage path check of the run of a backup, before the run starts and anything is sent to a
// sidecar: it tells why the check refuses the run, "" when it passes.
func refusedPaths(spec *model.RunSpec) string {
	if spec.PathCheck == nil {
		return ""
	}
	if _, err := spec.PathCheck.Check(); err != nil {
		return err.Error()
	}
	return ""
}

// start starts the backup of a cluster once the installation has completed its reconcile, no other operation runs on
// the cluster and no restore of the access entities of the cluster, or of every cluster, is unfinished, see
// accessRestoreGate. The readiness gate applies only here: once the backup runs it is tracked whatever the
// installation does meanwhile. A cluster the installation no longer has, or no longer backs up, fails the backup there, as does one the starting
// deadline passes before.
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
	// Storage paths that two writers of the backup share abort it, before it waits for anything
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
		// A backup that runs a schedule now prunes after it as a run of the schedule, which no restore may hold
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

// run announces the start of the backup on a cluster.
func (c *BackupController) run(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, out *announcements) {
	namespace, chiName, clusterName := chb.GetNamespace(), chb.TargetInstallation(), cluster.Name
	out.add(c.info(chb, a.EventActionCreate, api.BackupReasonStarted, "backup %s of cluster %s started", chb.Status.BackupName, clusterName))
	out.add(func() { metrics.BackupStarted(context.Background(), namespace, chiName, clusterName) })
}

// complete records the completion of the backup of a cluster, announcing it, with the hosts the backup was made on
// when they are not those it was to be made on. A backup that holds no data there completed all the same, which is
// warned about: it restores no data on the cluster.
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
