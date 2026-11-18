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
	"fmt"
	"slices"
	"strings"
	"time"

	batch "k8s.io/api/batch/v1"
	apiEquality "k8s.io/apimachinery/pkg/api/equality"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/controller/chbackup/metrics"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// RestoreController reconciles a ClickHouseRestore into a Job per cluster that restores the backup
// there. Each Job itself checks every host of its cluster before it changes anything.
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

// Reconcile drives a ClickHouseRestore through its phases. A restore runs at most once on every
// cluster: a finished, failed or lost restore is never started again.
func (c *RestoreController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if c.skip(ctx, req.Namespace) {
		return ctrl.Result{}, nil
	}

	cur := &api.ClickHouseRestore{}
	if err := c.Get(ctx, req.NamespacedName, cur); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cur.Status.Phase.IsTerminal() {
		return ctrl.Result{}, nil
	}

	next := cur.DeepCopy()
	var out announcements
	result, err := c.reconcile(ctx, next, holdsSchedules(cur), &out)
	if err != nil {
		return ctrl.Result{}, err
	}
	return c.commitStatus(ctx, next, !apiEquality.Semantic.DeepEqual(cur.Status, next.Status), result, out)
}

func (c *RestoreController) reconcile(ctx context.Context, chr *api.ClickHouseRestore, held bool, out *announcements) (ctrl.Result, error) {
	op := newRestoreOperation(chr)
	if !op.validated() {
		// The spec matters only for building the Jobs: once they exist, an edit cannot change them.
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
		// A validated restore holds the backup schedules of the clusters it restores, which suspend
		// their CronJobs in response; their Jobs start only from a reconcile that sees the hold recorded
		op.pin(target.topology.ClusterNames())
		if leftOut := leftOutWithoutStorage(target, chr.Spec.Cluster); leftOut != "" {
			out.add(c.warning(chr, a.EventActionReconcile, api.BackupReasonNoBackupStorage, "restore of backup %s %s",
				chr.Spec.BackupName, leftOut))
		}
	}

	jobs, err := c.observeClusterJobs(ctx, chr, chr.Status.Clusters, func(cluster string) string {
		return model.RestoreJobName(chr, cluster)
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	// Every cluster restores one backup: the one a Job restores already, created by a reconcile whose
	// status write was lost, else the one the spec names now, whatever it names later
	if chr.Status.BackupName == "" {
		chr.Status.BackupName = cmp.Or(jobsBackupName(jobs), chr.Spec.BackupName)
	}
	targets := c.newTargetResolver(ctx, chr.GetNamespace(), chr.Spec.ClickHouseInstallation, true)
	var result ctrl.Result
	for _, job := range jobs {
		clusterResult, err := c.reconcileCluster(ctx, chr, job, held, targets, out)
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

// reconcileCluster drives the restore on one of its clusters. held tells whether the status the
// reconcile started from holds the schedules already.
func (c *RestoreController) reconcileCluster(
	ctx context.Context,
	chr *api.ClickHouseRestore,
	observed clusterJob,
	held bool,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	cluster, job, state := observed.cluster, observed.job, observed.state
	switch state {
	case jobMissing:
		return c.start(ctx, chr, cluster, held, targets, out)
	case jobStale:
		waitOn(cluster, api.BackupReasonWaitingForGarbageCollection, staleOwnerMessage(kindJob, job.GetName()))
		return ctrl.Result{RequeueAfter: pendingRequeueInterval}, nil
	case jobConflict:
		// Unless the restore recorded a Job of its own there before, it changed nothing on the cluster, for
		// which it then holds no schedule
		failOn(cluster, api.BackupReasonNameConflict, takenNameMessage(kindJob, job))
		out.add(c.failed(chr, cluster))
		return ctrl.Result{}, nil
	case jobLost:
		message := fmt.Sprintf("Job %s is gone, the restore is not repeated", cluster.JobName)
		failOn(cluster, api.BackupReasonJobLost, message)
		out.add(c.failed(chr, cluster))
		return ctrl.Result{}, nil
	case jobActive:
		c.run(chr, cluster, job, out)
		return ctrl.Result{RequeueAfter: runningRequeueInterval}, nil
	case jobFailed:
		c.run(chr, cluster, job, out)
		reason := failureReason(job)
		failure, err := c.describeFailure(ctx, job, reason)
		if err != nil {
			return ctrl.Result{}, err
		}
		if failure.unchanged {
			reason = api.BackupReasonJobFailedUnchanged
		}
		failOn(cluster, reason, failure.message)
		out.add(c.failed(chr, cluster))
		return ctrl.Result{}, nil
	case jobSucceeded:
	default:
		return ctrl.Result{}, fmt.Errorf("%w: %d", errUnexpectedJobState, state)
	}

	c.run(chr, cluster, job, out)
	return ctrl.Result{}, c.complete(ctx, chr, cluster, job, out)
}

// start creates the restore Job of a cluster once the installation has completed its reconcile, the
// backup schedules of the cluster are suspended for the restore and no other operation runs on the
// cluster. The readiness gate applies only here: once the Job exists it is tracked whatever the
// installation does meanwhile. held tells whether the status the reconcile started from holds the
// schedules already.
func (c *RestoreController) start(
	ctx context.Context,
	chr *api.ClickHouseRestore,
	cluster *api.OperationClusterStatus,
	held bool,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	// A cluster recorded waiting for the installation the restore restores holds the schedules of the one whose
	// backup it reads no longer, see sourceClustersRead: as for any hold, the Job is created only from a status that
	// held them, which a reconcile records first. Read before the wait below records another reason
	sourceReleased := (chr.LocalSource() != "") && waitsForTarget(*cluster)
	target, wait, err := targets.forCluster(cluster.Name)
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

	// A validated restore holds the schedules of the cluster, which suspend their CronJobs in response;
	// a CronJob still running could otherwise start a backup right after the check for other operations.
	// A schedule tells the hold from the restores its cache holds, so one that read the restore before
	// its validation was recorded may still resume a CronJob, which then runs a missed run at once. The
	// CronJobs are checked only by a reconcile that started from a recorded hold, which every schedule
	// reading the restore since sees, and under the lock of the cluster, which a schedule takes to
	// resume a CronJob after it checked the restores past its cache.
	if !held {
		waiting := schedulesNotSuspended(chr.Spec.ClickHouseInstallation, cluster.Name)
		waitOn(cluster, waiting.reason, waiting.message)
		return ctrl.Result{RequeueAfter: waiting.requeue}, nil
	}
	if sourceReleased {
		waiting := schedulesNotSuspended(chr.LocalSource(), cluster.Name)
		waitOn(cluster, waiting.reason, waiting.message)
		return ctrl.Result{RequeueAfter: waiting.requeue}, nil
	}

	job := model.BuildRestoreJob(chr, target.chi, target.topology)
	gate := c.jobGate(chr, cluster.Name, target.topology.EnabledClusterNames())
	return c.startJob(ctx, chr, chr.Spec.ClickHouseInstallation, cluster.Name, job, gate, func(reason, message string) {
		waitOn(cluster, reason, message)
	}, func() {
		c.run(chr, cluster, job, out)
	})
}

// jobGate returns what holds the start of the restore Job of a cluster: the backup schedules of the
// cluster not suspended for long enough yet, and those of the cluster of the installation of the
// namespace whose backup the restore reads, or a scheduled run there. A restore that restores access
// entities restores them for the whole installation, as it does the user-defined functions kept in
// Keeper, which a backup running on any of its backup-enabled clusters meanwhile would capture half
// restored, whether the restore restores that cluster or not: each of its Jobs starts only once every
// one of them, as enabled names them, has its schedules suspended for long enough and runs no
// operation but the restore's own.
func (c *RestoreController) jobGate(chr *api.ClickHouseRestore, cluster string, enabled []string) *startGate {
	namespace, chiName := chr.GetNamespace(), chr.Spec.ClickHouseInstallation
	// The schedules of the installation of the namespace whose backup the restore reads are held for it too, as
	// the retention of a run there could delete that backup: the Job reads the cluster of the same name there,
	// once no scheduled run that started before the hold runs there either
	var sources []string
	sourceHeld := func(context.Context) (*pending, error) { return nil, nil }
	if source := chr.LocalSource(); source != "" {
		sources = operationLockKeys(namespace, source, cluster)
		sourceHeld = func(ctx context.Context) (*pending, error) {
			if wait, err := c.schedulesHeld(ctx, namespace, source, cluster); (err != nil) || (wait != nil) {
				return wait, err
			}
			running, err := c.activeJobs(ctx, c.apiReader, namespace,
				model.ClusterJobsSelector(source, cluster, api.ClickHouseBackupScheduleCRDResourceKind))
			if (err != nil) || !running {
				return nil, err
			}
			return &pending{
				reason:  api.BackupReasonOperationInProgress,
				message: operationInProgressMessage(source, cluster),
				requeue: pendingRequeueInterval,
			}, nil
		}
	}
	if !model.RestoresRBAC(chr) {
		return &startGate{sources: sources, check: func(ctx context.Context) (*pending, error) {
			if wait, err := c.schedulesHeld(ctx, namespace, chiName, cluster); (err != nil) || (wait != nil) {
				return wait, err
			}
			return sourceHeld(ctx)
		}}
	}
	return &startGate{clusters: enabled, sources: sources, check: func(ctx context.Context) (*pending, error) {
		for _, name := range enabled {
			if wait, err := c.schedulesHeld(ctx, namespace, chiName, name); (err != nil) || (wait != nil) {
				return wait, err
			}
			active, err := c.activeJobsOf(ctx, c.apiReader, namespace, model.ClusterJobsSelector(chiName, name, ""))
			if err != nil {
				return nil, err
			}
			if slices.ContainsFunc(active, func(job *batch.Job) bool { return !meta.IsControlledBy(job, chr) }) {
				return &pending{
					reason:  api.BackupReasonOperationInProgress,
					message: operationInProgressMessage(chiName, name),
					requeue: pendingRequeueInterval,
				}, nil
			}
		}
		return sourceHeld(ctx)
	}}
}

// schedulesHeld explains the wait of a restore of a cluster for its backup schedules to be suspended for
// long enough, nil when they are.
func (c *RestoreController) schedulesHeld(ctx context.Context, namespace, chiName, cluster string) (*pending, error) {
	suspended, err := c.schedulesSuspended(ctx, namespace, chiName, cluster)
	if (err != nil) || suspended {
		return nil, err
	}
	return schedulesNotSuspended(chiName, cluster), nil
}

// schedulesNotSuspended explains the wait of a restore of a cluster for its backup schedules to be
// suspended.
func schedulesNotSuspended(chiName, cluster string) *pending {
	return &pending{
		reason:  api.BackupReasonWaitingForSchedules,
		message: fmt.Sprintf("waiting for the backup schedules of cluster %s of ClickHouseInstallation %s to be suspended", cluster, chiName),
		requeue: pendingRequeueInterval,
	}
}

// schedulesSuspended reports whether every CronJob that backs up a cluster of the installation is
// suspended, and has been for long enough for the CronJob controller, which acts on its own cache,
// to have seen it. It reads past the cache, as the CronJob controller acts on the live state.
func (c *RestoreController) schedulesSuspended(ctx context.Context, namespace, chiName, cluster string) (bool, error) {
	cronJobs := &batch.CronJobList{}
	if err := c.apiReader.List(ctx, cronJobs, client.InNamespace(namespace), client.MatchingLabelsSelector{
		Selector: model.ClusterJobsSelector(chiName, cluster, api.ClickHouseBackupScheduleCRDResourceKind),
	}); err != nil {
		return false, err
	}
	for i := range cronJobs.Items {
		if !ptr.Deref(cronJobs.Items[i].Spec.Suspend, false) || (suspendedFor(&cronJobs.Items[i]) < pendingRequeueInterval) {
			return false, nil
		}
	}
	return true, nil
}

// suspendedFor returns how long the operator has had a CronJob suspended, zero when it has no record of
// the suspension it can read.
func suspendedFor(cronJob *batch.CronJob) time.Duration {
	suspended, recorded := suspendedAt(cronJob)
	if !recorded {
		return 0
	}
	return time.Since(suspended)
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

// holdsSchedules reports whether a restore keeps backup schedules of its installation suspended: from
// its validation, after which it may start at any moment, until it completes. A failed restore may
// have left a cluster partly restored, so it holds them until it is deleted, unless every Job of it
// that failed failed before it changed anything. A restore still waiting to be validated, possibly
// forever with an invalid spec, holds nothing; nor does one refused for its target.
func holdsSchedules(chr *api.ClickHouseRestore) bool {
	switch {
	case len(chr.Status.Clusters) == 0:
		return false
	case chr.Status.Phase == api.OperationPhaseCompleted:
		return false
	case chr.Status.Phase == api.OperationPhaseFailed:
		for _, cluster := range chr.Status.Clusters {
			if mayHaveChanged(cluster) {
				return true
			}
		}
		return false
	}
	return true
}

// mayHaveChanged reports whether a restore failed on a cluster after its Job may have changed something.
func mayHaveChanged(cluster api.OperationClusterStatus) bool {
	return (cluster.Phase == api.OperationPhaseFailed) && (cluster.JobName != "") && (cluster.Reason != api.BackupReasonJobFailedUnchanged)
}

// heldCluster returns the cluster whose schedules a restore holds, "" when it holds the schedules of
// every cluster: a restore of every cluster restores more than its Jobs touch, the access entities of
// the installation and its user-defined functions, and holds every schedule until it is done, and so
// does a restore of one cluster that restores access entities, as a recovery restore does. A restore of
// one cluster names it in its status, which an edit of its spec does not change.
func heldCluster(chr *api.ClickHouseRestore) string {
	if (chr.TargetCluster() == "") || (len(chr.Status.Clusters) != 1) || model.RestoresRBAC(chr) {
		return ""
	}
	return chr.Status.Clusters[0].Name
}

// restoresAccessEntitiesNow reports whether a restore restores the access entities of its installation,
// and may still do so: it was validated and has not finished.
func restoresAccessEntitiesNow(chr *api.ClickHouseRestore) bool {
	return model.RestoresRBAC(chr) && (len(chr.Status.Clusters) > 0) && !chr.Status.Phase.IsTerminal()
}

// accessRestoreGate returns what holds the start of a backup or verify Job on an installation: a restore
// of its access entities that has not finished, whichever clusters it restores. Its Jobs restore them,
// and the user-defined functions kept in Keeper, for the whole installation, one cluster after the other:
// a backup on any cluster meanwhile would capture them half restored. The restores are read past the
// cache: a restore Job starts only once the restore was validated, and finds a backup Job created
// before that running on its gate.
func (r *reconciler) accessRestoreGate(namespace, chiName string) *startGate {
	return &startGate{check: func(ctx context.Context) (*pending, error) {
		restores := &api.ClickHouseRestoreList{}
		if err := r.apiReader.List(ctx, restores, client.InNamespace(namespace)); err != nil {
			return nil, err
		}
		for i := range restores.Items {
			restore := &restores.Items[i]
			if (restore.Spec.ClickHouseInstallation == chiName) && restoresAccessEntitiesNow(restore) {
				return &pending{
					reason:  api.BackupReasonOperationInProgress,
					message: accessRestoreInProgressMessage(chiName, restore.GetName()),
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

// run records the running restore Job of a cluster, announcing the start of the restore there once.
func (c *RestoreController) run(chr *api.ClickHouseRestore, cluster *api.OperationClusterStatus, job *batch.Job, out *announcements) {
	if !runOn(cluster, job) {
		return
	}
	namespace, chiName, clusterName := chr.GetNamespace(), chr.Spec.ClickHouseInstallation, cluster.Name
	out.add(c.info(chr, a.EventActionCreate, api.BackupReasonJobCreated, "restore of backup %s on cluster %s started in Job %s",
		restoredBackupName(chr), clusterName, job.GetName()))
	out.add(func() { metrics.RestoreStarted(context.Background(), namespace, chiName, clusterName) })
}

// complete records the completion of the restore Job of a cluster, announcing it once. A restore that
// left access entities of the backup unrestored, as its Job tells, completed all the same, which is
// warned about: users, roles or grants the backup holds may be missing.
func (c *RestoreController) complete(
	ctx context.Context,
	chr *api.ClickHouseRestore,
	cluster *api.OperationClusterStatus,
	job *batch.Job,
	out *announcements,
) error {
	rbacNotRestored, err := c.restoreLeftRBAC(ctx, job)
	switch {
	case err != nil:
		return err
	case rbacNotRestored:
		if completeOn(cluster, job, api.BackupReasonRBACNotRestored, fmt.Sprintf("Job %s completed, but %s", job.GetName(), rbacNotRestoredCause)) {
			out.add(c.completed(chr, cluster))
		}
	case completeOn(cluster, job, api.BackupReasonCompleted, fmt.Sprintf("Job %s completed", job.GetName())):
		out.add(c.completed(chr, cluster))
	}
	return nil
}

// restoreLeftRBAC reports whether a succeeded restore Job restored a backup that holds access entities
// it did not restore, which its script tells in the termination message of its pod.
func (c *RestoreController) restoreLeftRBAC(ctx context.Context, job *batch.Job) (bool, error) {
	pods, err := c.jobPods(ctx, job)
	if err != nil {
		return false, err
	}
	for i := range pods {
		if model.JobRBACNotRestored(&pods[i]) {
			return true, nil
		}
	}
	return false, nil
}

// rbacNotRestoredCause tells why access entities the backup holds were not restored: the restore did not
// ask for them. A Job that restores them never tells it, whatever the ClickHouse version.
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

// restoredBackupName returns the backup a restore restores: the one its Jobs were created with, or,
// before there is one, the one its spec names.
func restoredBackupName(chr *api.ClickHouseRestore) string {
	return cmp.Or(chr.Status.BackupName, chr.Spec.BackupName)
}
