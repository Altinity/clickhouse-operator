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
	"strings"
	"time"

	batch "k8s.io/api/batch/v1"
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

// BackupController reconciles a ClickHouseBackup into a Job per cluster that makes the backup there
// and, when asked, a Job per cluster that verifies it.
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
// again.
func (c *BackupController) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	if c.skip(ctx, req.Namespace) {
		return ctrl.Result{}, nil
	}

	cur := &api.ClickHouseBackup{}
	if err := c.Get(ctx, req.NamespacedName, cur); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if cur.Status.Phase.IsTerminal() {
		return ctrl.Result{}, c.reportFinishedBackup(ctx, cur)
	}
	reportBackupOutcome(cur, everyCluster)

	next := cur.DeepCopy()
	var out announcements
	result, err := c.reconcile(ctx, next, &out)
	if err != nil {
		return ctrl.Result{}, err
	}
	return c.commitStatus(ctx, next, !apiEquality.Semantic.DeepEqual(cur.Status, next.Status), result, out)
}

func (c *BackupController) reconcile(ctx context.Context, chb *api.ClickHouseBackup, out *announcements) (ctrl.Result, error) {
	// A completed backup stays completed while it is verified: its Jobs may be gone by then, removed
	// by hand or by a policy that sets ttlSecondsAfterFinished, and the backup exists either way.
	if chb.Spec.Verify.IsTrue() && apiMeta.IsStatusConditionTrue(chb.Status.Conditions, api.BackupConditionReady) {
		return c.verify(ctx, chb, out)
	}
	op := newBackupOperation(chb)
	if !op.validated() {
		result, validated, err := c.validate(ctx, chb, op, out)
		if !validated || (err != nil) {
			return result, err
		}
	}

	jobs, err := c.observeClusterJobs(ctx, chb, chb.Status.Clusters, func(cluster string) string {
		return model.BackupJobName(chb, cluster)
	})
	if err != nil {
		return ctrl.Result{}, err
	}
	// Every cluster makes the backup under one name: the one a Job makes already, created by a reconcile
	// whose status write was lost, else the one the spec resolves to now, whatever it names later
	if chb.Status.BackupName == "" {
		chb.Status.BackupName = cmp.Or(jobsBackupName(jobs), model.ResolveBackupName(chb))
	}
	targets := c.newTargetResolver(ctx, chb.GetNamespace(), chb.TargetInstallation(), true)
	var result ctrl.Result
	for _, job := range jobs {
		clusterResult, err := c.reconcileCluster(ctx, chb, job, targets, out)
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
	if !chb.Spec.Verify.IsTrue() {
		chb.Status.Phase = api.OperationPhaseCompleted
		return ctrl.Result{}, nil
	}
	return c.verify(ctx, chb, out)
}

// validate resolves the clusters the backup targets once it is due, its spec is valid and its target can run it,
// and reports whether it did: until then the backup waits, and a backup its starting deadline passes before is
// refused, as is one whose target can never run it. A backup that runs a schedule records what it takes from
// the schedule first, and the target it names. The spec matters only for building the Jobs: once they exist, an
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
	op.pin(target.topology.ClusterNames())
	retainRequestedClusters(chb.GetNamespace(), chb.TargetInstallation(), target.chi)
	if leftOut := leftOutWithoutStorage(target, chb.TargetCluster()); leftOut != "" {
		out.add(c.warning(chb, a.EventActionReconcile, api.BackupReasonNoBackupStorage, "backup %s", leftOut))
	}
	return ctrl.Result{}, true, nil
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
		BackupOptions:          *chbs.Spec.BackupTemplate.BackupOptions.DeepCopy(),
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
	observed clusterJob,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	cluster, job, state := observed.cluster, observed.job, observed.state
	switch state {
	case jobMissing:
		return c.start(ctx, chb, cluster, targets, out)
	case jobStale:
		waitOn(cluster, api.BackupReasonWaitingForGarbageCollection, staleOwnerMessage(kindJob, job.GetName()))
		return ctrl.Result{RequeueAfter: pendingRequeueInterval}, nil
	case jobConflict:
		failOn(cluster, api.BackupReasonNameConflict, takenNameMessage(kindJob, job))
		out.add(c.failed(chb, cluster))
		return ctrl.Result{}, nil
	case jobLost:
		message := fmt.Sprintf("Job %s is gone, the backup is not repeated", cluster.JobName)
		failOn(cluster, api.BackupReasonJobLost, message)
		out.add(c.failed(chb, cluster))
		return ctrl.Result{}, nil
	case jobActive:
		c.run(chb, cluster, job, out)
		return ctrl.Result{RequeueAfter: runningRequeueInterval}, nil
	case jobFailed:
		c.run(chb, cluster, job, out)
		reason := failureReason(job)
		failure, err := c.describeFailure(ctx, job, reason)
		if err != nil {
			return ctrl.Result{}, err
		}
		failOn(cluster, reason, failure.message)
		out.add(c.failed(chb, cluster))
		return ctrl.Result{}, nil
	case jobSucceeded:
	default:
		return ctrl.Result{}, fmt.Errorf("%w: %d", errUnexpectedJobState, state)
	}

	c.run(chb, cluster, job, out)
	return ctrl.Result{}, c.complete(ctx, chb, cluster, job, out)
}

// start creates the backup Job of a cluster once the installation has completed its reconcile, no
// other operation runs on the cluster and no restore of the access entities of the installation is
// unfinished, see accessRestoreGate. The readiness gate applies only here: once the Job exists it
// is tracked whatever the installation does meanwhile. A cluster the installation no longer has, or
// no longer backs up, fails the backup there, as does one whose Job the starting deadline passes before.
func (c *BackupController) start(
	ctx context.Context,
	chb *api.ClickHouseBackup,
	cluster *api.OperationClusterStatus,
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
	job := model.BuildBackupJob(chb, target.chi, target.topology)
	gate := c.accessRestoreGate(chb.GetNamespace(), chb.TargetInstallation())
	result, err := c.startJob(ctx, chb, chb.TargetInstallation(), cluster.Name, job, gate, func(reason, message string) {
		waitOn(cluster, reason, message)
	}, func() {
		c.run(chb, cluster, job, out)
	})
	if (err == nil) && (cluster.Phase == api.OperationPhasePending) {
		result.RequeueAfter = untilStartingDeadline(chb, now, result.RequeueAfter)
	}
	return result, err
}

// run records the running backup Job of a cluster, announcing the start of the backup there once.
func (c *BackupController) run(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, job *batch.Job, out *announcements) {
	if !runOn(cluster, job) {
		return
	}
	namespace, chiName, clusterName := chb.GetNamespace(), chb.TargetInstallation(), cluster.Name
	out.add(c.info(chb, a.EventActionCreate, api.BackupReasonJobCreated, "backup %s of cluster %s started in Job %s",
		chb.Status.BackupName, clusterName, job.GetName()))
	out.add(func() { metrics.BackupStarted(context.Background(), namespace, chiName, clusterName) })
}

// complete records the completion of the backup Job of a cluster, announcing it once. A backup that
// holds no data there, as its Job tells, completed all the same, which is warned about: it restores no
// data on the cluster.
func (c *BackupController) complete(
	ctx context.Context,
	chb *api.ClickHouseBackup,
	cluster *api.OperationClusterStatus,
	job *batch.Job,
	out *announcements,
) error {
	withoutData, err := c.backupHoldsNoData(ctx, job)
	switch {
	case err != nil:
		return err
	case withoutData:
		noData := noDataCause(job)
		if completeOn(cluster, job, api.BackupReasonCompletedWithoutData,
			fmt.Sprintf("Job %s completed, but the backup holds no data: %s", job.GetName(), noData)) {
			out.add(c.completed(chb, cluster, job, noData))
		}
	case completeOn(cluster, job, api.BackupReasonCompleted, fmt.Sprintf("Job %s completed", job.GetName())):
		out.add(c.completed(chb, cluster, job, ""))
	}
	return nil
}

// completed announces the completion of the backup on a cluster, as a warning when noData tells why
// the backup holds no data there.
func (c *BackupController) completed(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, job *batch.Job, noData string) func() {
	namespace, chiName, clusterName := chb.GetNamespace(), chb.TargetInstallation(), cluster.Name
	duration, completion := jobDuration(job), jobCompletionTime(job)
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

// verify runs the Jobs that verify a completed backup on each of its clusters. Verification is
// advisory: the backup is in the storage either way, so the backup completes whatever
// verification concludes, with the conclusion in its Verified condition.
func (c *BackupController) verify(ctx context.Context, chb *api.ClickHouseBackup, out *announcements) (ctrl.Result, error) {
	targets := c.newTargetResolver(ctx, chb.GetNamespace(), chb.TargetInstallation(), false)
	var result ctrl.Result
	for i := range chb.Status.Clusters {
		cluster := &chb.Status.Clusters[i]
		if verificationConcluded(chb, cluster) {
			continue
		}
		clusterResult, err := c.verifyCluster(ctx, chb, cluster, targets, out)
		if err != nil {
			return ctrl.Result{}, err
		}
		result = shorterResult(result, clusterResult)
	}
	c.concludeVerify(chb)
	return result, nil
}

// verifyCluster runs the Job that verifies the backup on one of its clusters.
func (c *BackupController) verifyCluster(
	ctx context.Context,
	chb *api.ClickHouseBackup,
	cluster *api.OperationClusterStatus,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	name := model.VerifyJobName(chb, cluster.Name)
	job, state, err := c.observeJob(ctx, chb, name, cluster.JobName == name)
	if err != nil {
		return ctrl.Result{}, err
	}

	switch state {
	case jobMissing:
		return c.startVerify(ctx, chb, cluster, targets, out)
	case jobStale:
		cluster.Reason, cluster.Message = api.BackupReasonWaitingForGarbageCollection, staleOwnerMessage(kindJob, job.GetName())
		return ctrl.Result{RequeueAfter: pendingRequeueInterval}, nil
	case jobConflict:
		c.concludeVerifyOn(chb, cluster, api.BackupReasonNameConflict, takenNameMessage(kindJob, job), out)
		return ctrl.Result{}, nil
	case jobLost:
		c.concludeVerifyOn(chb, cluster, api.BackupReasonJobLost, fmt.Sprintf("verify Job %s is gone", name), out)
		return ctrl.Result{}, nil
	case jobActive:
		c.runVerify(chb, cluster, job, out)
		return ctrl.Result{RequeueAfter: runningRequeueInterval}, nil
	case jobFailed:
		c.runVerify(chb, cluster, job, out)
		reason := failureReason(job)
		failure, err := c.describeFailure(ctx, job, reason)
		if err != nil {
			return ctrl.Result{}, err
		}
		if reason == api.BackupReasonJobFailed {
			reason = api.BackupReasonVerificationFailed
		}
		c.concludeVerifyOn(chb, cluster, reason, failure.message, out)
		return ctrl.Result{}, nil
	case jobSucceeded:
	default:
		return ctrl.Result{}, fmt.Errorf("%w: %d", errUnexpectedJobState, state)
	}

	c.runVerify(chb, cluster, job, out)
	c.concludeVerifyOn(chb, cluster, api.BackupReasonVerified, fmt.Sprintf("Job %s verified the backup", job.GetName()), out)
	return ctrl.Result{}, nil
}

func (c *BackupController) startVerify(
	ctx context.Context,
	chb *api.ClickHouseBackup,
	cluster *api.OperationClusterStatus,
	targets *targetResolver,
	out *announcements,
) (ctrl.Result, error) {
	target, wait, err := targets.forCluster(cluster.Name)
	switch {
	case err != nil:
		return ctrl.Result{}, err
	case (wait != nil) && wait.refusesOneOff():
		c.concludeVerifyOn(chb, cluster, wait.reason, wait.message, out)
		return ctrl.Result{}, nil
	case wait != nil:
		cluster.Reason, cluster.Message = wait.reason, wait.message
		return ctrl.Result{RequeueAfter: wait.requeue}, nil
	}

	job := model.BuildVerifyJob(chb, target.chi, target.topology)
	gate := c.accessRestoreGate(chb.GetNamespace(), chb.TargetInstallation())
	return c.startJob(ctx, chb, chb.TargetInstallation(), cluster.Name, job, gate, func(reason, message string) {
		cluster.Reason, cluster.Message = reason, message
	}, func() {
		c.runVerify(chb, cluster, job, out)
	})
}

// runVerify records the running verify Job of a cluster, announcing the start of verification there
// once.
func (c *BackupController) runVerify(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, job *batch.Job, out *announcements) {
	if cluster.JobName == job.GetName() {
		return
	}
	cluster.JobName = job.GetName()
	cluster.Reason, cluster.Message = api.BackupReasonVerifying, fmt.Sprintf("Job %s verifies the backup", job.GetName())
	out.add(c.info(chb, a.EventActionCreate, api.BackupReasonVerifying, "backup %s of cluster %s is verified by Job %s",
		chb.Status.BackupName, cluster.Name, job.GetName()))
}

// concludeVerifyOn records the outcome of verification on a cluster.
func (c *BackupController) concludeVerifyOn(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus, reason, message string, out *announcements) {
	cluster.JobName = model.VerifyJobName(chb, cluster.Name)
	cluster.Reason, cluster.Message = reason, message
	if reason == api.BackupReasonVerified {
		out.add(c.info(chb, a.EventActionReconcile, reason, "backup %s of cluster %s verified", chb.Status.BackupName, cluster.Name))
		return
	}
	namespace, chiName, clusterName := chb.GetNamespace(), chb.TargetInstallation(), cluster.Name
	announce := c.warning(chb, a.EventActionReconcile, reason, "backup %s of cluster %s verification failed: %s",
		chb.Status.BackupName, clusterName, message)
	out.add(func() {
		announce()
		metrics.VerificationFailed(context.Background(), namespace, chiName, clusterName)
	})
}

// concludeVerify adds the verification of the clusters up into the Verified condition, and completes
// the backup once every cluster concluded.
func (c *BackupController) concludeVerify(chb *api.ClickHouseBackup) {
	concluded := true
	var running *api.OperationClusterStatus
	var failed []*api.OperationClusterStatus
	var messages []string
	for i := range chb.Status.Clusters {
		cluster := &chb.Status.Clusters[i]
		switch {
		case !verificationConcluded(chb, cluster):
			concluded = false
			// A cluster still telling how its backup went has neither a verify Job nor a reason to wait for one
			if (running == nil) && !backupCompletion(cluster.Reason) {
				running = cluster
			}
		case cluster.Reason != api.BackupReasonVerified:
			failed = append(failed, cluster)
			messages = append(messages, clusterMessage(len(chb.Status.Clusters), cluster.Name, cluster.Message))
		default:
			messages = append(messages, clusterMessage(len(chb.Status.Clusters), cluster.Name, cluster.Message))
		}
	}
	switch {
	case !concluded:
		if running != nil {
			c.setVerified(chb, meta.ConditionUnknown, running.Reason, clusterMessage(len(chb.Status.Clusters), running.Name, running.Message))
		}
		return
	case len(failed) > 0:
		c.setVerified(chb, meta.ConditionFalse, failed[0].Reason, strings.Join(messages, "; "))
	default:
		c.setVerified(chb, meta.ConditionTrue, api.BackupReasonVerified, strings.Join(messages, "; "))
	}
	chb.Status.Phase = api.OperationPhaseCompleted
}

func (c *BackupController) setVerified(chb *api.ClickHouseBackup, status meta.ConditionStatus, reason, message string) {
	setCondition(&chb.Status.Conditions, chb.GetGeneration(), api.BackupConditionVerified, status, reason, message)
}

// backupCompletion reports whether a reason of a cluster tells how the backup went there.
func backupCompletion(reason string) bool {
	return (reason == api.BackupReasonCompleted) || (reason == api.BackupReasonCompletedWithoutData)
}

// verificationConcluded reports whether verification concluded on a cluster: its verify Job was
// created, as the cluster records it, and finished or is gone.
func verificationConcluded(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus) bool {
	return (cluster.JobName == model.VerifyJobName(chb, cluster.Name)) && (cluster.Reason != api.BackupReasonVerifying)
}

// shorterResult returns the result that comes back sooner.
func shorterResult(x, y ctrl.Result) ctrl.Result {
	if x.Requeue || y.Requeue {
		return ctrl.Result{Requeue: true}
	}
	return ctrl.Result{RequeueAfter: shorterDelay(x.RequeueAfter, y.RequeueAfter)}
}
