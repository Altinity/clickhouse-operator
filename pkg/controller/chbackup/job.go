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
	"fmt"
	"slices"
	"sync"
	"time"
	"unicode/utf8"

	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"
	kubeSchema "k8s.io/apimachinery/pkg/runtime/schema"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	log "github.com/altinity/clickhouse-operator/pkg/announcer"
	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// errUnexpectedJobState is returned for a Job state an operation does not expect at that point, which
// only a change to the states themselves can bring about.
var errUnexpectedJobState = errors.New("unexpected Job state")

// jobState is what an operation's Job tells about the operation.
type jobState int

const (
	// jobMissing - the Job has not been created yet.
	jobMissing jobState = iota
	// jobLost - the Job was created, as the status records, and is gone.
	jobLost
	// jobStale - a Job of that name awaits garbage collection, see awaitsGarbageCollection.
	jobStale
	// jobConflict - a Job of that name stays, see awaitsGarbageCollection: no garbage collection removes
	// it.
	jobConflict
	// jobActive - the Job runs, or its pod may still start within jobStartTimeout.
	jobActive
	// jobNotStarted - no pod of the Job was seen started within jobStartTimeout, or the operator
	// stopped the Job for that. Only stateOf tells it: settle confirms it, stops the Job, and
	// reports it failed.
	jobNotStarted
	// jobSucceeded - the Job completed.
	jobSucceeded
	// jobFailed - the Job failed, or was stopped because no pod of it started.
	jobFailed
)

// stateOf returns the state of a Job the operation owns, as far as the Job alone tells it.
func stateOf(job *batch.Job) jobState {
	switch {
	case jobConditionTrue(job, batch.JobComplete):
		return jobSucceeded
	case jobConditionTrue(job, batch.JobFailed):
		return jobFailed
	case jobStopped(job) || jobStartTimedOut(job):
		return jobNotStarted
	default:
		return jobActive
	}
}

// jobStartTimedOut reports whether the Job has gone jobStartTimeout since its creation without a
// pod that started.
func jobStartTimedOut(job *batch.Job) bool {
	return !jobPodStarted(job) && (untilStartTimeout(job) <= 0)
}

// untilStartTimeout returns the time left until a Job without a started pod times out; never for
// a Job whose creation time is not known yet.
func untilStartTimeout(job *batch.Job) time.Duration {
	created := job.GetCreationTimestamp()
	if created.IsZero() {
		return jobStartTimeout
	}
	return time.Until(created.Add(jobStartTimeout))
}

// jobPodStarted reports whether a pod of the Job runs or ran, as recorded or as its pod counts tell.
func jobPodStarted(job *batch.Job) bool {
	return jobStartRecorded(job) || jobPodCountsStarted(job)
}

// jobPodCountsStarted reports whether the pod counts of the Job tell a pod that runs or ran. Ready
// counts the running pods where the cluster reports it; Active counts pending pods as well, so it
// tells nothing, and where Ready is not reported the pods themselves are asked once the start times
// out. Ready falls back to zero for a pod that stops being ready, on a node gone NotReady or behind
// a flapping probe, which is why a start is recorded once seen.
func jobPodCountsStarted(job *batch.Job) bool {
	return (ptr.Deref(job.Status.Ready, 0) > 0) || (job.Status.Succeeded > 0) || (job.Status.Failed > 0)
}

// jobStartRecorded reports whether the operator recorded a started pod on the Job.
func jobStartRecorded(job *batch.Job) bool {
	_, recorded := job.GetAnnotations()[model.AnnotationJobStarted]
	return recorded
}

// jobStopped reports whether the operator stopped the Job because no pod of it started.
func jobStopped(job *batch.Job) bool {
	_, stopped := job.GetAnnotations()[model.AnnotationJobNotStarted]
	return stopped
}

// jobInProgress reports whether a Job may still run an operation: it has neither finished nor been
// stopped. A Job past its start timeout that is not stopped yet counts, as its pending pod may
// still start.
func jobInProgress(job *batch.Job) bool {
	return !jobConditionTrue(job, batch.JobComplete) && !jobConditionTrue(job, batch.JobFailed) && !jobStopped(job)
}

// settle completes what the Job alone tells about its state and returns that state, never
// jobNotStarted. A start, once seen, is recorded on the Job, so that it never looks as if it had
// not started later on. A Job past its start timeout without a recorded start is checked against
// its pods, which may have started unseen, before it is stopped; a stopped Job is reported failed,
// with failureReason telling why.
//
// A conflict means the Job changed since it was read, its pod may have just started, so the Job is
// settled again on its live state.
func (r *reconciler) settle(ctx context.Context, owner client.Object, job *batch.Job) (jobState, error) {
	state, err := r.settleOnce(ctx, owner, job)
	if !apiErrors.IsConflict(err) {
		return state, err
	}
	live := &batch.Job{}
	if err := r.apiReader.Get(ctx, client.ObjectKeyFromObject(job), live); err != nil {
		return state, err
	}
	*job = *live
	return r.settleOnce(ctx, owner, job)
}

func (r *reconciler) settleOnce(ctx context.Context, owner client.Object, job *batch.Job) (jobState, error) {
	state := stateOf(job)
	switch {
	case state == jobActive:
		if !jobStartRecorded(job) && jobPodCountsStarted(job) {
			r.recordStart(ctx, owner, job)
		}
		return jobActive, nil
	case state != jobNotStarted:
		return state, nil
	case jobStopped(job):
		return jobFailed, nil
	}

	started, err := r.podStarted(ctx, job)
	switch {
	case err != nil:
		return state, err
	case started:
		r.recordStart(ctx, owner, job)
		return jobActive, nil
	}
	return jobFailed, r.stopJob(ctx, owner, job)
}

// podStarted reports whether a pod of the Job runs or ran, as its pods tell: the pod counts of the
// Job may have fallen back before any reconcile saw them, as for a pod that started while the
// operator was down. The pods are read past the cache, which holds none.
func (r *reconciler) podStarted(ctx context.Context, job *batch.Job) (bool, error) {
	pods, err := r.jobPods(ctx, job)
	if err != nil {
		return false, err
	}
	for i := range pods {
		if model.JobContainerStarted(&pods[i]) {
			return true, nil
		}
	}
	return false, nil
}

// jobPods lists the pods of a Job past the cache, which holds none.
func (r *reconciler) jobPods(ctx context.Context, job *batch.Job) ([]core.Pod, error) {
	if job.Spec.Selector == nil {
		return nil, nil
	}
	selector, err := meta.LabelSelectorAsSelector(job.Spec.Selector)
	if err != nil {
		return nil, err
	}
	// An empty selector would pick every pod of the namespace
	if selector.Empty() {
		return nil, nil
	}
	pods := &core.PodList{}
	if err := r.apiReader.List(ctx, pods, client.InNamespace(job.GetNamespace()), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// jobFailure describes a failed Job.
type jobFailure struct {
	message string
	// unchanged tells that the Job failed before it changed anything
	unchanged bool
}

// describeFailure describes a failed Job. A Job that failed by itself tells why: its script writes
// the error to the termination message of its pod, which the status takes over, as the Job, its
// pod and its log may all be gone by the time anybody looks. The exit codes of its pods tell whether
// it failed before it changed anything: only a Job that has pods, every one of which exited so,
// changed nothing for sure.
func (r *reconciler) describeFailure(ctx context.Context, job *batch.Job, reason string) (jobFailure, error) {
	failure := jobFailure{message: failureMessage(job, reason)}
	if reason != api.BackupReasonJobFailed {
		return failure, nil
	}
	pods, err := r.jobPods(ctx, job)
	if err != nil {
		return jobFailure{}, err
	}
	described := false
	failure.unchanged = len(pods) > 0
	for i := range pods {
		if message := model.JobTerminationMessage(&pods[i]); (message != "") && !described {
			failure.message = fmt.Sprintf("Job %s failed: %s", job.GetName(), truncateMessage(message))
			described = true
		}
		failure.unchanged = failure.unchanged && model.JobFailedUnchanged(&pods[i])
	}
	return failure, nil
}

// backupHoldsNoData reports whether a succeeded backup Job made a backup that holds no data on any
// host, which its script tells in the termination message of its pod, kept after its log is gone.
// A Job whose pods are gone tells nothing.
func (r *reconciler) backupHoldsNoData(ctx context.Context, job *batch.Job) (bool, error) {
	pods, err := r.jobPods(ctx, job)
	if err != nil {
		return false, err
	}
	for i := range pods {
		if model.JobBackupHoldsNoData(&pods[i]) {
			return true, nil
		}
	}
	return false, nil
}

// noDataCause tells why the backup a Job made holds no data, by the table pattern and the partitions
// the Job backed up.
func noDataCause(job *batch.Job) string {
	data := "data"
	if partitions := model.JobPartitions(job); partitions != "" {
		data = fmt.Sprintf("data in the partitions %q", partitions)
	}
	if tables := model.JobTables(job); tables != "" {
		return fmt.Sprintf("the tables pattern %q matches no table, or none of the tables it matches holds %s", tables, data)
	}
	return "there is no table to back up, or none of the tables holds " + data
}

// truncateMessage bounds a termination message for a condition and an event. It keeps the start,
// which tells what failed in an error the Job script wrote, and the end, which holds the error of
// a log tail.
func truncateMessage(message string) string {
	if len(message) <= terminationMessageMaxLength {
		return message
	}
	head := message[:terminationMessageHeadLength]
	for (head != "") && !utf8.RuneStart(message[len(head)]) {
		head = head[:len(head)-1]
	}
	tail := message[len(message)-(terminationMessageMaxLength-terminationMessageHeadLength):]
	for (tail != "") && !utf8.RuneStart(tail[0]) {
		tail = tail[1:]
	}
	return head + truncationMarker + tail
}

// recordStart records on the Job that a pod of it started. The Job is known to be active either way,
// so a record that fails only waits for the next reconcile to be written: the operation goes on.
func (r *reconciler) recordStart(ctx context.Context, owner client.Object, job *batch.Job) {
	base := job.DeepCopy()
	meta.SetMetaDataAnnotation(&job.ObjectMeta, model.AnnotationJobStarted, meta.Now().UTC().Format(time.RFC3339))
	if err := r.patchJob(ctx, owner, job, client.MergeFrom(base)); err != nil {
		log.V(1).M(r.meta(owner)).F().Warning("unable to record the start of Job %s: %v", job.GetName(), err)
	}
}

// markJobStopped prepares a Job whose pod never started to be stopped. A Job past its active
// deadline is failed by the Job controller, which deletes its pods: a pod that would start late
// cannot run an operation already reported failed, and a CronJob that forbids concurrent runs gets
// to start the next one.
func markJobStopped(job *batch.Job) {
	meta.SetMetaDataAnnotation(&job.ObjectMeta, model.AnnotationJobNotStarted, jobStartTimeout.String())
	job.Spec.ActiveDeadlineSeconds = ptr.To(jobNotStartedDeadlineSeconds)
}

// stopJob stops a Job whose pod never started. The stop applies only to the Job as it was read:
// one that changed meanwhile, possibly because its pod has just started, is reported as a conflict.
func (r *reconciler) stopJob(ctx context.Context, owner client.Object, job *batch.Job) error {
	base := job.DeepCopy()
	markJobStopped(job)
	return r.patchJob(ctx, owner, job, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{}))
}

// patchJob patches a Job the operation owns. A Job that is gone, or a cluster that refuses the
// change, leaves the Job to its own fate: the operation is reported on what is known.
func (r *reconciler) patchJob(ctx context.Context, owner client.Object, job *batch.Job, patch client.Patch) error {
	err := r.Patch(ctx, job, patch)
	switch {
	case apiErrors.IsNotFound(err):
		return nil
	case apiErrors.IsInvalid(err):
		log.V(1).M(r.meta(owner)).F().Warning("unable to patch Job %s: %v", job.GetName(), err)
		return nil
	}
	return err
}

func jobConditionTrue(job *batch.Job, conditionType batch.JobConditionType) bool {
	_, ok := jobCondition(job, conditionType)
	return ok
}

// jobCondition returns the condition of the given type a Job holds true.
func jobCondition(job *batch.Job, conditionType batch.JobConditionType) (batch.JobCondition, bool) {
	for _, condition := range job.Status.Conditions {
		if (condition.Type == conditionType) && (condition.Status == core.ConditionTrue) {
			return condition, true
		}
	}
	return batch.JobCondition{}, false
}

// failureReason returns the condition reason that describes why a failed Job failed.
func failureReason(job *batch.Job) string {
	if jobStopped(job) {
		return api.BackupReasonJobNotStarted
	}
	if condition, ok := jobCondition(job, batch.JobFailed); ok && (condition.Reason == batch.JobReasonDeadlineExceeded) {
		return api.BackupReasonDeadlineExceeded
	}
	return api.BackupReasonJobFailed
}

// jobStartTime returns when the Job was created, which is when its operation started.
func jobStartTime(job *batch.Job) meta.Time {
	creation := job.GetCreationTimestamp()
	if creation.IsZero() {
		return meta.Now()
	}
	return creation
}

// jobCompletionTime returns when the Job finished.
func jobCompletionTime(job *batch.Job) meta.Time {
	if job.Status.CompletionTime != nil {
		return *job.Status.CompletionTime
	}
	return meta.Now()
}

// jobFailureTime returns when a failed Job failed: when the operator stopped it because no pod of it
// started, or when the Job controller marked it failed.
func jobFailureTime(job *batch.Job) meta.Time {
	if jobStopped(job) {
		return meta.NewTime(jobStartTime(job).Add(jobStartTimeout))
	}
	if condition, ok := jobCondition(job, batch.JobFailed); ok && !condition.LastTransitionTime.IsZero() {
		return condition.LastTransitionTime
	}
	return meta.Now()
}

// jobDuration returns how long a finished Job ran.
func jobDuration(job *batch.Job) time.Duration {
	return jobCompletionTime(job).Sub(jobStartTime(job).Time)
}

// observeJob finds the Job of the given name an operation runs in. recorded tells whether the operation's
// status already names the Job, which turns its absence from "not created yet" into "lost".
func (r *reconciler) observeJob(ctx context.Context, owner client.Object, name string, recorded bool) (*batch.Job, jobState, error) {
	job := &batch.Job{}
	err := r.getConfirmed(ctx, kubeTypes.NamespacedName{Namespace: owner.GetNamespace(), Name: name}, job)
	switch {
	case apiErrors.IsNotFound(err) && recorded:
		return nil, jobLost, nil
	case apiErrors.IsNotFound(err):
		return nil, jobMissing, nil
	case err != nil:
		return nil, jobMissing, err
	case !meta.IsControlledBy(job, owner):
		stale, err := r.awaitsGarbageCollection(ctx, owner, job)
		switch {
		case err != nil:
			return nil, jobMissing, err
		case stale:
			return job, jobStale, nil
		}
		return job, jobConflict, nil
	}
	state, err := r.settle(ctx, owner, job)
	return job, state, err
}

// awaitsGarbageCollection reports whether an object of the name owner needs, which owner does not control,
// awaits garbage collection: its controller is gone, was replaced by another resource of its name, as a
// deleted predecessor of the owner is by the owner, or is being deleted; or the object itself is being
// deleted. Any other object stays: one another live resource controls, typically one whose name joined with
// its cluster gives the same name, and one no resource controls, as none controls what a deletion that
// orphaned its dependents left behind, or what somebody created by hand.
//
// The controller is read past the cache, by its kind, whatever that is. One that cannot be read, of a kind
// the operator may not read or the API server does not serve, is taken as live: nothing tells that it goes
// away.
func (r *reconciler) awaitsGarbageCollection(ctx context.Context, owner, obj client.Object) (bool, error) {
	controller := meta.GetControllerOf(obj)
	switch {
	case obj.GetDeletionTimestamp() != nil:
		return true, nil
	case controller == nil:
		return false, nil
	}
	controllerObject := &meta.PartialObjectMetadata{}
	controllerObject.SetGroupVersionKind(kubeSchema.FromAPIVersionAndKind(controller.APIVersion, controller.Kind))
	err := r.apiReader.Get(ctx, kubeTypes.NamespacedName{Namespace: obj.GetNamespace(), Name: controller.Name}, controllerObject)
	switch {
	case apiErrors.IsNotFound(err):
		return true, nil
	case apiMeta.IsNoMatchError(err) || apiErrors.IsForbidden(err):
		log.V(1).M(r.meta(owner)).F().Info("unable to read %s %s, which controls %s: %v", controller.Kind, controller.Name, obj.GetName(), err)
		return false, nil
	case err != nil:
		return false, err
	}
	return (controllerObject.GetUID() != controller.UID) || (controllerObject.GetDeletionTimestamp() != nil), nil
}

// takenNameMessage explains why an operation cannot run on a cluster: the object of the name it needs
// there, which stays, belongs to another resource, or to none.
func takenNameMessage(kind string, obj meta.Object) string {
	if meta.GetControllerOf(obj) == nil {
		return orphanMessage(kind, obj.GetName())
	}
	return nameConflictMessage(kind, obj.GetName(), objectOwner(obj))
}

// objectOwner names the resource that controls an object in a message, with the cluster of the
// installation its labels name.
func objectOwner(obj meta.Object) string {
	labels := obj.GetLabels()
	owner := unknownOwner
	if controller := meta.GetControllerOf(obj); controller != nil {
		owner = controller.Kind + " " + controller.Name
	}
	if (labels[model.LabelBackupCluster] != "") && (labels[model.LabelBackupCHI] != "") {
		owner = fmt.Sprintf("%s on cluster %s of %s %s", owner, labels[model.LabelBackupCluster],
			api.ClickHouseInstallationCRDResourceKind, labels[model.LabelBackupCHI])
	}
	return owner
}

// clusterJob is the Job an operation runs in on one of its clusters, as observeClusterJobs finds it.
type clusterJob struct {
	cluster *api.OperationClusterStatus
	job     *batch.Job
	state   jobState
}

// observeClusterJobs finds the Jobs of the clusters an operation has not finished on, named by
// jobName, before any of them is acted on: a Job that exists tells what the operation works on.
func (r *reconciler) observeClusterJobs(
	ctx context.Context,
	owner client.Object,
	clusters []api.OperationClusterStatus,
	jobName func(cluster string) string,
) ([]clusterJob, error) {
	var jobs []clusterJob
	for i := range clusters {
		cluster := &clusters[i]
		if cluster.Phase.IsTerminal() {
			continue
		}
		job, state, err := r.observeJob(ctx, owner, jobName(cluster.Name), cluster.JobName != "")
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, clusterJob{cluster: cluster, job: job, state: state})
	}
	return jobs, nil
}

// jobsBackupName returns the backup the first of the Jobs an operation owns works on, "" when there
// is none: the backup the operation works on on every cluster.
func jobsBackupName(jobs []clusterJob) string {
	for _, job := range jobs {
		switch job.state {
		case jobActive, jobFailed, jobSucceeded:
			if name := model.JobBackupName(job.job); name != "" {
				return name
			}
		}
	}
	return ""
}

// startGate holds the start of the Job of an operation while check tells why it waits. A gate that
// reads the state of other clusters of the installation names them, so that it runs under their locks
// as well as under the one of the cluster the Job runs on.
type startGate struct {
	// clusters are the clusters of the installation the gate reads, whose locks it runs under too
	clusters []string
	// sources are the clusters of another installation the gate reads, named by operationLockKey, whose locks it
	// runs under too
	sources []string
	check   func(context.Context) (*pending, error)
}

// lockedClusters returns the clusters a gate reads, besides the one the Job runs on, nil-safe.
func (g *startGate) lockedClusters() []string {
	if g == nil {
		return nil
	}
	return g.clusters
}

// lockedSources returns the lock keys of the clusters of other installations a gate reads, nil-safe.
func (g *startGate) lockedSources() []string {
	if g == nil {
		return nil
	}
	return g.sources
}

// createJob creates the Job of an operation on a cluster of owner's installation unless gate, when
// given, tells why the operation has to wait, or another operation on that cluster is in progress,
// and reports whether it did.
//
// Two operations on one cluster must never overlap: a restore drops what a backup reads, and both
// drive the same sidecars. The gate, the check and the create run under the cluster's lock, and the
// locks of the clusters the gate reads, and the check reads past the cache, so neither a concurrent
// reconcile in this operator nor a Job the cache has not seen yet can slip in between. Operations on
// different clusters drive different sidecars and run alongside.
func (r *reconciler) createJob(
	ctx context.Context,
	owner client.Object,
	chiName string,
	cluster string,
	job *batch.Job,
	gate *startGate,
) (bool, *pending, error) {
	unlock := lockKeys(append(operationLockKeys(owner.GetNamespace(), chiName, append([]string{cluster}, gate.lockedClusters()...)...),
		gate.lockedSources()...)...)
	defer unlock()

	if gate != nil {
		if blocked, err := gate.check(ctx); (err != nil) || (blocked != nil) {
			return false, blocked, err
		}
	}
	busy, err := r.operationInProgress(ctx, owner.GetNamespace(), chiName, cluster)
	if (err != nil) || busy {
		return false, nil, err
	}
	if err := ctrl.SetControllerReference(owner, job, r.scheme); err != nil {
		return false, nil, err
	}
	if err := r.Create(ctx, job); err != nil {
		return false, nil, err
	}
	return true, nil, nil
}

// startJob creates the Job of an operation on a cluster of owner's installation, as createJob does,
// and records the outcome: run records the Job created, wait why the operation waits for the gate or
// for another one on the cluster to finish. A Job of that name created meanwhile is found by the next
// reconcile.
func (r *reconciler) startJob(
	ctx context.Context,
	owner client.Object,
	chiName string,
	cluster string,
	job *batch.Job,
	gate *startGate,
	wait func(reason, message string),
	run func(),
) (ctrl.Result, error) {
	created, blocked, err := r.createJob(ctx, owner, chiName, cluster, job, gate)
	switch {
	case apiErrors.IsAlreadyExists(err):
		return ctrl.Result{Requeue: true}, nil
	case err != nil:
		return ctrl.Result{}, err
	case blocked != nil:
		wait(blocked.reason, blocked.message)
		return ctrl.Result{RequeueAfter: blocked.requeue}, nil
	case !created:
		wait(api.BackupReasonOperationInProgress, operationInProgressMessage(chiName, cluster))
		return ctrl.Result{RequeueAfter: pendingRequeueInterval}, nil
	}

	log.V(1).M(r.meta(owner)).F().Info("created Job %s", job.GetName())
	run()
	return ctrl.Result{RequeueAfter: runningRequeueInterval}, nil
}

// operationInProgress reports whether any backup, verify or restore Job of a cluster of the
// installation, scheduled ones included, is still running.
func (r *reconciler) operationInProgress(ctx context.Context, namespace, chiName, cluster string) (bool, error) {
	return r.activeJobs(ctx, r.apiReader, namespace, model.ClusterJobsSelector(chiName, cluster, ""))
}

// activeJobs reports whether any Job the selector picks may still run an operation.
func (r *reconciler) activeJobs(ctx context.Context, reader client.Reader, namespace string, selector k8sLabels.Selector) (bool, error) {
	jobs, err := r.activeJobsOf(ctx, reader, namespace, selector)
	return len(jobs) > 0, err
}

// activeJobsOf returns the Jobs the selector picks that may still run an operation.
func (r *reconciler) activeJobsOf(ctx context.Context, reader client.Reader, namespace string, selector k8sLabels.Selector) ([]*batch.Job, error) {
	jobs := &batch.JobList{}
	if err := reader.List(ctx, jobs, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
		return nil, err
	}
	var active []*batch.Job
	for i := range jobs.Items {
		if jobInProgress(&jobs.Items[i]) {
			active = append(active, &jobs.Items[i])
		}
	}
	return active, nil
}

// lockOperations locks the start of operations on the given clusters of an installation, and returns
// the unlock. Clusters are locked in sorted order, each once, so that two callers locking overlapping
// sets never wait for each other in a circle.
func lockOperations(namespace, chiName string, clusters ...string) func() {
	return lockKeys(operationLockKeys(namespace, chiName, clusters...)...)
}

// operationLockKeys returns the keys operationLocks locks the given clusters of an installation by.
func operationLockKeys(namespace, chiName string, clusters ...string) []string {
	keys := make([]string, 0, len(clusters))
	for _, cluster := range clusters {
		keys = append(keys, operationLockKey(namespace, chiName, cluster))
	}
	return keys
}

// lockKeys locks the given keys of operationLocks, of the clusters of one or more installations, and returns the
// unlock. Keys are locked in sorted order, each once, so that two callers locking overlapping sets never wait for
// each other in a circle.
func lockKeys(keys ...string) func() {
	keys = slices.Clone(keys)
	slices.Sort(keys)
	keys = slices.Compact(keys)
	unlocks := make([]func(), 0, len(keys))
	for _, key := range keys {
		unlocks = append(unlocks, operationLocks.lock(key))
	}
	return func() {
		for i := len(unlocks) - 1; i >= 0; i-- {
			unlocks[i]()
		}
	}
}

// operationLockKey is the key operationLocks locks a cluster of an installation by.
func operationLockKey(namespace, chiName, cluster string) string {
	return kubeTypes.NamespacedName{Namespace: namespace, Name: chiName}.String() + operationLockClusterSeparator + cluster
}

// operationLocks serializes the start of operations per cluster of an installation across every
// backup controller of the process.
var operationLocks = newKeyedLocks()

// keyedLocks keeps a lock for every key some goroutine holds or waits for, and drops it with the
// last of them, so installations come and go without the map growing.
type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	sync.Mutex
	// users counts the goroutines holding or waiting for the lock, guarded by keyedLocks.mu.
	users int
}

func newKeyedLocks() *keyedLocks {
	return &keyedLocks{locks: make(map[string]*keyedLock)}
}

// lock locks key and returns its unlock.
func (k *keyedLocks) lock(key string) func() {
	k.mu.Lock()
	l, ok := k.locks[key]
	if !ok {
		l = &keyedLock{}
		k.locks[key] = l
	}
	l.users++
	k.mu.Unlock()

	l.Lock()
	return func() {
		l.Unlock()

		k.mu.Lock()
		defer k.mu.Unlock()
		l.users--
		if l.users == 0 {
			delete(k.locks, key)
		}
	}
}
