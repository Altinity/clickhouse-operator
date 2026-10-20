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
	"fmt"
	"strings"

	batch "k8s.io/api/batch/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// operation is a view of the status fields a backup and a restore share, so both walk their
// phases through the same transitions: on each cluster they target, in a Job of its own, and as a
// whole, which the clusters add up to.
type operation struct {
	phase          *api.OperationPhase
	clusters       *[]api.OperationClusterStatus
	startTime      **meta.Time
	completionTime **meta.Time
	duration       *int64
	conditions     *[]meta.Condition
	generation     int64
	// cluster records the cluster specCluster names when the operation is validated, nil for an
	// operation that keeps no record of it
	cluster     *string
	specCluster string
}

func newBackupOperation(chb *api.ClickHouseBackup) *operation {
	return &operation{
		phase:          &chb.Status.Phase,
		clusters:       &chb.Status.Clusters,
		startTime:      &chb.Status.StartTime,
		completionTime: &chb.Status.CompletionTime,
		duration:       &chb.Status.DurationSeconds,
		conditions:     &chb.Status.Conditions,
		generation:     chb.GetGeneration(),
	}
}

func newRestoreOperation(chr *api.ClickHouseRestore) *operation {
	return &operation{
		phase:          &chr.Status.Phase,
		clusters:       &chr.Status.Clusters,
		startTime:      &chr.Status.StartTime,
		completionTime: &chr.Status.CompletionTime,
		duration:       &chr.Status.DurationSeconds,
		conditions:     &chr.Status.Conditions,
		generation:     chr.GetGeneration(),
		cluster:        &chr.Status.Cluster,
		specCluster:    chr.Spec.Cluster,
	}
}

func (o *operation) setCondition(conditionType string, status meta.ConditionStatus, reason, message string) {
	setCondition(o.conditions, o.generation, conditionType, status, reason, message)
}

// validated reports whether the operation was validated, which resolved the clusters it targets.
func (o *operation) validated() bool {
	return len(*o.clusters) > 0
}

// pin records the clusters the operation targets once its spec and target passed validation. They
// stay what they are: a cluster added to the installation later is not targeted, and one that leaves
// it fails the operation on that cluster.
func (o *operation) pin(clusters []string) {
	if o.cluster != nil {
		*o.cluster = o.specCluster
	}
	for _, name := range clusters {
		*o.clusters = append(*o.clusters, api.OperationClusterStatus{Name: name, Phase: api.OperationPhasePending})
	}
	*o.phase = api.OperationPhasePending
	o.setCondition(api.BackupConditionValidated, meta.ConditionTrue, api.BackupReasonValidated, messageValidated)
}

// wait records why the operation cannot be validated yet.
func (o *operation) wait(conditionType, reason, message string) {
	*o.phase = api.OperationPhasePending
	o.setCondition(conditionType, meta.ConditionFalse, reason, message)
}

// refuse records the operation as failed for good before it targets any cluster, as its target
// cannot ever run it.
func (o *operation) refuse(reason, message string) {
	o.setCondition(api.BackupConditionValidated, meta.ConditionFalse, reason, message)
	o.fail(reason, message)
}

// fail records the operation as failed, for good: nothing is ever retried.
func (o *operation) fail(reason, message string) {
	*o.phase = api.OperationPhaseFailed
	o.finish(meta.Now())
	o.setCondition(api.BackupConditionReady, meta.ConditionFalse, reason, message)
}

func (o *operation) finish(completion meta.Time) {
	*o.completionTime = &completion
	if *o.startTime != nil {
		*o.duration = int64(completion.Sub((*o.startTime).Time).Seconds())
	}
}

// waitOn records why the operation cannot start its Job on a cluster yet.
func waitOn(cluster *api.OperationClusterStatus, reason, message string) {
	cluster.Phase = api.OperationPhasePending
	cluster.Reason = reason
	cluster.Message = message
}

// runOn records the Job the operation runs in on a cluster and reports whether this starts the
// operation there. A Job found without a recorded name was created by a reconcile whose status
// write was lost.
func runOn(cluster *api.OperationClusterStatus, job *batch.Job) bool {
	started := (cluster.Phase != api.OperationPhaseRunning) || (cluster.JobName == "")
	cluster.Phase = api.OperationPhaseRunning
	cluster.JobName = job.GetName()
	cluster.Hosts = model.JobHosts(job)
	cluster.Storage = model.ObjectStorage(job)
	if cluster.StartTime == nil {
		start := jobStartTime(job)
		cluster.StartTime = &start
	}
	cluster.Reason = api.BackupReasonJobCreated
	cluster.Message = fmt.Sprintf("Job %s created", job.GetName())
	return started
}

// completeOn records the completion of the operation's Job on a cluster with the reason and the
// message that tell how it went, and reports whether it is news.
func completeOn(cluster *api.OperationClusterStatus, job *batch.Job, reason, message string) bool {
	if cluster.CompletionTime != nil {
		return false
	}
	completion := jobCompletionTime(job)
	cluster.Phase = api.OperationPhaseCompleted
	cluster.CompletionTime = &completion
	cluster.Reason = reason
	cluster.Message = message
	return true
}

// failOn records the operation as failed on a cluster, for good.
func failOn(cluster *api.OperationClusterStatus, reason, message string) {
	now := meta.Now()
	cluster.Phase = api.OperationPhaseFailed
	cluster.CompletionTime = &now
	cluster.Reason = reason
	cluster.Message = message
}

// operationOutcome is what the clusters of an operation add up to.
type operationOutcome struct {
	// finished tells that the operation finished on every cluster
	finished  bool
	completed []*api.OperationClusterStatus
	failed    []*api.OperationClusterStatus
}

// succeeded reports whether the operation finished successfully on every cluster.
func (out operationOutcome) succeeded() bool {
	return out.finished && (len(out.failed) == 0)
}

// aggregate adds up the clusters into the phase, the times and the JobCreated condition of the
// operation, and returns the outcome. Its Ready condition, which tells the outcome, is the caller's,
// as only the caller knows what makes a success.
func (o *operation) aggregate() operationOutcome {
	var out operationOutcome
	if !o.validated() {
		return out
	}
	out.finished = true
	started := false
	var waiting *api.OperationClusterStatus
	var jobs []string
	var completion *meta.Time
	for i := range *o.clusters {
		cluster := &(*o.clusters)[i]
		switch cluster.Phase {
		case api.OperationPhaseCompleted:
			out.completed = append(out.completed, cluster)
		case api.OperationPhaseFailed:
			out.failed = append(out.failed, cluster)
		case api.OperationPhasePending:
			// A cluster whose Job was found created meanwhile waits for nothing the condition could tell
			if (waiting == nil) && (cluster.Reason != "") {
				waiting = cluster
			}
		}
		out.finished = out.finished && cluster.Phase.IsTerminal()
		started = started || (cluster.Phase != api.OperationPhasePending)
		if cluster.JobName != "" {
			jobs = append(jobs, cluster.JobName)
		}
		if (cluster.StartTime != nil) && ((*o.startTime == nil) || cluster.StartTime.Before(*o.startTime)) {
			*o.startTime = cluster.StartTime.DeepCopy()
		}
		if later(cluster.CompletionTime, completion) {
			completion = cluster.CompletionTime
		}
	}

	switch {
	case waiting != nil:
		o.setCondition(api.BackupConditionJobCreated, meta.ConditionFalse, waiting.Reason, waiting.Message)
	case len(jobs) > 0:
		o.setCondition(api.BackupConditionJobCreated, meta.ConditionTrue, api.BackupReasonJobCreated, jobsCreatedMessage(jobs))
	}
	switch {
	case out.finished && (*o.completionTime == nil):
		finished := meta.Now()
		if completion != nil {
			finished = *completion.DeepCopy()
		}
		o.finish(finished)
		*o.phase = api.OperationPhaseRunning
		if len(out.failed) > 0 {
			*o.phase = api.OperationPhaseFailed
		}
	case out.finished:
	case started:
		*o.phase = api.OperationPhaseRunning
	default:
		*o.phase = api.OperationPhasePending
	}
	return out
}

// jobsCreatedMessage tells the Jobs an operation created.
func jobsCreatedMessage(jobs []string) string {
	if len(jobs) == 1 {
		return fmt.Sprintf("Job %s created", jobs[0])
	}
	return fmt.Sprintf("Jobs %s created", strings.Join(jobs, ", "))
}

// failureOf returns the reason and the message of the Ready condition of an operation that failed
// on some of its clusters: the reason of the first one, or, when it succeeded on others,
// PartiallyCompleted; the message tells every failed cluster.
func failureOf(out operationOutcome) (string, string) {
	var messages []string
	for _, cluster := range out.failed {
		messages = append(messages, clusterMessage(len(out.completed)+len(out.failed), cluster.Name, cluster.Message))
	}
	message := strings.Join(messages, "; ")
	if len(out.completed) == 0 {
		return out.failed[0].Reason, message
	}
	var completed []string
	for _, cluster := range out.completed {
		completed = append(completed, cluster.Name)
	}
	return api.BackupReasonPartiallyCompleted, fmt.Sprintf("completed on %s; %s", clusterList(completed), message)
}

// clusterMessage prefixes a message about a cluster with the cluster, unless the operation has no
// other cluster to tell it from.
func clusterMessage(clusters int, cluster, message string) string {
	if clusters <= 1 {
		return message
	}
	return fmt.Sprintf("cluster %s: %s", cluster, message)
}

// clusterList names clusters in a message.
func clusterList(clusters []string) string {
	if len(clusters) == 1 {
		return "cluster " + clusters[0]
	}
	return "clusters " + strings.Join(clusters, ", ")
}

// failureMessage describes a failed Job by its reason alone.
func failureMessage(job *batch.Job, reason string) string {
	switch reason {
	case api.BackupReasonDeadlineExceeded:
		return fmt.Sprintf("Job %s exceeded its active deadline", job.GetName())
	case api.BackupReasonJobNotStarted:
		// The pod is gone once the Job is stopped, while the events of both outlive it for a while
		return fmt.Sprintf("no pod of Job %s started within %s, see the events of the Job and its pod: kubectl get events -n %s",
			job.GetName(), jobStartTimeout, job.GetNamespace())
	}
	return fmt.Sprintf("Job %s failed, see its logs", job.GetName())
}
