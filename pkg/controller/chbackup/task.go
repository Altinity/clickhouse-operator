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
	"strings"
	"time"

	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	a "github.com/altinity/clickhouse-operator/pkg/controller/common/announcer"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// spec.taskID reruns a backup or a restore that finished, and resets a schedule that is aborted, when it changes to a
// value the status of the resource does not record, as status.taskID and status.taskIDsStarted and taskIDsCompleted
// record the task ids of an installation. The task id a resource is created with, or given before it first finished,
// is the one of its first run. One given while a backup or a restore runs reruns nothing: it is recorded done at once,
// with a TaskIDIgnored warning, as the outcome of the run it would follow is not known yet.

// newTaskID returns the spec.taskID the status does not record, "" for none.
func newTaskID(id *types.Id, task *api.BackupTaskStatus) string {
	if value := id.Value(); (value != "") && !task.Seen(value) {
		return value
	}
	return ""
}

// applyTaskID acts on a new spec.taskID of a backup: a backup that finished is rerun, see rerunCompleted and
// rerunUnfinished.
func (c *BackupController) applyTaskID(chb *api.ClickHouseBackup, out *announcements) {
	task := &chb.Status.BackupTaskStatus
	id := newTaskID(chb.Spec.TaskID, task)
	switch {
	case id == "":
		return
	case !chb.Status.Phase.IsTerminal() && (task.TaskID == ""):
		task.Start(id)
		return
	case !chb.Status.Phase.IsTerminal():
		task.Skip(id)
		out.add(c.warning(chb, a.EventActionReconcile, reasonTaskIDIgnored, "spec.taskID %s reruns nothing: the backup has not "+
			"finished; set another spec.taskID once it finished", id))
		return
	case (chb.Status.Phase == api.OperationPhaseCompleted) && (chb.Spec.BackupName != "") &&
		(model.ResolveBackupName(chb) == chb.Status.BackupName):
		task.Skip(id)
		out.add(c.warning(chb, a.EventActionReconcile, reasonTaskIDIgnored, "spec.taskID %s reruns nothing: backup %s completed "+
			"under spec.backupName, which a new backup cannot take; set another spec.backupName with another spec.taskID", id,
			chb.Status.BackupName))
		return
	}
	task.Start(id)
	now := meta.NewTime(c.clock.Now().Truncate(time.Second))
	if chb.Status.Phase == api.OperationPhaseCompleted {
		c.rerunCompleted(chb, id, now, out)
		return
	}
	c.rerunUnfinished(chb, id, now, out)
}

// rerunCompleted makes a completed backup make a new backup: it is validated again, its clusters and its name resolved
// anew, at the time of the rerun, which names the backup. The backup it made stays in the storage, which retention or a
// delete by hand removes, and status.previousBackupNames records it.
func (c *BackupController) rerunCompleted(chb *api.ClickHouseBackup, id string, now meta.Time, out *announcements) {
	previous := chb.Status.BackupName
	for _, cluster := range chb.Status.Clusters {
		runs.forget(runKeyOf(chb, api.ClickHouseBackupCRDResourceKind, cluster.Name, api.BackupRunOperationBackup))
	}
	status := api.ClickHouseBackupStatus{
		Phase:               api.OperationPhasePending,
		Schedule:            chb.Status.Schedule,
		RerunTime:           &now,
		PreviousBackupNames: chb.Status.PreviousBackupNames,
		BackupTaskStatus:    chb.Status.BackupTaskStatus,
	}
	status.PushPreviousBackupName(previous)
	chb.Status = status
	out.add(c.info(chb, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns the completed backup %s: a new backup, "+
		"named at %s, is made; backup %s stays in the storage, which retention or a delete by hand removes", id, previous,
		now.UTC().Format(time.RFC3339), previous))
}

// rerunUnfinished makes a backup that failed or was aborted make its backup again under its name, on the clusters it
// did not complete on, whose state starts anew: what the runs there did is kept in status.previousRuns, and the rerun
// replaces the copies of the backup they left. A backup refused before it targeted any cluster is validated again.
func (c *BackupController) rerunUnfinished(chb *api.ClickHouseBackup, id string, now meta.Time, out *announcements) {
	var clusters []string
	for i := range chb.Status.Clusters {
		cluster := &chb.Status.Clusters[i]
		if cluster.Phase == api.OperationPhaseCompleted {
			continue
		}
		chb.Status.PreviousRuns = api.MergePreviousRun(chb.Status.PreviousRuns, cluster.Name, runOfBackup(chb, cluster),
			func(action api.BackupRunAction) bool {
				return (action.Step == stepCreate) && (action.TimeFrom != "")
			})
		// The run that ended there, if the operator still keeps it, is no run of the rerun
		runs.forget(runKeyOf(chb, api.ClickHouseBackupCRDResourceKind, cluster.Name, api.BackupRunOperationBackup))
		*cluster = api.OperationClusterStatus{Name: cluster.Name, Phase: api.OperationPhasePending}
		clusters = append(clusters, cluster.Name)
	}
	restartOperation(newBackupOperation(chb))
	chb.Status.RerunTime = &now
	if len(clusters) == 0 {
		out.add(c.info(chb, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns the backup, refused before it "+
			"targeted any cluster: it is validated again", id))
		return
	}
	out.add(c.info(chb, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns backup %s under its name on %s, "+
		"replacing the copies of it the runs there left", id, chb.Status.BackupName, clusterList(clusters)))
}

// runOfBackup returns the run of a backup on a cluster as it ended, with the hosts the backup was made on there: those
// the run tells, else those the cluster recorded when the run started, under the name of the backup.
func runOfBackup(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus) *api.BackupRunStatus {
	run := cluster.Run
	if (run == nil) || (len(run.Hosts) > 0) || (chb.Status.BackupName == "") {
		return run
	}
	run = run.DeepCopy()
	for _, host := range cluster.Hosts {
		run.Hosts = append(run.Hosts, api.BackupRunHost{Host: host, Backup: chb.Status.BackupName})
	}
	return run
}

// restartOperation starts an operation again on the clusters a rerun reset: it is pending, its times are those of the
// clusters it keeps, and the Ready condition goes; one that targeted no cluster yet loses every condition, as it is
// validated again.
func restartOperation(op *operation) {
	*op.phase = api.OperationPhasePending
	*op.startTime = nil
	*op.completionTime = nil
	*op.duration = 0
	if !op.validated() {
		*op.conditions = nil
		return
	}
	apiMeta.RemoveStatusCondition(op.conditions, api.BackupConditionReady)
	for _, cluster := range *op.clusters {
		if (cluster.StartTime != nil) && ((*op.startTime == nil) || cluster.StartTime.Before(*op.startTime)) {
			*op.startTime = cluster.StartTime.DeepCopy()
		}
	}
}

// previousActions returns the commands the runs of a resource on a cluster inserted before spec.taskID reran it there.
func previousActions(runs []api.BackupPreviousRun, cluster string) []api.BackupRunAction {
	if previous := api.FindPreviousRun(runs, cluster); previous != nil {
		return previous.Actions
	}
	return nil
}

// applyTaskID acts on a new spec.taskID of a restore: one that failed or was aborted runs again on the clusters it did
// not complete on, resuming the downloads it made there, which status.previousRuns keeps. A completed restore is not
// rerun: a restore again would drop and recreate the tables it restored, which a new ClickHouseRestore asks for
// explicitly.
func (c *RestoreController) applyTaskID(chr *api.ClickHouseRestore, out *announcements) {
	task := &chr.Status.BackupTaskStatus
	id := newTaskID(chr.Spec.TaskID, task)
	switch {
	case id == "":
		return
	case !chr.Status.Phase.IsTerminal() && (task.TaskID == ""):
		task.Start(id)
		return
	case !chr.Status.Phase.IsTerminal():
		task.Skip(id)
		out.add(c.warning(chr, a.EventActionReconcile, reasonTaskIDIgnored, "spec.taskID %s reruns nothing: the restore has not "+
			"finished; set another spec.taskID once it finished", id))
		return
	case chr.Status.Phase == api.OperationPhaseCompleted:
		task.Skip(id)
		out.add(c.warning(chr, a.EventActionReconcile, reasonTaskIDIgnored, "spec.taskID %s reruns nothing: the restore of backup "+
			"%s completed, and a restore again would drop and recreate what it restored; create a new ClickHouseRestore for that",
			id, restoredBackupName(chr)))
		return
	}
	task.Start(id)
	var clusters []string
	for i := range chr.Status.Clusters {
		cluster := &chr.Status.Clusters[i]
		if cluster.Phase == api.OperationPhaseCompleted {
			continue
		}
		chr.Status.PreviousRuns = api.MergePreviousRun(chr.Status.PreviousRuns, cluster.Name, cluster.Run,
			func(action api.BackupRunAction) bool {
				return strings.HasPrefix(action.Command, commandDownload+" ") && (action.TimeFrom != "")
			})
		runs.forget(runKeyOf(chr, api.ClickHouseRestoreCRDResourceKind, cluster.Name, api.BackupRunOperationRestore))
		*cluster = api.OperationClusterStatus{Name: cluster.Name, Phase: api.OperationPhasePending}
		clusters = append(clusters, cluster.Name)
	}
	restartOperation(newRestoreOperation(chr))
	if len(clusters) == 0 {
		out.add(c.info(chr, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns the restore of backup %s, refused "+
			"before it targeted any cluster: it is validated again", id, restoredBackupName(chr)))
		return
	}
	out.add(c.info(chr, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns the restore of backup %s on %s, "+
		"resuming the downloads the runs there made", id, restoredBackupName(chr), clusterList(clusters)))
}

// applyTaskID acts on a new spec.taskID of a schedule: one that is aborted is reset, its Validated, Ready and phase
// dropped, so that the reconcile validates it again at once and tells its phase anew, an abort that holds still
// included. It runs no slot by itself; a schedule aborted as its last run failed stays so until a run succeeds.
func (c *ScheduleController) applyTaskID(chbs *api.ClickHouseBackupSchedule, out *announcements) {
	task := &chbs.Status.BackupTaskStatus
	id := newTaskID(chbs.Spec.TaskID, task)
	if id == "" {
		return
	}
	task.Start(id)
	if chbs.Status.Phase != api.SchedulePhaseAborted {
		return
	}
	reason := chbs.Status.Reason
	apiMeta.RemoveStatusCondition(&chbs.Status.Conditions, api.BackupConditionValidated)
	apiMeta.RemoveStatusCondition(&chbs.Status.Conditions, api.BackupConditionReady)
	chbs.Status.Phase, chbs.Status.Reason, chbs.Status.Message = "", "", ""
	out.add(c.info(chbs, a.EventActionUpdate, reasonTaskIDReset, "spec.taskID %s resets the schedule, aborted (%s): it is "+
		"validated again", id, reason))
}
