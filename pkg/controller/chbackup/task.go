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

func newTaskID(id *types.Id, task *api.BackupTaskStatus) string {
	if value := id.Value(); (value != "") && !task.Seen(value) {
		return value
	}
	return ""
}

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

// The old backup stays in storage until retention or a manual delete.
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

func (c *BackupController) rerunUnfinished(chb *api.ClickHouseBackup, id string, now meta.Time, out *announcements) {
	clusters := rerun(newBackupOperation(chb), &chb.Status.PreviousRuns, createdBackup,
		func(cluster *api.OperationClusterStatus) (*api.BackupRunStatus, runKey) {
			return runOfBackup(chb, cluster), runKeyOf(chb, api.ClickHouseBackupCRDResourceKind, cluster.Name, api.BackupRunOperationBackup)
		})
	chb.Status.RerunTime = &now
	if len(clusters) == 0 {
		out.add(c.info(chb, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns the backup, refused before it "+
			"targeted any cluster: it is validated again", id))
		return
	}
	out.add(c.info(chb, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns backup %s under its name on %s, "+
		"replacing the copies of it the runs there left", id, chb.Status.BackupName, clusterList(clusters)))
}

// A run that recorded no hosts made the backup only where it sent a command. A backup of that name on the other
// hosts belongs to another backup.
func runOfBackup(chb *api.ClickHouseBackup, cluster *api.OperationClusterStatus) *api.BackupRunStatus {
	run := cluster.Run
	if (run == nil) || (len(run.Hosts) > 0) {
		return run
	}
	run = run.DeepCopy()
	run.Hosts = hostsOfInsertedCommands(chb, cluster, run)
	return run
}

// rerun sends every cluster the operation did not complete back to Pending, and returns their names. The previous
// runs keep the actions of the ended run that the rerun needs. The run gives the ended run of a cluster and its key.
func rerun(op *operation, previous *[]api.BackupPreviousRun, keep func(api.BackupRunAction) bool,
	run func(*api.OperationClusterStatus) (*api.BackupRunStatus, runKey)) []string {
	var clusters []string
	for i := range *op.clusters {
		cluster := &(*op.clusters)[i]
		if cluster.Phase == api.OperationPhaseCompleted {
			continue
		}
		ended, key := run(cluster)
		*previous = api.MergePreviousRun(*previous, cluster.Name, ended, keep)
		// Forget the ended run, so the rerun does not treat it as its own.
		runs.forget(key)
		*cluster = api.OperationClusterStatus{Name: cluster.Name, Phase: api.OperationPhasePending}
		clusters = append(clusters, cluster.Name)
	}
	restartOperation(op)
	return clusters
}

// A backup rerun replaces the backups the earlier run created.
func createdBackup(action api.BackupRunAction) bool {
	return (action.Step == stepCreate) && (action.TimeFrom != "")
}

// A restore rerun resumes the downloads the earlier run started.
func startedDownload(action api.BackupRunAction) bool {
	return strings.HasPrefix(action.Command, commandDownload+" ") && (action.TimeFrom != "")
}

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

func previousActions(runs []api.BackupPreviousRun, cluster string) []api.BackupRunAction {
	if previous := api.FindPreviousRun(runs, cluster); previous != nil {
		return previous.Actions
	}
	return nil
}

// A completed restore is not rerun, because that would drop and recreate its tables.
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
	clusters := rerun(newRestoreOperation(chr), &chr.Status.PreviousRuns, startedDownload,
		func(cluster *api.OperationClusterStatus) (*api.BackupRunStatus, runKey) {
			return cluster.Run, runKeyOf(chr, api.ClickHouseRestoreCRDResourceKind, cluster.Name, api.BackupRunOperationRestore)
		})
	if len(clusters) == 0 {
		out.add(c.info(chr, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns the restore of backup %s, refused "+
			"before it targeted any cluster: it is validated again", id, restoredBackupName(chr)))
		return
	}
	out.add(c.info(chr, a.EventActionReconcile, reasonTaskIDRerun, "spec.taskID %s reruns the restore of backup %s on %s, "+
		"resuming the downloads the runs there made", id, restoredBackupName(chr), clusterList(clusters)))
}

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
