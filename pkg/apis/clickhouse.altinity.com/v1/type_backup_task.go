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

package v1

import (
	"slices"
)

// backupTaskIDsMax bounds the task ids, and the earlier backup names, a backup resource records, as an installation
// bounds its task ids.
const backupTaskIDsMax = 10

// BackupTaskStatus records the spec.taskID values of a backup, a restore or a schedule, as the status of an
// installation does: TaskID is the one the resource acts on, TaskIDsStarted those it started acting on and
// TaskIDsCompleted those it is done with, newest first, 10 at most each. A value of spec.taskID none of them holds is
// new: it reruns a backup or a restore that finished, and resets a schedule that is aborted.
type BackupTaskStatus struct {
	// TaskID is the spec.taskID the resource acts on.
	TaskID string `json:"taskID,omitempty" yaml:"taskID,omitempty"`
	// TaskIDsStarted are the task ids the resource started acting on, newest first.
	TaskIDsStarted []string `json:"taskIDsStarted,omitempty" yaml:"taskIDsStarted,omitempty"`
	// TaskIDsCompleted are the task ids the resource is done with, newest first: the run each started finished, or the
	// id came while a run was in progress and reran nothing.
	TaskIDsCompleted []string `json:"taskIDsCompleted,omitempty" yaml:"taskIDsCompleted,omitempty"`
}

// Seen reports whether the status records the task id, nil-safe.
func (s *BackupTaskStatus) Seen(id string) bool {
	if s == nil {
		return false
	}
	return (s.TaskID == id) || slices.Contains(s.TaskIDsStarted, id) || slices.Contains(s.TaskIDsCompleted, id)
}

// Start records the task id as the one the resource acts on from now.
func (s *BackupTaskStatus) Start(id string) {
	s.TaskID = id
	s.TaskIDsStarted = pushBounded(s.TaskIDsStarted, id)
}

// Complete records the task id the resource acts on as done, once.
func (s *BackupTaskStatus) Complete() {
	if (s.TaskID == "") || slices.Contains(s.TaskIDsCompleted, s.TaskID) {
		return
	}
	s.TaskIDsCompleted = pushBounded(s.TaskIDsCompleted, s.TaskID)
}

// Skip records a task id the resource does nothing for as done, leaving the one it acts on.
func (s *BackupTaskStatus) Skip(id string) {
	s.TaskIDsCompleted = pushBounded(s.TaskIDsCompleted, id)
}

// pushBounded puts the value first in the list, which keeps backupTaskIDsMax values at most.
func pushBounded(list []string, value string) []string {
	list = append([]string{value}, list...)
	if len(list) > backupTaskIDsMax {
		list = list[:backupTaskIDsMax]
	}
	return list
}

// BackupPreviousRun is what the runs of a backup or a restore on a cluster did before spec.taskID reran it there, which
// the rerun takes for its own: a backup replaces the copies of its backup they left, a restore resumes the downloads
// they made.
type BackupPreviousRun struct {
	// Cluster is the name of the cluster.
	Cluster string `json:"cluster" yaml:"cluster"`
	// Hosts are the hosts the runs worked on, with the name of the backup on each.
	Hosts []BackupRunHost `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	// Actions are the commands of the runs that changed what the hosts keep: the creates of a backup, the downloads of a
	// restore.
	Actions []BackupRunAction `json:"actions,omitempty" yaml:"actions,omitempty"`
}

// FindPreviousRun returns what the runs on the cluster did before a rerun, nil for nothing.
func FindPreviousRun(runs []BackupPreviousRun, cluster string) *BackupPreviousRun {
	for i := range runs {
		if runs[i].Cluster == cluster {
			return &runs[i]
		}
	}
	return nil
}

// MergePreviousRun adds to what the runs on the cluster did before a rerun the hosts of a run and those of its actions
// keep tells, and returns the runs.
func MergePreviousRun(runs []BackupPreviousRun, cluster string, run *BackupRunStatus, keep func(action BackupRunAction) bool) []BackupPreviousRun {
	if run == nil {
		return runs
	}
	previous := FindPreviousRun(runs, cluster)
	if previous == nil {
		runs = append(runs, BackupPreviousRun{Cluster: cluster})
		previous = &runs[len(runs)-1]
	}
	for _, host := range run.Hosts {
		if !slices.Contains(previous.Hosts, host) {
			previous.Hosts = append(previous.Hosts, host)
		}
	}
	for _, action := range run.Actions {
		if !keep(action) {
			continue
		}
		if !slices.ContainsFunc(previous.Actions, func(kept BackupRunAction) bool {
			return (kept.Step == action.Step) && (kept.Host == action.Host) && (kept.Command == action.Command)
		}) {
			previous.Actions = append(previous.Actions, action)
		}
	}
	return runs
}
