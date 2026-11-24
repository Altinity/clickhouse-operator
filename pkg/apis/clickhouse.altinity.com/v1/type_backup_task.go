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

const backupTaskIDsMax = 10

// BackupTaskStatus: a new spec.taskID reruns a finished backup or restore, or resets an aborted schedule.
type BackupTaskStatus struct {
	TaskID           string   `json:"taskID,omitempty" yaml:"taskID,omitempty"`
	TaskIDsStarted   []string `json:"taskIDsStarted,omitempty" yaml:"taskIDsStarted,omitempty"`
	TaskIDsCompleted []string `json:"taskIDsCompleted,omitempty" yaml:"taskIDsCompleted,omitempty"`
}

func (s *BackupTaskStatus) Seen(id string) bool {
	if s == nil {
		return false
	}
	return (s.TaskID == id) || slices.Contains(s.TaskIDsStarted, id) || slices.Contains(s.TaskIDsCompleted, id)
}

func (s *BackupTaskStatus) Start(id string) {
	s.TaskID = id
	s.TaskIDsStarted = pushBounded(s.TaskIDsStarted, id)
}

func (s *BackupTaskStatus) Complete() {
	if (s.TaskID == "") || slices.Contains(s.TaskIDsCompleted, s.TaskID) {
		return
	}
	s.TaskIDsCompleted = pushBounded(s.TaskIDsCompleted, s.TaskID)
}

func (s *BackupTaskStatus) Skip(id string) {
	s.TaskIDsCompleted = pushBounded(s.TaskIDsCompleted, id)
}

func pushBounded(list []string, value string) []string {
	list = append([]string{value}, list...)
	if len(list) > backupTaskIDsMax {
		list = list[:backupTaskIDsMax]
	}
	return list
}

// BackupPreviousRun lets a spec.taskID rerun replace the backups, or resume the downloads, of earlier runs.
type BackupPreviousRun struct {
	Cluster string            `json:"cluster" yaml:"cluster"`
	Hosts   []BackupRunHost   `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	Actions []BackupRunAction `json:"actions,omitempty" yaml:"actions,omitempty"`
}

func FindPreviousRun(runs []BackupPreviousRun, cluster string) *BackupPreviousRun {
	for i := range runs {
		if runs[i].Cluster == cluster {
			return &runs[i]
		}
	}
	return nil
}

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
