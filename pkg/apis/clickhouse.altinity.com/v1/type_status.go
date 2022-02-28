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

// ChiStatus defines status section of ClickHouseInstallation resource
type ChiStatus struct {
	CHOpVersion       string                  `json:"chop-version,omitempty"     yaml:"chop-version,omitempty"`
	CHOpCommit        string                  `json:"chop-commit,omitempty"      yaml:"chop-commit,omitempty"`
	CHOpDate          string                  `json:"chop-date,omitempty"        yaml:"chop-date,omitempty"`
	ClustersCount     int                     `json:"clusters"                   yaml:"clusters"`
	ShardsCount       int                     `json:"shards"                     yaml:"shards"`
	ReplicasCount     int                     `json:"replicas"                   yaml:"replicas"`
	HostsCount        int                     `json:"hosts"                      yaml:"hosts"`
	Status            string                  `json:"status"                     yaml:"status"`
	TaskID            string                  `json:"taskID,omitempty"           yaml:"taskID,omitempty"`
	TaskIDsStarted    []string                `json:"taskIDsStarted,omitempty"   yaml:"taskIDsStarted,omitempty"`
	TaskIDsCompleted  []string                `json:"taskIDsCompleted,omitempty" yaml:"taskIDsCompleted,omitempty"`
	Action            string                  `json:"action,omitempty"           yaml:"action,omitempty"`
	Actions           []string                `json:"actions,omitempty"          yaml:"actions,omitempty"`
	Error             string                  `json:"error,omitempty"            yaml:"error,omitempty"`
	Errors            []string                `json:"errors,omitempty"           yaml:"errors,omitempty"`
	UpdatedHostsCount int                     `json:"updated,omitempty"          yaml:"updated,omitempty"`
	AddedHostsCount   int                     `json:"added,omitempty"            yaml:"added,omitempty"`
	DeletedHostsCount int                     `json:"deleted,omitempty"          yaml:"deleted,omitempty"`
	DeleteHostsCount  int                     `json:"delete,omitempty"           yaml:"delete,omitempty"`
	Pods              []string                `json:"pods,omitempty"             yaml:"pods,omitempty"`
	FQDNs             []string                `json:"fqdns,omitempty"            yaml:"fqdns,omitempty"`
	Endpoint          string                  `json:"endpoint,omitempty"         yaml:"endpoint,omitempty"`
	Generation        int64                   `json:"generation,omitempty"       yaml:"generation,omitempty"`
	NormalizedCHI     *ClickHouseInstallation `json:"normalized,omitempty"       yaml:"normalized,omitempty"`
}

const (
	maxActions = 100
	maxErrors  = 100
	maxTaskIDs = 100
)

// PushAction pushes action into status
func (s *ChiStatus) PushAction(action string) {
	s.Actions = append([]string{action}, s.Actions...)
	if len(s.Actions) > maxActions {
		s.Actions = s.Actions[:maxActions]
	}
}

// SetAndPushError sets and pushes error into status
func (s *ChiStatus) SetAndPushError(error string) {
	s.Error = error
	s.Errors = append([]string{error}, s.Errors...)
	if len(s.Errors) > maxErrors {
		s.Errors = s.Errors[:maxErrors]
	}
}

// PushTaskIDStarted pushes task id into status
func (s *ChiStatus) PushTaskIDStarted() {
	s.TaskIDsStarted = append([]string{s.TaskID}, s.TaskIDsStarted...)
	if len(s.TaskIDsStarted) > maxTaskIDs {
		s.TaskIDsStarted = s.TaskIDsStarted[:maxTaskIDs]
	}
}

// PushTaskIDCompleted pushes task id into status
func (s *ChiStatus) PushTaskIDCompleted() {
	s.TaskIDsCompleted = append([]string{s.TaskID}, s.TaskIDsCompleted...)
	if len(s.TaskIDsCompleted) > maxTaskIDs {
		s.TaskIDsCompleted = s.TaskIDsCompleted[:maxTaskIDs]
	}
}

// ReconcileStart marks reconcile start
func (s *ChiStatus) ReconcileStart(DeleteHostsCount int) {
	s.Status = StatusInProgress
	s.UpdatedHostsCount = 0
	s.AddedHostsCount = 0
	s.DeletedHostsCount = 0
	s.DeleteHostsCount = DeleteHostsCount
	s.PushTaskIDStarted()
}

// ReconcileComplete marks reconcile completion
func (s *ChiStatus) ReconcileComplete(chi *ClickHouseInstallation) {
	s.Status = StatusCompleted
	s.Action = ""
	s.PushTaskIDCompleted()
	s.Generation = chi.Generation
}

// DeleteStart marks deletion start
func (s *ChiStatus) DeleteStart() {
	s.Status = StatusTerminating
	s.UpdatedHostsCount = 0
	s.AddedHostsCount = 0
	s.DeletedHostsCount = 0
	s.DeleteHostsCount = 0
	s.PushTaskIDStarted()
}
