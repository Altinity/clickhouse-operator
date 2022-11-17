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
	"sort"

	"github.com/altinity/clickhouse-operator/pkg/util"
)

// ChiStatus defines status section of ClickHouseInstallation resource
type ChiStatus struct {
	CHOpVersion            string                  `json:"chop-version,omitempty"        yaml:"chop-version,omitempty"`
	CHOpCommit             string                  `json:"chop-commit,omitempty"         yaml:"chop-commit,omitempty"`
	CHOpDate               string                  `json:"chop-date,omitempty"           yaml:"chop-date,omitempty"`
	CHOpIP                 string                  `json:"chop-ip,omitempty"             yaml:"chop-ip,omitempty"`
	ClustersCount          int                     `json:"clusters,omitempty"            yaml:"clusters,omitempty"`
	ShardsCount            int                     `json:"shards,omitempty"              yaml:"shards,omitempty"`
	ReplicasCount          int                     `json:"replicas,omitempty"            yaml:"replicas,omitempty"`
	HostsCount             int                     `json:"hosts,omitempty"               yaml:"hosts,omitempty"`
	Status                 string                  `json:"status,omitempty"              yaml:"status,omitempty"`
	TaskID                 string                  `json:"taskID,omitempty"              yaml:"taskID,omitempty"`
	TaskIDsStarted         []string                `json:"taskIDsStarted,omitempty"      yaml:"taskIDsStarted,omitempty"`
	TaskIDsCompleted       []string                `json:"taskIDsCompleted,omitempty"    yaml:"taskIDsCompleted,omitempty"`
	Action                 string                  `json:"action,omitempty"              yaml:"action,omitempty"`
	Actions                []string                `json:"actions,omitempty"             yaml:"actions,omitempty"`
	Error                  string                  `json:"error,omitempty"               yaml:"error,omitempty"`
	Errors                 []string                `json:"errors,omitempty"              yaml:"errors,omitempty"`
	UpdatedHostsCount      int                     `json:"updated,omitempty"             yaml:"updated,omitempty"`
	AddedHostsCount        int                     `json:"added,omitempty"               yaml:"added,omitempty"`
	DeletedHostsCount      int                     `json:"deleted,omitempty"             yaml:"deleted,omitempty"`
	DeleteHostsCount       int                     `json:"delete,omitempty"              yaml:"delete,omitempty"`
	Pods                   []string                `json:"pods,omitempty"                yaml:"pods,omitempty"`
	PodIPs                 []string                `json:"pod-ips,omitempty"             yaml:"pod-ips,omitempty"`
	FQDNs                  []string                `json:"fqdns,omitempty"               yaml:"fqdns,omitempty"`
	Endpoint               string                  `json:"endpoint,omitempty"            yaml:"endpoint,omitempty"`
	NormalizedCHI          *ClickHouseInstallation `json:"normalized,omitempty"          yaml:"normalized,omitempty"`
	NormalizedCHICompleted *ClickHouseInstallation `json:"normalizedCompleted,omitempty" yaml:"normalizedCompleted,omitempty"`
}

const (
	maxActions = 10
	maxErrors  = 10
	maxTaskIDs = 10
)

// PushAction pushes action into status
func (s *ChiStatus) PushAction(action string) {
	if s == nil {
		return
	}
	s.Actions = append([]string{action}, s.Actions...)
	s.TrimActions()
}

// TripActions trims actions
func (s *ChiStatus) TrimActions() {
	if s == nil {
		return
	}
	if len(s.Actions) > maxActions {
		s.Actions = s.Actions[:maxActions]
	}
}

// PushError sets and pushes error into status
func (s *ChiStatus) PushError(error string) {
	if s == nil {
		return
	}
	s.Errors = append([]string{error}, s.Errors...)
	if len(s.Errors) > maxErrors {
		s.Errors = s.Errors[:maxErrors]
	}
}

// SetAndPushError sets and pushes error into status
func (s *ChiStatus) SetAndPushError(error string) {
	if s == nil {
		return
	}
	s.Error = error
	s.Errors = append([]string{error}, s.Errors...)
	if len(s.Errors) > maxErrors {
		s.Errors = s.Errors[:maxErrors]
	}
}

// PushTaskIDStarted pushes task id into status
func (s *ChiStatus) PushTaskIDStarted() {
	if s == nil {
		return
	}
	s.TaskIDsStarted = append([]string{s.TaskID}, s.TaskIDsStarted...)
	if len(s.TaskIDsStarted) > maxTaskIDs {
		s.TaskIDsStarted = s.TaskIDsStarted[:maxTaskIDs]
	}
}

// PushTaskIDCompleted pushes task id into status
func (s *ChiStatus) PushTaskIDCompleted() {
	if s == nil {
		return
	}
	s.TaskIDsCompleted = append([]string{s.TaskID}, s.TaskIDsCompleted...)
	if len(s.TaskIDsCompleted) > maxTaskIDs {
		s.TaskIDsCompleted = s.TaskIDsCompleted[:maxTaskIDs]
	}
}

// ReconcileStart marks reconcile start
func (s *ChiStatus) ReconcileStart(DeleteHostsCount int) {
	if s == nil {
		return
	}
	s.Status = StatusInProgress
	s.UpdatedHostsCount = 0
	s.AddedHostsCount = 0
	s.DeletedHostsCount = 0
	s.DeleteHostsCount = DeleteHostsCount
	s.PushTaskIDStarted()
}

// ReconcileComplete marks reconcile completion
func (s *ChiStatus) ReconcileComplete() {
	if s == nil {
		return
	}
	s.Status = StatusCompleted
	s.Action = ""
	s.PushTaskIDCompleted()
}

// DeleteStart marks deletion start
func (s *ChiStatus) DeleteStart() {
	if s == nil {
		return
	}
	s.Status = StatusTerminating
	s.UpdatedHostsCount = 0
	s.AddedHostsCount = 0
	s.DeletedHostsCount = 0
	s.DeleteHostsCount = 0
	s.PushTaskIDStarted()
}

type CopyCHIStatusOptions struct {
	Actions     bool
	Errors      bool
	Normalized  bool
	MainFields  bool
	WholeStatus bool
}

func (s *ChiStatus) MergeActions(from *ChiStatus) {
	if s == nil {
		return
	}
	if from == nil {
		return
	}
	s.Actions = util.MergeStringArrays(s.Actions, from.Actions)
	sort.Sort(sort.Reverse(sort.StringSlice(s.Actions)))
	s.TrimActions()
}

func (s *ChiStatus) CopyFrom(from *ChiStatus, opts CopyCHIStatusOptions) {
	if s == nil {
		return
	}

	if from == nil {
		return
	}

	if opts.Actions {
		s.Action = from.Action
		s.MergeActions(from)
	}

	if opts.Errors {
		s.Error = from.Error
		s.Errors = util.MergeStringArrays(s.Errors, from.Errors)
		sort.Sort(sort.Reverse(sort.StringSlice(s.Errors)))
	}

	if opts.MainFields {
		s.CHOpVersion = from.CHOpVersion
		s.CHOpCommit = from.CHOpCommit
		s.CHOpDate = from.CHOpDate
		s.CHOpIP = from.CHOpIP
		s.ClustersCount = from.ClustersCount
		s.ShardsCount = from.ShardsCount
		s.ReplicasCount = from.ReplicasCount
		s.HostsCount = from.HostsCount
		s.Status = from.Status
		s.TaskID = from.TaskID
		s.TaskIDsStarted = from.TaskIDsStarted
		s.TaskIDsCompleted = from.TaskIDsCompleted
		s.Action = from.Action
		s.MergeActions(from)
		s.Error = from.Error
		s.Errors = from.Errors
		s.UpdatedHostsCount = from.UpdatedHostsCount
		s.AddedHostsCount = from.AddedHostsCount
		s.DeletedHostsCount = from.DeletedHostsCount
		s.DeleteHostsCount = from.DeleteHostsCount
		s.Pods = from.Pods
		s.PodIPs = from.PodIPs
		s.FQDNs = from.FQDNs
		s.Endpoint = from.Endpoint
		s.NormalizedCHI = from.NormalizedCHI

	}

	if opts.Normalized {
		s.NormalizedCHI = from.NormalizedCHI
	}

	if opts.WholeStatus {
		s.CHOpVersion = from.CHOpVersion
		s.CHOpCommit = from.CHOpCommit
		s.CHOpDate = from.CHOpDate
		s.CHOpIP = from.CHOpIP
		s.ClustersCount = from.ClustersCount
		s.ShardsCount = from.ShardsCount
		s.ReplicasCount = from.ReplicasCount
		s.HostsCount = from.HostsCount
		s.Status = from.Status
		s.TaskID = from.TaskID
		s.TaskIDsStarted = from.TaskIDsStarted
		s.TaskIDsCompleted = from.TaskIDsCompleted
		s.Action = from.Action
		s.MergeActions(from)
		s.Error = from.Error
		s.Errors = from.Errors
		s.UpdatedHostsCount = from.UpdatedHostsCount
		s.AddedHostsCount = from.AddedHostsCount
		s.DeletedHostsCount = from.DeletedHostsCount
		s.DeleteHostsCount = from.DeleteHostsCount
		s.Pods = from.Pods
		s.PodIPs = from.PodIPs
		s.FQDNs = from.FQDNs
		s.Endpoint = from.Endpoint
		s.NormalizedCHI = from.NormalizedCHI
		s.NormalizedCHICompleted = from.NormalizedCHICompleted
	}
}

func (s *ChiStatus) GetFQDNs() []string {
	if s == nil {
		return nil
	}
	return s.FQDNs
}

func (s *ChiStatus) GetCHOpIP() string {
	if s == nil {
		return ""
	}
	return s.CHOpIP
}

func (s *ChiStatus) GetNormalizedCHICompleted() *ClickHouseInstallation {
	if s == nil {
		return nil
	}
	return s.NormalizedCHICompleted
}

func (s *ChiStatus) GetNormalizedCHI() *ClickHouseInstallation {
	if s == nil {
		return nil
	}
	return s.NormalizedCHI
}

func (s *ChiStatus) GetStatus() string {
	if s == nil {
		return ""
	}
	return s.Status
}

func (s *ChiStatus) GetPods() []string {
	if s == nil {
		return nil
	}
	return s.Pods
}

func (s *ChiStatus) GetPodIPS() []string {
	if s == nil {
		return nil
	}
	return s.PodIPs
}
