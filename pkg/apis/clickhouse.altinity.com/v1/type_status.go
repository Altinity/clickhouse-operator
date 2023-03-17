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
	CHOpVersion            string                  `json:"chop-version,omitempty"           yaml:"chop-version,omitempty"`
	CHOpCommit             string                  `json:"chop-commit,omitempty"            yaml:"chop-commit,omitempty"`
	CHOpDate               string                  `json:"chop-date,omitempty"              yaml:"chop-date,omitempty"`
	CHOpIP                 string                  `json:"chop-ip,omitempty"                yaml:"chop-ip,omitempty"`
	ClustersCount          int                     `json:"clusters,omitempty"               yaml:"clusters,omitempty"`
	ShardsCount            int                     `json:"shards,omitempty"                 yaml:"shards,omitempty"`
	ReplicasCount          int                     `json:"replicas,omitempty"               yaml:"replicas,omitempty"`
	HostsCount             int                     `json:"hosts,omitempty"                  yaml:"hosts,omitempty"`
	Status                 string                  `json:"status,omitempty"                 yaml:"status,omitempty"`
	TaskID                 string                  `json:"taskID,omitempty"                 yaml:"taskID,omitempty"`
	TaskIDsStarted         []string                `json:"taskIDsStarted,omitempty"         yaml:"taskIDsStarted,omitempty"`
	TaskIDsCompleted       []string                `json:"taskIDsCompleted,omitempty"       yaml:"taskIDsCompleted,omitempty"`
	Action                 string                  `json:"action,omitempty"                 yaml:"action,omitempty"`
	Actions                []string                `json:"actions,omitempty"                yaml:"actions,omitempty"`
	Error                  string                  `json:"error,omitempty"                  yaml:"error,omitempty"`
	Errors                 []string                `json:"errors,omitempty"                 yaml:"errors,omitempty"`
	HostsUpdatedCount      int                     `json:"hostsUpdated,omitempty"           yaml:"hostsUpdated,omitempty"`
	HostsAddedCount        int                     `json:"hostsAdded,omitempty"             yaml:"hostsAdded,omitempty"`
	HostsUnchangedCount    int                     `json:"hostsUnchanged,omitempty"         yaml:"hostsUnchanged,omitempty"`
	HostsFailedCount       int                     `json:"hostsFailed,omitempty"            yaml:"hostsFailed,omitempty"`
	HostsCompletedCount    int                     `json:"hostsCompleted,omitempty"         yaml:"hostsCompleted,omitempty"`
	HostsDeletedCount      int                     `json:"hostsDeleted,omitempty"           yaml:"hostsDeleted,omitempty"`
	HostsDeleteCount       int                     `json:"hostsDelete,omitempty"            yaml:"hostsDelete,omitempty"`
	Pods                   []string                `json:"pods,omitempty"                   yaml:"pods,omitempty"`
	PodIPs                 []string                `json:"pod-ips,omitempty"                yaml:"pod-ips,omitempty"`
	FQDNs                  []string                `json:"fqdns,omitempty"                  yaml:"fqdns,omitempty"`
	Endpoint               string                  `json:"endpoint,omitempty"               yaml:"endpoint,omitempty"`
	NormalizedCHI          *ClickHouseInstallation `json:"normalized,omitempty"             yaml:"normalized,omitempty"`
	NormalizedCHICompleted *ClickHouseInstallation `json:"normalizedCompleted,omitempty"    yaml:"normalizedCompleted,omitempty"`
	HostsWithTablesCreated []string                `json:"hostsWithTablesCreated,omitempty" yaml:"hostsWithTablesCreated,omitempty"`
}

const (
	maxActions = 10
	maxErrors  = 10
	maxTaskIDs = 10
)

// PushHostTablesCreated pushes host to the list of hosts with created tables
func (s *ChiStatus) PushHostTablesCreated(host string) {
	if s == nil {
		return
	}
	if util.InArray(host, s.HostsWithTablesCreated) {
		return
	}

	s.HostsWithTablesCreated = append(s.HostsWithTablesCreated, host)
}

// SyncHostTablesCreated syncs list of hosts with tables created with actual list of hosts
func (s *ChiStatus) SyncHostTablesCreated() {
	if s == nil {
		return
	}
	if s.FQDNs == nil {
		return
	}
	s.HostsWithTablesCreated = util.IntersectStringArrays(s.HostsWithTablesCreated, s.FQDNs)
}

// PushAction pushes action into status
func (s *ChiStatus) PushAction(action string) {
	if s == nil {
		return
	}
	s.Actions = append([]string{action}, s.Actions...)
	s.TrimActions()
}

// TrimActions trims actions
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
	s.HostsUpdatedCount = 0
	s.HostsAddedCount = 0
	s.HostsCompletedCount = 0
	s.HostsDeletedCount = 0
	s.HostsDeleteCount = DeleteHostsCount
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
	s.HostsUpdatedCount = 0
	s.HostsAddedCount = 0
	s.HostsCompletedCount = 0
	s.HostsDeletedCount = 0
	s.HostsDeleteCount = 0
	s.PushTaskIDStarted()
}

// CopyCHIStatusOptions specifies what to copy in CHI status options
type CopyCHIStatusOptions struct {
	Actions           bool
	Errors            bool
	Normalized        bool
	MainFields        bool
	WholeStatus       bool
	InheritableFields bool
}

// MergeActions merges actions
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

// CopyFrom copies
func (s *ChiStatus) CopyFrom(from *ChiStatus, opts CopyCHIStatusOptions) {
	if s == nil {
		return
	}

	if from == nil {
		return
	}

	if opts.InheritableFields {
		s.TaskIDsStarted = from.TaskIDsStarted
		s.TaskIDsCompleted = from.TaskIDsCompleted
		s.Actions = from.Actions
		s.Errors = from.Errors
		s.HostsWithTablesCreated = from.HostsWithTablesCreated
	}

	if opts.Actions {
		s.Action = from.Action
		s.MergeActions(from)
		s.HostsWithTablesCreated = nil
		if len(from.HostsWithTablesCreated) > 0 {
			s.HostsWithTablesCreated = append(s.HostsWithTablesCreated, from.HostsWithTablesCreated...)
		}
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
		s.HostsUpdatedCount = from.HostsUpdatedCount
		s.HostsAddedCount = from.HostsAddedCount
		s.HostsCompletedCount = from.HostsCompletedCount
		s.HostsDeletedCount = from.HostsDeletedCount
		s.HostsDeleteCount = from.HostsDeleteCount
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
		s.HostsUpdatedCount = from.HostsUpdatedCount
		s.HostsAddedCount = from.HostsAddedCount
		s.HostsCompletedCount = from.HostsCompletedCount
		s.HostsDeletedCount = from.HostsDeletedCount
		s.HostsDeleteCount = from.HostsDeleteCount
		s.Pods = from.Pods
		s.PodIPs = from.PodIPs
		s.FQDNs = from.FQDNs
		s.Endpoint = from.Endpoint
		s.NormalizedCHI = from.NormalizedCHI
		s.NormalizedCHICompleted = from.NormalizedCHICompleted
	}
}

// GetFQDNs is a getter
func (s *ChiStatus) GetFQDNs() []string {
	if s == nil {
		return nil
	}
	return s.FQDNs
}

// GetCHOpIP is a getter
func (s *ChiStatus) GetCHOpIP() string {
	if s == nil {
		return ""
	}
	return s.CHOpIP
}

// GetNormalizedCHICompleted is a getter
func (s *ChiStatus) GetNormalizedCHICompleted() *ClickHouseInstallation {
	if s == nil {
		return nil
	}
	return s.NormalizedCHICompleted
}

// HasNormalizedCHICompleted is a checker
func (s *ChiStatus) HasNormalizedCHICompleted() bool {
	return s.GetNormalizedCHICompleted() != nil
}

// GetNormalizedCHI is a getter
func (s *ChiStatus) GetNormalizedCHI() *ClickHouseInstallation {
	if s == nil {
		return nil
	}
	return s.NormalizedCHI
}

// HasNormalizedCHI is a checker
func (s *ChiStatus) HasNormalizedCHI() bool {
	return s.GetNormalizedCHI() != nil
}

// GetStatus is a getter
func (s *ChiStatus) GetStatus() string {
	if s == nil {
		return ""
	}
	return s.Status
}

// GetPods is a getter
func (s *ChiStatus) GetPods() []string {
	if s == nil {
		return nil
	}
	return s.Pods
}

// GetPodIPS is a getter
func (s *ChiStatus) GetPodIPS() []string {
	if s == nil {
		return nil
	}
	return s.PodIPs
}

// HostUpdated updates updated hosts counter
func (s *ChiStatus) HostUpdated() {
	if s == nil {
		return
	}
	s.HostsUpdatedCount++
	s.HostsCompletedCount++
}

// HostAdded updates added hosts counter
func (s *ChiStatus) HostAdded() {
	if s == nil {
		return
	}
	s.HostsAddedCount++
	s.HostsCompletedCount++
}

// HostUnchanged updates unchanged hosts counter
func (s *ChiStatus) HostUnchanged() {
	if s == nil {
		return
	}
	s.HostsUnchangedCount++
	s.HostsCompletedCount++
}

// HostFailed updates failed hosts counter
func (s *ChiStatus) HostFailed() {
	if s == nil {
		return
	}
	s.HostsFailedCount++
	s.HostsCompletedCount++
}
