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
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/version"
	"sort"
	"sync"
)

const (
	maxActions = 10
	maxErrors  = 10
	maxTaskIDs = 10
)

// ChiStatus defines status section of ClickHouseInstallation resource.
//
// Note: application level reads and writes to ChiStatus fields should be done through synchronized getter/setter functions.
// While all of these fields need to be exported for JSON and YAML serialization/deserialization, we can at least audit
// that application logic sticks to the synchronized getter/setters by auditing whether all explicit Go field-level
// accesses are strictly within _this_ source file OR the generated deep copy source file.
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

	mu sync.RWMutex
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

// Beginning of synchronized mutator functions

type FillStatusParams struct {
	CHOpIP              string
	ClustersCount       int
	ShardsCount         int
	HostsCount          int
	TaskID              string
	HostsUpdatedCount   int
	HostsAddedCount     int
	HostsCompletedCount int
	HostsDeleteCount    int
	HostsDeletedCount   int
	Pods                []string
	FQDNs               []string
	Endpoint            string
	NormalizedCHI       *ClickHouseInstallation
}

// Fill is a synchronized setter for a fairly large number of fields. We take a struct type "params" argument to avoid
// confusion of similarly typed positional arguments, and to avoid defining a lot of separate synchronized setters
// for these fields that are typically all set together at once (during "fills").
func (in *ChiStatus) Fill(params *FillStatusParams) {
	doWithWriteLock(in, func(s *ChiStatus) {
		// We always set these (build-hardcoded) version fields.
		s.CHOpVersion = version.Version
		s.CHOpCommit = version.GitSHA
		s.CHOpDate = version.BuiltAt

		// Now, set fields from the provided input.
		s.CHOpIP = params.CHOpIP
		s.ClustersCount = params.ClustersCount
		s.ShardsCount = params.ShardsCount
		s.HostsCount = params.HostsCount
		s.TaskID = params.TaskID
		s.HostsUpdatedCount = params.HostsUpdatedCount
		s.HostsAddedCount = params.HostsAddedCount
		s.HostsCompletedCount = params.HostsCompletedCount
		s.HostsDeleteCount = params.HostsDeleteCount
		s.HostsDeletedCount = params.HostsDeletedCount
		s.Pods = params.Pods
		s.FQDNs = params.FQDNs
		s.Endpoint = params.Endpoint
		s.NormalizedCHI = params.NormalizedCHI
	})
}

// SetError sets status error
func (s *ChiStatus) SetError(err string) {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.Error = err
	})
}

// SetAndPushError sets and pushes error into status
func (s *ChiStatus) SetAndPushError(err string) {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.Error = err
		s.Errors = append([]string{err}, s.Errors...)
		if len(s.Errors) > maxErrors {
			s.Errors = s.Errors[:maxErrors]
		}
	})
}

// PushHostTablesCreated pushes host to the list of hosts with created tables
func (s *ChiStatus) PushHostTablesCreated(host string) {
	doWithWriteLock(s, func(s *ChiStatus) {
		if util.InArray(host, s.HostsWithTablesCreated) {
			return
		}
		s.HostsWithTablesCreated = append(s.HostsWithTablesCreated, host)
	})
}

// SyncHostTablesCreated syncs list of hosts with tables created with actual list of hosts
func (s *ChiStatus) SyncHostTablesCreated() {
	doWithWriteLock(s, func(s *ChiStatus) {
		if s.FQDNs == nil {
			return
		}
		s.HostsWithTablesCreated = util.IntersectStringArrays(s.HostsWithTablesCreated, s.FQDNs)
	})
}

// SetAction action setter
func (s *ChiStatus) SetAction(action string) {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.Action = action
	})
}

// HasNormalizedCHICompleted is a checker
func (s *ChiStatus) HasNormalizedCHICompleted() bool {
	return s.GetNormalizedCHICompleted() != nil
}

// HasNormalizedCHI is a checker
func (s *ChiStatus) HasNormalizedCHI() bool {
	return s.GetNormalizedCHI() != nil
}

// PushAction pushes action into status
func (s *ChiStatus) PushAction(action string) {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.Actions = append([]string{action}, s.Actions...)
		trimActionsNoSync(s)
	})
}

// PushError sets and pushes error into status
func (s *ChiStatus) PushError(error string) {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.Errors = append([]string{error}, s.Errors...)
		if len(s.Errors) > maxErrors {
			s.Errors = s.Errors[:maxErrors]
		}
	})
}

func (s *ChiStatus) SetPodIPs(podIPs []string) {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.PodIPs = podIPs
	})
}

func (s *ChiStatus) HostDeleted() {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.HostsDeletedCount++
	})
}

// HostUpdated updates the counter of updated hosts
func (s *ChiStatus) HostUpdated() {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.HostsUpdatedCount++
		s.HostsCompletedCount++
	})
}

// HostAdded updates added hosts counter
func (s *ChiStatus) HostAdded() {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.HostsAddedCount++
		s.HostsCompletedCount++
	})
}

// HostUnchanged updates unchanged hosts counter
func (s *ChiStatus) HostUnchanged() {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.HostsUnchangedCount++
		s.HostsCompletedCount++
	})
}

// HostFailed updates failed hosts counter
func (s *ChiStatus) HostFailed() {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.HostsFailedCount++
		s.HostsCompletedCount++
	})
}

// ReconcileStart marks reconcile start
func (s *ChiStatus) ReconcileStart(deleteHostsCount int) {
	doWithWriteLock(s, func(s *ChiStatus) {
		if s == nil {
			return
		}
		s.Status = StatusInProgress
		s.HostsUpdatedCount = 0
		s.HostsAddedCount = 0
		s.HostsCompletedCount = 0
		s.HostsDeletedCount = 0
		s.HostsDeleteCount = deleteHostsCount
		pushTaskIDStartedNoSync(s)
	})
}

// ReconcileComplete marks reconcile completion
func (s *ChiStatus) ReconcileComplete() {
	doWithWriteLock(s, func(s *ChiStatus) {
		if s == nil {
			return
		}
		s.Status = StatusCompleted
		s.Action = ""
		pushTaskIDCompletedNoSync(s)
	})
}

// DeleteStart marks deletion start
func (s *ChiStatus) DeleteStart() {
	doWithWriteLock(s, func(s *ChiStatus) {
		if s == nil {
			return
		}
		s.Status = StatusTerminating
		s.HostsUpdatedCount = 0
		s.HostsAddedCount = 0
		s.HostsCompletedCount = 0
		s.HostsDeletedCount = 0
		s.HostsDeleteCount = 0
		pushTaskIDStartedNoSync(s)
	})
}

// CopyFrom copies the state of a given ChiStatus f into the receiver ChiStatus of the call.
func (s *ChiStatus) CopyFrom(f *ChiStatus, opts CopyCHIStatusOptions) {
	doWithWriteLock(s, func(s *ChiStatus) {
		doWithReadLock(f, func(from *ChiStatus) {
			if s == nil || from == nil {
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
				mergeActionsNoSync(s, from)
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
				mergeActionsNoSync(s, from)
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
				mergeActionsNoSync(s, from)
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
		})
	})
}

func (s *ChiStatus) ClearNormalizedCHI() {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.NormalizedCHI = nil
	})
}

func (s *ChiStatus) SetNormalizedCompletedFromCurrentNormalized() {
	doWithWriteLock(s, func(s *ChiStatus) {
		s.NormalizedCHICompleted = s.NormalizedCHI
	})
}

// Beginning of synchronized getter functions

func (s *ChiStatus) GetCHOpVersion() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.CHOpVersion
	})
}
func (s *ChiStatus) GetCHOpCommit() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.CHOpCommit
	})
}

func (s *ChiStatus) GetCHOpDate() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.CHOpDate
	})
}

func (s *ChiStatus) GetCHOpIP() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.CHOpIP
	})
}

func (s *ChiStatus) GetClustersCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.ClustersCount
	})
}

func (s *ChiStatus) GetShardsCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.ShardsCount
	})
}

func (s *ChiStatus) GetReplicasCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.ReplicasCount
	})
}

func (s *ChiStatus) GetHostsCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.HostsCount
	})
}

func (s *ChiStatus) GetStatus() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.Status
	})
}

func (s *ChiStatus) GetTaskID() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.TaskID
	})
}

func (s *ChiStatus) GetTaskIDsStarted() []string {
	return getStringArrWithReadLock(s, func(s *ChiStatus) []string {
		return s.TaskIDsStarted
	})
}

func (s *ChiStatus) GetTaskIDsCompleted() []string {
	return getStringArrWithReadLock(s, func(s *ChiStatus) []string {
		return s.TaskIDsCompleted
	})
}

func (s *ChiStatus) GetAction() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.Action
	})
}

func (s *ChiStatus) GetActions() []string {
	return getStringArrWithReadLock(s, func(s *ChiStatus) []string {
		return s.Actions
	})
}

func (s *ChiStatus) GetError() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.Error
	})
}

func (s *ChiStatus) GetErrors() []string {
	return getStringArrWithReadLock(s, func(s *ChiStatus) []string {
		return s.Errors
	})
}

func (s *ChiStatus) GetHostsUpdatedCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.HostsUpdatedCount
	})
}

func (s *ChiStatus) GetHostsAddedCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.HostsAddedCount
	})
}

func (s *ChiStatus) GetHostsUnchangedCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.HostsUnchangedCount
	})
}

func (s *ChiStatus) GetHostsFailedCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.HostsFailedCount
	})
}

func (s *ChiStatus) GetHostsCompletedCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.HostsCompletedCount
	})
}

func (s *ChiStatus) GetHostsDeletedCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.HostsDeletedCount
	})
}

func (s *ChiStatus) GetHostsDeleteCount() int {
	return getIntWithReadLock(s, func(s *ChiStatus) int {
		return s.HostsDeleteCount
	})
}

func (s *ChiStatus) GetPods() []string {
	return getStringArrWithReadLock(s, func(s *ChiStatus) []string {
		return s.Pods
	})
}

func (s *ChiStatus) GetPodIPs() []string {
	return getStringArrWithReadLock(s, func(s *ChiStatus) []string {
		return s.PodIPs
	})
}

func (s *ChiStatus) GetFQDNs() []string {
	return getStringArrWithReadLock(s, func(s *ChiStatus) []string {
		return s.FQDNs
	})
}

func (s *ChiStatus) GetEndpoint() string {
	return getStringWithReadLock(s, func(s *ChiStatus) string {
		return s.Endpoint
	})
}

func (s *ChiStatus) GetNormalizedCHI() *ClickHouseInstallation {
	return getInstallationWithReadLock(s, func(s *ChiStatus) *ClickHouseInstallation {
		return s.NormalizedCHI
	})
}

func (s *ChiStatus) GetNormalizedCHICompleted() *ClickHouseInstallation {
	return getInstallationWithReadLock(s, func(s *ChiStatus) *ClickHouseInstallation {
		return s.NormalizedCHICompleted
	})
}

func (s *ChiStatus) GetHostsWithTablesCreated() []string {
	return getStringArrWithReadLock(s, func(s *ChiStatus) []string {
		return s.HostsWithTablesCreated
	})
}

// Begin helpers

func doWithWriteLock(s *ChiStatus, f func(s *ChiStatus)) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	f(s)
}

func doWithReadLock(s *ChiStatus, f func(s *ChiStatus)) {
	if s == nil {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	f(s)
}

func getIntWithReadLock(s *ChiStatus, f func(s *ChiStatus) int) int {
	var zeroVal int
	if s == nil {
		return zeroVal
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return f(s)
}

func getStringWithReadLock(s *ChiStatus, f func(s *ChiStatus) string) string {
	var zeroVal string
	if s == nil {
		return zeroVal
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return f(s)
}

func getInstallationWithReadLock(s *ChiStatus, f func(s *ChiStatus) *ClickHouseInstallation) *ClickHouseInstallation {
	var zeroVal *ClickHouseInstallation
	if s == nil {
		return zeroVal
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return f(s)
}

func getStringArrWithReadLock(s *ChiStatus, f func(s *ChiStatus) []string) []string {
	emptyArr := make([]string, 0, 0)
	if s == nil {
		return emptyArr
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return f(s)
}

// mergeActionsNoSync merges the actions of from into those of s (without synchronization, because synchronized
// functions call into this).
func mergeActionsNoSync(s *ChiStatus, from *ChiStatus) {
	s.Actions = util.MergeStringArrays(s.Actions, from.Actions)
	sort.Sort(sort.Reverse(sort.StringSlice(s.Actions)))
	trimActionsNoSync(s)
}

// trimActionsNoSync trims actions (without synchronization, because synchronized functions call into this).
func trimActionsNoSync(s *ChiStatus) {
	if len(s.Actions) > maxActions {
		s.Actions = s.Actions[:maxActions]
	}
}

// pushTaskIDStartedNoSync pushes task id into status
func pushTaskIDStartedNoSync(s *ChiStatus) {
	s.TaskIDsStarted = append([]string{s.TaskID}, s.TaskIDsStarted...)
	if len(s.TaskIDsStarted) > maxTaskIDs {
		s.TaskIDsStarted = s.TaskIDsStarted[:maxTaskIDs]
	}
}

// PushTaskIDCompleted pushes task id into status
func pushTaskIDCompletedNoSync(s *ChiStatus) {
	s.TaskIDsCompleted = append([]string{s.TaskID}, s.TaskIDsCompleted...)
	if len(s.TaskIDsCompleted) > maxTaskIDs {
		s.TaskIDsCompleted = s.TaskIDsCompleted[:maxTaskIDs]
	}
}
