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
	"sync"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/util"
	"github.com/altinity/clickhouse-operator/pkg/version"
)

const (
	maxActions = 10
	maxErrors  = 10
	maxTaskIDs = 10
)

// Possible CR statuses
const (
	StatusInProgress  = "InProgress"
	StatusCompleted   = "Completed"
	StatusAborted     = "Aborted"
	StatusTerminating = "Terminating"
)

// Status defines status section of the custom resource.
//
// Note: application level reads and writes to Status fields should be done through synchronized getter/setter functions.
// While all of these fields need to be exported for JSON and YAML serialization/deserialization, we can at least audit
// that application logic sticks to the synchronized getter/setters by auditing whether all explicit Go field-level
// accesses are strictly within _this_ source file OR the generated deep copy source file.
type Status struct {
	CHOpVersion              string                  `json:"chop-version,omitempty"             yaml:"chop-version,omitempty"`
	CHOpCommit               string                  `json:"chop-commit,omitempty"              yaml:"chop-commit,omitempty"`
	CHOpDate                 string                  `json:"chop-date,omitempty"                yaml:"chop-date,omitempty"`
	CHOpIP                   string                  `json:"chop-ip,omitempty"                  yaml:"chop-ip,omitempty"`
	ClustersCount            int                     `json:"clusters,omitempty"                 yaml:"clusters,omitempty"`
	ShardsCount              int                     `json:"shards,omitempty"                   yaml:"shards,omitempty"`
	ReplicasCount            int                     `json:"replicas,omitempty"                 yaml:"replicas,omitempty"`
	HostsCount               int                     `json:"hosts,omitempty"                    yaml:"hosts,omitempty"`
	Status                   string                  `json:"status,omitempty"                   yaml:"status,omitempty"`
	TaskID                   string                  `json:"taskID,omitempty"                   yaml:"taskID,omitempty"`
	TaskIDsStarted           []string                `json:"taskIDsStarted,omitempty"           yaml:"taskIDsStarted,omitempty"`
	TaskIDsCompleted         []string                `json:"taskIDsCompleted,omitempty"         yaml:"taskIDsCompleted,omitempty"`
	Action                   string                  `json:"action,omitempty"                   yaml:"action,omitempty"`
	Actions                  []string                `json:"actions,omitempty"                  yaml:"actions,omitempty"`
	Error                    string                  `json:"error,omitempty"                    yaml:"error,omitempty"`
	Errors                   []string                `json:"errors,omitempty"                   yaml:"errors,omitempty"`
	HostsUpdatedCount        int                     `json:"hostsUpdated,omitempty"             yaml:"hostsUpdated,omitempty"`
	HostsAddedCount          int                     `json:"hostsAdded,omitempty"               yaml:"hostsAdded,omitempty"`
	HostsUnchangedCount      int                     `json:"hostsUnchanged,omitempty"           yaml:"hostsUnchanged,omitempty"`
	HostsFailedCount         int                     `json:"hostsFailed,omitempty"              yaml:"hostsFailed,omitempty"`
	HostsCompletedCount      int                     `json:"hostsCompleted,omitempty"           yaml:"hostsCompleted,omitempty"`
	HostsDeletedCount        int                     `json:"hostsDeleted,omitempty"             yaml:"hostsDeleted,omitempty"`
	HostsDeleteCount         int                     `json:"hostsDelete,omitempty"              yaml:"hostsDelete,omitempty"`
	Pods                     []string                `json:"pods,omitempty"                     yaml:"pods,omitempty"`
	PodIPs                   []string                `json:"pod-ips,omitempty"                  yaml:"pod-ips,omitempty"`
	FQDNs                    []string                `json:"fqdns,omitempty"                    yaml:"fqdns,omitempty"`
	Endpoint                 string                  `json:"endpoint,omitempty"                 yaml:"endpoint,omitempty"`
	Endpoints                []string                `json:"endpoints,omitempty"                yaml:"endpoints,omitempty"`
	NormalizedCR             *ClickHouseInstallation `json:"normalized,omitempty"               yaml:"normalized,omitempty"`
	NormalizedCRCompleted    *ClickHouseInstallation `json:"normalizedCompleted,omitempty"      yaml:"normalizedCompleted,omitempty"`
	HostsWithTablesCreated   []string                `json:"hostsWithTablesCreated,omitempty"   yaml:"hostsWithTablesCreated,omitempty"`
	HostsWithReplicaCaughtUp []string                `json:"hostsWithReplicaCaughtUp,omitempty" yaml:"hostsWithReplicaCaughtUp,omitempty"`
	UsedTemplates            []*TemplateRef          `json:"usedTemplates,omitempty"            yaml:"usedTemplates,omitempty"`

	mu sync.RWMutex `json:"-" yaml:"-"`
}

// FillStatusParams is a struct used to fill status params
type FillStatusParams struct {
	CHOpIP              string
	ClustersCount       int
	ShardsCount         int
	HostsCount          int
	TaskID              string
	HostsUpdatedCount   int
	HostsAddedCount     int
	HostsUnchangedCount int
	HostsCompletedCount int
	HostsDeleteCount    int
	HostsDeletedCount   int
	Pods                []string
	FQDNs               []string
	Endpoint            string
	Endpoints           []string
	NormalizedCR        *ClickHouseInstallation
}

// Fill is a synchronized setter for a fairly large number of fields. We take a struct type "params" argument to avoid
// confusion of similarly typed positional arguments, and to avoid defining a lot of separate synchronized setters
// for these fields that are typically all set together at once (during "fills").
func (s *Status) Fill(params *FillStatusParams) {
	doWithWriteLock(s, func(s *Status) {
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
		s.HostsUnchangedCount = params.HostsUnchangedCount
		s.HostsCompletedCount = params.HostsCompletedCount
		s.HostsDeleteCount = params.HostsDeleteCount
		s.HostsDeletedCount = params.HostsDeletedCount
		s.Pods = params.Pods
		s.FQDNs = params.FQDNs
		s.Endpoint = params.Endpoint
		s.Endpoints = append([]string{}, params.Endpoints...)
		s.NormalizedCR = params.NormalizedCR
	})
}

// SetError sets status error
func (s *Status) SetError(err string) {
	doWithWriteLock(s, func(s *Status) {
		s.Error = err
	})
}

// PushError sets and pushes error into status
func (s *Status) PushError(error string) {
	doWithWriteLock(s, func(s *Status) {
		s.Errors = append([]string{error}, s.Errors...)
		if len(s.Errors) > maxErrors {
			s.Errors = s.Errors[:maxErrors]
		}
	})
}

// SetAndPushError sets and pushes error into status
func (s *Status) SetAndPushError(err string) {
	doWithWriteLock(s, func(s *Status) {
		s.Error = err
		s.Errors = append([]string{err}, s.Errors...)
		if len(s.Errors) > maxErrors {
			s.Errors = s.Errors[:maxErrors]
		}
	})
}

// PushHostReplicaCaughtUp pushes host to the list of hosts with replica caught-up
func (s *Status) PushHostReplicaCaughtUp(host string) {
	doWithWriteLock(s, func(s *Status) {
		if util.InArray(host, s.HostsWithReplicaCaughtUp) {
			return
		}
		s.HostsWithReplicaCaughtUp = append(s.HostsWithReplicaCaughtUp, host)
	})
}

// PushHostTablesCreated pushes host to the list of hosts with created tables
func (s *Status) PushHostTablesCreated(host string) {
	doWithWriteLock(s, func(s *Status) {
		if util.InArray(host, s.HostsWithTablesCreated) {
			return
		}
		s.HostsWithTablesCreated = append(s.HostsWithTablesCreated, host)
	})
}

// SyncHostTablesCreated syncs list of hosts with tables created with actual list of hosts
func (s *Status) SyncHostTablesCreated() {
	doWithWriteLock(s, func(s *Status) {
		if s.FQDNs == nil {
			return
		}
		s.HostsWithTablesCreated = util.IntersectStringArrays(s.HostsWithTablesCreated, s.FQDNs)
	})
}

// PushUsedTemplate pushes used templates to the list of used templates
func (s *Status) PushUsedTemplate(templateRefs ...*TemplateRef) {
	if len(templateRefs) > 0 {
		doWithWriteLock(s, func(s *Status) {
			s.UsedTemplates = append(s.UsedTemplates, templateRefs...)
		})
	}
}

// GetUsedTemplatesCount gets used templates count
func (s *Status) GetUsedTemplatesCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return len(s.UsedTemplates)
	})
}

// SetAction action setter
func (s *Status) SetAction(action string) {
	doWithWriteLock(s, func(s *Status) {
		s.Action = action
	})
}

// PushAction pushes action into status
func (s *Status) PushAction(action string) {
	doWithWriteLock(s, func(s *Status) {
		s.Actions = append([]string{action}, s.Actions...)
		trimActionsNoSync(s)
	})
}

// HasNormalizedCRCompleted is a checker
func (s *Status) HasNormalizedCRCompleted() bool {
	return s.GetNormalizedCRCompleted() != nil
}

// HasNormalizedCR is a checker
func (s *Status) HasNormalizedCR() bool {
	return s.GetNormalizedCR() != nil
}

// SetPodIPs sets pod IPs
func (s *Status) SetPodIPs(podIPs []string) {
	doWithWriteLock(s, func(s *Status) {
		s.PodIPs = podIPs
	})
}

// HostDeleted increments deleted hosts counter
func (s *Status) HostDeleted() {
	doWithWriteLock(s, func(s *Status) {
		s.HostsDeletedCount++
	})
}

// HostUpdated increments updated hosts counter
func (s *Status) HostUpdated() {
	doWithWriteLock(s, func(s *Status) {
		s.HostsUpdatedCount++
	})
}

// HostAdded increments added hosts counter
func (s *Status) HostAdded() {
	doWithWriteLock(s, func(s *Status) {
		s.HostsAddedCount++
	})
}

// HostUnchanged increments unchanged hosts counter
func (s *Status) HostUnchanged() {
	doWithWriteLock(s, func(s *Status) {
		s.HostsUnchangedCount++
	})
}

// HostFailed increments failed hosts counter
func (s *Status) HostFailed() {
	doWithWriteLock(s, func(s *Status) {
		s.HostsFailedCount++
	})
}

// HostCompleted increments completed hosts counter
func (s *Status) HostCompleted() {
	doWithWriteLock(s, func(s *Status) {
		s.HostsCompletedCount++
	})
}

// ReconcileStart marks reconcile start
func (s *Status) ReconcileStart(deleteHostsCount int) {
	doWithWriteLock(s, func(s *Status) {
		if s == nil {
			return
		}
		s.Status = StatusInProgress
		s.HostsUpdatedCount = 0
		s.HostsAddedCount = 0
		s.HostsUnchangedCount = 0
		s.HostsCompletedCount = 0
		s.HostsDeletedCount = 0
		s.HostsDeleteCount = deleteHostsCount
		pushTaskIDStartedNoSync(s)
	})
}

// ReconcileComplete marks reconcile completion
func (s *Status) ReconcileComplete() {
	doWithWriteLock(s, func(s *Status) {
		if s == nil {
			return
		}
		s.Status = StatusCompleted
		s.Action = ""
		pushTaskIDCompletedNoSync(s)
	})
}

// ReconcileAbort marks reconcile abortion
func (s *Status) ReconcileAbort() {
	doWithWriteLock(s, func(s *Status) {
		if s == nil {
			return
		}
		s.Status = StatusAborted
		s.Action = ""
		pushTaskIDCompletedNoSync(s)
	})
}

// DeleteStart marks deletion start
func (s *Status) DeleteStart() {
	doWithWriteLock(s, func(s *Status) {
		if s == nil {
			return
		}
		s.Status = StatusTerminating
		s.HostsUpdatedCount = 0
		s.HostsAddedCount = 0
		s.HostsUnchangedCount = 0
		s.HostsCompletedCount = 0
		s.HostsDeletedCount = 0
		s.HostsDeleteCount = 0
		pushTaskIDStartedNoSync(s)
	})
}

func prepareOptions(opts types.CopyStatusOptions) types.CopyStatusOptions {
	if opts.FieldGroupInheritable {
		opts.Copy.TaskIDsStarted = true
		opts.Copy.TaskIDsCompleted = true
		opts.Copy.Actions = true
		opts.Copy.Errors = true
		opts.Copy.HostsWithTablesCreated = true
		opts.Copy.UsedTemplates = true
	}

	if opts.FieldGroupActions {
		opts.Copy.Action = true
		opts.Merge.Actions = true
		opts.Copy.HostsWithTablesCreated = true
		opts.Copy.UsedTemplates = true
	}

	if opts.FieldGroupErrors {
		opts.Copy.Error = true
		opts.Merge.Errors = true
	}

	if opts.FieldGroupMain {
		opts.Copy.CHOpVersion = true
		opts.Copy.CHOpCommit = true
		opts.Copy.CHOpDate = true
		opts.Copy.CHOpIP = true
		opts.Copy.ClustersCount = true
		opts.Copy.ShardsCount = true
		opts.Copy.ReplicasCount = true
		opts.Copy.HostsCount = true
		opts.Copy.Status = true
		opts.Copy.TaskID = true
		opts.Copy.TaskIDsStarted = true
		opts.Copy.TaskIDsCompleted = true
		opts.Copy.Action = true
		opts.Merge.Actions = true
		opts.Copy.Error = true
		opts.Copy.Errors = true
		opts.Copy.HostsUpdatedCount = true
		opts.Copy.HostsAddedCount = true
		opts.Copy.HostsUnchangedCount = true
		opts.Copy.HostsCompletedCount = true
		opts.Copy.HostsDeletedCount = true
		opts.Copy.HostsDeleteCount = true
		opts.Copy.HostsWithTablesCreated = true
		opts.Copy.Pods = true
		opts.Copy.PodIPs = true
		opts.Copy.FQDNs = true
		opts.Copy.Endpoint = true
		opts.Copy.NormalizedCR = true
		opts.Copy.UsedTemplates = true
	}

	if opts.FieldGroupNormalized {
		opts.Copy.NormalizedCR = true
	}

	if opts.FieldGroupWholeStatus {
		opts.Copy.CHOpVersion = true
		opts.Copy.CHOpCommit = true
		opts.Copy.CHOpDate = true
		opts.Copy.CHOpIP = true
		opts.Copy.ClustersCount = true
		opts.Copy.ShardsCount = true
		opts.Copy.ReplicasCount = true
		opts.Copy.HostsCount = true
		opts.Copy.Status = true
		opts.Copy.TaskID = true
		opts.Copy.TaskIDsStarted = true
		opts.Copy.TaskIDsCompleted = true
		opts.Copy.Action = true
		opts.Merge.Actions = true
		opts.Copy.Error = true
		opts.Copy.Errors = true
		opts.Copy.HostsUpdatedCount = true
		opts.Copy.HostsAddedCount = true
		opts.Copy.HostsUnchangedCount = true
		opts.Copy.HostsCompletedCount = true
		opts.Copy.HostsDeletedCount = true
		opts.Copy.HostsDeleteCount = true
		opts.Copy.HostsWithTablesCreated = true
		opts.Copy.Pods = true
		opts.Copy.PodIPs = true
		opts.Copy.FQDNs = true
		opts.Copy.Endpoint = true
		opts.Copy.NormalizedCR = true
		opts.Copy.NormalizedCRCompleted = true
		opts.Copy.UsedTemplates = true
	}

	return opts
}

// CopyFrom copies the state of a given Status f into the receiver Status of the call.
func (s *Status) CopyFrom(f *Status, opts types.CopyStatusOptions) {
	doWithWriteLock(s, func(s *Status) {
		doWithReadLock(f, func(from *Status) {
			if s == nil || from == nil {
				return
			}

			opts = prepareOptions(opts)

			// Copy fields
			if opts.Copy.CHOpVersion {
				s.CHOpVersion = from.CHOpVersion
			}
			if opts.Copy.CHOpCommit {
				s.CHOpCommit = from.CHOpCommit
			}
			if opts.Copy.CHOpDate {
				s.CHOpDate = from.CHOpDate
			}
			if opts.Copy.CHOpIP {
				s.CHOpIP = from.CHOpIP
			}
			if opts.Copy.ClustersCount {
				s.ClustersCount = from.ClustersCount
			}
			if opts.Copy.ShardsCount {
				s.ShardsCount = from.ShardsCount
			}
			if opts.Copy.ReplicasCount {
				s.ReplicasCount = from.ReplicasCount
			}
			if opts.Copy.HostsCount {
				s.HostsCount = from.HostsCount
			}
			if opts.Copy.Status {
				s.Status = from.Status
			}
			if opts.Copy.TaskID {
				s.TaskID = from.TaskID
			}
			if opts.Copy.TaskIDsStarted {
				s.TaskIDsStarted = from.TaskIDsStarted
			}
			if opts.Copy.TaskIDsCompleted {
				s.TaskIDsCompleted = from.TaskIDsCompleted
			}
			if opts.Copy.Action {
				s.Action = from.Action
			}
			if opts.Merge.Actions {
				mergeActionsNoSync(s, from)
			}
			if opts.Copy.Error {
				s.Error = from.Error
			}
			if opts.Copy.Errors {
				s.Errors = from.Errors
			}
			if opts.Merge.Errors {
				s.Errors = util.MergeStringArrays(s.Errors, from.Errors)
				sort.Sort(sort.Reverse(sort.StringSlice(s.Errors)))
			}
			if opts.Copy.HostsUpdatedCount {
				s.HostsUpdatedCount = from.HostsUpdatedCount
			}
			if opts.Copy.HostsAddedCount {
				s.HostsAddedCount = from.HostsAddedCount
			}
			if opts.Copy.HostsUnchangedCount {
				s.HostsUnchangedCount = from.HostsUnchangedCount
			}
			if opts.Copy.HostsCompletedCount {
				s.HostsCompletedCount = from.HostsCompletedCount
			}
			if opts.Copy.HostsDeletedCount {
				s.HostsDeletedCount = from.HostsDeletedCount
			}
			if opts.Copy.HostsDeleteCount {
				s.HostsDeleteCount = from.HostsDeleteCount
			}
			if opts.Copy.Pods {
				s.Pods = from.Pods
			}
			if opts.Copy.PodIPs {
				s.PodIPs = from.PodIPs
			}
			if opts.Copy.FQDNs {
				s.FQDNs = from.FQDNs
			}
			if opts.Copy.Endpoint {
				s.Endpoint = from.Endpoint
				s.Endpoints = from.Endpoints
			}
			if opts.Copy.NormalizedCR {
				s.NormalizedCR = from.NormalizedCR
			}
			if opts.Copy.NormalizedCRCompleted {
				s.NormalizedCRCompleted = from.NormalizedCRCompleted
			}
			if opts.Copy.HostsWithTablesCreated {
				s.HostsWithTablesCreated = nil
				if len(from.HostsWithTablesCreated) > 0 {
					s.HostsWithTablesCreated = append(s.HostsWithTablesCreated, from.HostsWithTablesCreated...)
				}
				s.HostsWithReplicaCaughtUp = nil
				if len(from.HostsWithReplicaCaughtUp) > 0 {
					s.HostsWithReplicaCaughtUp = append(s.HostsWithReplicaCaughtUp, from.HostsWithReplicaCaughtUp...)
				}
			}
			if opts.Copy.UsedTemplates {
				if len(from.UsedTemplates) > len(s.UsedTemplates) {
					s.UsedTemplates = nil
					s.UsedTemplates = append(s.UsedTemplates, from.UsedTemplates...)
				}
			}
		})
	})
}

// ClearNormalizedCR clears normalized CR in status
func (s *Status) ClearNormalizedCR() {
	doWithWriteLock(s, func(s *Status) {
		s.NormalizedCR = nil
	})
}

// SetNormalizedCompletedFromCurrentNormalized sets completed CR from current CR
func (s *Status) SetNormalizedCompletedFromCurrentNormalized() {
	doWithWriteLock(s, func(s *Status) {
		s.NormalizedCRCompleted = s.NormalizedCR
	})
}

// GetCHOpVersion gets operator version
func (s *Status) GetCHOpVersion() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.CHOpVersion
	})
}

// GetCHOpCommit gets operator build commit
func (s *Status) GetCHOpCommit() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.CHOpCommit
	})
}

// GetCHOpDate gets operator build date
func (s *Status) GetCHOpDate() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.CHOpDate
	})
}

// GetCHOpIP gets operator pod's IP
func (s *Status) GetCHOpIP() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.CHOpIP
	})
}

// GetClustersCount gets clusters count
func (s *Status) GetClustersCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.ClustersCount
	})
}

// GetShardsCount gets shards count
func (s *Status) GetShardsCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.ShardsCount
	})
}

// GetReplicasCount gets replicas count
func (s *Status) GetReplicasCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.ReplicasCount
	})
}

// GetHostsCount gets hosts count
func (s *Status) GetHostsCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.HostsCount
	})
}

// GetStatus gets status
func (s *Status) GetStatus() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.Status
	})
}

// GetTaskID gets task ipd
func (s *Status) GetTaskID() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.TaskID
	})
}

// GetTaskIDsStarted gets started task id
func (s *Status) GetTaskIDsStarted() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.TaskIDsStarted
	})
}

// GetTaskIDsCompleted gets completed task id
func (s *Status) GetTaskIDsCompleted() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.TaskIDsCompleted
	})
}

// GetAction gets last action
func (s *Status) GetAction() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.Action
	})
}

// GetActions gets all actions
func (s *Status) GetActions() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.Actions
	})
}

// GetError gets last error
func (s *Status) GetError() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.Error
	})
}

// GetErrors gets all errors
func (s *Status) GetErrors() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.Errors
	})
}

// GetHostsUpdatedCount gets updated hosts counter
func (s *Status) GetHostsUpdatedCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.HostsUpdatedCount
	})
}

// GetHostsAddedCount gets added hosts counter
func (s *Status) GetHostsAddedCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.HostsAddedCount
	})
}

// GetHostsUnchangedCount gets unchanged hosts counter
func (s *Status) GetHostsUnchangedCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.HostsUnchangedCount
	})
}

// GetHostsFailedCount gets failed hosts counter
func (s *Status) GetHostsFailedCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.HostsFailedCount
	})
}

// GetHostsCompletedCount gets completed hosts counter
func (s *Status) GetHostsCompletedCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.HostsCompletedCount
	})
}

// GetHostsDeletedCount gets deleted hosts counter
func (s *Status) GetHostsDeletedCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.HostsDeletedCount
	})
}

// GetHostsDeleteCount gets hosts to be deleted counter
func (s *Status) GetHostsDeleteCount() int {
	return getIntWithReadLock(s, func(s *Status) int {
		return s.HostsDeleteCount
	})
}

// GetPods gets list of pods
func (s *Status) GetPods() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.Pods
	})
}

// GetPodIPs gets list of pod ips
func (s *Status) GetPodIPs() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.PodIPs
	})
}

// GetFQDNs gets list of all FQDNs of hosts
func (s *Status) GetFQDNs() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.FQDNs
	})
}

// GetEndpoint gets API endpoint
func (s *Status) GetEndpoint() string {
	return getStringWithReadLock(s, func(s *Status) string {
		return s.Endpoint
	})
}

// GetNormalizedCR gets target CR
func (s *Status) GetNormalizedCR() *ClickHouseInstallation {
	return getCRWithReadLock(s, func(s *Status) *ClickHouseInstallation {
		return s.NormalizedCR
	})
}

// GetNormalizedCRCompleted gets completed CR
func (s *Status) GetNormalizedCRCompleted() *ClickHouseInstallation {
	return getCRWithReadLock(s, func(s *Status) *ClickHouseInstallation {
		return s.NormalizedCRCompleted
	})
}

// GetHostsWithTablesCreated gets hosts with created tables
func (s *Status) GetHostsWithTablesCreated() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.HostsWithTablesCreated
	})
}

// GetHostsWithReplicaCatchUp gets hosts with replica catch-up
func (s *Status) GetHostsWithReplicaCatchUp() []string {
	return getStringArrWithReadLock(s, func(s *Status) []string {
		return s.HostsWithReplicaCaughtUp

	})
}

// Begin helpers

func doWithWriteLock(s *Status, f func(*Status)) {
	if s == nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	f(s)
}

func doWithReadLock(s *Status, f func(*Status)) {
	if s == nil {
		return
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	f(s)
}

func getIntWithReadLock(s *Status, f func(*Status) int) int {
	var zeroVal int
	if s == nil {
		return zeroVal
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return f(s)
}

func getStringWithReadLock(s *Status, f func(*Status) string) string {
	var zeroVal string
	if s == nil {
		return zeroVal
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return f(s)
}

func getCRWithReadLock(s *Status, f func(*Status) *ClickHouseInstallation) *ClickHouseInstallation {
	var zeroVal *ClickHouseInstallation
	if s == nil {
		return zeroVal
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	return f(s)
}

func getStringArrWithReadLock(s *Status, f func(*Status) []string) []string {
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
func mergeActionsNoSync(s *Status, from *Status) {
	s.Actions = util.MergeStringArrays(s.Actions, from.Actions)
	sort.Sort(sort.Reverse(sort.StringSlice(s.Actions)))
	trimActionsNoSync(s)
}

// trimActionsNoSync trims actions (without synchronization, because synchronized functions call into this).
func trimActionsNoSync(s *Status) {
	if len(s.Actions) > maxActions {
		s.Actions = s.Actions[:maxActions]
	}
}

// pushTaskIDStartedNoSync pushes task id into status
func pushTaskIDStartedNoSync(s *Status) {
	s.TaskIDsStarted = append([]string{s.TaskID}, s.TaskIDsStarted...)
	if len(s.TaskIDsStarted) > maxTaskIDs {
		s.TaskIDsStarted = s.TaskIDsStarted[:maxTaskIDs]
	}
}

// pushTaskIDCompletedNoSync pushes task id into status w/o sync
func pushTaskIDCompletedNoSync(s *Status) {
	s.TaskIDsCompleted = append([]string{s.TaskID}, s.TaskIDsCompleted...)
	if len(s.TaskIDsCompleted) > maxTaskIDs {
		s.TaskIDsCompleted = s.TaskIDsCompleted[:maxTaskIDs]
	}
}
