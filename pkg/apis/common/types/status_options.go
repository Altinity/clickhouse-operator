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

package types

// CopyStatusOptions specifies what parts to copy in status
type CopyStatusOptions struct {
	CopyStatusFieldGroup
	CopyStatusField
}

type CopyStatusFieldGroup struct {
	FieldGroupActions     bool
	FieldGroupErrors      bool
	FieldGroupNormalized  bool
	FieldGroupMain        bool
	FieldGroupWholeStatus bool
	FieldGroupInheritable bool
}

type CopyStatusField struct {
	Copy  Status
	Merge Status
}

// UpdateStatusOptions defines how to update CHI status
type UpdateStatusOptions struct {
	CopyStatusOptions
	TolerateAbsence bool
}

type Status struct {
	CHOpVersion            bool
	CHOpCommit             bool
	CHOpDate               bool
	CHOpIP                 bool
	ClustersCount          bool
	ShardsCount            bool
	ReplicasCount          bool
	HostsCount             bool
	Status                 bool
	TaskID                 bool
	TaskIDsStarted         bool
	TaskIDsCompleted       bool
	Action                 bool
	Actions                bool
	Error                  bool
	Errors                 bool
	HostsUpdatedCount      bool
	HostsAddedCount        bool
	HostsUnchangedCount    bool
	HostsFailedCount       bool
	HostsCompletedCount    bool
	HostsDeletedCount      bool
	HostsDeleteCount       bool
	Pods                   bool
	PodIPs                 bool
	FQDNs                  bool
	Endpoint               bool
	NormalizedCR           bool
	NormalizedCRCompleted  bool
	ActionPlan             bool
	HostsWithTablesCreated bool
	UsedTemplates          bool
}
