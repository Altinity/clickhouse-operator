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
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// ChkStatus defines status section of ClickHouseKeeper resource
type ChkStatus struct {
	CHOpVersion string `json:"chop-version,omitempty"           yaml:"chop-version,omitempty"`
	CHOpCommit  string `json:"chop-commit,omitempty"            yaml:"chop-commit,omitempty"`
	CHOpDate    string `json:"chop-date,omitempty"              yaml:"chop-date,omitempty"`
	CHOpIP      string `json:"chop-ip,omitempty"                yaml:"chop-ip,omitempty"`

	Status string `json:"status,omitempty"                 yaml:"status,omitempty"`

	// Replicas is the number of number of desired replicas in the cluster
	Replicas int32 `json:"replicas,omitempty"`

	// ReadyReplicas is the number of number of ready replicas in the cluster
	ReadyReplicas []apiChi.ZookeeperNode `json:"readyReplicas,omitempty"`

	Pods                   []string                      `json:"pods,omitempty"                   yaml:"pods,omitempty"`
	PodIPs                 []string                      `json:"pod-ips,omitempty"                yaml:"pod-ips,omitempty"`
	FQDNs                  []string                      `json:"fqdns,omitempty"                  yaml:"fqdns,omitempty"`
	NormalizedCHK          *ClickHouseKeeperInstallation `json:"normalized,omitempty"             yaml:"normalized,omitempty"`
	NormalizedCHKCompleted *ClickHouseKeeperInstallation `json:"normalizedCompleted,omitempty"    yaml:"normalizedCompleted,omitempty"`
}

// CopyFrom copies the state of a given ChiStatus f into the receiver ChiStatus of the call.
func (s *ChkStatus) CopyFrom(from *ChkStatus, opts apiChi.CopyStatusOptions) {
	if s == nil || from == nil {
		return
	}

	if opts.InheritableFields {
	}

	if opts.MainFields {
		s.CHOpVersion = from.CHOpVersion
		s.CHOpCommit = from.CHOpCommit
		s.CHOpDate = from.CHOpDate
		s.CHOpIP = from.CHOpIP
		s.Status = from.Status
		s.Replicas = from.Replicas
		s.ReadyReplicas = from.ReadyReplicas
		s.Pods = from.Pods
		s.PodIPs = from.PodIPs
		s.FQDNs = from.FQDNs
		s.NormalizedCHK = from.NormalizedCHK
	}

	if opts.Normalized {
		s.NormalizedCHK = from.NormalizedCHK
	}

	if opts.WholeStatus {
		s.CHOpVersion = from.CHOpVersion
		s.CHOpCommit = from.CHOpCommit
		s.CHOpDate = from.CHOpDate
		s.CHOpIP = from.CHOpIP
		s.Status = from.Status
		s.Replicas = from.Replicas
		s.ReadyReplicas = from.ReadyReplicas
		s.Pods = from.Pods
		s.PodIPs = from.PodIPs
		s.FQDNs = from.FQDNs
		s.NormalizedCHK = from.NormalizedCHK
		s.NormalizedCHKCompleted = from.NormalizedCHKCompleted
	}
}

// HasNormalizedCHKCompleted is a checker
func (s *ChkStatus) HasNormalizedCHKCompleted() bool {
	return s.GetNormalizedCHKCompleted() != nil
}

// HasNormalizedCHK is a checker
func (s *ChkStatus) HasNormalizedCHK() bool {
	return s.GetNormalizedCHK() != nil
}

// ClearNormalizedCHK clears normalized CHK in status
func (s *ChkStatus) ClearNormalizedCHK() {
	s.NormalizedCHK = nil
}

// GetNormalizedCHK gets target CHK
func (s *ChkStatus) GetNormalizedCHK() *ClickHouseKeeperInstallation {
	return s.NormalizedCHK
}

// GetNormalizedCHKCompleted gets completed CHI
func (s *ChkStatus) GetNormalizedCHKCompleted() *ClickHouseKeeperInstallation {
	return s.NormalizedCHKCompleted
}
