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

import "github.com/altinity/clickhouse-operator/pkg/util"

// KeeperServiceType describes how keeper endpoints are discovered for a KeeperRef.
type KeeperServiceType string

const (
	// KeeperServiceTypeReplicas discovers per-host services, one ZK node per keeper replica.
	KeeperServiceTypeReplicas KeeperServiceType = "Replicas"
	// KeeperServiceTypeService uses the CR-level headless service as a single ZK node entry.
	KeeperServiceTypeService KeeperServiceType = "Service"
)

// IsEmpty returns true if no service type is set.
func (t KeeperServiceType) IsEmpty() bool {
	return t == ""
}

// KeeperRef defines a reference to a ClickHouseKeeperInstallation (CHK) resource.
type KeeperRef struct {
	// Name is the name of the CHK custom resource.
	Name string `json:"name,omitempty" yaml:"name,omitempty"`
	// Namespace is the namespace of the CHK resource. Defaults to the CHI namespace if omitted.
	// +optional
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"`
	// ServiceType controls how keeper endpoints are discovered (case-insensitive):
	//   "Replicas" (default) — enumerate per-host services, one ZK node per keeper replica
	//   "Service"            — use the CR-level headless service as a single ZK node entry
	// +optional
	ServiceType KeeperServiceType `json:"serviceType,omitempty" yaml:"serviceType,omitempty"`
}

// HasName returns true if the reference has a non-empty name.
func (r *KeeperRef) HasName() bool {
	return r != nil && r.Name != ""
}

// IsEmpty returns true if the reference is nil or has an empty name.
func (r *KeeperRef) IsEmpty() bool {
	return r == nil || r.Name == ""
}

// GetNamespace returns the namespace, defaulting to defaultNs if empty.
func (r *KeeperRef) GetNamespace(defaultNamespace string) string {
	if r == nil || r.Namespace == "" {
		return defaultNamespace
	}
	return r.Namespace
}

// GetServiceType returns the service type, defaulting to Replicas if empty. The raw value
// is read un-normalized from the spec, so fold any accepted casing to the canonical const;
// an unrecognized value passes through unchanged so the resolver can report it as invalid.
func (r *KeeperRef) GetServiceType() KeeperServiceType {
	if r == nil || r.ServiceType.IsEmpty() {
		return KeeperServiceTypeReplicas
	}
	return KeeperServiceType(
		util.FoldEnum(
			string(r.ServiceType),
			string(KeeperServiceTypeReplicas),
			string(KeeperServiceTypeService),
		),
	)
}
