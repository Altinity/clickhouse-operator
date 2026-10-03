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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseRestore defines a one-off restore of a remote backup into a ClickHouseInstallation.
// The operator reconciles it into a Kubernetes Job that restores the schema on all replicas
// (ON CLUSTER) and the data on the first replica of each shard, letting native ClickHouse
// replication synchronize the remaining replicas.
//
// For safety the operator runs preflight checks before any data is touched and, by default,
// refuses to overwrite a non-empty target. Restoring into a fresh, empty CHI is the
// recommended path.
type ClickHouseRestore struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ClickHouseRestoreSpec   `json:"spec"             yaml:"spec"`
	Status ClickHouseRestoreStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// ClickHouseRestoreSpec defines the desired state of a ClickHouseRestore.
type ClickHouseRestoreSpec struct {
	// ClickHouseInstallation is the name of the target CHI in the same namespace.
	// It is strongly recommended this be a fresh, empty installation.
	ClickHouseInstallation string `json:"clickHouseInstallation" yaml:"clickHouseInstallation"`
	// BackupName is the remote backup to restore.
	BackupName string `json:"backupName" yaml:"backupName"`
	// SchemaOnly restores table/database definitions only (no data).
	SchemaOnly bool `json:"schemaOnly,omitempty" yaml:"schemaOnly,omitempty"`
	// Overwrite allows restoring over existing, non-empty tables. When false (default)
	// the operator refuses the restore if target tables already contain data.
	Overwrite bool `json:"overwrite,omitempty" yaml:"overwrite,omitempty"`
	// ValidateTopology, when true (default), refuses the restore if the target cluster's
	// shard/replica layout differs from the backup, preventing ReplicatedMergeTree
	// ZooKeeper/Keeper path corruption.
	ValidateTopology *bool `json:"validateTopology,omitempty" yaml:"validateTopology,omitempty"`
	// Image optionally overrides the container image used by the restore Job.
	Image string `json:"image,omitempty" yaml:"image,omitempty"`
	// ClickHouseCredentialsSecretName optionally references a Secret providing
	// CLICKHOUSE_USER and CLICKHOUSE_PASSWORD for the restore Job to authenticate.
	ClickHouseCredentialsSecretName string `json:"clickHouseCredentialsSecretName,omitempty" yaml:"clickHouseCredentialsSecretName,omitempty"`
}

// ClickHouseRestoreStatus defines the observed state of a ClickHouseRestore.
type ClickHouseRestoreStatus struct {
	// Phase is one of Pending, Running, Completed, Failed.
	Phase string `json:"phase,omitempty" yaml:"phase,omitempty"`
	// JobName is the name of the Kubernetes Job driving the restore.
	JobName string `json:"jobName,omitempty" yaml:"jobName,omitempty"`
	// StartTime is when the restore Job started.
	StartTime *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the restore Job finished.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// DurationSeconds is the restore duration in seconds (completion - start).
	DurationSeconds int64 `json:"durationSeconds,omitempty" yaml:"durationSeconds,omitempty"`
	// Conditions represent the latest available observations of the restore state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// IsTopologyValidationEnabled reports whether topology validation is on (default true).
func (spec *ClickHouseRestoreSpec) IsTopologyValidationEnabled() bool {
	if spec == nil || spec.ValidateTopology == nil {
		return true
	}
	return *spec.ValidateTopology
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseRestoreList defines a list of ClickHouseRestore resources.
type ClickHouseRestoreList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseRestore `json:"items" yaml:"items"`
}
