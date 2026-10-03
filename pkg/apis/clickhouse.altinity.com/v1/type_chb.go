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

// ReplicaSelection defines which replicas of a shard are involved in a backup.
type ReplicaSelection string

const (
	// ReplicaSelectionFirstPerShard backs up a single (first) replica per shard.
	// This is correct and storage-efficient for Replicated* table engines, whose
	// data is identical across replicas.
	ReplicaSelectionFirstPerShard ReplicaSelection = "FirstPerShard"
	// ReplicaSelectionAllReplicas backs up every replica of every shard.
	// Required for clusters that hold non-replicated (plain MergeTree) or local
	// Distributed tables, which differ between replicas.
	ReplicaSelectionAllReplicas ReplicaSelection = "AllReplicas"
)

// Backup/restore phases reported in the CR Status.
const (
	BackupPhasePending   = "Pending"
	BackupPhaseRunning   = "Running"
	BackupPhaseCompleted = "Completed"
	BackupPhaseFailed    = "Failed"
)

// Condition types used across backup/restore custom resources.
const (
	// ConditionValidated is True once spec preflight checks have passed.
	ConditionValidated = "Validated"
	// ConditionJobCreated is True once the operator has created the driving Job/CronJob.
	ConditionJobCreated = "JobCreated"
	// ConditionReady is True once the operation finished successfully.
	ConditionReady = "Ready"
	// ConditionVerified is True once a backup has passed verification (spec.verify).
	ConditionVerified = "Verified"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackup defines a one-off backup of a ClickHouseInstallation. The operator
// reconciles it into a Kubernetes Job that triggers `clickhouse-backup` (running as a
// sidecar in the ClickHouse pods) to create and upload a remote backup.
type ClickHouseBackup struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ClickHouseBackupSpec   `json:"spec"             yaml:"spec"`
	Status ClickHouseBackupStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// ClickHouseBackupSpec defines the desired state of a ClickHouseBackup.
type ClickHouseBackupSpec struct {
	// ClickHouseInstallation is the name of the target CHI in the same namespace.
	ClickHouseInstallation string `json:"clickHouseInstallation" yaml:"clickHouseInstallation"`
	// BackupName is the optional explicit remote backup name. When empty the operator
	// generates a deterministic name from the CR name and creation timestamp.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// SchemaOnly backs up table/database definitions only (no data).
	SchemaOnly bool `json:"schemaOnly,omitempty" yaml:"schemaOnly,omitempty"`
	// ReplicaSelection controls which replicas are backed up. Defaults to FirstPerShard.
	ReplicaSelection ReplicaSelection `json:"replicaSelection,omitempty" yaml:"replicaSelection,omitempty"`
	// Image optionally overrides the container image used by the trigger Job.
	Image string `json:"image,omitempty" yaml:"image,omitempty"`
	// ClickHouseCredentialsSecretName optionally references a Secret providing
	// CLICKHOUSE_USER and CLICKHOUSE_PASSWORD for the trigger Job to authenticate.
	ClickHouseCredentialsSecretName string `json:"clickHouseCredentialsSecretName,omitempty" yaml:"clickHouseCredentialsSecretName,omitempty"`
	// Tables optionally restricts the backup to tables matching this clickhouse-backup
	// pattern (e.g. "db.table", "db.*"). Empty backs up everything.
	Tables string `json:"tables,omitempty" yaml:"tables,omitempty"`
	// Partitions optionally restricts the backup to the given partition ids.
	Partitions []string `json:"partitions,omitempty" yaml:"partitions,omitempty"`
	// DiffFromRemote, when set to an existing remote backup name, makes this an
	// incremental backup (clickhouse-backup --diff-from-remote).
	DiffFromRemote string `json:"diffFromRemote,omitempty" yaml:"diffFromRemote,omitempty"`
	// KeepLastRemote, when set, keeps only the N most recent remote backups
	// (clickhouse-backup BACKUPS_TO_KEEP_REMOTE) - older ones are pruned on create.
	KeepLastRemote *int32 `json:"keepLastRemote,omitempty" yaml:"keepLastRemote,omitempty"`
	// Verify, when true, runs a verification job after the backup that downloads the
	// remote backup and checks its integrity (without touching cluster data).
	Verify bool `json:"verify,omitempty" yaml:"verify,omitempty"`
}

// ClickHouseBackupStatus defines the observed state of a ClickHouseBackup.
type ClickHouseBackupStatus struct {
	// Phase is one of Pending, Running, Completed, Failed.
	Phase string `json:"phase,omitempty" yaml:"phase,omitempty"`
	// BackupName is the resolved remote backup name.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// JobName is the name of the Kubernetes Job driving the backup.
	JobName string `json:"jobName,omitempty" yaml:"jobName,omitempty"`
	// StartTime is when the backup Job started.
	StartTime *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the backup Job finished.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// DurationSeconds is the backup duration in seconds (completion - start).
	DurationSeconds int64 `json:"durationSeconds,omitempty" yaml:"durationSeconds,omitempty"`
	// Conditions represent the latest available observations of the backup state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// GetReplicaSelection returns the replica selection, defaulting to FirstPerShard.
func (spec *ClickHouseBackupSpec) GetReplicaSelection() ReplicaSelection {
	if spec == nil || spec.ReplicaSelection == "" {
		return ReplicaSelectionFirstPerShard
	}
	return spec.ReplicaSelection
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackupList defines a list of ClickHouseBackup resources.
type ClickHouseBackupList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseBackup `json:"items" yaml:"items"`
}
