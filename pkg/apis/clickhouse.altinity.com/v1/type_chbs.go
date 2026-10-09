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

// ClickHouseBackupSchedule defines a recurring backup of a ClickHouseInstallation.
// The operator reconciles it into a Kubernetes CronJob that triggers `clickhouse-backup`
// on the configured schedule. Native CronJob semantics handle scheduling, suspension,
// concurrency and job history; remote retention is delegated to clickhouse-backup
// (BACKUPS_TO_KEEP_REMOTE in the sidecar).
type ClickHouseBackupSchedule struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ClickHouseBackupScheduleSpec   `json:"spec"             yaml:"spec"`
	Status ClickHouseBackupScheduleStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// ClickHouseBackupScheduleSpec defines the desired state of a ClickHouseBackupSchedule.
type ClickHouseBackupScheduleSpec struct {
	// ClickHouseInstallation is the name of the target CHI in the same namespace.
	ClickHouseInstallation string `json:"clickHouseInstallation" yaml:"clickHouseInstallation"`
	// Schedule is a cron expression in standard Kubernetes CronJob format.
	Schedule string `json:"schedule" yaml:"schedule"`
	// Suspend pauses creation of new backup jobs. Existing jobs are unaffected.
	Suspend *bool `json:"suspend,omitempty" yaml:"suspend,omitempty"`
	// ConcurrencyPolicy controls how concurrent executions are treated.
	// One of Forbid (default), Allow, Replace.
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty" yaml:"concurrencyPolicy,omitempty"`
	// StartingDeadlineSeconds is the deadline in seconds for starting a missed job.
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty" yaml:"startingDeadlineSeconds,omitempty"`
	// SuccessfulJobsHistoryLimit is how many successful finished jobs to retain. Default 3.
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty" yaml:"successfulJobsHistoryLimit,omitempty"`
	// FailedJobsHistoryLimit is how many failed finished jobs to retain. Default 1.
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty" yaml:"failedJobsHistoryLimit,omitempty"`
	// BackupTemplate is the backup specification stamped out on each scheduled run.
	BackupTemplate ClickHouseBackupTemplateSpec `json:"backupTemplate,omitempty" yaml:"backupTemplate,omitempty"`
}

// ClickHouseBackupTemplateSpec is the subset of backup options applied to each scheduled backup.
type ClickHouseBackupTemplateSpec struct {
	// BackupNamePrefix is prepended to the generated, timestamped backup name.
	BackupNamePrefix string `json:"backupNamePrefix,omitempty" yaml:"backupNamePrefix,omitempty"`
	// SchemaOnly backs up table/database definitions only (no data).
	SchemaOnly bool `json:"schemaOnly,omitempty" yaml:"schemaOnly,omitempty"`
	// ReplicaSelection controls which replicas are backed up. Defaults to FirstPerShard.
	ReplicaSelection ReplicaSelection `json:"replicaSelection,omitempty" yaml:"replicaSelection,omitempty"`
	// Image optionally overrides the container image used by the trigger Job.
	Image string `json:"image,omitempty" yaml:"image,omitempty"`
	// ClickHouseCredentialsSecretName optionally references a Secret providing
	// CLICKHOUSE_USER and CLICKHOUSE_PASSWORD for the trigger Job to authenticate.
	ClickHouseCredentialsSecretName string `json:"clickHouseCredentialsSecretName,omitempty" yaml:"clickHouseCredentialsSecretName,omitempty"`
	// Tables optionally restricts scheduled backups to tables matching this pattern.
	Tables string `json:"tables,omitempty" yaml:"tables,omitempty"`
	// Partitions optionally restricts scheduled backups to the given partition ids.
	Partitions []string `json:"partitions,omitempty" yaml:"partitions,omitempty"`
	// KeepLastRemote keeps only the N most recent remote backups (retention).
	KeepLastRemote *int32 `json:"keepLastRemote,omitempty" yaml:"keepLastRemote,omitempty"`
}

// ClickHouseBackupScheduleStatus defines the observed state of a ClickHouseBackupSchedule.
type ClickHouseBackupScheduleStatus struct {
	// CronJobName is the name of the managed Kubernetes CronJob.
	CronJobName string `json:"cronJobName,omitempty" yaml:"cronJobName,omitempty"`
	// LastScheduleTime is the last time a backup job was scheduled.
	LastScheduleTime *meta.Time `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	// Conditions represent the latest available observations of the schedule state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// GetReplicaSelection returns the replica selection, defaulting to FirstPerShard.
func (spec *ClickHouseBackupTemplateSpec) GetReplicaSelection() ReplicaSelection {
	if spec == nil || spec.ReplicaSelection == "" {
		return ReplicaSelectionFirstPerShard
	}
	return spec.ReplicaSelection
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackupScheduleList defines a list of ClickHouseBackupSchedule resources.
type ClickHouseBackupScheduleList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseBackupSchedule `json:"items" yaml:"items"`
}
