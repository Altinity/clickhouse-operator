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
	"fmt"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseRestore defines a one-off restore of a remote backup into a ClickHouseInstallation.
// The operator reconciles it into a Kubernetes Job per cluster that restores the schema on every
// replica of the cluster and the data on the first replica of each shard, from which native
// replication fills the remaining replicas.
//
// Before anything is touched the Job checks that every target shard holds the backup and, by
// default, refuses to restore over a non-empty target. Restoring into a fresh, empty CHI is the
// recommended path.
type ClickHouseRestore struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ClickHouseRestoreSpec   `json:"spec"             yaml:"spec"`
	Status ClickHouseRestoreStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// ClickHouseRestoreSpec defines the desired state of a ClickHouseRestore.
type ClickHouseRestoreSpec struct {
	// ClickHouseInstallation is the name of the target CHI in the same namespace, immutable.
	// It is strongly recommended this be a fresh, empty installation.
	ClickHouseInstallation string `json:"clickHouseInstallation" yaml:"clickHouseInstallation"`
	// Cluster optionally restricts the restore to one cluster of the CHI, immutable. Empty restores
	// every backup-enabled cluster, each in a Job of its own.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// BackupName is the remote backup to restore.
	BackupName string `json:"backupName" yaml:"backupName"`
	// SourceInstallation and SourceNamespace name the installation, and its namespace, that made the backup, when not
	// the restored one, immutable: the restore reads the backup from the remote paths the {installation} and
	// {namespace} macros give there. Empty, each is that of the restored installation.
	SourceInstallation string `json:"sourceInstallation,omitempty" yaml:"sourceInstallation,omitempty"`
	SourceNamespace    string `json:"sourceNamespace,omitempty"    yaml:"sourceNamespace,omitempty"`
	// SchemaOnly restores table/database definitions only (no data).
	SchemaOnly *types.StringBool `json:"schemaOnly,omitempty" yaml:"schemaOnly,omitempty"`
	// Overwrite allows restoring over existing tables, which are dropped first. When false
	// (default) the restore is refused if any target host already holds user tables.
	Overwrite *types.StringBool `json:"overwrite,omitempty" yaml:"overwrite,omitempty"`
	// ValidateTopology, when true (default), refuses the restore unless every target shard
	// holds the backup, so a backup taken from a different layout is not half-applied.
	ValidateTopology *types.StringBool `json:"validateTopology,omitempty" yaml:"validateTopology,omitempty"`
	// Tables optionally restricts the restore to tables matching this clickhouse-backup pattern
	// (e.g. "db.table", "db.*"), in the schema and the data alike. Empty restores everything.
	Tables string `json:"tables,omitempty" yaml:"tables,omitempty"`
	// RBAC also restores the users, roles, grants and other access entities the backup holds. They
	// belong to the whole installation, so only a restore of every cluster may set it.
	RBAC *types.StringBool `json:"rbac,omitempty" yaml:"rbac,omitempty"`
	// ActiveDeadlineSeconds bounds how long the restore Job may run. Defaults to 24 hours.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" yaml:"activeDeadlineSeconds,omitempty"`
}

// TargetCluster returns the cluster the restore targets, "" for every backup-enabled cluster: the one
// recorded when it was validated, else the one its spec names. The spec is immutable where the API
// server enforces it; the record keeps the target where it does not, so that an edit cannot turn a
// restore of one cluster into one of every cluster, which restores access entities and holds every
// schedule. The installation needs no record: an edit of it moves what the restore holds and starts to
// the other installation as one, while the Jobs it runs already keep the schedules of their own cluster
// held, and other operations off it, by their labels. Nil-safe.
func (chr *ClickHouseRestore) TargetCluster() string {
	switch {
	case chr == nil:
		return ""
	case len(chr.Status.Clusters) > 0:
		return chr.Status.Cluster
	}
	return chr.Spec.Cluster
}

// LocalSource returns the installation of the restore's own namespace whose backup the restore reads when it is
// not the restored one, empty when the restore reads the backup of the restored installation, or of one in
// another namespace. Nil-safe.
func (chr *ClickHouseRestore) LocalSource() string {
	switch {
	case chr == nil:
		return ""
	case (chr.Spec.SourceNamespace != "") && (chr.Spec.SourceNamespace != chr.GetNamespace()):
		return ""
	case chr.Spec.SourceInstallation == chr.Spec.ClickHouseInstallation:
		return ""
	}
	return chr.Spec.SourceInstallation
}

// IsTopologyValidationEnabled reports whether topology validation is on (default true). Anything but a false value,
// one that is no boolean included, leaves it on: a Job is built from the spec as it is then, which an edit after the
// validation of the restore may have left holding such a value.
func (spec *ClickHouseRestoreSpec) IsTopologyValidationEnabled() bool {
	return (spec == nil) || !spec.ValidateTopology.IsFalse()
}

// GetActiveDeadlineSeconds returns the Job deadline, defaulting to 24 hours.
func (spec *ClickHouseRestoreSpec) GetActiveDeadlineSeconds() int64 {
	if spec == nil {
		return defaultBackupActiveDeadlineSeconds
	}
	return activeDeadlineSeconds(spec.ActiveDeadlineSeconds)
}

// Validate checks the spec against the allowed character sets and values.
func (spec *ClickHouseRestoreSpec) Validate() error {
	if spec == nil {
		return nil
	}
	if spec.ClickHouseInstallation == "" {
		return fmt.Errorf("%w: clickHouseInstallation is required", ErrInvalidBackupSpec)
	}
	if spec.BackupName == "" {
		return fmt.Errorf("%w: backupName is required", ErrInvalidBackupSpec)
	}
	if err := validateBackupName("backupName", spec.BackupName, BackupNameMaxLength); err != nil {
		return err
	}
	if err := validateTables(spec.Tables); err != nil {
		return err
	}
	if err := validateBackupSource("sourceInstallation", spec.SourceInstallation, BackupSourceInstallationMaxLength); err != nil {
		return err
	}
	if err := validateBackupSource("sourceNamespace", spec.SourceNamespace, BackupSourceNamespaceMaxLength); err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value *types.StringBool
	}{
		{"schemaOnly", spec.SchemaOnly},
		{"overwrite", spec.Overwrite},
		{"validateTopology", spec.ValidateTopology},
		{"rbac", spec.RBAC},
	} {
		if err := validateStringBool(field.name, field.value); err != nil {
			return err
		}
	}
	return validateActiveDeadlineSeconds(spec.ActiveDeadlineSeconds)
}

// ClickHouseRestoreStatus defines the observed state of a ClickHouseRestore.
type ClickHouseRestoreStatus struct {
	// Phase is one of Pending, Running, Completed, Failed.
	Phase OperationPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// BackupName is the remote backup the restore Jobs restore, as it was when the first Job was created.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// Cluster is the cluster spec.cluster named when the restore was validated, empty for a restore of every
	// backup-enabled cluster. What the restore holds and whether it restores access entities follow it,
	// whatever the spec names later.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// Clusters are the clusters the restore targets, resolved once it is validated, and its state on each.
	// +listType=map
	// +listMapKey=name
	Clusters []OperationClusterStatus `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	// StartTime is when the first restore Job started.
	StartTime *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the restore finished on every cluster.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// DurationSeconds is the restore duration in seconds (completion - start).
	DurationSeconds int64 `json:"durationSeconds,omitempty" yaml:"durationSeconds,omitempty"`
	// Conditions represent the latest available observations of the restore state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseRestoreList defines a list of ClickHouseRestore resources.
type ClickHouseRestoreList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseRestore `json:"items" yaml:"items"`
}
