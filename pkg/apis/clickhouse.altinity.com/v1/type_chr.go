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
	"sort"
	"strings"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseRestore defines a one-off restore of a backup into a ClickHouseInstallation.
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
	// Cluster is immutable. Empty restores every backup-enabled cluster.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// BackupName is the backup to restore.
	BackupName string `json:"backupName" yaml:"backupName"`
	// SourceInstallation and SourceNamespace name the installation, and its namespace, that made the backup, when not
	// the restored one, immutable: the restore reads the backup from the storage paths the {installation} and
	// {namespace} macros give there. Empty, each is that of the restored installation.
	SourceInstallation string `json:"sourceInstallation,omitempty" yaml:"sourceInstallation,omitempty"`
	SourceNamespace    string `json:"sourceNamespace,omitempty"    yaml:"sourceNamespace,omitempty"`
	// SchemaOnly restores table/database definitions only (no data).
	SchemaOnly *types.StringBool `json:"schemaOnly,omitempty" yaml:"schemaOnly,omitempty"`
	// Overwrite without Tables recreates the backup databases (restore --rm), dropping tables the backup lacks.
	Overwrite *types.StringBool `json:"overwrite,omitempty" yaml:"overwrite,omitempty"`
	// ValidateTopology, when true (default), refuses the restore unless every target shard
	// holds the backup, so a backup taken from a different layout is not half-applied.
	ValidateTopology *types.StringBool `json:"validateTopology,omitempty" yaml:"validateTopology,omitempty"`
	// Tables are clickhouse-backup patterns such as "db.*". Empty restores all tables.
	Tables []string `json:"tables,omitempty" yaml:"tables,omitempty"`
	// Partitions restores only the data of these partition ids into existing tables, without the schema.
	Partitions []string `json:"partitions,omitempty" yaml:"partitions,omitempty"`
	// SkipTables are clickhouse-backup patterns. System databases are always skipped.
	SkipTables       []string `json:"skipTables,omitempty" yaml:"skipTables,omitempty"`
	SkipTableEngines []string `json:"skipTableEngines,omitempty" yaml:"skipTableEngines,omitempty"`
	// DatabaseMapping maps a backup database (key) to a target database (value).
	DatabaseMapping map[string]string `json:"databaseMapping,omitempty" yaml:"databaseMapping,omitempty"`
	// ConfigOverrides are KEY=VALUE clickhouse-backup env vars for this restore, such as DOWNLOAD_CONCURRENCY=8.
	ConfigOverrides []string `json:"configOverrides,omitempty" yaml:"configOverrides,omitempty"`
	// RBAC also restores the users, roles, grants and other access entities the backup holds. They
	// belong to the whole installation, so only a restore of every cluster may set it.
	RBAC *types.StringBool `json:"rbac,omitempty" yaml:"rbac,omitempty"`
	// ReplicatedCopyToDetached (default true) downloads to every replica, so replicas take parts from detached, not the network.
	ReplicatedCopyToDetached *types.StringBool `json:"replicatedCopyToDetached,omitempty" yaml:"replicatedCopyToDetached,omitempty"`
	// ResumeDownload (default true) makes a spec.taskID rerun resume earlier downloads instead of downloading again.
	ResumeDownload *types.StringBool `json:"resumeDownload,omitempty" yaml:"resumeDownload,omitempty"`
	// ActiveDeadlineSeconds applies per cluster. Default 24 hours.
	ActiveDeadlineSeconds *int64          `json:"activeDeadlineSeconds,omitempty" yaml:"activeDeadlineSeconds,omitempty"`
	Timeouts              *BackupTimeouts `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`
	// A new TaskID reruns a failed or aborted restore on the clusters where it did not complete.
	TaskID *types.Id `json:"taskID,omitempty" yaml:"taskID,omitempty"`
}

// TargetCluster prefers the cluster recorded at validation, so an edit cannot widen a one-cluster restore
// where the API server does not enforce immutability.
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

// Any value except false, even an invalid one, keeps it on, because an edit after validation can set such a value.
func (spec *ClickHouseRestoreSpec) IsTopologyValidationEnabled() bool {
	return (spec == nil) || !spec.ValidateTopology.IsFalse()
}

func (spec *ClickHouseRestoreSpec) IsReplicatedCopyToDetached() bool {
	return (spec == nil) || !spec.ReplicatedCopyToDetached.IsFalse()
}

func (spec *ClickHouseRestoreSpec) IsResumeDownload() bool {
	return (spec == nil) || !spec.ResumeDownload.IsFalse()
}

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
	if err := validateTables("tables", spec.Tables); err != nil {
		return err
	}
	if err := validateRestoreFilters(spec); err != nil {
		return err
	}
	if err := ValidateConfigOverrides("configOverrides", spec.ConfigOverrides); err != nil {
		return err
	}
	// A partitions restore skips the schema step, which also restores the access entities.
	if len(spec.Partitions) > 0 && spec.SchemaOnly.IsTrue() {
		return fmt.Errorf("%w: partitions restore data, which schemaOnly leaves out", ErrInvalidBackupSpec)
	}
	if len(spec.Partitions) > 0 && spec.RBAC.IsTrue() {
		return fmt.Errorf("%w: partitions restore no schema, which the access entities rbac asks for are restored with",
			ErrInvalidBackupSpec)
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
		{"replicatedCopyToDetached", spec.ReplicatedCopyToDetached},
		{"resumeDownload", spec.ResumeDownload},
	} {
		if err := validateStringBool(field.name, field.value); err != nil {
			return err
		}
	}
	if err := spec.Timeouts.Validate(); err != nil {
		return err
	}
	return validateActiveDeadlineSeconds(spec.ActiveDeadlineSeconds)
}

// ClickHouseRestoreStatus defines the observed state of a ClickHouseRestore.
type ClickHouseRestoreStatus struct {
	Phase OperationPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// BackupName is recorded when the restore starts, so a later spec edit does not change it.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// Cluster is the cluster spec.cluster named when the restore was validated, empty for a restore of every
	// backup-enabled cluster. What the restore holds and whether it restores access entities follow it,
	// whatever the spec names later.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// Clusters are the clusters the restore targets, resolved once it is validated, and its state on each.
	// +listType=map
	// +listMapKey=name
	Clusters  []OperationClusterStatus `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	StartTime *meta.Time               `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the restore finished on every cluster.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// DurationSeconds is the restore duration in seconds (completion - start).
	DurationSeconds int64 `json:"durationSeconds,omitempty" yaml:"durationSeconds,omitempty"`
	// Conditions represent the latest available observations of the restore state.
	Conditions   []meta.Condition    `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	PreviousRuns []BackupPreviousRun `json:"previousRuns,omitempty" yaml:"previousRuns,omitempty"`

	BackupTaskStatus `json:",inline" yaml:",inline"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseRestoreList defines a list of ClickHouseRestore resources.
type ClickHouseRestoreList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseRestore `json:"items" yaml:"items"`
}

// Each value must stay one token of a clickhouse-backup command.
func validateRestoreFilters(spec *ClickHouseRestoreSpec) error {
	for _, partition := range spec.Partitions {
		if !backupPartitionRegexp.MatchString(partition) {
			return fmt.Errorf("%w: partition %q must match %s", ErrInvalidBackupSpec, partition, backupPartitionRegexp)
		}
	}
	if err := validateTables("skipTables", spec.SkipTables); err != nil {
		return err
	}
	if err := validateEngines("skipTableEngines", spec.SkipTableEngines); err != nil {
		return err
	}
	targets := map[string]string{}
	for source, target := range spec.DatabaseMapping {
		for _, name := range []string{source, target} {
			if !backupDatabaseRegexp.MatchString(name) || restoreSystemDatabases[strings.ToLower(name)] {
				return fmt.Errorf("%w: databaseMapping %q: %q must match %s and name no system database", ErrInvalidBackupSpec,
					source+":"+target, name, backupDatabaseRegexp)
			}
		}
		if source == target {
			return fmt.Errorf("%w: databaseMapping maps %q to itself", ErrInvalidBackupSpec, source)
		}
		if other, taken := targets[target]; taken {
			first, second := other, source
			if second < first {
				first, second = second, first
			}
			return fmt.Errorf("%w: databaseMapping maps both %q and %q to %q", ErrInvalidBackupSpec, first, second, target)
		}
		targets[target] = source
	}
	for source := range spec.DatabaseMapping {
		if _, mapped := targets[source]; mapped {
			return fmt.Errorf("%w: databaseMapping maps %q, which another database is mapped to", ErrInvalidBackupSpec, source)
		}
	}
	return nil
}

func (spec *ClickHouseRestoreSpec) GetDatabaseMapping() string {
	if spec == nil {
		return ""
	}
	pairs := make([]string, 0, len(spec.DatabaseMapping))
	for source, target := range spec.DatabaseMapping {
		pairs = append(pairs, source+":"+target)
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}
