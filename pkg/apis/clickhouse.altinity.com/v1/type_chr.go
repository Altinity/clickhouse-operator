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
// The operator drives the clickhouse-backup sidecars of every cluster it targets through SQL: it restores the schema
// on every replica of the cluster and the data on the first replica of each shard, from which native replication
// fills the remaining replicas, from the copies of the data they made in their detached directories by default.
//
// Before anything is touched the operator checks that every target shard holds the backup and, by
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
	// every backup-enabled cluster.
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
	// Overwrite, with Tables empty, recreates the databases of the backup whole (clickhouse-backup restore --rm),
	// dropping their tables the backup does not hold. With Tables, or when false (default), only the tables of the
	// backup are recreated - clickhouse-backup drops those that exist anyway - and every other table is left alone.
	Overwrite *types.StringBool `json:"overwrite,omitempty" yaml:"overwrite,omitempty"`
	// ValidateTopology, when true (default), refuses the restore unless every target shard
	// holds the backup, so a backup taken from a different layout is not half-applied.
	ValidateTopology *types.StringBool `json:"validateTopology,omitempty" yaml:"validateTopology,omitempty"`
	// Tables optionally restricts the restore to tables matching these clickhouse-backup patterns, one per item
	// (e.g. "db.table", "db.*"), joined by commas, in the schema and the data alike. Empty restores everything.
	Tables []string `json:"tables,omitempty" yaml:"tables,omitempty"`
	// Partitions optionally restricts the restore to the data of the given partition ids of the tables
	// (clickhouse-backup --partitions), downloaded and attached into the tables as they are, as the Altinity usual
	// backup workflow restores partitions: the schema is not restored, nothing is dropped but those partitions, which
	// clickhouse-backup drops before it attaches them, and the tables must exist. Not with schemaOnly or rbac.
	Partitions []string `json:"partitions,omitempty" yaml:"partitions,omitempty"`
	// SkipTables optionally leaves out of the restore the tables matching these clickhouse-backup patterns, one per
	// item (e.g. "db.table", "db.*"), on top of the system databases (CLICKHOUSE_SKIP_TABLES).
	SkipTables []string `json:"skipTables,omitempty" yaml:"skipTables,omitempty"`
	// SkipTableEngines optionally leaves out of the restore the tables of these engines, e.g. Kafka
	// (CLICKHOUSE_SKIP_TABLE_ENGINES).
	SkipTableEngines []string `json:"skipTableEngines,omitempty" yaml:"skipTableEngines,omitempty"`
	// DatabaseMapping optionally restores the tables of a database of the backup, the key, into another database,
	// the value (clickhouse-backup --restore-database-mapping).
	DatabaseMapping map[string]string `json:"databaseMapping,omitempty" yaml:"databaseMapping,omitempty"`
	// RBAC also restores the users, roles, grants and other access entities the backup holds. They
	// belong to the whole installation, so only a restore of every cluster may set it.
	RBAC *types.StringBool `json:"rbac,omitempty" yaml:"rbac,omitempty"`
	// ReplicatedCopyToDetached, when true (default), restores the data on every replica of a shard: its replicas but the
	// first copy the parts of the backup into the detached directories of their Replicated*MergeTree tables
	// (clickhouse-backup restore --data --replicated-copy-to-detached), all at once, then the first replica attaches
	// them, and the other replicas take the parts replication tells them of from their own detached directories instead
	// of fetching them over the network. False downloads the schema alone on the replicas but the first of each shard,
	// which restores the data, and leaves the other replicas to fetch it by replication: less disk and download, more
	// replication traffic.
	ReplicatedCopyToDetached *types.StringBool `json:"replicatedCopyToDetached,omitempty" yaml:"replicatedCopyToDetached,omitempty"`
	// ResumeDownload, when true (default), has a rerun of the restore by spec.taskID resume the downloads its earlier
	// runs left on each host (download --hardlink-exists-files), from another installation too. False has a rerun start
	// over: it deletes the local copies of the backup its earlier runs downloaded and downloads the backup again. The
	// first run of the restore is the same either way.
	ResumeDownload *types.StringBool `json:"resumeDownload,omitempty" yaml:"resumeDownload,omitempty"`
	// ActiveDeadlineSeconds bounds how long the restore may run on a cluster. Defaults to 24 hours.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" yaml:"activeDeadlineSeconds,omitempty"`
	// Timeouts bound how the operator waits for the commands of the restore on the sidecars. A field left unset is that
	// of the operator configuration.
	Timeouts *BackupTimeouts `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`
	// TaskID reruns a restore that failed or was aborted when it changes to a value the status does not record: the
	// restore runs again on the clusters it did not complete on, resuming the downloads it made there. A completed
	// restore, or one that runs, is not rerun.
	TaskID *types.Id `json:"taskID,omitempty" yaml:"taskID,omitempty"`
}

// TargetCluster returns the cluster the restore targets, "" for every backup-enabled cluster: the one
// recorded when it was validated, else the one its spec names. The spec is immutable where the API
// server enforces it; the record keeps the target where it does not, so that an edit cannot turn a
// restore of one cluster into one of every cluster, which restores access entities and holds every
// schedule. The installation needs no record: an edit of it moves what the restore holds and starts to
// the other installation as one, while the runs it started already keep the schedules of their own cluster
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
// one that is no boolean included, leaves it on: a run starts from the spec as it is then, which an edit after the
// validation of the restore may have left holding such a value.
func (spec *ClickHouseRestoreSpec) IsTopologyValidationEnabled() bool {
	return (spec == nil) || !spec.ValidateTopology.IsFalse()
}

// IsReplicatedCopyToDetached reports whether the replicas of a shard but the one that restores its data copy the data
// into their detached directories (default true). Anything but a false value leaves it on, as for validateTopology.
func (spec *ClickHouseRestoreSpec) IsReplicatedCopyToDetached() bool {
	return (spec == nil) || !spec.ReplicatedCopyToDetached.IsFalse()
}

// IsResumeDownload reports whether a rerun of the restore resumes the downloads of its earlier runs (default true),
// rather than deleting them to download the backup again. Anything but a false value leaves it on, as for
// validateTopology.
func (spec *ClickHouseRestoreSpec) IsResumeDownload() bool {
	return (spec == nil) || !spec.ResumeDownload.IsFalse()
}

// GetActiveDeadlineSeconds returns the deadline of the restore, defaulting to 24 hours.
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
	// A restore of partitions attaches their data into the tables as they are, without the schema step, which
	// restores the access entities too
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
	// Phase is one of Pending, Running, Completed, Failed, Aborted.
	Phase OperationPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// BackupName is the backup the restore restores, as it was when it started on its first cluster.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// Cluster is the cluster spec.cluster named when the restore was validated, empty for a restore of every
	// backup-enabled cluster. What the restore holds and whether it restores access entities follow it,
	// whatever the spec names later.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// Clusters are the clusters the restore targets, resolved once it is validated, and its state on each.
	// +listType=map
	// +listMapKey=name
	Clusters []OperationClusterStatus `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	// StartTime is when the restore started on its first cluster.
	StartTime *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the restore finished on every cluster.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// DurationSeconds is the restore duration in seconds (completion - start).
	DurationSeconds int64 `json:"durationSeconds,omitempty" yaml:"durationSeconds,omitempty"`
	// Conditions represent the latest available observations of the restore state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	// PreviousRuns are what the runs of the restore did on the clusters spec.taskID reran it on: the rerun resumes the
	// downloads they made, which a restore from another installation would otherwise refuse as local copies of the
	// backup it did not download.
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

// validateRestoreFilters checks the partitions, the tables and engines left out, and the database mapping of a
// restore: every value becomes a single token of a clickhouse-backup command of the restore.
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

// GetDatabaseMapping returns the database mapping as clickhouse-backup takes it, "source:target" pairs separated by
// commas, sorted, empty for none, nil-safe
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
