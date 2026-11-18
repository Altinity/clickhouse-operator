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
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// ReplicaSelection defines which replicas of a shard are involved in a backup.
type ReplicaSelection string

const (
	// ReplicaSelectionFirstPerShard backs up a single (first) replica per shard.
	// This is correct and storage-efficient for Replicated* table engines, whose
	// data is identical across replicas.
	ReplicaSelectionFirstPerShard ReplicaSelection = "FirstPerShard"
	// ReplicaSelectionAllReplicas backs up every replica of every shard. A shard counts as backed up once any of its
	// replicas made the backup.
	// Required for clusters that hold non-replicated (plain MergeTree) or local
	// Distributed tables, which differ between replicas.
	ReplicaSelectionAllReplicas ReplicaSelection = "AllReplicas"
)

// OperationPhase is the lifecycle phase of a backup or a restore.
type OperationPhase string

// Backup/restore phases reported in the CR Status.
const (
	OperationPhasePending   OperationPhase = "Pending"
	OperationPhaseRunning   OperationPhase = "Running"
	OperationPhaseCompleted OperationPhase = "Completed"
	OperationPhaseFailed    OperationPhase = "Failed"
	// OperationPhaseAborted: the operator stopped the operation, or never started it, for a reason other than a
	// failure of the operation itself: an invalid spec, a missing or stopped installation, a deadline exceeded.
	OperationPhaseAborted OperationPhase = "Aborted"
)

// IsTerminal reports whether the operation has finished, successfully or not.
func (p OperationPhase) IsTerminal() bool {
	return (p == OperationPhaseCompleted) || (p == OperationPhaseFailed) || (p == OperationPhaseAborted)
}

// OperationClusterStatus is the observed state of a backup or a restore on one of the clusters it
// targets. The operator runs the operation on every cluster on its own, driving the clickhouse-backup sidecars of the
// cluster through SQL, see BackupRunStatus.
type OperationClusterStatus struct {
	// Name is the name of the cluster.
	Name string `json:"name" yaml:"name"`
	// Phase is one of Pending, Running, Completed, Failed, Aborted.
	Phase OperationPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// Hosts are the hosts of the cluster the operation drives.
	Hosts []string `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	// ConfigSecret is the Secret of the clickhouse-backup configuration of the cluster the operation keeps the backup
	// by, or reads it by, as it started: it tells the storage the backup lies in.
	ConfigSecret string `json:"configSecret,omitempty" yaml:"configSecret,omitempty"`
	// StartTime is when the operation started on the cluster.
	StartTime *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the operation finished on the cluster.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// Reason tells, in one word, why the cluster waits, or how its operation went.
	Reason string `json:"reason,omitempty" yaml:"reason,omitempty"`
	// Message tells the same in a sentence.
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
	// Run is the state of the run of the operation on the cluster, which the operator resumes after a restart.
	Run *BackupRunStatus `json:"run,omitempty" yaml:"run,omitempty"`
}

// Operations of a BackupRunStatus.
const (
	BackupRunOperationBackup  = "backup"
	BackupRunOperationRestore = "restore"
)

// BackupRunStatus is the state of a run of a backup or a restore on one cluster: the operator drives
// the clickhouse-backup sidecars of the cluster through the integration tables system.backup_actions and
// system.backup_list of their ClickHouse, and records here what it needs to resume the run after a restart.
type BackupRunStatus struct {
	// Operation is what the run does: backup or restore.
	Operation string `json:"operation" yaml:"operation"`
	// Step is the step the run is at.
	Step string `json:"step,omitempty" yaml:"step,omitempty"`
	// Mutated tells that a command that changes something may have reached a sidecar.
	Mutated bool `json:"mutated,omitempty" yaml:"mutated,omitempty"`
	// Hosts are the hosts the run works on, with the name of the backup on each.
	Hosts []BackupRunHost `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	// Actions are the commands the run inserted into system.backup_actions, the last one of every host and step.
	Actions []BackupRunAction `json:"actions,omitempty" yaml:"actions,omitempty"`
	// Plan holds what the run decided before it changed anything, which the steps after a restart follow.
	Plan map[string]string `json:"plan,omitempty" yaml:"plan,omitempty"`
}

// BackupRunHost is a host a run works on.
type BackupRunHost struct {
	// Host is the host name.
	Host string `json:"host" yaml:"host"`
	// Backup is the name of the backup on the host.
	Backup string `json:"backup,omitempty" yaml:"backup,omitempty"`
	// Base is the backup the backup on the host is incremental from, empty for a full one.
	Base string `json:"base,omitempty" yaml:"base,omitempty"`
}

// BackupRunAction is a command a run inserted into system.backup_actions of a host.
type BackupRunAction struct {
	// Step is the step of the run the command belongs to.
	Step string `json:"step" yaml:"step"`
	// Host is the host whose system.backup_actions the command was inserted into.
	Host string `json:"host" yaml:"host"`
	// Command is the clickhouse-backup command.
	Command string `json:"command" yaml:"command"`
	// TimeFrom is the time of the ClickHouse of the host, UTC "YYYY-MM-DD hh:mm:ss", just before the command was
	// inserted: the row of the command is its first row that started then or later.
	TimeFrom string `json:"timeFrom" yaml:"timeFrom"`
	// Status is the status of the command as the sidecar last told it: in progress, success or error.
	Status string `json:"status,omitempty" yaml:"status,omitempty"`
	// Error is the error the sidecar told.
	Error string `json:"error,omitempty" yaml:"error,omitempty"`
}

// Condition types used across backup/restore custom resources.
const (
	// BackupConditionValidated is True once spec preflight checks have passed.
	BackupConditionValidated = "Validated"
	// BackupConditionStarted is True once the operator has started the operation on its clusters.
	BackupConditionStarted = "Started"
	// BackupConditionReady is True once the operation finished successfully.
	BackupConditionReady = "Ready"
	// BackupConditionLastBackupSucceeded tells whether the last finished run of a schedule succeeded.
	BackupConditionLastBackupSucceeded = "LastBackupSucceeded"
)

// Condition reasons used across backup/restore custom resources.
const (
	// BackupReasonValidated: the spec and the target installation passed validation.
	BackupReasonValidated = "Validated"
	// BackupReasonCHIRecovered: the spec and the target installation passed validation,
	// and the installation was restored from a backup, so its storage path may be the one
	// of the installation the backup came from.
	BackupReasonCHIRecovered = "CHIRecovered"
	// BackupReasonInvalidSpec: the spec failed validation, or that of the schedule a backup runs.
	BackupReasonInvalidSpec = "InvalidSpec"
	// BackupReasonCHINotFound: the target installation does not exist.
	BackupReasonCHINotFound = "CHINotFound"
	// BackupReasonScheduleNotFound: the schedule a backup runs does not exist.
	BackupReasonScheduleNotFound = "ScheduleNotFound"
	// BackupReasonWaitingForScheduledTime: the backup waits for the time it is scheduled at.
	BackupReasonWaitingForScheduledTime = "WaitingForScheduledTime"
	// BackupReasonStartingDeadlineExceeded: the backup did not start within startingDeadlineSeconds of the
	// time it was due.
	BackupReasonStartingDeadlineExceeded = "StartingDeadlineExceeded"
	// BackupReasonCHINotReady: the target installation has not completed its reconcile.
	BackupReasonCHINotReady = "CHINotReady"
	// BackupReasonCHIStopped: the target installation is stopped, so its hosts are unreachable.
	BackupReasonCHIStopped = "CHIStopped"
	// BackupReasonClusterNotFound: the target cluster does not exist in the installation, or is not one the schedule a
	// backup runs backs up.
	BackupReasonClusterNotFound = "ClusterNotFound"
	// BackupReasonBackupDisabled: backup is not enabled on the target cluster(s).
	BackupReasonBackupDisabled = "BackupDisabled"
	// BackupReasonNoBackupStorage: the target cluster(s) turn backup on without the settings of a
	// storage, neither configSecretRef nor podTemplate, which leaves their backup off.
	BackupReasonNoBackupStorage = "NoBackupStorage"
	// BackupReasonNoTargetHosts: the target resolves to no hosts.
	BackupReasonNoTargetHosts = "NoTargetHosts"
	// BackupReasonNoClickHouseAccess: the operator configuration sets no ClickHouse credentials, clickhouse.access, which
	// the operator drives clickhouse-backup with through SQL: backup stays off until it does.
	BackupReasonNoClickHouseAccess = "NoClickHouseAccess"
	// BackupReasonOperationInProgress: another operation of the cluster runs (a backup or a restore), or, for a
	// backup, a restore of access entities, which waits for every cluster of the
	// installation; a restore also waits for a scheduled run of the installation whose backup it reads.
	BackupReasonOperationInProgress = "OperationInProgress"
	// BackupReasonWaitingForGarbageCollection: a ClickHouseRestore of the name a recovery needs belongs to another
	// owner, typically a deleted predecessor of the same name, and awaits garbage collection.
	BackupReasonWaitingForGarbageCollection = "WaitingForGarbageCollection"
	// BackupReasonNameConflict: a ClickHouseRestore of the name a recovery needs on a cluster belongs to another
	// resource, which no garbage collection removes: the joined names of two resources and their clusters coincide.
	// The recovery cannot run on that cluster until one of them is renamed.
	BackupReasonNameConflict = "NameConflict"
	// BackupReasonWaitingForSchedules: a restore waits until every schedule it holds - of the clusters it restores, or of
	// every cluster for access entities, and of the clusters of the installation whose backup it reads - holds its runs.
	BackupReasonWaitingForSchedules = "WaitingForSchedules"
	// BackupReasonStarted: the operation started on a cluster, or, on an installation, the ClickHouseRestore that
	// recovers it was created.
	BackupReasonStarted = "Started"
	// BackupReasonCompleted: the operation finished successfully.
	BackupReasonCompleted = "Completed"
	// BackupReasonPartiallyCompleted: the operation finished on every cluster it targets, successfully
	// on some of them and not on others.
	BackupReasonPartiallyCompleted = "PartiallyCompleted"
	// BackupReasonRBACNotRestored: a restore finished successfully, and the backup holds users, roles,
	// grants or other access entities it did not restore.
	BackupReasonRBACNotRestored = "RBACNotRestored"
	// BackupReasonCompletedWithoutData: a backup finished successfully and holds no data on any host:
	// its tables pattern matches no table, or none of the tables it backs up holds data.
	BackupReasonCompletedWithoutData = "CompletedWithoutData"
	// BackupReasonFailed: the operation failed on a cluster.
	BackupReasonFailed = "Failed"
	// BackupReasonFailedUnchanged: the operation failed on a cluster before it changed anything: a check failed, or
	// no sidecar can have received a command that changes something.
	BackupReasonFailedUnchanged = "FailedUnchanged"
	// BackupReasonStoragePathConflict: the storage path check refused the backup before anything was sent: two writers
	// of a backup of one name at once - two replicas of a shard of a backup of every replica, two shards, or two
	// backup-enabled clusters of the installation - would write it at one storage path or object disk path of one
	// storage, every path expanded with the macros of each host; or the check cannot tell the paths of the target
	// cluster beside other backup-enabled clusters. Aborts the backup, and the schedule until its storage settings
	// change.
	BackupReasonStoragePathConflict = "StoragePathConflict"
	// BackupReasonDeadlineExceeded: the operation ran past activeDeadlineSeconds.
	BackupReasonDeadlineExceeded = "DeadlineExceeded"
	// BackupReasonScheduled: the schedule runs.
	BackupReasonScheduled = "Scheduled"
	// BackupReasonLastBackupFailed: the last finished run of the schedule failed. The schedule runs on.
	BackupReasonLastBackupFailed = "LastBackupFailed"
	// BackupReasonLastBackupWithoutData: the last finished run of the schedule succeeded and made a
	// backup that holds no data. The schedule runs on.
	BackupReasonLastBackupWithoutData = "LastBackupWithoutData"
	// BackupReasonSuspended: the schedule is suspended by its spec.
	BackupReasonSuspended = "Suspended"
	// BackupReasonSuspendedForRestore: the schedule is held on a cluster while a restore of the cluster
	// is validated and about to start, or runs, while the cluster waits to be restored from the backup
	// spec.backup.recovery names, and while a restore of another installation of the namespace that
	// reads the backup of the cluster may start, or runs.
	BackupReasonSuspendedForRestore = "SuspendedForRestore"
	// BackupReasonSuspendedForFailedRestore: the schedule is held on a cluster by a restore of the
	// cluster that failed and may have left it partly restored, until that restore is deleted.
	BackupReasonSuspendedForFailedRestore = "SuspendedForFailedRestore"
	// BackupReasonSharesSourcePrefix: the schedule is held because the installation was restored from
	// a backup carrying the backup name prefix the schedule defaults to: the installation that made the
	// backup may run a schedule of the same name on the same storage path, and the two would
	// overwrite and prune each other's backups. Setting spec.backupNamePrefix releases it.
	BackupReasonSharesSourcePrefix = "SharesSourcePrefix"
	// BackupReasonUnprunedHosts: hosts left the hosts a schedule with retention backs up, so
	// retention no longer prunes the backups they made. Only the event that announces it
	// carries this reason; the schedule's status keeps the hosts.
	BackupReasonUnprunedHosts = "UnprunedHosts"
	// BackupReasonRetentionHeld: a change of the entries or the time zone of a schedule is held, its retention
	// keeping every backup the schedule kept before it until it expires as that schedule would have deleted it, or
	// until spec.retention.releaseHeld releases it. Only the event that announces it carries this reason; the
	// schedule's status keeps the change.
	BackupReasonRetentionHeld = "RetentionHeld"
	// BackupReasonRetentionHeldExpired: every backup a held change of a schedule held has expired, and the change
	// left status.retention.previous. Only the event that announces it carries this reason.
	BackupReasonRetentionHeldExpired = "RetentionHeldExpired"
)

const (
	// defaultBackupActiveDeadlineSeconds bounds a backup or a restore when the spec sets no deadline.
	defaultBackupActiveDeadlineSeconds int64 = 24 * 60 * 60
	// backupTablesMaxLength bounds the --tables pattern. Mirrored by the CRD schema maxLength.
	backupTablesMaxLength = 1024
	// BackupNameMaxLength bounds backup names. Mirrored by the CRD schema maxLength.
	BackupNameMaxLength = 128
	// BackupStartingDeadlineMaxSeconds bounds startingDeadlineSeconds, so the deadline it makes stays a time.Duration:
	// the CRD schema maximum mirrors it, which the API server enforces before the operator reads a spec.
	BackupStartingDeadlineMaxSeconds = math.MaxInt32
	// BackupNameScheduleSuffix is the form of the suffix a scheduled run appends to the backup name prefix, the time
	// the run was due, as messages tell it.
	BackupNameScheduleSuffix = "-YYYYMMDDHHMMSS"
	// BackupNameScheduleSuffixLength is the length of the suffix a scheduled run appends.
	BackupNameScheduleSuffixLength = len(BackupNameScheduleSuffix)
	// BackupNamePrefixMaxLength leaves room for the schedule suffix, and for the "T" of the "-YYYYMMDDTHHMMSS" suffix
	// of a backup that runs a schedule once, so a generated name still fits BackupNameMaxLength. Mirrored by the CRD
	// schema maxLength.
	BackupNamePrefixMaxLength = BackupNameMaxLength - BackupNameScheduleSuffixLength - 1
	// BackupSourceInstallationMaxLength and BackupSourceNamespaceMaxLength bound the installation, and its
	// namespace, a restore reads the backup of: an object name and a namespace name. Mirrored by the CRD schema
	// maxLength.
	BackupSourceInstallationMaxLength = validation.DNS1123SubdomainMaxLength
	BackupSourceNamespaceMaxLength    = validation.DNS1123LabelMaxLength
)

var (
	// ErrInvalidBackupSpec is returned when a backup, schedule or restore spec fails validation.
	ErrInvalidBackupSpec = errors.New("invalid backup spec")
	// ErrActiveDeadlineNotPositive, an ErrInvalidBackupSpec, is returned for an activeDeadlineSeconds
	// that is not positive.
	ErrActiveDeadlineNotPositive = fmt.Errorf("%w: activeDeadlineSeconds must be positive", ErrInvalidBackupSpec)
)

var (
	// backupNameRegexp bounds backup names: they become storage paths and the command
	// line of clickhouse-backup actions. No "..", which clickhouse-backup strips from names, so
	// the backup would land under a different name. Mirrored by the CRD schema patterns.
	backupNameRegexp = regexp.MustCompile(`^[A-Za-z0-9](\.?[A-Za-z0-9_-])*\.?$`)
	// backupTablesRegexp bounds clickhouse-backup --tables patterns: db.table, db.*, comma-separated.
	// No leading "-": the operator passes the pattern as an argument, which must never read as a flag.
	backupTablesRegexp = regexp.MustCompile(`^[A-Za-z0-9_.*?,][A-Za-z0-9_.*?,-]*$`)
	// scheduledBackupNameRegexp matches the end of the names the runs of a schedule make, "-YYYYMMDDHHMMSS", which
	// the retention of a schedule prunes: a one-off backup of such a name would join the series of the schedule whose
	// prefix it carries, and push out the backups the schedule made.
	scheduledBackupNameRegexp = regexp.MustCompile(`-[0-9]{14}$`)
	// backupPartitionRegexp bounds one clickhouse-backup --partitions id.
	backupPartitionRegexp = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	// backupEngineRegexp bounds a table engine a restore leaves out. Mirrored by the CRD schema patterns.
	backupEngineRegexp = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`)
	// backupDatabaseRegexp bounds a database of the database mapping of a restore: no ":" or ",", which separate the
	// pairs of the mapping, nor anything a command line would split. Mirrored by the CRD schema patterns.
	backupDatabaseRegexp = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$`)
	// restoreSystemDatabases are the databases a database mapping must not name, as the restore never touches them
	restoreSystemDatabases = map[string]bool{"system": true, "information_schema": true, "_temporary_and_external_tables": true}
	// backupSourceRegexp bounds the installation and the namespace a restore reads the backup of: the values of the
	// {installation} and {namespace} macros of the storage paths there, which the operator puts into the command line of
	// clickhouse-backup actions. Mirrored by the CRD schema patterns.
	backupSourceRegexp = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
)

// validateBackupName checks an optional backup name or prefix against the allowed character set
// and length.
func validateBackupName(field, value string, maxLength int) error {
	if (len(value) > maxLength) || ((value != "") && !backupNameRegexp.MatchString(value)) {
		return fmt.Errorf("%w: %s %q must match %s and be at most %d characters", ErrInvalidBackupSpec,
			field, value, backupNameRegexp, maxLength)
	}
	return nil
}

// validateBackupSource checks the optional installation or namespace a restore reads the backup of against the allowed
// character set and length.
func validateBackupSource(field, value string, maxLength int) error {
	if (len(value) > maxLength) || ((value != "") && !backupSourceRegexp.MatchString(value)) {
		return fmt.Errorf("%w: %s %q must match %s and be at most %d characters", ErrInvalidBackupSpec,
			field, value, backupSourceRegexp, maxLength)
	}
	return nil
}

// validateTables checks an optional clickhouse-backup table pattern against the allowed character set
// and length.
func validateTables(tables string) error {
	if (len(tables) > backupTablesMaxLength) || ((tables != "") && !backupTablesRegexp.MatchString(tables)) {
		return fmt.Errorf("%w: tables %q must match %s and be at most %d characters", ErrInvalidBackupSpec,
			tables, backupTablesRegexp, backupTablesMaxLength)
	}
	return nil
}

// activeDeadlineSeconds returns the deadline of the operation a spec sets, defaulting to 24 hours.
func activeDeadlineSeconds(deadline *int64) int64 {
	if (deadline == nil) || (*deadline <= 0) {
		return defaultBackupActiveDeadlineSeconds
	}
	return *deadline
}

// validateStringBool checks an optional on/off field of a spec. Its CRD schema accepts any value, as it takes a
// bool, an integer and a string alike, so a value that is none of these is refused here.
func validateStringBool(field string, value *types.StringBool) error {
	if value.HasValue() && !value.IsValid() {
		return fmt.Errorf("%w: %s %q is not a boolean", ErrInvalidBackupSpec, field, value.String())
	}
	return nil
}

// validateActiveDeadlineSeconds checks the optional deadline of the operation a spec sets.
func validateActiveDeadlineSeconds(deadline *int64) error {
	if (deadline != nil) && (*deadline <= 0) {
		return ErrActiveDeadlineNotPositive
	}
	return nil
}

// BackupOptions are the backup knobs shared by one-off and scheduled backups.
type BackupOptions struct {
	// SchemaOnly backs up table/database definitions only (no data).
	SchemaOnly *types.StringBool `json:"schemaOnly,omitempty" yaml:"schemaOnly,omitempty"`
	// ReplicaSelection controls which replicas are backed up. Defaults to FirstPerShard.
	ReplicaSelection ReplicaSelection `json:"replicaSelection,omitempty" yaml:"replicaSelection,omitempty"`
	// Tables optionally restricts the backup to tables matching this clickhouse-backup
	// pattern (e.g. "db.table", "db.*"). Empty backs up everything.
	Tables string `json:"tables,omitempty" yaml:"tables,omitempty"`
	// Partitions optionally restricts the backup to the given partition ids.
	Partitions []string `json:"partitions,omitempty" yaml:"partitions,omitempty"`
	// ActiveDeadlineSeconds bounds how long a backup may run on a cluster. Defaults to 24 hours.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" yaml:"activeDeadlineSeconds,omitempty"`
}

// GetReplicaSelection returns the replica selection, defaulting to FirstPerShard.
func (o *BackupOptions) GetReplicaSelection() ReplicaSelection {
	if (o == nil) || (o.ReplicaSelection == "") {
		return ReplicaSelectionFirstPerShard
	}
	return o.ReplicaSelection
}

// GetActiveDeadlineSeconds returns the deadline of the backup, defaulting to 24 hours.
func (o *BackupOptions) GetActiveDeadlineSeconds() int64 {
	if o == nil {
		return defaultBackupActiveDeadlineSeconds
	}
	return activeDeadlineSeconds(o.ActiveDeadlineSeconds)
}

// isSet reports whether any of the options is set.
func (o *BackupOptions) isSet() bool {
	return (o != nil) &&
		((o.SchemaOnly != nil) || (o.ReplicaSelection != "") || (o.Tables != "") || (o.Partitions != nil) || (o.ActiveDeadlineSeconds != nil))
}

// Validate checks the options against the allowed character sets and values.
func (o *BackupOptions) Validate() error {
	if o == nil {
		return nil
	}
	if err := validateStringBool("schemaOnly", o.SchemaOnly); err != nil {
		return err
	}
	switch o.ReplicaSelection {
	case "", ReplicaSelectionFirstPerShard, ReplicaSelectionAllReplicas:
	default:
		return fmt.Errorf("%w: replicaSelection %q is not one of %s, %s", ErrInvalidBackupSpec,
			o.ReplicaSelection, ReplicaSelectionFirstPerShard, ReplicaSelectionAllReplicas)
	}
	if err := validateTables(o.Tables); err != nil {
		return err
	}
	for _, partition := range o.Partitions {
		if !backupPartitionRegexp.MatchString(partition) {
			return fmt.Errorf("%w: partition %q must match %s", ErrInvalidBackupSpec, partition, backupPartitionRegexp)
		}
	}
	return validateActiveDeadlineSeconds(o.ActiveDeadlineSeconds)
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackup defines a one-off backup of a ClickHouseInstallation. The operator drives the clickhouse-backup
// sidecar it runs in every backup-enabled cluster through SQL, creating and uploading a backup on every cluster.
// Every cluster backs up under the same backup name.
type ClickHouseBackup struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ClickHouseBackupSpec   `json:"spec"             yaml:"spec"`
	Status ClickHouseBackupStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// ClickHouseBackupSpec defines the desired state of a ClickHouseBackup.
type ClickHouseBackupSpec struct {
	// ClickHouseInstallation is the name of the target CHI in the same namespace, immutable. Exactly one of it and
	// ScheduleRef is set.
	ClickHouseInstallation string `json:"clickHouseInstallation,omitempty" yaml:"clickHouseInstallation,omitempty"`
	// ScheduleRef names a ClickHouseBackupSchedule in the same namespace whose backup this one makes once,
	// immutable: the installation, the backup options, the backup name prefix and, unless Cluster narrows them to
	// one, the clusters are the schedule's, as they are when the backup is validated. Its backup stays out of the
	// schedule's retention.
	ScheduleRef *BackupScheduleRef `json:"scheduleRef,omitempty" yaml:"scheduleRef,omitempty"`
	// Cluster optionally restricts the backup to one cluster of the CHI, immutable. Empty backs up
	// every backup-enabled cluster. With ScheduleRef, one of the clusters the schedule backs up.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// BackupName is the optional explicit backup name. When empty the operator generates a
	// deterministic name from the CR name and the time the backup is due. A name ending like the backups of a
	// schedule, "-YYYYMMDD-HHMMSS", is refused, and so is any name with ScheduleRef: such a backup is always named
	// with the schedule's backup name prefix, which keeps it recognizable as the schedule's.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// DiffFromRemote, when set to the name of a backup in the storage, makes this an
	// incremental backup (clickhouse-backup --diff-from-remote).
	DiffFromRemote string `json:"diffFromRemote,omitempty" yaml:"diffFromRemote,omitempty"`
	// ScheduledTime is the RFC 3339 time, with its offset, the backup is due at. Empty makes it due when it is
	// created. It can be changed until the backup is validated, at that time or later.
	ScheduledTime string `json:"scheduledTime,omitempty" yaml:"scheduledTime,omitempty"`
	// StartingDeadlineSeconds bounds how late the backup may start on a cluster, counted from the time it is due:
	// a cluster the backup does not start on by then fails, as does the backup when it is not validated by then.
	// Unset waits as long as it takes.
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty" yaml:"startingDeadlineSeconds,omitempty"`
	// ConfigOverrides are clickhouse-backup settings the backup is made with in place of those of the sidecar, as
	// KEY=VALUE of the env var of the setting, e.g. S3_STORAGE_CLASS=STANDARD_IA, see type_backup_overrides.go. Not
	// with ScheduleRef: a backup that runs a schedule takes no overrides.
	ConfigOverrides []string `json:"configOverrides,omitempty" yaml:"configOverrides,omitempty"`
	// Timeouts bound how the operator waits for the commands of the backup on the sidecars. A field left unset is
	// that of the schedule the backup runs, with ScheduleRef, else that of the operator configuration.
	Timeouts *BackupTimeouts `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`
	// TaskID reruns a backup that finished when it changes to a value the status does not record: a completed backup
	// makes a new backup, named anew at the time of the rerun, and one that did not complete makes its backup again,
	// under its name, on the clusters it did not complete on. A value set while the backup runs reruns nothing.
	TaskID *types.Id `json:"taskID,omitempty" yaml:"taskID,omitempty"`

	BackupOptions `json:",inline" yaml:",inline"`
}

// BackupScheduleRef names the ClickHouseBackupSchedule a backup makes the backup of once.
type BackupScheduleRef struct {
	// Name is the name of the schedule in the namespace of the backup.
	Name string `json:"name" yaml:"name"`
}

// GetScheduleName returns the name of the schedule a backup runs once, empty for none, nil-safe.
func (spec *ClickHouseBackupSpec) GetScheduleName() string {
	if (spec == nil) || (spec.ScheduleRef == nil) {
		return ""
	}
	return spec.ScheduleRef.Name
}

// DueTime returns the time the backup is due: the time spec.taskID last reran it, else the scheduled time, or the
// creation time without one. A scheduled time that does not parse, which validation refuses, counts as none.
func (chb *ClickHouseBackup) DueTime() time.Time {
	if chb.Status.RerunTime != nil {
		return chb.Status.RerunTime.Time
	}
	if due, err := time.Parse(time.RFC3339, chb.Spec.ScheduledTime); err == nil {
		return due
	}
	return chb.GetCreationTimestamp().Time
}

// StartingDeadline returns the time by which the backup must start on every cluster, false when it may start any
// time.
func (chb *ClickHouseBackup) StartingDeadline() (time.Time, bool) {
	if chb.Spec.StartingDeadlineSeconds == nil {
		return time.Time{}, false
	}
	return chb.DueTime().Add(time.Duration(*chb.Spec.StartingDeadlineSeconds) * time.Second), true
}

// TargetInstallation returns the installation the backup targets: the one its spec names, or the one of the
// schedule it runs, as the backup recorded it.
func (chb *ClickHouseBackup) TargetInstallation() string {
	if chb.Spec.ScheduleRef != nil {
		return chb.Status.Schedule.GetClickHouseInstallation()
	}
	return chb.Spec.ClickHouseInstallation
}

// TargetCluster returns the cluster the backup targets, empty for every backup-enabled cluster: the one its spec
// names, or, for a backup that runs a schedule, the one it recorded.
func (chb *ClickHouseBackup) TargetCluster() string {
	if chb.Spec.ScheduleRef != nil {
		return chb.Status.Schedule.GetCluster()
	}
	return chb.Spec.Cluster
}

// GetBackupOptions returns the options the backup makes its backup with: those of its spec, or, for a backup that
// runs a schedule, those it recorded of the schedule, none before it recorded them.
func (chb *ClickHouseBackup) GetBackupOptions() *BackupOptions {
	if chb.Spec.ScheduleRef == nil {
		return &chb.Spec.BackupOptions
	}
	if options := chb.Status.Schedule.GetBackupOptions(); options != nil {
		return options
	}
	return &BackupOptions{}
}

// Validate checks the spec against the allowed character sets and values.
func (spec *ClickHouseBackupSpec) Validate() error {
	if spec == nil {
		return nil
	}
	switch {
	case (spec.ClickHouseInstallation == "") == (spec.ScheduleRef == nil):
		return fmt.Errorf("%w: exactly one of clickHouseInstallation and scheduleRef is required", ErrInvalidBackupSpec)
	case spec.ScheduleRef == nil:
	case len(validation.IsDNS1123Subdomain(spec.ScheduleRef.Name)) > 0:
		return fmt.Errorf("%w: scheduleRef.name %q is not the name of a resource", ErrInvalidBackupSpec, spec.ScheduleRef.Name)
	case spec.BackupOptions.isSet():
		return fmt.Errorf("%w: a backup that runs a schedule takes its options from the schedule: schemaOnly, "+
			"replicaSelection, tables, partitions and activeDeadlineSeconds are not set with scheduleRef", ErrInvalidBackupSpec)
	case spec.BackupName != "":
		return fmt.Errorf("%w: a backup that runs a schedule is named with the schedule's backupNamePrefix: "+
			"backupName is not set with scheduleRef", ErrInvalidBackupSpec)
	case len(spec.ConfigOverrides) > 0:
		return fmt.Errorf("%w: a backup that runs a schedule takes no configOverrides: they are not set with scheduleRef",
			ErrInvalidBackupSpec)
	}
	if err := ValidateConfigOverrides("configOverrides", spec.ConfigOverrides); err != nil {
		return err
	}
	if err := validateBackupName("backupName", spec.BackupName, BackupNameMaxLength); err != nil {
		return err
	}
	if scheduledBackupNameRegexp.MatchString(spec.BackupName) {
		return fmt.Errorf("%w: backupName %q ends like the backups a schedule makes, %s, which the "+
			"retention of a schedule of that prefix would prune", ErrInvalidBackupSpec, spec.BackupName, BackupNameScheduleSuffix)
	}
	if err := validateBackupName("diffFromRemote", spec.DiffFromRemote, BackupNameMaxLength); err != nil {
		return err
	}
	if spec.ScheduledTime != "" {
		if _, err := time.Parse(time.RFC3339, spec.ScheduledTime); err != nil {
			return fmt.Errorf("%w: scheduledTime %q is not an RFC 3339 time with its offset", ErrInvalidBackupSpec, spec.ScheduledTime)
		}
	}
	if (spec.StartingDeadlineSeconds != nil) && ((*spec.StartingDeadlineSeconds < 0) || (*spec.StartingDeadlineSeconds > BackupStartingDeadlineMaxSeconds)) {
		return fmt.Errorf("%w: startingDeadlineSeconds must be between 0 and %d", ErrInvalidBackupSpec, BackupStartingDeadlineMaxSeconds)
	}
	if err := spec.Timeouts.Validate(); err != nil {
		return err
	}
	return spec.BackupOptions.Validate()
}

// ClickHouseBackupStatus defines the observed state of a ClickHouseBackup.
type ClickHouseBackupStatus struct {
	// Phase is one of Pending, Running, Completed, Failed, Aborted.
	Phase OperationPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// BackupName is the resolved backup name.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// Schedule is what a backup that runs a schedule takes from it, recorded anew on every reconcile until the backup
	// is validated and kept from then on.
	Schedule *BackupScheduleRecord `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	// Clusters are the clusters the backup targets, resolved once it is validated, and its state on each.
	// +listType=map
	// +listMapKey=name
	Clusters []OperationClusterStatus `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	// StartTime is when the backup started on its first cluster.
	StartTime *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the backup finished on every cluster.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// DurationSeconds is the backup duration in seconds (completion - start).
	DurationSeconds int64 `json:"durationSeconds,omitempty" yaml:"durationSeconds,omitempty"`
	// Conditions represent the latest available observations of the backup state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	// RerunTime is when spec.taskID last reran the backup: the backup is due then, which names a new backup and starts
	// its starting deadline.
	RerunTime *meta.Time `json:"rerunTime,omitempty" yaml:"rerunTime,omitempty"`
	// PreviousBackupNames are the backups the backup made before spec.taskID reran it once it completed, newest first,
	// 10 at most: they stay in the storage, which retention or a delete by hand removes.
	PreviousBackupNames []string `json:"previousBackupNames,omitempty" yaml:"previousBackupNames,omitempty"`
	// PreviousRuns are what the runs of the backup did on the clusters spec.taskID reran it on, under the same name:
	// the rerun replaces the copies of the backup they left.
	PreviousRuns []BackupPreviousRun `json:"previousRuns,omitempty" yaml:"previousRuns,omitempty"`

	BackupTaskStatus `json:",inline" yaml:",inline"`
}

// PushPreviousBackupName records a backup the backup made before a rerun, 10 at most.
func (status *ClickHouseBackupStatus) PushPreviousBackupName(name string) {
	if name != "" {
		status.PreviousBackupNames = pushBounded(status.PreviousBackupNames, name)
	}
}

// BackupScheduleRecord is what a backup that runs a schedule takes from it: the backup is made with these, whatever
// the schedule becomes once the backup is validated.
type BackupScheduleRecord struct {
	// Name is the name of the schedule.
	Name string `json:"name" yaml:"name"`
	// Generation is the generation of the schedule the record was taken of.
	Generation int64 `json:"generation,omitempty" yaml:"generation,omitempty"`
	// ClickHouseInstallation is the installation the schedule backs up.
	ClickHouseInstallation string `json:"clickHouseInstallation,omitempty" yaml:"clickHouseInstallation,omitempty"`
	// Cluster is the cluster the backup targets, empty for every cluster the schedule backs up.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// BackupNamePrefix is the backup name prefix of the schedule, which names the backup.
	BackupNamePrefix string `json:"backupNamePrefix,omitempty" yaml:"backupNamePrefix,omitempty"`
	// Schedules are the schedule's entries, which tell whether the backup is incremental, and which backups the
	// retention that follows it keeps, as after a scheduled run.
	Schedules []BackupScheduleEntry `json:"schedules,omitempty" yaml:"schedules,omitempty"`
	// TimeZone is the schedule's timeZone.
	TimeZone string `json:"timeZone,omitempty" yaml:"timeZone,omitempty"`
	// KeepBackups are the backups the schedule's retention keeps beyond its entries, spec.retention.keepBackups.
	KeepBackups []BackupRetentionPin `json:"keepBackups,omitempty" yaml:"keepBackups,omitempty"`
	// PreviousSchedules are the changes of the schedule whose backups its retention holds, status.retention.previous.
	PreviousSchedules []RetiredScheduleRules `json:"previousSchedules,omitempty" yaml:"previousSchedules,omitempty"`
	// Timeouts are the schedule's timeouts, which those of the backup override field by field.
	Timeouts *BackupTimeouts `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`

	BackupOptions `json:",inline" yaml:",inline"`
}

// GetClickHouseInstallation returns the installation the record names, nil-safe.
func (record *BackupScheduleRecord) GetClickHouseInstallation() string {
	if record == nil {
		return ""
	}
	return record.ClickHouseInstallation
}

// GetCluster returns the cluster the record names, nil-safe.
func (record *BackupScheduleRecord) GetCluster() string {
	if record == nil {
		return ""
	}
	return record.Cluster
}

// GetBackupNamePrefix returns the backup name prefix the record holds, nil-safe.
func (record *BackupScheduleRecord) GetBackupNamePrefix() string {
	if record == nil {
		return ""
	}
	return record.BackupNamePrefix
}

// GetTimeouts returns the timeouts the record holds, nil-safe.
func (record *BackupScheduleRecord) GetTimeouts() *BackupTimeouts {
	if record == nil {
		return nil
	}
	return record.Timeouts
}

// GetBackupOptions returns the options the record holds, nil-safe.
func (record *BackupScheduleRecord) GetBackupOptions() *BackupOptions {
	if record == nil {
		return nil
	}
	return &record.BackupOptions
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackupList defines a list of ClickHouseBackup resources.
type ClickHouseBackupList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseBackup `json:"items" yaml:"items"`
}
