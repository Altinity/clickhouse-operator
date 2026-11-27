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
	"slices"
	"strings"
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
	// ReplicaSelectionAllReplicas backs up every replica of every shard.
	// A shard counts as backed up when one of its replicas made the backup.
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
	// OperationPhaseAborted: stopped or never started for a reason other than a failure, such as an invalid spec.
	OperationPhaseAborted OperationPhase = "Aborted"
)

// IsTerminal reports whether the operation has finished, successfully or not.
func (p OperationPhase) IsTerminal() bool {
	return (p == OperationPhaseCompleted) || (p == OperationPhaseFailed) || (p == OperationPhaseAborted)
}

type OperationClusterStatus struct {
	// Name is the name of the cluster.
	Name  string         `json:"name" yaml:"name"`
	Phase OperationPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	Hosts []string       `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	// ConfigSecret is recorded at start and identifies the storage that holds the backup.
	ConfigSecret   string     `json:"configSecret,omitempty" yaml:"configSecret,omitempty"`
	StartTime      *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// Reason tells, in one word, why the cluster waits, or how its operation went.
	Reason string `json:"reason,omitempty" yaml:"reason,omitempty"`
	// Message tells the same in a sentence.
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
	// Run lets the operator resume the run after a restart.
	Run *BackupRunStatus `json:"run,omitempty" yaml:"run,omitempty"`
}

// BackupRunOperation is what a run does on the sidecars of a cluster.
type BackupRunOperation string

const (
	BackupRunOperationBackup  BackupRunOperation = "backup"
	BackupRunOperationRestore BackupRunOperation = "restore"
	BackupRunOperationDelete  BackupRunOperation = "delete"
)

// BackupRunStep is the step of a run, such as create, upload or download.
type BackupRunStep string

type BackupRunStatus struct {
	Operation BackupRunOperation `json:"operation" yaml:"operation"`
	Step      BackupRunStep      `json:"step,omitempty" yaml:"step,omitempty"`
	// Mutated is true when a command that changes state may have reached a sidecar.
	Mutated bool            `json:"mutated,omitempty" yaml:"mutated,omitempty"`
	Hosts   []BackupRunHost `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	// Actions keep the last command per host and step.
	Actions []BackupRunAction `json:"actions,omitempty" yaml:"actions,omitempty"`
	// Plan holds decisions made before anything changed, so steps after a restart follow them.
	Plan *BackupRunPlan `json:"plan,omitempty" yaml:"plan,omitempty"`
}

// BackupRunPlan holds the decisions a run made before it changed anything.
type BackupRunPlan struct {
	// BackupName, Key and NamingPrefix name the backup a backup run makes.
	BackupName   string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	Key          string `json:"key,omitempty" yaml:"key,omitempty"`
	NamingPrefix string `json:"namingPrefix,omitempty" yaml:"namingPrefix,omitempty"`
	// SourceEnv points a restore at the storage path of the installation that made the backup.
	SourceEnv string `json:"sourceEnv,omitempty" yaml:"sourceEnv,omitempty"`
	// SourceShown is that storage path, as shown in logs and events.
	SourceShown string `json:"sourceShown,omitempty" yaml:"sourceShown,omitempty"`
	// ActionEnv holds the --env flags every restore command gets.
	ActionEnv string `json:"actionEnv,omitempty" yaml:"actionEnv,omitempty"`
	// HadLocal are the hosts that held a complete local copy, so they skip the download.
	HadLocal []string `json:"hadLocal,omitempty" yaml:"hadLocal,omitempty"`
	// Resumed are the hosts that resume a partial local copy.
	Resumed []string `json:"resumed,omitempty" yaml:"resumed,omitempty"`
	// Replaced are the hosts that download again over a copy an earlier run left.
	Replaced []string `json:"replaced,omitempty" yaml:"replaced,omitempty"`
	// SchemaCopies are the hosts that hold a local copy of the schema alone.
	SchemaCopies []string `json:"schemaCopies,omitempty" yaml:"schemaCopies,omitempty"`
	// DataHosts restore the data of their shard. CopyHosts copy it to detached.
	DataHosts []string `json:"dataHosts,omitempty" yaml:"dataHosts,omitempty"`
	CopyHosts []string `json:"copyHosts,omitempty" yaml:"copyHosts,omitempty"`
	// CopyToDetached is true when the replicas of a shard copy its data to detached.
	CopyToDetached bool `json:"copyToDetached,omitempty" yaml:"copyToDetached,omitempty"`
	// Listed is true when the copies of the backup in the storage were listed, so HoldsRBAC is known.
	Listed bool `json:"listed,omitempty" yaml:"listed,omitempty"`
	// HoldsRBAC is true when the backup holds access entities.
	HoldsRBAC bool `json:"holdsRBAC,omitempty" yaml:"holdsRBAC,omitempty"`
	// GiveUp is true when a delete run goes on past failures, as the installation is deleted.
	GiveUp bool `json:"giveUp,omitempty" yaml:"giveUp,omitempty"`
}

type BackupRunHost struct {
	Host   string `json:"host" yaml:"host"`
	Backup string `json:"backup,omitempty" yaml:"backup,omitempty"`
	// Base is the backup this one is incremental from, empty for a full backup.
	Base string `json:"base,omitempty" yaml:"base,omitempty"`
}

type BackupRunAction struct {
	Step    BackupRunStep `json:"step" yaml:"step"`
	Host    string        `json:"host" yaml:"host"`
	Command string        `json:"command" yaml:"command"`
	// TimeFrom is the host ClickHouse time (UTC) just before the insert; the command row is the first at or after it.
	TimeFrom string `json:"timeFrom" yaml:"timeFrom"`
	Status   string `json:"status,omitempty" yaml:"status,omitempty"`
	Error    string `json:"error,omitempty" yaml:"error,omitempty"`
}

// Condition types used across backup/restore custom resources.
const (
	// BackupConditionValidated is True once spec preflight checks have passed.
	BackupConditionValidated = "Validated"
	BackupConditionStarted   = "Started"
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
	// BackupReasonNoBackupStorage: the target clusters enable backup without storageConfig, so the backup cannot run.
	BackupReasonNoBackupStorage = "NoBackupStorage"
	// BackupReasonNoTargetHosts: the target resolves to no hosts.
	BackupReasonNoTargetHosts = "NoTargetHosts"
	// BackupReasonNoClickHouseAccess: the operator config sets no clickhouse.access credentials, which backup needs.
	BackupReasonNoClickHouseAccess = "NoClickHouseAccess"
	// BackupReasonOperationInProgress: another backup or restore runs on the cluster, or an RBAC restore of the
	// installation, or a scheduled run of the restore source installation.
	BackupReasonOperationInProgress = "OperationInProgress"
	// BackupReasonWaitingForGarbageCollection: the ClickHouseRestore name a recovery needs is held by a deleted owner.
	BackupReasonWaitingForGarbageCollection = "WaitingForGarbageCollection"
	// BackupReasonNameConflict: two resource names joined with their cluster names collide; one must be renamed.
	BackupReasonNameConflict = "NameConflict"
	// BackupReasonWaitingForSchedules: a restore waits until every schedule it holds has paused its runs.
	BackupReasonWaitingForSchedules = "WaitingForSchedules"
	// BackupReasonStarted: on an installation, it means the ClickHouseRestore that recovers it was created.
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
	BackupReasonFailed               = "Failed"
	// BackupReasonFailedUnchanged: failed before any state-changing command can have reached a sidecar.
	BackupReasonFailedUnchanged = "FailedUnchanged"
	// BackupReasonStoragePathConflict: two replicas, shards or clusters would write one backup name to one storage path.
	// A schedule stays aborted until its storage settings change.
	BackupReasonStoragePathConflict   = "StoragePathConflict"
	BackupReasonDeadlineExceeded      = "DeadlineExceeded"
	BackupReasonScheduled             = "Scheduled"
	BackupReasonLastBackupFailed      = "LastBackupFailed"
	BackupReasonLastBackupWithoutData = "LastBackupWithoutData"
	// BackupReasonSuspended: the schedule is suspended by its spec.
	BackupReasonSuspended = "Suspended"
	// BackupReasonSuspendedForRestore: a restore or recovery of the cluster, or a restore reading its backup, is pending or runs.
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
	// BackupReasonRetentionHeld: after a schedule or time zone change, retention keeps what the old rules kept
	// until it expires or spec.retention.releaseHeld. Event only.
	BackupReasonRetentionHeld = "RetentionHeld"
	// BackupReasonRetentionHeldExpired: every backup a held change kept has expired. Event only.
	BackupReasonRetentionHeldExpired = "RetentionHeldExpired"
)

const (
	defaultBackupActiveDeadlineSeconds int64 = 24 * 60 * 60
	// backupTablesMaxLength bounds the --tables pattern. Mirrored by the CRD schema maxLength.
	backupTablesMaxLength = 1024
	BackupTablesMaxItems  = 64 // mirrored by the CRD schema maxItems
	// BackupNameMaxLength bounds backup names. Mirrored by the CRD schema maxLength.
	BackupNameMaxLength = 128
	// BackupStartingDeadlineMaxSeconds bounds startingDeadlineSeconds, so the deadline it makes stays a time.Duration:
	// the CRD schema maximum mirrors it, which the API server enforces before the operator reads a spec.
	BackupStartingDeadlineMaxSeconds = math.MaxInt32
	BackupActiveDeadlineMaxSeconds   = math.MaxInt32 // mirrored by the CRD schema maximum
	// BackupNameScheduleSuffix is the form of the suffix a scheduled run appends to the backup name prefix, the time
	// the run was due, as messages tell it.
	BackupNameScheduleSuffix = "-YYYYMMDDHHMMSS"
	// BackupNameScheduleSuffixLength is the length of the suffix a scheduled run appends.
	BackupNameScheduleSuffixLength = len(BackupNameScheduleSuffix)
	// BackupNamePrefixMaxLength leaves room for the suffix and the "T" of a one-time run. Mirrored by the CRD maxLength.
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
	// No "," because it joins the patterns of a list. No leading "-", so the argument never reads as a flag.
	backupTablesRegexp = regexp.MustCompile(`^[A-Za-z0-9_.*?][A-Za-z0-9_.*?-]*$`)
	// A one-off backup with a scheduled-run name would join that schedule's retention and push out its backups.
	scheduledBackupNameRegexp = regexp.MustCompile(`-[0-9]{14}$`)
	// backupPartitionRegexp bounds one clickhouse-backup --partitions id.
	backupPartitionRegexp = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	backupEngineRegexp    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,127}$`) // mirrored by the CRD schema patterns
	// No ":" or ",", which separate the mapping pairs. Mirrored by the CRD schema patterns.
	backupDatabaseRegexp   = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_-]{0,127}$`)
	restoreSystemDatabases = map[string]bool{"system": true, "information_schema": true, "_temporary_and_external_tables": true}
	// Fills the {installation} and {namespace} macros in clickhouse-backup command lines. Mirrored by the CRD schema patterns.
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

func validateTables(field string, tables []string) error {
	if len(tables) > BackupTablesMaxItems {
		return fmt.Errorf("%w: %s holds more than %d patterns", ErrInvalidBackupSpec, field, BackupTablesMaxItems)
	}
	for _, pattern := range tables {
		if !backupTablesRegexp.MatchString(pattern) {
			return fmt.Errorf("%w: %s %q must match %s", ErrInvalidBackupSpec, field, pattern, backupTablesRegexp)
		}
	}
	if joined := strings.Join(tables, ","); len(joined) > backupTablesMaxLength {
		return fmt.Errorf("%w: %s must be at most %d characters joined by commas", ErrInvalidBackupSpec, field, backupTablesMaxLength)
	}
	return nil
}

func validateEngines(field string, engines []string) error {
	if len(engines) > BackupTablesMaxItems {
		return fmt.Errorf("%w: %s holds more than %d engines", ErrInvalidBackupSpec, field, BackupTablesMaxItems)
	}
	for _, engine := range engines {
		if !backupEngineRegexp.MatchString(engine) {
			return fmt.Errorf("%w: %s %q must match %s", ErrInvalidBackupSpec, field, engine, backupEngineRegexp)
		}
	}
	return nil
}

func activeDeadlineSeconds(deadline *int64) int64 {
	if (deadline == nil) || (*deadline <= 0) {
		return defaultBackupActiveDeadlineSeconds
	}
	return min(*deadline, BackupActiveDeadlineMaxSeconds)
}

// validateStringBool checks an optional on/off field of a spec. Its CRD schema accepts any value, as it takes a
// bool, an integer and a string alike, so a value that is none of these is refused here.
func validateStringBool(field string, value *types.StringBool) error {
	if value.HasValue() && !value.IsValid() {
		return fmt.Errorf("%w: %s %q is not a boolean", ErrInvalidBackupSpec, field, value.String())
	}
	return nil
}

func validateActiveDeadlineSeconds(deadline *int64) error {
	if (deadline != nil) && (*deadline <= 0) {
		return ErrActiveDeadlineNotPositive
	}
	if (deadline != nil) && (*deadline > BackupActiveDeadlineMaxSeconds) {
		return fmt.Errorf("%w: activeDeadlineSeconds must be at most %d", ErrInvalidBackupSpec, BackupActiveDeadlineMaxSeconds)
	}
	return nil
}

// BackupOptions are the backup knobs shared by one-off and scheduled backups.
type BackupOptions struct {
	// SchemaOnly backs up table/database definitions only (no data).
	SchemaOnly *types.StringBool `json:"schemaOnly,omitempty" yaml:"schemaOnly,omitempty"`
	// ReplicaSelection controls which replicas are backed up. Defaults to FirstPerShard.
	ReplicaSelection ReplicaSelection `json:"replicaSelection,omitempty" yaml:"replicaSelection,omitempty"`
	// Tables are clickhouse-backup patterns such as "db.*". Empty backs up all tables.
	Tables []string `json:"tables,omitempty" yaml:"tables,omitempty"`
	// SkipTables are clickhouse-backup patterns. System databases are always skipped.
	SkipTables       []string `json:"skipTables,omitempty" yaml:"skipTables,omitempty"`
	SkipTableEngines []string `json:"skipTableEngines,omitempty" yaml:"skipTableEngines,omitempty"`
	// Partitions optionally restricts the backup to the given partition ids.
	Partitions []string `json:"partitions,omitempty" yaml:"partitions,omitempty"`
	// ActiveDeadlineSeconds applies per cluster. Default 24 hours.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" yaml:"activeDeadlineSeconds,omitempty"`
}

// GetReplicaSelection returns the replica selection, defaulting to FirstPerShard.
func (o *BackupOptions) GetReplicaSelection() ReplicaSelection {
	if (o == nil) || (o.ReplicaSelection == "") {
		return ReplicaSelectionFirstPerShard
	}
	return o.ReplicaSelection
}

func (o *BackupOptions) GetActiveDeadlineSeconds() int64 {
	if o == nil {
		return defaultBackupActiveDeadlineSeconds
	}
	return activeDeadlineSeconds(o.ActiveDeadlineSeconds)
}

// isSet reports whether any of the options is set.
func (o *BackupOptions) isSet() bool {
	return (o != nil) &&
		((o.SchemaOnly != nil) || (o.ReplicaSelection != "") || (o.Tables != nil) || (o.SkipTables != nil) || (o.SkipTableEngines != nil) || (o.Partitions != nil) || (o.ActiveDeadlineSeconds != nil))
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
	if err := validateTables("tables", o.Tables); err != nil {
		return err
	}
	if err := validateTables("skipTables", o.SkipTables); err != nil {
		return err
	}
	if err := validateEngines("skipTableEngines", o.SkipTableEngines); err != nil {
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

// ClickHouseBackup is a one-off backup of a ClickHouseInstallation.
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
	// ScheduleRef runs a ClickHouseBackupSchedule once, with its installation, clusters, options and name prefix. Immutable.
	// Schedule retention deletes it once it is older than the oldest backup the schedule keeps.
	ScheduleRef *BackupScheduleRef `json:"scheduleRef,omitempty" yaml:"scheduleRef,omitempty"`
	// Cluster is immutable. Empty backs up every backup-enabled cluster. With ScheduleRef, it must be a schedule cluster.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// BackupName is the optional explicit backup name. When empty the operator generates a
	// deterministic name from the CR name and the time the backup is due. A name ending like the backups of a
	// schedule, "-YYYYMMDDHHMMSS", is refused, and so is any name with ScheduleRef: such a backup is always named
	// with the schedule's backup name prefix, which keeps it recognizable as the schedule's.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// DiffFromRemote, when set to the name of a backup in the storage, makes this an
	// incremental backup (clickhouse-backup --diff-from-remote).
	DiffFromRemote string `json:"diffFromRemote,omitempty" yaml:"diffFromRemote,omitempty"`
	// ScheduledTime is the RFC 3339 time, with its offset, the backup is due at. Empty makes it due when it is
	// created. It can be changed until the backup is validated, at that time or later.
	ScheduledTime string `json:"scheduledTime,omitempty" yaml:"scheduledTime,omitempty"`
	// StartingDeadlineSeconds counts from the due time. A cluster not started by then fails. Unset means no limit.
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty" yaml:"startingDeadlineSeconds,omitempty"`
	// ConfigOverrides are KEY=VALUE clickhouse-backup env vars for this backup, such as S3_STORAGE_CLASS=STANDARD_IA.
	ConfigOverrides []string        `json:"configOverrides,omitempty" yaml:"configOverrides,omitempty"`
	Timeouts        *BackupTimeouts `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`
	// A new TaskID reruns a finished backup: a completed one under a new name, a failed one on its failed clusters.
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
			"replicaSelection, tables, skipTables, skipTableEngines, partitions and activeDeadlineSeconds are not set with scheduleRef", ErrInvalidBackupSpec)
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
	Phase OperationPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// BackupName is the resolved backup name.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// Schedule is what a backup that runs a schedule takes from it, recorded anew on every reconcile until the backup
	// is validated and kept from then on.
	Schedule *BackupScheduleRecord `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	// Clusters are the clusters the backup targets, resolved once it is validated, and its state on each.
	// +listType=map
	// +listMapKey=name
	Clusters  []OperationClusterStatus `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	StartTime *meta.Time               `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the backup finished on every cluster.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// DurationSeconds is the backup duration in seconds (completion - start).
	DurationSeconds int64 `json:"durationSeconds,omitempty" yaml:"durationSeconds,omitempty"`
	// Conditions represent the latest available observations of the backup state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
	// RerunTime is the due time set by the last spec.taskID rerun.
	RerunTime *meta.Time `json:"rerunTime,omitempty" yaml:"rerunTime,omitempty"`
	// PreviousBackupNames stay in the storage until retention or a manual delete removes them.
	PreviousBackupNames []string            `json:"previousBackupNames,omitempty" yaml:"previousBackupNames,omitempty"`
	PreviousRuns        []BackupPreviousRun `json:"previousRuns,omitempty" yaml:"previousRuns,omitempty"`
	// Backups is the backup in the storage of each cluster, as the run listed it when it completed.
	Backups []StoredBackup `json:"backups,omitempty" yaml:"backups,omitempty"`
	// BackupsListTime is when a run last listed Backups.
	BackupsListTime *meta.Time `json:"backupsListTime,omitempty" yaml:"backupsListTime,omitempty"`

	BackupTaskStatus `json:",inline" yaml:",inline"`
}

// StoredBackupState tells whether every shard holds a backup.
type StoredBackupState string

const (
	// StoredBackupComplete: every shard holds the backup, and no host reports it broken.
	StoredBackupComplete StoredBackupState = "Complete"
	// StoredBackupIncomplete: a shard does not hold the backup. A run may still write it.
	StoredBackupIncomplete StoredBackupState = "Incomplete"
	// StoredBackupBroken: a host reports the backup broken.
	StoredBackupBroken StoredBackupState = "Broken"
)

// StoredBackupsMax is the maximum length of status.backups. The newest backups are kept.
const StoredBackupsMax = 1024

// StoredBackup is a backup in the remote storage of a cluster, as a run listed it in system.backup_list.
type StoredBackup struct {
	// Cluster is the cluster whose storage holds the backup.
	Cluster string `json:"cluster" yaml:"cluster"`
	// Name is the backup name. With a naming template, the name of the backup of the first shard.
	Name string `json:"name" yaml:"name"`
	// Created is the earliest creation time that a shard lists.
	Created *meta.Time `json:"created,omitempty" yaml:"created,omitempty"`
	// Size is the sum of the sizes that the hosts list, in bytes.
	Size  int64             `json:"size,omitempty" yaml:"size,omitempty"`
	State StoredBackupState `json:"state,omitempty" yaml:"state,omitempty"`
	// Base is the backup that an incremental backup needs. Empty for a full backup.
	Base string `json:"base,omitempty" yaml:"base,omitempty"`
}

// ReplaceStoredBackups replaces the backups of the cluster in the list with the backups given. The result is newest
// first, at most StoredBackupsMax long.
func ReplaceStoredBackups(list []StoredBackup, cluster string, backups []StoredBackup) []StoredBackup {
	var result []StoredBackup
	for _, backup := range list {
		if backup.Cluster != cluster {
			result = append(result, backup)
		}
	}
	for _, backup := range backups {
		backup.Cluster = cluster
		result = append(result, backup)
	}
	SortStoredBackups(result)
	if len(result) > StoredBackupsMax {
		result = result[:StoredBackupsMax]
	}
	return result
}

// KeepStoredBackupsOf drops the backups of the clusters that are not given.
func KeepStoredBackupsOf(list []StoredBackup, clusters []string) []StoredBackup {
	var result []StoredBackup
	for _, backup := range list {
		if slices.Contains(clusters, backup.Cluster) {
			result = append(result, backup)
		}
	}
	return result
}

// SortStoredBackups sorts the list newest first, then by cluster and name.
func SortStoredBackups(list []StoredBackup) {
	created := func(backup StoredBackup) time.Time {
		if backup.Created == nil {
			return time.Time{}
		}
		return backup.Created.Time
	}
	slices.SortStableFunc(list, func(x, y StoredBackup) int {
		if c := created(y).Compare(created(x)); c != 0 {
			return c
		}
		if c := strings.Compare(x.Cluster, y.Cluster); c != 0 {
			return c
		}
		return strings.Compare(y.Name, x.Name)
	})
}

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
	// Schedules decide whether the backup is incremental and which backups retention keeps.
	Schedules         []BackupScheduleEntry  `json:"schedules,omitempty" yaml:"schedules,omitempty"`
	TimeZone          string                 `json:"timeZone,omitempty" yaml:"timeZone,omitempty"`
	KeepBackups       []BackupRetentionPin   `json:"keepBackups,omitempty" yaml:"keepBackups,omitempty"`
	PreviousSchedules []RetiredScheduleRules `json:"previousSchedules,omitempty" yaml:"previousSchedules,omitempty"`
	// The backup timeouts override these field by field.
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
