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
	// ReplicaSelectionAllReplicas backs up every replica of every shard.
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
)

// IsTerminal reports whether the operation has finished, successfully or not.
func (p OperationPhase) IsTerminal() bool {
	return (p == OperationPhaseCompleted) || (p == OperationPhaseFailed)
}

// OperationClusterStatus is the observed state of a backup or a restore on one of the clusters it
// targets. Every cluster runs in a Job of its own.
type OperationClusterStatus struct {
	// Name is the name of the cluster.
	Name string `json:"name" yaml:"name"`
	// Phase is one of Pending, Running, Completed, Failed.
	Phase OperationPhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// JobName is the Job that drives the operation on the cluster: the backup or restore Job, then,
	// while a backup is verified, the verify Job.
	JobName string `json:"jobName,omitempty" yaml:"jobName,omitempty"`
	// Hosts are the hosts of the cluster the Job drives.
	Hosts []string `json:"hosts,omitempty" yaml:"hosts,omitempty"`
	// Storage is the storage of the cluster the Job keeps the backup in, or reads it from, as the Job was built.
	Storage string `json:"storage,omitempty" yaml:"storage,omitempty"`
	// StartTime is when the Job of the cluster started.
	StartTime *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the Job of the cluster finished.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// Reason tells, in one word, why the cluster waits, or how its operation went.
	Reason string `json:"reason,omitempty" yaml:"reason,omitempty"`
	// Message tells the same in a sentence.
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
}

// Condition types used across backup/restore custom resources.
const (
	// BackupConditionValidated is True once spec preflight checks have passed.
	BackupConditionValidated = "Validated"
	// BackupConditionJobCreated is True once the operator has created the driving Job/CronJob.
	BackupConditionJobCreated = "JobCreated"
	// BackupConditionReady is True once the operation finished successfully.
	BackupConditionReady = "Ready"
	// BackupConditionVerified is True once a backup has passed verification (spec.verify).
	BackupConditionVerified = "Verified"
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
	// storage, neither a storage with a secretRef nor podTemplate, which leaves their backup off.
	BackupReasonNoBackupStorage = "NoBackupStorage"
	// BackupReasonNoTargetHosts: the target resolves to no hosts.
	BackupReasonNoTargetHosts = "NoTargetHosts"
	// BackupReasonOperationInProgress: another operation of the cluster runs (a backup, its verification or a restore),
	// or, for a backup or a verification, a restore of access entities, which waits for every cluster of the
	// installation; a restore also waits for a scheduled run of the installation whose backup it reads.
	BackupReasonOperationInProgress = "OperationInProgress"
	// BackupReasonWaitingForGarbageCollection: a Job or CronJob of the expected name belongs to another
	// owner, typically a deleted predecessor of the same name, and awaits garbage collection.
	BackupReasonWaitingForGarbageCollection = "WaitingForGarbageCollection"
	// BackupReasonNameConflict: a Job, CronJob or ClickHouseRestore of the name the operation needs on a
	// cluster belongs to another resource, which no garbage collection removes: the joined names of two
	// resources and their clusters coincide. The operation cannot run on that cluster until one of them
	// is renamed.
	BackupReasonNameConflict = "NameConflict"
	// BackupReasonWaitingForSchedules: a restore waits until every schedule it holds - of the clusters it restores, or of
	// every cluster for access entities, and of the clusters of the installation whose backup it reads - holds its runs.
	BackupReasonWaitingForSchedules = "WaitingForSchedules"
	// BackupReasonJobCreated: the driving Job or CronJob was created, or, on an installation, the
	// ClickHouseRestore that recovers it.
	BackupReasonJobCreated = "JobCreated"
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
	// BackupReasonJobFailed: the driving Job failed.
	BackupReasonJobFailed = "JobFailed"
	// BackupReasonJobFailedUnchanged: the driving Job of a restore failed before it changed
	// anything: a check failed, or no sidecar can have received a request that changes something.
	BackupReasonJobFailedUnchanged = "JobFailedUnchanged"
	// BackupReasonJobLost: the driving Job disappeared before it finished.
	BackupReasonJobLost = "JobLost"
	// BackupReasonJobNotStarted: the driving Job's pod never started (admission, image pull, quota).
	BackupReasonJobNotStarted = "JobNotStarted"
	// BackupReasonDeadlineExceeded: the driving Job ran past activeDeadlineSeconds.
	BackupReasonDeadlineExceeded = "DeadlineExceeded"
	// BackupReasonVerifying: the verification Job is running.
	BackupReasonVerifying = "Verifying"
	// BackupReasonVerified: the backup passed verification.
	BackupReasonVerified = "Verified"
	// BackupReasonVerificationFailed: the backup failed verification.
	BackupReasonVerificationFailed = "VerificationFailed"
	// BackupReasonScheduled: the schedule's CronJob is active.
	BackupReasonScheduled = "Scheduled"
	// BackupReasonLastBackupFailed: the last finished run of the schedule failed. The CronJob stays
	// active and runs the schedule on.
	BackupReasonLastBackupFailed = "LastBackupFailed"
	// BackupReasonLastBackupWithoutData: the last finished run of the schedule succeeded and made a
	// backup that holds no data. The CronJob runs the schedule on.
	BackupReasonLastBackupWithoutData = "LastBackupWithoutData"
	// BackupReasonSuspended: the schedule is suspended by its spec.
	BackupReasonSuspended = "Suspended"
	// BackupReasonSuspendedForRestore: the schedule is held on a cluster while a restore of the cluster
	// is validated and about to start, or runs, while the cluster waits to be restored from the backup
	// spec.backup.recovery names, and while a restore of another installation of the namespace that
	// reads the backup of the cluster may start, or runs. A CronJob a restore held stays suspended
	// through a reconcile of the installation once no restore holds it any longer, until the reconcile
	// completes.
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
)

const (
	// defaultBackupActiveDeadlineSeconds bounds a backup/restore Job when the spec sets no deadline.
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
	BackupNameScheduleSuffix = "-YYYYMMDD-HHMMSS"
	// BackupNameScheduleSuffixLength is the length of the suffix a scheduled run appends.
	BackupNameScheduleSuffixLength = len(BackupNameScheduleSuffix)
	// BackupNamePrefixMaxLength leaves room for the schedule suffix, so a generated name still fits
	// BackupNameMaxLength. Mirrored by the CRD schema maxLength.
	BackupNamePrefixMaxLength = BackupNameMaxLength - BackupNameScheduleSuffixLength
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
	// No leading "-": a Job passes the pattern as an argument, which must never read as a flag.
	backupTablesRegexp = regexp.MustCompile(`^[A-Za-z0-9_.*?,][A-Za-z0-9_.*?,-]*$`)
	// scheduledBackupNameRegexp matches the end of the names the runs of a schedule make, "-YYYYMMDD-HHMMSS", which
	// the retention of a schedule prunes: a one-off backup of such a name would join the series of the schedule whose
	// prefix it carries, and push out the backups the schedule made.
	scheduledBackupNameRegexp = regexp.MustCompile(`-[0-9]{8}-[0-9]{6}$`)
	// backupPartitionRegexp bounds one clickhouse-backup --partitions id.
	backupPartitionRegexp = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	// backupSourceRegexp bounds the installation and the namespace a restore reads the backup of: the values of the
	// {installation} and {namespace} macros of the storage paths there, which a Job puts into the command line of
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

// activeDeadlineSeconds returns the Job deadline a spec sets, defaulting to 24 hours.
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

// validateActiveDeadlineSeconds checks the optional Job deadline a spec sets.
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
	// ActiveDeadlineSeconds bounds how long a backup Job may run. Defaults to 24 hours.
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" yaml:"activeDeadlineSeconds,omitempty"`
}

// GetReplicaSelection returns the replica selection, defaulting to FirstPerShard.
func (o *BackupOptions) GetReplicaSelection() ReplicaSelection {
	if (o == nil) || (o.ReplicaSelection == "") {
		return ReplicaSelectionFirstPerShard
	}
	return o.ReplicaSelection
}

// GetActiveDeadlineSeconds returns the Job deadline, defaulting to 24 hours.
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

// ClickHouseBackup defines a one-off backup of a ClickHouseInstallation. The operator
// reconciles it into a Kubernetes Job per cluster that drives the clickhouse-backup sidecar the
// operator runs in every backup-enabled cluster, creating and uploading a backup. Every
// cluster backs up under the same backup name.
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
	// every backup-enabled cluster, each in a Job of its own. With ScheduleRef, one of the clusters the
	// schedule backs up.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// BackupName is the optional explicit backup name. When empty the operator generates a
	// deterministic name from the CR name and the time the backup is due. A name ending like the backups of a
	// schedule, "-YYYYMMDD-HHMMSS", is refused, and so is any name with ScheduleRef: such a backup is always named
	// with the schedule's backup name prefix, which keeps it recognizable as the schedule's.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`
	// DiffFromRemote, when set to the name of a backup in the storage, makes this an
	// incremental backup (clickhouse-backup --diff-from-remote).
	DiffFromRemote string `json:"diffFromRemote,omitempty" yaml:"diffFromRemote,omitempty"`
	// Verify, when true, runs a verification job after the backup that downloads the
	// backup and checks its integrity (without touching cluster data).
	Verify *types.StringBool `json:"verify,omitempty" yaml:"verify,omitempty"`
	// ScheduledTime is the RFC 3339 time, with its offset, the backup is due at. Empty makes it due when it is
	// created. It can be changed until the backup is validated, at that time or later.
	ScheduledTime string `json:"scheduledTime,omitempty" yaml:"scheduledTime,omitempty"`
	// StartingDeadlineSeconds bounds how late the backup may start on a cluster, counted from the time it is due:
	// a cluster whose Job is not created by then fails, as does the backup when it is not validated by then.
	// Unset waits as long as it takes.
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty" yaml:"startingDeadlineSeconds,omitempty"`

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

// DueTime returns the time the backup is due: the scheduled time, or the creation time without one. A scheduled
// time that does not parse, which validation refuses, counts as none.
func (chb *ClickHouseBackup) DueTime() time.Time {
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
	if err := validateStringBool("verify", spec.Verify); err != nil {
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
	return spec.BackupOptions.Validate()
}

// ClickHouseBackupStatus defines the observed state of a ClickHouseBackup.
type ClickHouseBackupStatus struct {
	// Phase is one of Pending, Running, Completed, Failed.
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
	// StartTime is when the first backup Job started.
	StartTime *meta.Time `json:"startTime,omitempty" yaml:"startTime,omitempty"`
	// CompletionTime is when the backup finished on every cluster.
	CompletionTime *meta.Time `json:"completionTime,omitempty" yaml:"completionTime,omitempty"`
	// DurationSeconds is the backup duration in seconds (completion - start).
	DurationSeconds int64 `json:"durationSeconds,omitempty" yaml:"durationSeconds,omitempty"`
	// Conditions represent the latest available observations of the backup state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
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
