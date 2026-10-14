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

	batch "k8s.io/api/batch/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackupSchedule defines a recurring backup of a ClickHouseInstallation.
// The operator reconciles it into a Kubernetes CronJob per cluster that drives the clickhouse-backup
// sidecar on the configured schedule. The CronJobs of a schedule share its cron schedule, so the
// runs of one slot make one backup name on every cluster. Native CronJob semantics handle
// scheduling, suspension, concurrency and job history.
type ClickHouseBackupSchedule struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ClickHouseBackupScheduleSpec   `json:"spec"             yaml:"spec"`
	Status ClickHouseBackupScheduleStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// ClickHouseBackupScheduleSpec defines the desired state of a ClickHouseBackupSchedule.
type ClickHouseBackupScheduleSpec struct {
	// ClickHouseInstallation is the name of the target CHI in the same namespace, immutable.
	ClickHouseInstallation string `json:"clickHouseInstallation" yaml:"clickHouseInstallation"`
	// Cluster optionally restricts the backups to one cluster of the CHI, immutable. Empty backs up
	// every backup-enabled cluster, each by a CronJob of its own.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// Schedule is a cron expression in standard Kubernetes CronJob format.
	Schedule string `json:"schedule" yaml:"schedule"`
	// TimeZone is the IANA time zone the schedule is read in, as the CronJob's timeZone. Empty reads it in
	// the time zone of the Kubernetes controller manager, UTC as a rule.
	TimeZone string `json:"timeZone,omitempty" yaml:"timeZone,omitempty"`
	// Suspend pauses creation of new backup jobs. Existing jobs are unaffected.
	Suspend *types.StringBool `json:"suspend,omitempty" yaml:"suspend,omitempty"`
	// ConcurrencyPolicy controls how concurrent executions are treated.
	// One of Forbid (default), Allow, Replace.
	ConcurrencyPolicy string `json:"concurrencyPolicy,omitempty" yaml:"concurrencyPolicy,omitempty"`
	// StartingDeadlineSeconds is the deadline in seconds for starting a missed job.
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty" yaml:"startingDeadlineSeconds,omitempty"`
	// SuccessfulJobsHistoryLimit is how many successful finished jobs to retain, at least 1. Default 3.
	SuccessfulJobsHistoryLimit *int32 `json:"successfulJobsHistoryLimit,omitempty" yaml:"successfulJobsHistoryLimit,omitempty"`
	// FailedJobsHistoryLimit is how many failed finished jobs to retain, at least 1. Default 1.
	FailedJobsHistoryLimit *int32 `json:"failedJobsHistoryLimit,omitempty" yaml:"failedJobsHistoryLimit,omitempty"`
	// BackupTemplate is the backup specification stamped out on each scheduled run.
	BackupTemplate ClickHouseBackupTemplateSpec `json:"backupTemplate,omitempty" yaml:"backupTemplate,omitempty"`
	// Retention tells which of the backups the schedule made are deleted: a rule of the series of backups
	// named with its backupNamePrefix, not of any one of them.
	Retention BackupRetention `json:"retention,omitempty" yaml:"retention,omitempty"`
}

// BackupRetention tells which backups of a schedule's series are deleted after each successful run.
type BackupRetention struct {
	// KeepLast, when greater than zero, keeps the N most recent backups of the series that hold
	// data, and apart from them the N most recent that hold none; older ones are deleted after each
	// successful run. Zero keeps all.
	KeepLast *int32 `json:"keepLast,omitempty" yaml:"keepLast,omitempty"`
}

// GetKeepLast returns the retention count, zero meaning keep all, nil-safe.
func (retention *BackupRetention) GetKeepLast() int32 {
	if (retention == nil) || (retention.KeepLast == nil) || (*retention.KeepLast < 0) {
		return 0
	}
	return *retention.KeepLast
}

// Validate checks the retention against the allowed values.
func (retention *BackupRetention) Validate() error {
	if (retention != nil) && (retention.KeepLast != nil) && (*retention.KeepLast < 0) {
		return fmt.Errorf("%w: retention.keepLast must not be negative", ErrInvalidBackupSpec)
	}
	return nil
}

// GetTimeZone returns the time zone the schedule is read in, nil when it names none, nil-safe.
func (spec *ClickHouseBackupScheduleSpec) GetTimeZone() *string {
	if (spec == nil) || (spec.TimeZone == "") {
		return nil
	}
	return &spec.TimeZone
}

// IsSuspended reports whether the schedule is suspended, nil-safe.
func (spec *ClickHouseBackupScheduleSpec) IsSuspended() bool {
	return (spec != nil) && spec.Suspend.IsTrue()
}

// Validate checks the spec against the allowed character sets and values.
func (spec *ClickHouseBackupScheduleSpec) Validate() error {
	if spec == nil {
		return nil
	}
	if spec.ClickHouseInstallation == "" {
		return fmt.Errorf("%w: clickHouseInstallation is required", ErrInvalidBackupSpec)
	}
	if spec.Schedule == "" {
		return fmt.Errorf("%w: schedule is required", ErrInvalidBackupSpec)
	}
	if err := validateStringBool("suspend", spec.Suspend); err != nil {
		return err
	}
	switch batch.ConcurrencyPolicy(spec.ConcurrencyPolicy) {
	case "", batch.AllowConcurrent, batch.ForbidConcurrent, batch.ReplaceConcurrent:
	default:
		return fmt.Errorf("%w: concurrencyPolicy %q is not one of %s, %s, %s", ErrInvalidBackupSpec,
			spec.ConcurrencyPolicy, batch.AllowConcurrent, batch.ForbidConcurrent, batch.ReplaceConcurrent)
	}
	// The operator learns from the Job of a run how it went, the backup it made and the hosts it
	// backed up: a run whose Job the CronJob deletes as soon as it finishes may go unreported.
	if (spec.SuccessfulJobsHistoryLimit != nil) && (*spec.SuccessfulJobsHistoryLimit < 1) {
		return fmt.Errorf("%w: successfulJobsHistoryLimit must be at least 1", ErrInvalidBackupSpec)
	}
	if (spec.FailedJobsHistoryLimit != nil) && (*spec.FailedJobsHistoryLimit < 1) {
		return fmt.Errorf("%w: failedJobsHistoryLimit must be at least 1", ErrInvalidBackupSpec)
	}
	if err := spec.Retention.Validate(); err != nil {
		return err
	}
	return spec.BackupTemplate.Validate()
}

// ClickHouseBackupTemplateSpec is the backup specification applied to each scheduled backup.
type ClickHouseBackupTemplateSpec struct {
	// BackupNamePrefix is prepended to the generated, timestamped backup name. It also names the
	// series retention prunes: only backups named with this prefix and the time of a run. Defaults to
	// "<namespace>-<installation>-<schedule>", shortened with a hash when longer than
	// BackupNamePrefixMaxLength.
	BackupNamePrefix string `json:"backupNamePrefix,omitempty" yaml:"backupNamePrefix,omitempty"`

	BackupOptions `json:",inline" yaml:",inline"`
}

// Validate checks the template against the allowed character sets and values.
func (spec *ClickHouseBackupTemplateSpec) Validate() error {
	if spec == nil {
		return nil
	}
	if err := validateBackupName("backupNamePrefix", spec.BackupNamePrefix, BackupNamePrefixMaxLength); err != nil {
		return err
	}
	return spec.BackupOptions.Validate()
}

// ClickHouseBackupScheduleStatus defines the observed state of a ClickHouseBackupSchedule.
type ClickHouseBackupScheduleStatus struct {
	// Clusters are the clusters the schedule backs up, and a cluster it no longer backs up while
	// retention leaves backups of it unpruned, with the state of the runs on each.
	// +listType=map
	// +listMapKey=name
	Clusters []ScheduleClusterStatus `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	// LastScheduleTime is the last time a backup job was scheduled on any cluster.
	LastScheduleTime *meta.Time `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	// LastSuccessfulTime is the oldest of the last successful runs of the clusters the schedule backs
	// up: by then every cluster had a backup. Unset while a cluster has none.
	LastSuccessfulTime *meta.Time `json:"lastSuccessfulTime,omitempty" yaml:"lastSuccessfulTime,omitempty"`
	// LastBackupName is the newest backup a scheduled run made on every cluster the schedule
	// backs up. While a cluster lags, the retention of the others may have pruned it already, so the
	// per-cluster names tell what each cluster still holds.
	LastBackupName string `json:"lastBackupName,omitempty" yaml:"lastBackupName,omitempty"`
	// LastFailureTime is the last time a scheduled backup job failed on any cluster.
	LastFailureTime *meta.Time `json:"lastFailureTime,omitempty" yaml:"lastFailureTime,omitempty"`
	// LastFailureMessage tells why the last failed scheduled backup job failed.
	LastFailureMessage string `json:"lastFailureMessage,omitempty" yaml:"lastFailureMessage,omitempty"`
	// Conditions represent the latest available observations of the schedule state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`
}

// ScheduleClusterStatus is the observed state of the runs of a schedule on one cluster.
type ScheduleClusterStatus struct {
	// Name is the name of the cluster.
	Name string `json:"name" yaml:"name"`
	// CronJobName is the name of the CronJob that runs the schedule on the cluster.
	CronJobName string `json:"cronJobName,omitempty" yaml:"cronJobName,omitempty"`
	// Storage is the storage the runs of the cluster keep their backups in, as its CronJob was last built.
	Storage string `json:"storage,omitempty" yaml:"storage,omitempty"`
	// LastScheduleTime is the last time a backup job was scheduled on the cluster.
	LastScheduleTime *meta.Time `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	// LastSuccessfulTime is the last time a scheduled backup job of the cluster completed successfully.
	LastSuccessfulTime *meta.Time `json:"lastSuccessfulTime,omitempty" yaml:"lastSuccessfulTime,omitempty"`
	// LastBackupName is the backup the last successful scheduled run of the cluster made.
	LastBackupName string `json:"lastBackupName,omitempty" yaml:"lastBackupName,omitempty"`
	// LastFailureTime is the last time a scheduled backup job of the cluster failed.
	LastFailureTime *meta.Time `json:"lastFailureTime,omitempty" yaml:"lastFailureTime,omitempty"`
	// LastFailureMessage tells why the last failed scheduled backup job of the cluster failed.
	LastFailureMessage string `json:"lastFailureMessage,omitempty" yaml:"lastFailureMessage,omitempty"`
	// RetentionHosts lists the hosts of the cluster the schedule backs up that a scheduled run has backed
	// up: those that hold its backups, which retention, when on, prunes.
	RetentionHosts []string `json:"retentionHosts,omitempty" yaml:"retentionHosts,omitempty"`
	// UnprunedHosts lists the hosts that left RetentionHosts while retention was on. No run reaches
	// them any more, so retention no longer prunes the backups they made, which stay until
	// deleted by hand. A host that rejoins RetentionHosts is dropped from it.
	UnprunedHosts []string `json:"unprunedHosts,omitempty" yaml:"unprunedHosts,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackupScheduleList defines a list of ClickHouseBackupSchedule resources.
type ClickHouseBackupScheduleList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseBackupSchedule `json:"items" yaml:"items"`
}
