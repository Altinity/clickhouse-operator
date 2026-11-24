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
	"regexp"
	"time"

	meta "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackupSchedule is a recurring backup of a ClickHouseInstallation.
type ClickHouseBackupSchedule struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ClickHouseBackupScheduleSpec   `json:"spec"             yaml:"spec"`
	Status ClickHouseBackupScheduleStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

type BackupPeriod string

const (
	BackupPeriodMinute BackupPeriod = "minute" // ignores the entry time
	BackupPeriodHour   BackupPeriod = "hour"   // uses only the minute of the entry time
	BackupPeriod6Hours BackupPeriod = "6hours" // at the entry hour modulo 6
	BackupPeriodDaily  BackupPeriod = "daily"
	BackupPeriodWeekly BackupPeriod = "weekly" // day 0 is Sunday
	// A day past the end of a shorter month moves to its last day
	BackupPeriodMonthly BackupPeriod = "monthly"
)

// Ordered from the shortest to the longest.
var BackupPeriods = []BackupPeriod{
	BackupPeriodMinute, BackupPeriodHour, BackupPeriod6Hours, BackupPeriodDaily, BackupPeriodWeekly, BackupPeriodMonthly,
}

// When several entries are due at one time, the run belongs to the heaviest entry. The others count it too.
func (p BackupPeriod) Weight() float64 {
	switch p {
	case BackupPeriodMinute:
		return 1.0 / 1440
	case BackupPeriodHour:
		return 1.0 / 24
	case BackupPeriod6Hours:
		return 1.0 / 4
	case BackupPeriodDaily:
		return 1
	case BackupPeriodWeekly:
		return 7
	case BackupPeriodMonthly:
		return 30
	}
	return 0
}

func (p BackupPeriod) Span(t time.Time, n int) time.Time {
	switch p {
	case BackupPeriodMinute:
		return t.Add(time.Duration(n) * time.Minute)
	case BackupPeriodHour:
		return t.Add(time.Duration(n) * time.Hour)
	case BackupPeriod6Hours:
		return t.Add(time.Duration(n) * 6 * time.Hour)
	case BackupPeriodDaily:
		return t.AddDate(0, 0, n)
	case BackupPeriodWeekly:
		return t.AddDate(0, 0, 7*n)
	case BackupPeriodMonthly:
		return t.AddDate(0, n, 0)
	}
	return t
}

func (p BackupPeriod) IsValid() bool {
	return p.Weight() > 0
}

func (p BackupPeriod) RequiresTime() bool {
	return (p == BackupPeriodDaily) || (p == BackupPeriodWeekly) || (p == BackupPeriodMonthly)
}

func (p BackupPeriod) RequiresDay() bool {
	return (p == BackupPeriodWeekly) || (p == BackupPeriodMonthly)
}

type BackupScheduleEntry struct {
	Period BackupPeriod `json:"period" yaml:"period"`
	// Time is "HH:MM" in the schedule time zone. All entries share one time, so entries due together make one backup.
	Time string `json:"time,omitempty" yaml:"time,omitempty"`
	// Day is 0..6 for weekly (0 = Sunday), 1..31 for monthly (clamped to the last day of the month).
	Day *int32 `json:"day,omitempty" yaml:"day,omitempty"`
	// Keep is how many newest backups of this entry retention keeps.
	Keep int32 `json:"keep" yaml:"keep"`
	// Incremental runs are incremental while the chain is shorter than Keep, then the next run is full.
	Incremental *types.StringBool `json:"incremental,omitempty" yaml:"incremental,omitempty"`
	// When entries share a slot, only the heaviest entry applies its overrides and incremental setting.
	ConfigOverrides []string `json:"configOverrides,omitempty" yaml:"configOverrides,omitempty"`
}

func (entry *BackupScheduleEntry) GetDay() int {
	if (entry == nil) || (entry.Day == nil) {
		return -1
	}
	return int(*entry.Day)
}

func (entry *BackupScheduleEntry) IsIncremental() bool {
	return (entry != nil) && entry.Incremental.IsTrue()
}

func (entry *BackupScheduleEntry) ParseTime() (hour, minute int, err error) {
	if (entry == nil) || (entry.Time == "") {
		return 0, 0, nil
	}
	if !backupScheduleTimeRegexp.MatchString(entry.Time) {
		return 0, 0, fmt.Errorf("%w: schedules[%s].time %q is not HH:MM", ErrInvalidBackupSpec, entry.Period, entry.Time)
	}
	hour = int(entry.Time[0]-'0')*10 + int(entry.Time[1]-'0')
	minute = int(entry.Time[3]-'0')*10 + int(entry.Time[4]-'0')
	return hour, minute, nil
}

func (entry *BackupScheduleEntry) Validate() error {
	if entry == nil {
		return nil
	}
	if !entry.Period.IsValid() {
		return fmt.Errorf("%w: schedules period %q is not one of %s, %s, %s, %s, %s, %s", ErrInvalidBackupSpec, entry.Period,
			BackupPeriodMinute, BackupPeriodHour, BackupPeriod6Hours, BackupPeriodDaily, BackupPeriodWeekly, BackupPeriodMonthly)
	}
	if _, _, err := entry.ParseTime(); err != nil {
		return err
	}
	if entry.Period.RequiresTime() && (entry.Time == "") {
		return fmt.Errorf("%w: schedules[%s].time is required", ErrInvalidBackupSpec, entry.Period)
	}
	switch {
	case entry.Period.RequiresDay() && (entry.Day == nil):
		return fmt.Errorf("%w: schedules[%s].day is required", ErrInvalidBackupSpec, entry.Period)
	case !entry.Period.RequiresDay() && (entry.Day != nil):
		return fmt.Errorf("%w: schedules[%s].day is set only for weekly and monthly", ErrInvalidBackupSpec, entry.Period)
	case (entry.Period == BackupPeriodWeekly) && ((*entry.Day < 0) || (*entry.Day > 6)):
		return fmt.Errorf("%w: schedules[weekly].day must be 0..6, 0 being Sunday", ErrInvalidBackupSpec)
	case (entry.Period == BackupPeriodMonthly) && ((*entry.Day < 1) || (*entry.Day > 31)):
		return fmt.Errorf("%w: schedules[monthly].day must be 1..31", ErrInvalidBackupSpec)
	}
	if entry.Keep < 1 {
		return fmt.Errorf("%w: schedules[%s].keep must be at least 1", ErrInvalidBackupSpec, entry.Period)
	}
	if err := ValidateConfigOverrides(fmt.Sprintf("schedules[%s].configOverrides", entry.Period), entry.ConfigOverrides); err != nil {
		return err
	}
	return validateStringBool(fmt.Sprintf("schedules[%s].incremental", entry.Period), entry.Incremental)
}

// ClickHouseBackupScheduleSpec defines the desired state of a ClickHouseBackupSchedule.
type ClickHouseBackupScheduleSpec struct {
	// ClickHouseInstallation is the name of the target CHI in the same namespace, immutable.
	ClickHouseInstallation string `json:"clickHouseInstallation" yaml:"clickHouseInstallation"`
	// Cluster is immutable. Empty backs up every backup-enabled cluster.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// Schedules run at the slots of every entry. A run counts for every entry due at that time.
	Schedules []BackupScheduleEntry `json:"schedules" yaml:"schedules"`
	// TimeZone is an IANA name. Default UTC.
	TimeZone string `json:"timeZone,omitempty" yaml:"timeZone,omitempty"`
	// Suspend pauses new runs. A run in progress goes on.
	Suspend *types.StringBool `json:"suspend,omitempty" yaml:"suspend,omitempty"`
	// StartingDeadlineSeconds skips a slot missed by more. Unset runs the latest missed slot, however late.
	StartingDeadlineSeconds    *int64 `json:"startingDeadlineSeconds,omitempty" yaml:"startingDeadlineSeconds,omitempty"`
	SuccessfulRunsHistoryLimit *int32 `json:"successfulRunsHistoryLimit,omitempty" yaml:"successfulRunsHistoryLimit,omitempty"`
	FailedRunsHistoryLimit     *int32 `json:"failedRunsHistoryLimit,omitempty" yaml:"failedRunsHistoryLimit,omitempty"`

	BackupOptions `json:",inline" yaml:",inline"`
	// Timeouts also apply to a ClickHouseBackup with scheduleRef.
	Timeouts *BackupTimeouts `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`

	// BackupNamePrefix can be set once: retention prunes only backups with this prefix, so a new one leaves old backups unpruned.
	BackupNamePrefix string `json:"backupNamePrefix,omitempty" yaml:"backupNamePrefix,omitempty"`

	Retention *BackupRetention `json:"retention,omitempty" yaml:"retention,omitempty"`

	// A new TaskID resets an aborted schedule. It does not start a run.
	TaskID *types.Id `json:"taskID,omitempty" yaml:"taskID,omitempty"`
}

const (
	RetentionHold                      = "Hold"
	RetentionPrune                     = "Prune"
	BackupRetentionKeepBackupsMaxItems = 512 // mirrored by the CRD schema maxItems
	// Above it, the two oldest changes merge into one, which holds their backups until the later expiry.
	ScheduleRetentionPreviousMaxItems = 16
)

type BackupRetention struct {
	// OnScheduleChange: Hold (default) keeps old backups until they expire under the old rules. Prune applies at once.
	OnScheduleChange string `json:"onScheduleChange,omitempty" yaml:"onScheduleChange,omitempty"`
	// KeepBackups are kept in addition to the entries, such as manual backups.
	// +listType=atomic
	KeepBackups []BackupRetentionPin `json:"keepBackups,omitempty" yaml:"keepBackups,omitempty"`
	// ReleaseHeld is a changeID from status.retention.previous. It releases that change and all earlier ones.
	ReleaseHeld string `json:"releaseHeld,omitempty" yaml:"releaseHeld,omitempty"`
}

type BackupRetentionPin struct {
	// Name is any shard backup name with a naming template, or only the time when the template has no {prefix}.
	Name string `json:"name" yaml:"name"`
	// Until is RFC 3339. Unset keeps a held backup until its latest expiry, and any other backup while it is listed.
	Until string `json:"until,omitempty" yaml:"until,omitempty"`
}

func (pin *BackupRetentionPin) GetUntil() time.Time {
	if (pin == nil) || (pin.Until == "") {
		return time.Time{}
	}
	until, err := time.Parse(time.RFC3339, pin.Until)
	if err != nil {
		return time.Time{}
	}
	return until
}

func (r *BackupRetention) GetOnScheduleChange() string {
	if (r == nil) || (r.OnScheduleChange == "") {
		return RetentionHold
	}
	return r.OnScheduleChange
}

func (r *BackupRetention) GetKeepBackups() []BackupRetentionPin {
	if r == nil {
		return nil
	}
	return r.KeepBackups
}

func (r *BackupRetention) GetReleaseHeld() string {
	if r == nil {
		return ""
	}
	return r.ReleaseHeld
}

func (r *BackupRetention) Validate() error {
	if r == nil {
		return nil
	}
	switch r.OnScheduleChange {
	case "", RetentionHold, RetentionPrune:
	default:
		return fmt.Errorf("%w: retention.onScheduleChange %q is not one of %s, %s", ErrInvalidBackupSpec, r.OnScheduleChange,
			RetentionHold, RetentionPrune)
	}
	if len(r.KeepBackups) > BackupRetentionKeepBackupsMaxItems {
		return fmt.Errorf("%w: retention.keepBackups holds more than %d backups", ErrInvalidBackupSpec, BackupRetentionKeepBackupsMaxItems)
	}
	for _, pin := range r.KeepBackups {
		if pin.Name == "" {
			return fmt.Errorf("%w: retention.keepBackups holds an empty name", ErrInvalidBackupSpec)
		}
		if err := validateBackupName("retention.keepBackups", pin.Name, BackupNameMaxLength); err != nil {
			return err
		}
		if pin.Until != "" {
			if _, err := time.Parse(time.RFC3339, pin.Until); err != nil {
				return fmt.Errorf("%w: retention.keepBackups %s: until %q is no time as RFC 3339, such as 2026-12-31T00:00:00Z",
					ErrInvalidBackupSpec, pin.Name, pin.Until)
			}
		}
	}
	if r.ReleaseHeld != "" {
		if _, err := time.Parse(time.RFC3339, r.ReleaseHeld); err != nil {
			return fmt.Errorf("%w: retention.releaseHeld %q is no changeID of status.retention.previous, a time as RFC 3339",
				ErrInvalidBackupSpec, r.ReleaseHeld)
		}
	}
	return nil
}

var backupScheduleTimeRegexp = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`) // mirrored by the CRD schema pattern

func (spec *ClickHouseBackupScheduleSpec) GetLocation() (*time.Location, error) {
	if (spec == nil) || (spec.TimeZone == "") {
		return time.UTC, nil
	}
	location, err := time.LoadLocation(spec.TimeZone)
	if err != nil {
		return nil, fmt.Errorf("%w: timeZone %q: %v", ErrInvalidBackupSpec, spec.TimeZone, err)
	}
	return location, nil
}

// IsSuspended reports whether the schedule is suspended, nil-safe.
func (spec *ClickHouseBackupScheduleSpec) IsSuspended() bool {
	return (spec != nil) && spec.Suspend.IsTrue()
}

func SharedScheduleTime(entries []BackupScheduleEntry) string {
	for i := range entries {
		if entries[i].Time != "" {
			return entries[i].Time
		}
	}
	return ""
}

func ResolveScheduleTimes(entries []BackupScheduleEntry) []BackupScheduleEntry {
	shared := SharedScheduleTime(entries)
	if shared == "" {
		shared = "00:00"
	}
	resolved := make([]BackupScheduleEntry, len(entries))
	for i := range entries {
		entries[i].DeepCopyInto(&resolved[i])
		if resolved[i].Time == "" {
			resolved[i].Time = shared
		}
	}
	return resolved
}

func (spec *ClickHouseBackupScheduleSpec) validateSchedules() error {
	if len(spec.Schedules) == 0 {
		return fmt.Errorf("%w: schedules needs at least one entry", ErrInvalidBackupSpec)
	}
	periods := map[BackupPeriod]bool{}
	sharedTime := ""
	incremental := 0
	for i := range spec.Schedules {
		entry := &spec.Schedules[i]
		if err := entry.Validate(); err != nil {
			return err
		}
		if periods[entry.Period] {
			return fmt.Errorf("%w: schedules has more than one %s entry", ErrInvalidBackupSpec, entry.Period)
		}
		periods[entry.Period] = true
		if entry.Time != "" {
			if (sharedTime != "") && (entry.Time != sharedTime) {
				return fmt.Errorf("%w: schedules set times %s and %s: every entry that sets a time sets the same",
					ErrInvalidBackupSpec, sharedTime, entry.Time)
			}
			sharedTime = entry.Time
		}
		if entry.IsIncremental() {
			incremental++
		}
	}
	if incremental > 1 {
		return fmt.Errorf("%w: at most one schedules entry is incremental", ErrInvalidBackupSpec)
	}
	return nil
}

// Validate checks the spec against the allowed character sets and values.
func (spec *ClickHouseBackupScheduleSpec) Validate() error {
	if spec == nil {
		return nil
	}
	if spec.ClickHouseInstallation == "" {
		return fmt.Errorf("%w: clickHouseInstallation is required", ErrInvalidBackupSpec)
	}
	if err := spec.validateSchedules(); err != nil {
		return err
	}
	if _, err := spec.GetLocation(); err != nil {
		return err
	}
	if err := validateStringBool("suspend", spec.Suspend); err != nil {
		return err
	}
	if (spec.StartingDeadlineSeconds != nil) && ((*spec.StartingDeadlineSeconds < 0) || (*spec.StartingDeadlineSeconds > BackupStartingDeadlineMaxSeconds)) {
		return fmt.Errorf("%w: startingDeadlineSeconds must be between 0 and %d", ErrInvalidBackupSpec, BackupStartingDeadlineMaxSeconds)
	}
	if (spec.SuccessfulRunsHistoryLimit != nil) && (*spec.SuccessfulRunsHistoryLimit < 1) {
		return fmt.Errorf("%w: successfulRunsHistoryLimit must be at least 1", ErrInvalidBackupSpec)
	}
	if (spec.FailedRunsHistoryLimit != nil) && (*spec.FailedRunsHistoryLimit < 1) {
		return fmt.Errorf("%w: failedRunsHistoryLimit must be at least 1", ErrInvalidBackupSpec)
	}
	if err := validateBackupName("backupNamePrefix", spec.BackupNamePrefix, BackupNamePrefixMaxLength); err != nil {
		return err
	}
	if err := spec.Retention.Validate(); err != nil {
		return err
	}
	if err := spec.Timeouts.Validate(); err != nil {
		return err
	}
	return spec.BackupOptions.Validate()
}

type SchedulePhase string

const (
	SchedulePhaseActive    SchedulePhase = "Active"
	SchedulePhaseSuspended SchedulePhase = "Suspended"
	// SchedulePhaseAborted also covers a failed last run, until the next successful run.
	SchedulePhaseAborted SchedulePhase = "Aborted"
)

// ClickHouseBackupScheduleStatus defines the observed state of a ClickHouseBackupSchedule.
type ClickHouseBackupScheduleStatus struct {
	Phase            SchedulePhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	Reason           string        `json:"reason,omitempty" yaml:"reason,omitempty"`
	Message          string        `json:"message,omitempty" yaml:"message,omitempty"`
	NextScheduleTime *meta.Time    `json:"nextScheduleTime,omitempty" yaml:"nextScheduleTime,omitempty"`
	// +listType=map
	// +listMapKey=period
	Entries []ScheduleEntryStatus `json:"entries,omitempty" yaml:"entries,omitempty"`
	// A removed cluster stays in Clusters while it has unpruned backups.
	// +listType=map
	// +listMapKey=name
	Clusters         []ScheduleClusterStatus `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	LastScheduleTime *meta.Time              `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	// LastSuccessfulTime is the oldest of the last successful runs of the clusters the schedule backs
	// up: by then every cluster had a backup. Unset while a cluster has none.
	LastSuccessfulTime *meta.Time `json:"lastSuccessfulTime,omitempty" yaml:"lastSuccessfulTime,omitempty"`
	// LastBackupName may already be pruned on other clusters while a cluster lags. See the per-cluster names.
	LastBackupName     string                   `json:"lastBackupName,omitempty" yaml:"lastBackupName,omitempty"`
	LastFailureTime    *meta.Time               `json:"lastFailureTime,omitempty" yaml:"lastFailureTime,omitempty"`
	LastFailureMessage string                   `json:"lastFailureMessage,omitempty" yaml:"lastFailureMessage,omitempty"`
	Retention          *ScheduleRetentionStatus `json:"retention,omitempty" yaml:"retention,omitempty"`
	// Active holds at most one run per cluster. The operator resumes them after a restart.
	// +listType=map
	// +listMapKey=name
	Active     []ScheduledRun `json:"active,omitempty" yaml:"active,omitempty"`
	RecentRuns []ScheduledRun `json:"recentRuns,omitempty" yaml:"recentRuns,omitempty"`
	// Backups are the backups of the schedule in the storage of each cluster, as retention listed them after the last
	// successful run on the cluster, without those it deleted. Newest first.
	Backups []StoredBackup `json:"backups,omitempty" yaml:"backups,omitempty"`
	// Conditions represent the latest available observations of the schedule state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`

	BackupTaskStatus `json:",inline" yaml:",inline"`
}

type ScheduledRun struct {
	Slot meta.Time `json:"slot" yaml:"slot"`
	// BackupName is the first shard backup with a naming template.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`

	OperationClusterStatus `json:",inline" yaml:",inline"`
}

type ScheduleRetentionStatus struct {
	AppliedSchedules []BackupScheduleEntry `json:"appliedSchedules,omitempty" yaml:"appliedSchedules,omitempty"`
	AppliedTimeZone  string                `json:"appliedTimeZone,omitempty" yaml:"appliedTimeZone,omitempty"`
	// Previous holds unreleased changes, oldest first. Retention keeps their backups until they expire under the old rules.
	Previous []RetiredScheduleRules `json:"previous,omitempty" yaml:"previous,omitempty"`
	// ChangeID is the newest change in Previous, for spec.retention.releaseHeld.
	ChangeID string `json:"changeID,omitempty" yaml:"changeID,omitempty"`
}

func (status *ScheduleRetentionStatus) GetPrevious() []RetiredScheduleRules {
	if status == nil {
		return nil
	}
	return status.Previous
}

type RetiredScheduleRules struct {
	// ChangeID is the RFC 3339 UTC time of the change.
	ChangeID  string                `json:"changeID" yaml:"changeID"`
	Schedules []BackupScheduleEntry `json:"schedules,omitempty" yaml:"schedules,omitempty"`
	TimeZone  string                `json:"timeZone,omitempty" yaml:"timeZone,omitempty"`
	// KeepAll marks merged changes: every backup made before the change is held until Expires.
	KeepAll bool `json:"keepAll,omitempty" yaml:"keepAll,omitempty"`
	// Expires is RFC 3339. For a merged change, it is the later expiry.
	Expires string `json:"expires,omitempty" yaml:"expires,omitempty"`
}

func (rules *RetiredScheduleRules) HeldUntil() time.Time {
	if rules == nil {
		return time.Time{}
	}
	expires, _ := time.Parse(time.RFC3339, rules.Expires)
	return expires
}

type ScheduleEntryStatus struct {
	Period BackupPeriod `json:"period" yaml:"period"`
	// LastScheduleTime counts a run even if a longer entry owns it.
	LastScheduleTime *meta.Time `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	LastBackupName   string     `json:"lastBackupName,omitempty" yaml:"lastBackupName,omitempty"`
}

func (status *ClickHouseBackupScheduleStatus) GetEntry(period BackupPeriod) *ScheduleEntryStatus {
	if status == nil {
		return nil
	}
	for i := range status.Entries {
		if status.Entries[i].Period == period {
			return &status.Entries[i]
		}
	}
	return nil
}

// ScheduleClusterStatus is the observed state of the runs of a schedule on one cluster.
type ScheduleClusterStatus struct {
	// Name is the name of the cluster.
	Name string `json:"name" yaml:"name"`
	// ConfigSecret identifies the storage of the unpruned backups of a removed cluster.
	ConfigSecret string `json:"configSecret,omitempty" yaml:"configSecret,omitempty"`
	// LastScheduleTime is a slot time, not a start time.
	LastScheduleTime   *meta.Time `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	LastSuccessfulTime *meta.Time `json:"lastSuccessfulTime,omitempty" yaml:"lastSuccessfulTime,omitempty"`
	LastBackupName     string     `json:"lastBackupName,omitempty" yaml:"lastBackupName,omitempty"`
	LastFailureTime    *meta.Time `json:"lastFailureTime,omitempty" yaml:"lastFailureTime,omitempty"`
	LastFailureMessage string     `json:"lastFailureMessage,omitempty" yaml:"lastFailureMessage,omitempty"`
	// RetentionHosts lists the hosts of the cluster the schedule backs up that a scheduled run has backed
	// up: those that hold its backups, which retention, when on, prunes.
	RetentionHosts []string `json:"retentionHosts,omitempty" yaml:"retentionHosts,omitempty"`
	// UnprunedHosts lists the hosts that left RetentionHosts while retention was on. No run reaches
	// them any more, so retention no longer prunes the backups they made, which stay until
	// deleted by hand. A host that rejoins RetentionHosts is dropped from it.
	UnprunedHosts []string `json:"unprunedHosts,omitempty" yaml:"unprunedHosts,omitempty"`
	// HeldBackups are backups the current schedule would delete but a held change keeps, as of the last successful run.
	HeldBackups []HeldBackup `json:"heldBackups,omitempty" yaml:"heldBackups,omitempty"`
	// BackupsListTime is when retention last listed the backups of the cluster in status.backups.
	BackupsListTime *meta.Time `json:"backupsListTime,omitempty" yaml:"backupsListTime,omitempty"`
}

type HeldBackup struct {
	// Name is the first shard backup with a naming template.
	Name string `json:"name" yaml:"name"`
	// Expires is RFC 3339, empty for a backup held until the change is released.
	Expires string `json:"expires,omitempty" yaml:"expires,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackupScheduleList defines a list of ClickHouseBackupSchedule resources.
type ClickHouseBackupScheduleList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseBackupSchedule `json:"items" yaml:"items"`
}
