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

// ClickHouseBackupSchedule defines a recurring backup of a ClickHouseInstallation.
// The operator runs it itself at the slots of the finest of its entries, on every cluster it backs up; a run whose
// time matches a coarser entry counts for it as well. The runs of one slot make one backup name on every cluster, and
// live in the status of the schedule alone: no resource is created for a run. After a successful run the backups of
// the schedule's series that no entry keeps are deleted.
type ClickHouseBackupSchedule struct {
	meta.TypeMeta   `json:",inline"            yaml:",inline"`
	meta.ObjectMeta `json:"metadata,omitempty" yaml:"metadata,omitempty"`

	Spec   ClickHouseBackupScheduleSpec   `json:"spec"             yaml:"spec"`
	Status ClickHouseBackupScheduleStatus `json:"status,omitempty" yaml:"status,omitempty"`
}

// BackupPeriod is how often an entry of a schedule is due.
type BackupPeriod string

// Backup periods of a schedule entry.
const (
	// BackupPeriodMinute is due every minute; its time, if any, is not used.
	BackupPeriodMinute BackupPeriod = "minute"
	// BackupPeriodHour is due every hour, at the minute of the entry's time.
	BackupPeriodHour BackupPeriod = "hour"
	// BackupPeriod6Hours is due every 6 hours, at the hour of the entry's time modulo 6 and its minute.
	BackupPeriod6Hours BackupPeriod = "6hours"
	// BackupPeriodDaily is due every day, at the entry's time.
	BackupPeriodDaily BackupPeriod = "daily"
	// BackupPeriodWeekly is due every week, on the entry's day (0 = Sunday) at its time.
	BackupPeriodWeekly BackupPeriod = "weekly"
	// BackupPeriodMonthly is due every month, on the entry's day, clamped to the last day of a shorter month, at
	// its time.
	BackupPeriodMonthly BackupPeriod = "monthly"
)

// BackupPeriods lists the periods from the shortest to the longest.
var BackupPeriods = []BackupPeriod{
	BackupPeriodMinute, BackupPeriodHour, BackupPeriod6Hours, BackupPeriodDaily, BackupPeriodWeekly, BackupPeriodMonthly,
}

// Weight returns the length of the period in days, 0 for an unknown period. Of the entries due at one time, the run
// is that of the entry of the largest weight: the others count it as theirs.
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

// Span returns the time n periods after t: n months by the calendar for a monthly period, t for an unknown one.
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

// IsValid reports whether the period is one of BackupPeriods.
func (p BackupPeriod) IsValid() bool {
	return p.Weight() > 0
}

// RequiresTime reports whether an entry of the period must set its time.
func (p BackupPeriod) RequiresTime() bool {
	return (p == BackupPeriodDaily) || (p == BackupPeriodWeekly) || (p == BackupPeriodMonthly)
}

// RequiresDay reports whether an entry of the period must set its day, which no other entry may set.
func (p BackupPeriod) RequiresDay() bool {
	return (p == BackupPeriodWeekly) || (p == BackupPeriodMonthly)
}

// BackupScheduleEntry is one period a schedule backs up at and how many of its backups it keeps.
type BackupScheduleEntry struct {
	// Period is how often the entry is due: minute, hour, 6hours, daily, weekly or monthly.
	Period BackupPeriod `json:"period" yaml:"period"`
	// Time is the "HH:MM" the entry is due at, in the schedule's time zone. Required for daily, weekly and monthly.
	// Every entry that sets a time sets the same. For hour only its minute is used, for 6hours its hour modulo 6 and
	// its minute; an hour or 6hours entry that sets none takes the time the other entries share, 00:00 if none does,
	// so that every slot of a coarser entry is a slot of the finest one.
	Time string `json:"time,omitempty" yaml:"time,omitempty"`
	// Day is the day the entry is due on: for weekly 0..6, 0 being Sunday; for monthly 1..31, clamped to the last day
	// of a shorter month. Required for weekly and monthly, forbidden otherwise.
	Day *int32 `json:"day,omitempty" yaml:"day,omitempty"`
	// Keep is how many of the newest backups due at a slot of the entry are kept, at least 1.
	Keep int32 `json:"keep" yaml:"keep"`
	// Incremental makes the runs of the entry incremental backups on the newest backup of the series, as long as the
	// chain of incremental backups is shorter than Keep; the run after makes a full backup again. At most one entry.
	Incremental *types.StringBool `json:"incremental,omitempty" yaml:"incremental,omitempty"`
	// ConfigOverrides are clickhouse-backup settings the runs of the entry make their backups with in place of those
	// of the sidecar, as KEY=VALUE of the env var of the setting, e.g. S3_STORAGE_CLASS=GLACIER_IR for a monthly
	// entry, see type_backup_overrides.go: those of the heaviest entry of a slot, which a lighter one of the slot
	// leaves alone, as it does its incremental.
	ConfigOverrides []string `json:"configOverrides,omitempty" yaml:"configOverrides,omitempty"`
}

// GetDay returns the day of the entry, -1 when it sets none, nil-safe.
func (entry *BackupScheduleEntry) GetDay() int {
	if (entry == nil) || (entry.Day == nil) {
		return -1
	}
	return int(*entry.Day)
}

// IsIncremental reports whether the runs of the entry make incremental backups, nil-safe.
func (entry *BackupScheduleEntry) IsIncremental() bool {
	return (entry != nil) && entry.Incremental.IsTrue()
}

// ParseTime returns the hour and the minute of the entry's time, 0 and 0 when it sets none.
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

// Validate checks the entry on its own, nil-safe.
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
	// Cluster optionally restricts the backups to one cluster of the CHI, immutable. Empty backs up
	// every backup-enabled cluster.
	Cluster string `json:"cluster,omitempty" yaml:"cluster,omitempty"`
	// Schedules are the backup periods, at most one per period. All entries that have a time share it.
	Schedules []BackupScheduleEntry `json:"schedules" yaml:"schedules"`
	// TimeZone is the IANA time zone the schedules are read in, and the run matches its time against the entries in.
	// Empty reads them in UTC.
	TimeZone string `json:"timeZone,omitempty" yaml:"timeZone,omitempty"`
	// Suspend pauses new runs. A run in progress goes on.
	Suspend *types.StringBool `json:"suspend,omitempty" yaml:"suspend,omitempty"`
	// StartingDeadlineSeconds is how late, in seconds, a run may start after its slot, as one that waited for a run
	// in progress or a restore: a slot missed by more is skipped. Unset, the latest missed slot runs however late.
	// Runs never overlap: a slot that comes while a run of the cluster is in progress waits for it.
	StartingDeadlineSeconds *int64 `json:"startingDeadlineSeconds,omitempty" yaml:"startingDeadlineSeconds,omitempty"`
	// SuccessfulRunsHistoryLimit is how many successful finished runs status.recentRuns keeps, at least 1. Default 3.
	SuccessfulRunsHistoryLimit *int32 `json:"successfulRunsHistoryLimit,omitempty" yaml:"successfulRunsHistoryLimit,omitempty"`
	// FailedRunsHistoryLimit is how many failed finished runs status.recentRuns keeps, at least 1. Default 1.
	FailedRunsHistoryLimit *int32 `json:"failedRunsHistoryLimit,omitempty" yaml:"failedRunsHistoryLimit,omitempty"`

	// BackupOptions are the options every run makes its backup with, as a ClickHouseBackup sets them.
	BackupOptions `json:",inline" yaml:",inline"`
	// Timeouts bound how the operator waits for the commands of every run on the sidecars, and of a ClickHouseBackup
	// that runs the schedule and sets none of its own. A field left unset is that of the operator configuration.
	Timeouts *BackupTimeouts `json:"timeouts,omitempty" yaml:"timeouts,omitempty"`

	// BackupNamePrefix is prepended to the generated, timestamped backup name of every run. It also names the
	// series retention prunes: only backups named with this prefix and the time of a slot. Defaults to
	// "<namespace>-<installation>-<schedule>", shortened with a hash when longer than BackupNamePrefixMaxLength.
	// It can be set once and is immutable after, as a changed prefix would leave the backups of the former series
	// unpruned. Setting it later replaces the default prefix the same way: it is meant for a schedule that has not
	// run yet, such as one held for a recovery.
	BackupNamePrefix string `json:"backupNamePrefix,omitempty" yaml:"backupNamePrefix,omitempty"`

	// Retention tells what retention keeps beyond the entries: backups pinned by name, and the backups the schedule
	// kept before a change of its entries or time zone, held until they expire or are released.
	Retention *BackupRetention `json:"retention,omitempty" yaml:"retention,omitempty"`

	// TaskID resets a schedule that is aborted when it changes to a value the status does not record: the schedule is
	// validated again at once, and its phase told anew. It runs no slot by itself.
	TaskID *types.Id `json:"taskID,omitempty" yaml:"taskID,omitempty"`
}

// What retention does with the backups the entries kept before a change of a schedule.
const (
	// RetentionHold, the default, keeps every backup the schedule kept before a change of its entries or time zone
	// until it expires as the schedule before the change would have deleted it, or until the change is released:
	// retention tells which of them the schedule as it is would delete, and when each expires.
	RetentionHold = "Hold"
	// RetentionPrune applies a change at once: retention keeps what the schedule as it is keeps, as the Altinity
	// usual backup workflow does without the backups its dialog of a change pins.
	RetentionPrune = "Prune"
	// BackupRetentionKeepBackupsMaxItems bounds keepBackups. Mirrored by the CRD schema maxItems.
	BackupRetentionKeepBackupsMaxItems = 512
	// ScheduleRetentionPreviousMaxItems bounds status.retention.previous: the oldest two changes beyond it are
	// merged into one that holds every backup made before the newer of them, until the later of their expiries.
	ScheduleRetentionPreviousMaxItems = 16
)

// BackupRetention is what retention keeps beyond the entries of a schedule.
type BackupRetention struct {
	// OnScheduleChange is what retention does with the backups the entries kept before a change of the entries or
	// of the time zone: Hold (default) keeps them until they expire as the entries before the change would have
	// deleted them, or until the change is released by ReleaseHeld; Prune applies the change at once.
	OnScheduleChange string `json:"onScheduleChange,omitempty" yaml:"onScheduleChange,omitempty"`
	// KeepBackups are backups retention keeps beyond the entries, as the Altinity usual backup workflow keeps the
	// backups its dialog of a schedule change pins and those made by hand.
	// +listType=atomic
	KeepBackups []BackupRetentionPin `json:"keepBackups,omitempty" yaml:"keepBackups,omitempty"`
	// ReleaseHeld, set to the changeID of status.retention.previous, releases the backups held for that change and
	// for every earlier one: the next run deletes those neither the entries nor KeepBackups keep. A later change is
	// never released by it.
	ReleaseHeld string `json:"releaseHeld,omitempty" yaml:"releaseHeld,omitempty"`
}

// BackupRetentionPin is a backup retention keeps beyond the entries of a schedule, and until when.
type BackupRetentionPin struct {
	// Name is the name of the backup, or with a naming template the name of the backup of any shard, or the time
	// alone when the template holds no {prefix}.
	Name string `json:"name" yaml:"name"`
	// Until, a time as RFC 3339, is when the pin ends: retention keeps the backup until then, and treats it as any
	// other backup after. Unset, a backup the schedule kept before a change of it, held for status.retention.previous,
	// is kept until it expires as that schedule would have deleted it, the latest expiry of the changes that hold it;
	// any other backup, as one made by hand, is kept as long as it is named here.
	Until string `json:"until,omitempty" yaml:"until,omitempty"`
}

// GetUntil returns when the pin ends, the zero time when it sets none or a time that does not parse, nil-safe.
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

// GetOnScheduleChange returns what retention does on a change of the schedule, RetentionHold when unset, nil-safe.
func (r *BackupRetention) GetOnScheduleChange() string {
	if (r == nil) || (r.OnScheduleChange == "") {
		return RetentionHold
	}
	return r.OnScheduleChange
}

// GetKeepBackups returns the pinned backups, nil-safe.
func (r *BackupRetention) GetKeepBackups() []BackupRetentionPin {
	if r == nil {
		return nil
	}
	return r.KeepBackups
}

// GetReleaseHeld returns the change released, "" for none, nil-safe.
func (r *BackupRetention) GetReleaseHeld() string {
	if r == nil {
		return ""
	}
	return r.ReleaseHeld
}

// Validate checks the retention of a schedule, nil-safe.
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

// backupScheduleTimeRegexp is the "HH:MM" form of the time of a schedule entry. Mirrored by the CRD schema pattern.
var backupScheduleTimeRegexp = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)

// GetTimeZone returns the time zone the schedule is read in, nil when it names none, nil-safe.
func (spec *ClickHouseBackupScheduleSpec) GetTimeZone() *string {
	if (spec == nil) || (spec.TimeZone == "") {
		return nil
	}
	return &spec.TimeZone
}

// GetLocation returns the time zone the schedule is read in, UTC when it names none, nil-safe.
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

// SharedTime returns the time the entries that set one share, "" when none sets one, nil-safe. Validate makes sure
// they share one.
func (spec *ClickHouseBackupScheduleSpec) SharedTime() string {
	if spec == nil {
		return ""
	}
	return SharedScheduleTime(spec.Schedules)
}

// SharedScheduleTime returns the time of the first entry that sets one, "" when none does.
func SharedScheduleTime(entries []BackupScheduleEntry) string {
	for i := range entries {
		if entries[i].Time != "" {
			return entries[i].Time
		}
	}
	return ""
}

// ResolveScheduleTimes returns a copy of the entries in which every entry that sets no time, an hour or a 6hours one,
// takes the shared time, "00:00" when no entry sets one: the times the slots of the entries are matched by.
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

// GetIncrementalEntry returns the entry whose runs make incremental backups, nil when none does, nil-safe.
func (spec *ClickHouseBackupScheduleSpec) GetIncrementalEntry() *BackupScheduleEntry {
	if spec == nil {
		return nil
	}
	for i := range spec.Schedules {
		if spec.Schedules[i].IsIncremental() {
			return &spec.Schedules[i]
		}
	}
	return nil
}

// GetEntry returns the entry of the period, nil when there is none, nil-safe.
func (spec *ClickHouseBackupScheduleSpec) GetEntry(period BackupPeriod) *BackupScheduleEntry {
	if spec == nil {
		return nil
	}
	for i := range spec.Schedules {
		if spec.Schedules[i].Period == period {
			return &spec.Schedules[i]
		}
	}
	return nil
}

// validateSchedules checks the entries of a schedule: at least one, each valid, one per period, one time shared by
// those that set one, at most one incremental.
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
	// The schedule runs at the slots of the finest entry. Those of a daily or finer one are every day, at the shared
	// time, which every coarser slot is; a weekly one fires on its weekday only, which a monthly day mostly is not.
	if periods[BackupPeriodWeekly] && periods[BackupPeriodMonthly] &&
		!periods[BackupPeriodDaily] && !periods[BackupPeriod6Hours] && !periods[BackupPeriodHour] && !periods[BackupPeriodMinute] {
		return fmt.Errorf("%w: schedules with a weekly and a monthly entry need a daily or finer one too, "+
			"as the runs are those of the finest entry", ErrInvalidBackupSpec)
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

// SchedulePhase is the lifecycle phase of a ClickHouseBackupSchedule.
type SchedulePhase string

// Schedule phases reported in the CR Status.
const (
	// SchedulePhaseActive: the schedule runs.
	SchedulePhaseActive SchedulePhase = "Active"
	// SchedulePhaseSuspended: the spec suspends the schedule.
	SchedulePhaseSuspended SchedulePhase = "Suspended"
	// SchedulePhaseAborted: the schedule cannot run, Reason and Message tell why: an invalid spec, a missing
	// installation or cluster, backup off, or a last run that failed, until the next successful one.
	SchedulePhaseAborted SchedulePhase = "Aborted"
)

// ClickHouseBackupScheduleStatus defines the observed state of a ClickHouseBackupSchedule.
type ClickHouseBackupScheduleStatus struct {
	// Phase is one of Active, Suspended, Aborted.
	Phase SchedulePhase `json:"phase,omitempty" yaml:"phase,omitempty"`
	// Reason tells, in one word, why the schedule is in its phase.
	Reason string `json:"reason,omitempty" yaml:"reason,omitempty"`
	// Message tells the same in a sentence.
	Message string `json:"message,omitempty" yaml:"message,omitempty"`
	// NextScheduleTime is the next slot of the schedule.
	NextScheduleTime *meta.Time `json:"nextScheduleTime,omitempty" yaml:"nextScheduleTime,omitempty"`
	// Entries are the state of the runs of each entry of the schedule.
	// +listType=map
	// +listMapKey=period
	Entries []ScheduleEntryStatus `json:"entries,omitempty" yaml:"entries,omitempty"`
	// Clusters are the clusters the schedule backs up, and a cluster it no longer backs up while
	// retention leaves backups of it unpruned, with the state of the runs on each.
	// +listType=map
	// +listMapKey=name
	Clusters []ScheduleClusterStatus `json:"clusters,omitempty" yaml:"clusters,omitempty"`
	// LastScheduleTime is the last time a backup was scheduled.
	LastScheduleTime *meta.Time `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	// LastSuccessfulTime is the oldest of the last successful runs of the clusters the schedule backs
	// up: by then every cluster had a backup. Unset while a cluster has none.
	LastSuccessfulTime *meta.Time `json:"lastSuccessfulTime,omitempty" yaml:"lastSuccessfulTime,omitempty"`
	// LastBackupName is the newest backup a scheduled run made on every cluster the schedule
	// backs up. While a cluster lags, the retention of the others may have pruned it already, so the
	// per-cluster names tell what each cluster still holds.
	LastBackupName string `json:"lastBackupName,omitempty" yaml:"lastBackupName,omitempty"`
	// LastFailureTime is the last time a scheduled backup failed on any cluster.
	LastFailureTime *meta.Time `json:"lastFailureTime,omitempty" yaml:"lastFailureTime,omitempty"`
	// LastFailureMessage tells why the last failed scheduled backup failed.
	LastFailureMessage string `json:"lastFailureMessage,omitempty" yaml:"lastFailureMessage,omitempty"`
	// Retention records the entries and time zone the runs apply, and the changes whose backups retention holds.
	Retention *ScheduleRetentionStatus `json:"retention,omitempty" yaml:"retention,omitempty"`
	// Active are the runs in progress, one per cluster at most, which the operator resumes after a restart.
	// +listType=map
	// +listMapKey=name
	Active []ScheduledRun `json:"active,omitempty" yaml:"active,omitempty"`
	// RecentRuns are the newest finished runs, newest first: as many successful ones as successfulRunsHistoryLimit
	// and failed ones as failedRunsHistoryLimit.
	RecentRuns []ScheduledRun `json:"recentRuns,omitempty" yaml:"recentRuns,omitempty"`
	// Conditions represent the latest available observations of the schedule state.
	Conditions []meta.Condition `json:"conditions,omitempty" yaml:"conditions,omitempty"`

	BackupTaskStatus `json:",inline" yaml:",inline"`
}

// ScheduledRun is a run of a schedule on one cluster, the one Name names: the backup of a slot.
type ScheduledRun struct {
	// Slot is the time the run was due.
	Slot meta.Time `json:"slot" yaml:"slot"`
	// BackupName is the backup the run makes, with a naming template the backup of the first shard.
	BackupName string `json:"backupName,omitempty" yaml:"backupName,omitempty"`

	OperationClusterStatus `json:",inline" yaml:",inline"`
}

// ScheduleRetentionStatus records the entries and time zone the runs of a schedule apply, and the changes of them
// whose backups retention holds.
type ScheduleRetentionStatus struct {
	// AppliedSchedules and AppliedTimeZone are the entries and the time zone of the schedule the runs apply.
	AppliedSchedules []BackupScheduleEntry `json:"appliedSchedules,omitempty" yaml:"appliedSchedules,omitempty"`
	AppliedTimeZone  string                `json:"appliedTimeZone,omitempty" yaml:"appliedTimeZone,omitempty"`
	// Previous are the entries and time zones the schedule had before each change not released yet, oldest first:
	// retention keeps every backup they kept when the change was made, until it expires as they would have deleted
	// it. A change is dropped once every backup it holds has expired, at its expires.
	Previous []RetiredScheduleRules `json:"previous,omitempty" yaml:"previous,omitempty"`
	// ChangeID is that of the newest change of Previous, which spec.retention.releaseHeld releases with every earlier
	// one; empty with none.
	ChangeID string `json:"changeID,omitempty" yaml:"changeID,omitempty"`
}

// GetPrevious returns the changes whose backups retention holds, nil-safe.
func (status *ScheduleRetentionStatus) GetPrevious() []RetiredScheduleRules {
	if status == nil {
		return nil
	}
	return status.Previous
}

// RetiredScheduleRules are the entries and the time zone a schedule had before a change.
type RetiredScheduleRules struct {
	// ChangeID is when the change was made, as RFC 3339 in UTC, which identifies it.
	ChangeID string `json:"changeID" yaml:"changeID"`
	// Schedules and TimeZone are the entries and the time zone before the change.
	Schedules []BackupScheduleEntry `json:"schedules,omitempty" yaml:"schedules,omitempty"`
	TimeZone  string                `json:"timeZone,omitempty" yaml:"timeZone,omitempty"`
	// KeepAll holds every backup made before the change, whatever the entries, until Expires: the oldest changes
	// merged into one beyond ScheduleRetentionPreviousMaxItems.
	KeepAll bool `json:"keepAll,omitempty" yaml:"keepAll,omitempty"`
	// Expires, a time as RFC 3339, is when every backup the change holds has expired: a backup made before the change
	// expires at the latest keep periods of the entry that lasts longest after the change; of a merge, the later of
	// those of the changes merged.
	Expires string `json:"expires,omitempty" yaml:"expires,omitempty"`
}

// HeldUntil returns the time every backup a change of the schedule holds expires by, its Expires.
func (rules *RetiredScheduleRules) HeldUntil() time.Time {
	if rules == nil {
		return time.Time{}
	}
	expires, _ := time.Parse(time.RFC3339, rules.Expires)
	return expires
}

// ScheduleEntryStatus is the observed state of the runs of one entry of a schedule.
type ScheduleEntryStatus struct {
	// Period is the period of the entry.
	Period BackupPeriod `json:"period" yaml:"period"`
	// LastScheduleTime is the last time a run of the entry started: a run whose time matches the entry, made
	// for it, or for an entry of a longer period that matches as well.
	LastScheduleTime *meta.Time `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	// LastBackupName is the backup the last successful run of the entry made.
	LastBackupName string `json:"lastBackupName,omitempty" yaml:"lastBackupName,omitempty"`
}

// GetEntry returns the status of the entry of the period, nil when there is none, nil-safe.
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
	// ConfigSecret is the Secret of the clickhouse-backup configuration the runs of the cluster keep their backups by,
	// as of the last run: for a cluster the schedule no longer backs up, it tells the storage its unpruned backups lie
	// in.
	ConfigSecret string `json:"configSecret,omitempty" yaml:"configSecret,omitempty"`
	// LastScheduleTime is the slot of the last run that started on the cluster.
	LastScheduleTime *meta.Time `json:"lastScheduleTime,omitempty" yaml:"lastScheduleTime,omitempty"`
	// LastSuccessfulTime is the last time a scheduled backup of the cluster completed successfully.
	LastSuccessfulTime *meta.Time `json:"lastSuccessfulTime,omitempty" yaml:"lastSuccessfulTime,omitempty"`
	// LastBackupName is the backup the last successful scheduled run of the cluster made.
	LastBackupName string `json:"lastBackupName,omitempty" yaml:"lastBackupName,omitempty"`
	// LastFailureTime is the last time a scheduled backup of the cluster failed.
	LastFailureTime *meta.Time `json:"lastFailureTime,omitempty" yaml:"lastFailureTime,omitempty"`
	// LastFailureMessage tells why the last failed scheduled backup of the cluster failed.
	LastFailureMessage string `json:"lastFailureMessage,omitempty" yaml:"lastFailureMessage,omitempty"`
	// RetentionHosts lists the hosts of the cluster the schedule backs up that a scheduled run has backed
	// up: those that hold its backups, which retention, when on, prunes.
	RetentionHosts []string `json:"retentionHosts,omitempty" yaml:"retentionHosts,omitempty"`
	// UnprunedHosts lists the hosts that left RetentionHosts while retention was on. No run reaches
	// them any more, so retention no longer prunes the backups they made, which stay until
	// deleted by hand. A host that rejoins RetentionHosts is dropped from it.
	UnprunedHosts []string `json:"unprunedHosts,omitempty" yaml:"unprunedHosts,omitempty"`
	// HeldBackups lists the backups the last successful scheduled run of the cluster found held for a change of the
	// schedule, status.retention.previous, which the schedule as it is would delete, each with when it expires: add
	// those to keep longer to spec.retention.keepBackups, with an until, or release the change at once with
	// spec.retention.releaseHeld.
	HeldBackups []HeldBackup `json:"heldBackups,omitempty" yaml:"heldBackups,omitempty"`
}

// HeldBackup is a backup retention holds for a change of a schedule.
type HeldBackup struct {
	// Name is the name of the backup, with a naming template that of the backup of the first shard.
	Name string `json:"name" yaml:"name"`
	// Expires, a time as RFC 3339, is when the hold ends: as the schedule before the change would have deleted the
	// backup, the latest of the changes that hold it. Empty for a backup held until the change is released.
	Expires string `json:"expires,omitempty" yaml:"expires,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ClickHouseBackupScheduleList defines a list of ClickHouseBackupSchedule resources.
type ClickHouseBackupScheduleList struct {
	meta.TypeMeta `json:",inline"  yaml:",inline"`
	meta.ListMeta `json:"metadata" yaml:"metadata"`
	Items         []ClickHouseBackupSchedule `json:"items" yaml:"items"`
}
