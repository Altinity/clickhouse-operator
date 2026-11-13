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

package chbackup

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// The entries of a schedule run on one CronJob, on the cron schedule of the finest entry. A run matches the time it
// was due at, in the schedule's time zone, against the entries, the orchestrator script exactly as MatchingEntries
// does: a minute entry matches every minute; an hour entry matches its minute; a 6hours entry its hour modulo 6 and its minute; a daily entry its hour and
// minute; a weekly entry those and its weekday, 0 being Sunday; a monthly entry those and its day of the month, a day
// past the end of a shorter month meaning its last day. An entry that sets no time takes the time the others share,
// see api.ResolveScheduleTimes. Validation makes every entry that sets a time set the same, so that every slot of a
// coarser entry is a slot of the finest one.

const (
	// envSchedules carries the entries of a schedule to the Job of a scheduled run, as EntriesEnv formats them.
	envSchedules = "SCHEDULES"
	// envScheduleTimezone is the IANA time zone of the schedule, empty for UTC: a scheduled run matches the time it
	// was due against the entries, and names its backup after it, in this zone.
	envScheduleTimezone = "SCHEDULE_TIMEZONE"
)

// finest returns the entry of the shortest period, nil when there is none.
func finest(entries []api.BackupScheduleEntry) *api.BackupScheduleEntry {
	var shortest *api.BackupScheduleEntry
	for i := range entries {
		if (shortest == nil) || (entries[i].Period.Weight() < shortest.Period.Weight()) {
			shortest = &entries[i]
		}
	}
	return shortest
}

// FinestCron returns the cron expression of the finest of the entries, the schedule the CronJob runs on: every slot
// of a coarser entry is one of its. A monthly entry on a day past the 28th fires on the 28th to the 31st, and the run
// matches whether it is the day the entry means in the month. "" when there is no entry, or it does not validate.
func FinestCron(entries []api.BackupScheduleEntry) string {
	entries = api.ResolveScheduleTimes(entries)
	entry := finest(entries)
	if entry == nil {
		return ""
	}
	hour, minute, err := entry.ParseTime()
	if err != nil {
		return ""
	}
	switch entry.Period {
	case api.BackupPeriodMinute:
		return "* * * * *"
	case api.BackupPeriodHour:
		return fmt.Sprintf("%d * * * *", minute)
	case api.BackupPeriod6Hours:
		h := hour % 6
		return fmt.Sprintf("%d %d,%d,%d,%d * * *", minute, h, h+6, h+12, h+18)
	case api.BackupPeriodDaily:
		return fmt.Sprintf("%d %d * * *", minute, hour)
	case api.BackupPeriodWeekly:
		return fmt.Sprintf("%d %d * * %d", minute, hour, entry.GetDay())
	case api.BackupPeriodMonthly:
		if day := entry.GetDay(); day <= 28 {
			return fmt.Sprintf("%d %d %d * *", minute, hour, day)
		}
		return fmt.Sprintf("%d %d 28-31 * *", minute, hour)
	}
	return ""
}

// lastDayOfMonth returns the number of days of the month t falls in.
func lastDayOfMonth(t time.Time) int {
	return time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// matches reports whether the entry, its time resolved, is due at the minute of t, a wall-clock time in the
// schedule's time zone.
func matches(entry api.BackupScheduleEntry, t time.Time) bool {
	hour, minute, err := entry.ParseTime()
	if err != nil {
		return false
	}
	if entry.Period == api.BackupPeriodMinute {
		return true
	}
	if t.Minute() != minute {
		return false
	}
	switch entry.Period {
	case api.BackupPeriodHour:
		return true
	case api.BackupPeriod6Hours:
		return t.Hour()%6 == hour%6
	case api.BackupPeriodDaily:
		return t.Hour() == hour
	case api.BackupPeriodWeekly:
		return (t.Hour() == hour) && (int(t.Weekday()) == entry.GetDay())
	case api.BackupPeriodMonthly:
		return (t.Hour() == hour) && (t.Day() == min(entry.GetDay(), lastDayOfMonth(t)))
	}
	return false
}

// MatchingEntries returns the entries a run due at t matches, t being the wall-clock time in the schedule's time
// zone (t.In(location)): only its hour, minute, weekday and day of the month count. An entry that sets no time takes
// the shared one. They come in the order of the spec.
func MatchingEntries(entries []api.BackupScheduleEntry, t time.Time) []api.BackupScheduleEntry {
	var matched []api.BackupScheduleEntry
	for _, entry := range api.ResolveScheduleTimes(entries) {
		if matches(entry, t) {
			matched = append(matched, entry)
		}
	}
	return matched
}

// Winner returns the entry of the largest weight among the matched ones, the one a run is made for: the others count
// it as theirs. The first of equal weights, which validation, one entry per period, rules out. nil when none matched.
func Winner(matched []api.BackupScheduleEntry) *api.BackupScheduleEntry {
	var winner *api.BackupScheduleEntry
	for i := range matched {
		if (winner == nil) || (matched[i].Period.Weight() > winner.Period.Weight()) {
			winner = &matched[i]
		}
	}
	return winner
}

// EntriesEnv formats the entries for the orchestrator script: entries joined by ";", each
// "period=<p> time=HH:MM [day=N] keep=N [incremental=true]", the time resolved as MatchingEntries resolves it; a
// minute entry, which matches every minute, carries no time.
func EntriesEnv(entries []api.BackupScheduleEntry) string {
	var formatted []string
	for _, entry := range api.ResolveScheduleTimes(entries) {
		fields := []string{"period=" + string(entry.Period)}
		if (entry.Time != "") && (entry.Period != api.BackupPeriodMinute) {
			fields = append(fields, "time="+entry.Time)
		}
		if entry.Day != nil {
			fields = append(fields, fmt.Sprintf("day=%d", *entry.Day))
		}
		fields = append(fields, fmt.Sprintf("keep=%d", entry.Keep))
		if entry.IsIncremental() {
			fields = append(fields, "incremental=true")
		}
		formatted = append(formatted, strings.Join(fields, " "))
	}
	return strings.Join(formatted, ";")
}

// The slots of a schedule entry are wall-clock times in the schedule's time zone: hour slots are every hour at the
// minute of the entry's time, 6hours slots at the hours h, h+6, h+12, h+18 (h being the hour of the time modulo 6),
// daily slots at the time every day, weekly slots on the weekday of the entry, monthly slots on its day of the month,
// clamped to the last day of a shorter month. A wall-clock time a DST change skips is moved forward by the length of
// the gap, as time.Date normalizes it; one a DST change repeats is taken once, at the first of its instants.

// LatestSlot returns the latest slot of the entry at or before now, in the time zone tz, nil meaning UTC. The entry
// is taken as given: resolve its time first, see api.ResolveScheduleTimes. An entry that does not validate yields the
// zero time.
func LatestSlot(entry api.BackupScheduleEntry, tz *time.Location, now time.Time) time.Time {
	if tz == nil {
		tz = time.UTC
	}
	hour, minute, err := entry.ParseTime()
	if err != nil {
		return time.Time{}
	}
	local := now.In(tz)
	switch entry.Period {
	case api.BackupPeriodMinute:
		return now.Truncate(time.Minute)
	case api.BackupPeriodHour:
		// Every instant has a local minute, so the slot is found in absolute time, which a DST change never upsets
		back := time.Duration((local.Minute()-minute+60)%60)*time.Minute +
			time.Duration(local.Second())*time.Second + time.Duration(local.Nanosecond())
		return local.Add(-back)
	case api.BackupPeriod6Hours:
		for day := 0; day <= 1; day++ {
			for k := 3; k >= 0; k-- {
				slot := time.Date(local.Year(), local.Month(), local.Day()-day, hour%6+6*k, minute, 0, 0, tz)
				if !slot.After(now) {
					return slot
				}
			}
		}
	case api.BackupPeriodDaily:
		for day := 0; day <= 2; day++ {
			slot := time.Date(local.Year(), local.Month(), local.Day()-day, hour, minute, 0, 0, tz)
			if !slot.After(now) {
				return slot
			}
		}
	case api.BackupPeriodWeekly:
		back := (int(local.Weekday()) - entry.GetDay() + 7) % 7
		for week := 0; week <= 2; week++ {
			slot := time.Date(local.Year(), local.Month(), local.Day()-back-7*week, hour, minute, 0, 0, tz)
			if !slot.After(now) {
				return slot
			}
		}
	case api.BackupPeriodMonthly:
		for month := 0; month <= 2; month++ {
			slot := monthlySlot(local.Year(), local.Month()-time.Month(month), entry.GetDay(), hour, minute, tz)
			if !slot.After(now) {
				return slot
			}
		}
	}
	return time.Time{}
}

// NextSlot returns the earliest slot of the entry after now, in the time zone tz, nil meaning UTC. An entry that does
// not validate yields the zero time.
func NextSlot(entry api.BackupScheduleEntry, tz *time.Location, now time.Time) time.Time {
	if tz == nil {
		tz = time.UTC
	}
	hour, minute, err := entry.ParseTime()
	if err != nil {
		return time.Time{}
	}
	local := now.In(tz)
	switch entry.Period {
	case api.BackupPeriodMinute:
		return now.Truncate(time.Minute).Add(time.Minute)
	case api.BackupPeriodHour:
		if latest := LatestSlot(entry, tz, now); !latest.IsZero() {
			return latest.Add(time.Hour)
		}
	case api.BackupPeriod6Hours:
		for day := 0; day <= 1; day++ {
			for k := 0; k <= 3; k++ {
				slot := time.Date(local.Year(), local.Month(), local.Day()+day, hour%6+6*k, minute, 0, 0, tz)
				if slot.After(now) {
					return slot
				}
			}
		}
	case api.BackupPeriodDaily:
		for day := 0; day <= 2; day++ {
			slot := time.Date(local.Year(), local.Month(), local.Day()+day, hour, minute, 0, 0, tz)
			if slot.After(now) {
				return slot
			}
		}
	case api.BackupPeriodWeekly:
		ahead := (entry.GetDay() - int(local.Weekday()) + 7) % 7
		for week := 0; week <= 2; week++ {
			slot := time.Date(local.Year(), local.Month(), local.Day()+ahead+7*week, hour, minute, 0, 0, tz)
			if slot.After(now) {
				return slot
			}
		}
	case api.BackupPeriodMonthly:
		for month := 0; month <= 2; month++ {
			slot := monthlySlot(local.Year(), local.Month()+time.Month(month), entry.GetDay(), hour, minute, tz)
			if slot.After(now) {
				return slot
			}
		}
	}
	return time.Time{}
}

// monthlySlot returns the slot of a monthly entry in the month, its day clamped to the last day of the month.
func monthlySlot(year int, month time.Month, day, hour, minute int, tz *time.Location) time.Time {
	first := time.Date(year, month, 1, 0, 0, 0, 0, tz)
	last := time.Date(first.Year(), first.Month()+1, 0, 0, 0, 0, 0, tz).Day()
	if day > last {
		day = last
	}
	return time.Date(first.Year(), first.Month(), day, hour, minute, 0, 0, tz)
}

// DueEntry is an entry of a schedule that is due, with the slot it is due at.
type DueEntry struct {
	Entry api.BackupScheduleEntry
	Slot  time.Time
}

// DueEntries returns the entries due at now: those whose latest slot is later than the slot lastFired records for
// their period and, when deadline is positive, at most deadline before now. They come sorted by the weight of their
// period, the largest first, which is the entry the run is made for: the others count it as theirs.
func DueEntries(entries []api.BackupScheduleEntry, tz *time.Location, now time.Time,
	lastFired map[api.BackupPeriod]time.Time, deadline time.Duration) []DueEntry {
	var due []DueEntry
	for _, entry := range api.ResolveScheduleTimes(entries) {
		slot := LatestSlot(entry, tz, now)
		if slot.IsZero() {
			continue
		}
		if fired, ok := lastFired[entry.Period]; ok && !slot.After(fired) {
			continue
		}
		if (deadline > 0) && (now.Sub(slot) > deadline) {
			continue
		}
		due = append(due, DueEntry{Entry: entry, Slot: slot})
	}
	sort.SliceStable(due, func(i, j int) bool {
		return due[i].Entry.Period.Weight() > due[j].Entry.Period.Weight()
	})
	return due
}

// NextScheduleTime returns the earliest slot of any of the entries after now, the zero time when none has one: the
// next run of the CronJob that matches an entry, for status.nextScheduleTime.
func NextScheduleTime(entries []api.BackupScheduleEntry, tz *time.Location, now time.Time) time.Time {
	var next time.Time
	for _, entry := range api.ResolveScheduleTimes(entries) {
		if slot := NextSlot(entry, tz, now); !slot.IsZero() && (next.IsZero() || slot.Before(next)) {
			next = slot
		}
	}
	return next
}

// retentionRules formats what of the entries and the time zone of a schedule decides which backups retention keeps:
// the period, the time, the day and the keep of every entry, and the zone the slots are read in.
func retentionRules(entries []api.BackupScheduleEntry, timeZone string) string {
	var formatted []string
	for _, entry := range api.ResolveScheduleTimes(entries) {
		formatted = append(formatted, fmt.Sprintf("%s %s %d %d", entry.Period, entry.Time, entry.GetDay(), entry.Keep))
	}
	slices.Sort(formatted)
	return strings.Join(formatted, ";") + "|" + timeZone
}

// TrackScheduleRetention records in the status of a schedule whose spec is valid the entries and the time zone its
// CronJobs apply, and, when a change of them is held, spec.retention.onScheduleChange Hold, the entries and the time
// zone before the change, whose backups retention keeps until spec.retention.releaseHeld releases it. It drops the
// changes releaseHeld releases, and merges the oldest two changes beyond ScheduleRetentionPreviousMaxItems into one
// that holds every backup made before the newer of them. It returns the changeID of a change it has just held, "" for
// none: a schedule seen for the first time, as after an upgrade, holds nothing.
func TrackScheduleRetention(chbs *api.ClickHouseBackupSchedule, now time.Time) string {
	status := chbs.Status.Retention
	if status == nil {
		status = &api.ScheduleRetentionStatus{}
		chbs.Status.Retention = status
	}
	held := ""
	spec := retentionRules(chbs.Spec.Schedules, chbs.Spec.TimeZone)
	switch {
	case len(status.AppliedSchedules) == 0:
	case retentionRules(status.AppliedSchedules, status.AppliedTimeZone) == spec:
	case chbs.Spec.Retention.GetOnScheduleChange() == api.RetentionHold:
		// A changeID is a time, later than that of any change before, which a release of an earlier one never releases
		changed := now.UTC().Truncate(time.Second)
		if latest, err := time.Parse(time.RFC3339, status.ChangeID); (err == nil) && !changed.After(latest) {
			changed = latest.Add(time.Second)
		}
		held = changed.Format(time.RFC3339)
		status.Previous = append(status.Previous, api.RetiredScheduleRules{
			ChangeID: held, Schedules: status.AppliedSchedules, TimeZone: status.AppliedTimeZone,
		})
	}
	status.AppliedSchedules = slices.Clone(chbs.Spec.DeepCopy().Schedules)
	status.AppliedTimeZone = chbs.Spec.TimeZone
	if release, err := time.Parse(time.RFC3339, chbs.Spec.Retention.GetReleaseHeld()); err == nil {
		status.Previous = slices.DeleteFunc(status.Previous, func(previous api.RetiredScheduleRules) bool {
			changed, err := time.Parse(time.RFC3339, previous.ChangeID)
			return (err == nil) && !changed.After(release)
		})
	}
	for len(status.Previous) > api.ScheduleRetentionPreviousMaxItems {
		merged := api.RetiredScheduleRules{ChangeID: status.Previous[1].ChangeID, TimeZone: status.Previous[1].TimeZone, KeepAll: true}
		status.Previous = append([]api.RetiredScheduleRules{merged}, status.Previous[2:]...)
	}
	status.ChangeID = ""
	if len(status.Previous) > 0 {
		status.ChangeID = status.Previous[len(status.Previous)-1].ChangeID
	}
	if len(status.Previous) == 0 {
		status.Previous = nil
	}
	return held
}

// PreviousSchedulesEnv formats the changes of a schedule whose backups retention holds for the orchestrator script:
// changes joined by "#", each "<unix seconds of the change>|<time zone>|<entries as EntriesEnv formats them>", the
// entries "*" for a change that holds every backup made before it. A change whose changeID is no time is left out.
func PreviousSchedulesEnv(previous []api.RetiredScheduleRules) string {
	var formatted []string
	for _, rules := range previous {
		changed, err := time.Parse(time.RFC3339, rules.ChangeID)
		if err != nil {
			continue
		}
		entries := EntriesEnv(rules.Schedules)
		if rules.KeepAll || (entries == "") {
			entries = "*"
		}
		formatted = append(formatted, fmt.Sprintf("%d|%s|%s", changed.Unix(), rules.TimeZone, entries))
	}
	return strings.Join(formatted, "#")
}
