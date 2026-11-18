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
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// The entries of a schedule run at the slots of the finest entry. A run matches the time it was due at, in the
// schedule's time zone, against the entries, as MatchingEntries does: a minute entry matches every minute; an hour entry matches its minute; a 6hours entry its hour modulo 6 and its minute; a daily entry its hour and
// minute; a weekly entry those and its weekday, 0 being Sunday; a monthly entry those and its day of the month, a day
// past the end of a shorter month meaning its last day. An entry that sets no time takes the time the others share,
// see api.ResolveScheduleTimes. Validation makes every entry that sets a time set the same, so that every slot of a
// coarser entry is a slot of the finest one.

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
// next run of the schedule, for status.nextScheduleTime.
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
// runs apply, and, when a change of them is held, spec.retention.onScheduleChange Hold, the entries and the time
// zone before the change, whose backups retention keeps until spec.retention.releaseHeld releases it. It drops the
// changes releaseHeld releases, and merges the oldest two changes beyond ScheduleRetentionPreviousMaxItems into one
// that holds every backup made before the newer of them. It returns the changeID of a change it has just held, "" for
// none: a schedule seen for the first time holds nothing.
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
		change := api.RetiredScheduleRules{ChangeID: held, Schedules: status.AppliedSchedules, TimeZone: status.AppliedTimeZone}
		change.Expires = formatHeldUntil(entriesExpiry(changed, change.Schedules))
		status.Previous = append(status.Previous, change)
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
		merged := api.RetiredScheduleRules{ChangeID: status.Previous[1].ChangeID, TimeZone: status.Previous[1].TimeZone, KeepAll: true,
			Expires: formatHeldUntil(later(status.Previous[0].HeldUntil(), status.Previous[1].HeldUntil()))}
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

// entriesExpiry returns when every backup made before a change at changed that the entries before it kept has
// expired: the latest keep periods of the entries after the change.
func entriesExpiry(changed time.Time, entries []api.BackupScheduleEntry) time.Time {
	latest := changed
	for _, entry := range entries {
		if until := entry.Period.Span(changed, int(entry.Keep)); until.After(latest) {
			latest = until
		}
	}
	return latest
}

// formatHeldUntil formats the end of a hold as RFC 3339, "" for never.
func formatHeldUntil(until time.Time) string {
	if until.IsZero() {
		return ""
	}
	return until.UTC().Format(time.RFC3339)
}

// pinTagRegexp finds the time a backup name ends with.
var pinTagRegexp = regexp.MustCompile(`[0-9]{8}T?[0-9]{6}`)

// pinnedBefore reports whether spec.retention.keepBackups may name, by a pin that sets no until, a scheduled backup
// made before the change: such a pin lasts as long as the hold of the change, and would last for good once the change
// is dropped.
func pinnedBefore(pins []api.BackupRetentionPin, change *api.RetiredScheduleRules) bool {
	changed, err := time.Parse(time.RFC3339, change.ChangeID)
	if err != nil {
		return false
	}
	tag := SlotTag(changed, change.TimeZone)
	for i := range pins {
		if pins[i].Until != "" {
			continue
		}
		found := pinTagRegexp.FindAllString(pins[i].Name, -1)
		if len(found) == 0 {
			continue
		}
		if last := found[len(found)-1]; !strings.Contains(last, "T") && (last < tag) {
			return true
		}
	}
	return false
}

// ExpireScheduleRetention drops from the status of a schedule the changes every backup of which has expired, as their
// expires tells, and the backups status.clusters[].heldBackups tells held until a time past. A change stays while a pin
// of spec.retention.keepBackups that sets no until may name a backup it holds, whose pin ends with the hold. It returns
// the changeIDs of the changes it dropped.
func ExpireScheduleRetention(chbs *api.ClickHouseBackupSchedule, now time.Time) []string {
	status := chbs.Status.Retention
	var expired []string
	if status != nil {
		pins := chbs.Spec.Retention.GetKeepBackups()
		status.Previous = slices.DeleteFunc(status.Previous, func(change api.RetiredScheduleRules) bool {
			until := change.HeldUntil()
			if until.IsZero() || now.Before(until) || pinnedBefore(pins, &change) {
				return false
			}
			expired = append(expired, change.ChangeID)
			return true
		})
		status.ChangeID = ""
		if len(status.Previous) > 0 {
			status.ChangeID = status.Previous[len(status.Previous)-1].ChangeID
		} else {
			status.Previous = nil
		}
	}
	for i := range chbs.Status.Clusters {
		cluster := &chbs.Status.Clusters[i]
		cluster.HeldBackups = slices.DeleteFunc(cluster.HeldBackups, func(held api.HeldBackup) bool {
			expires, err := time.Parse(time.RFC3339, held.Expires)
			return (err == nil) && !now.Before(expires)
		})
		if len(cluster.HeldBackups) == 0 {
			cluster.HeldBackups = nil
		}
	}
	return expired
}
