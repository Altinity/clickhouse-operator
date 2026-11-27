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
	"cmp"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

func lastDayOfMonth(t time.Time) int {
	return time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

// inGapSlot matches a slot whose time is in a spring-forward gap, which Go moves forward (02:30 to 03:30).
// wall is a wall-clock time in the zone, stored as UTC.
func inGapSlot(period api.BackupPeriod, hour, minute, day int, zone *time.Location, wall time.Time, tolerance int) bool {
	if (zone == nil) || (zone == time.UTC) {
		return false
	}
	switch period {
	case api.BackupPeriodDaily, api.BackupPeriodWeekly, api.BackupPeriodMonthly:
	default:
		return false
	}
	within := time.Minute
	if tolerance > 0 {
		within = time.Duration(tolerance) * time.Second
	}
	for back := 0; back <= 1; back++ {
		configured := time.Date(wall.Year(), wall.Month(), wall.Day()-back, hour, minute, 0, 0, time.UTC)
		switch period {
		case api.BackupPeriodWeekly:
			if int(configured.Weekday()) != day {
				continue
			}
		case api.BackupPeriodMonthly:
			if configured.Day() != min(day, lastDayOfMonth(configured)) {
				continue
			}
		}
		slot := time.Date(configured.Year(), configured.Month(), configured.Day(), hour, minute, 0, 0, zone)
		shown := time.Date(slot.Year(), slot.Month(), slot.Day(), slot.Hour(), slot.Minute(), 0, 0, time.UTC)
		if shown.Equal(configured) {
			continue
		}
		if diff := wall.Sub(shown); (diff >= 0) && (diff < within) {
			return true
		}
	}
	return false
}

// start is when the clock went back, and delta is by how much.
func secondOccurrence(t time.Time) (start time.Time, delta time.Duration, ok bool) {
	start, _ = t.ZoneBounds()
	if start.IsZero() {
		return time.Time{}, 0, false
	}
	_, before := start.Add(-time.Nanosecond).Zone()
	_, after := t.Zone()
	delta = time.Duration(before-after) * time.Second
	return start, delta, (delta > 0) && (t.Sub(start) < delta)
}

// Only the first occurrence of a repeated wall-clock time runs the slot.
func firstOccurrence(t time.Time) time.Time {
	if _, delta, ok := secondOccurrence(t); ok {
		return t.Add(-delta)
	}
	return t
}

// t is in the schedule time zone.
func matches(entry api.BackupScheduleEntry, scheduleTime string, t time.Time) bool {
	hour, minute, err := api.ParseScheduleTime(scheduleTime)
	if err != nil {
		return false
	}
	if entry.Period == api.BackupPeriodMinute {
		return true
	}
	wall := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), 0, time.UTC)
	if inGapSlot(entry.Period, hour, minute, entry.GetDay(), t.Location(), wall, 0) {
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

// t is in the schedule time zone. Only its hour, minute, weekday and day of month count.
func MatchingEntries(entries []api.BackupScheduleEntry, scheduleTime string, t time.Time) []api.BackupScheduleEntry {
	var matched []api.BackupScheduleEntry
	for _, entry := range entries {
		if matches(entry, scheduleTime, t) {
			matched = append(matched, entry)
		}
	}
	return matched
}

// scheduleTime is spec.time. tz nil means UTC. An invalid time gives the zero time.
// On a fall-back day only the first occurrence is a slot, because a second would get the same backup name.
func LatestSlot(entry api.BackupScheduleEntry, scheduleTime string, tz *time.Location, now time.Time) time.Time {
	if tz == nil {
		tz = time.UTC
	}
	hour, minute, err := api.ParseScheduleTime(scheduleTime)
	if err != nil {
		return time.Time{}
	}
	local := now.In(tz)
	switch entry.Period {
	case api.BackupPeriodMinute:
		slot := now.Truncate(time.Minute)
		if start, _, repeated := secondOccurrence(slot.In(tz)); repeated {
			slot = start.Add(-time.Minute)
		}
		return slot
	case api.BackupPeriodHour:
		// Work in absolute time: every instant has a local minute, so DST changes do not matter here.
		back := time.Duration((local.Minute()-minute+60)%60)*time.Minute +
			time.Duration(local.Second())*time.Second + time.Duration(local.Nanosecond())
		return firstOccurrence(local.Add(-back))
	case api.BackupPeriod6Hours:
		for day := 0; day <= 1; day++ {
			for k := 3; k >= 0; k-- {
				slot := firstOccurrence(time.Date(local.Year(), local.Month(), local.Day()-day, hour%6+6*k, minute, 0, 0, tz))
				if !slot.After(now) {
					return slot
				}
			}
		}
	case api.BackupPeriodDaily:
		for day := 0; day <= 2; day++ {
			slot := firstOccurrence(time.Date(local.Year(), local.Month(), local.Day()-day, hour, minute, 0, 0, tz))
			if !slot.After(now) {
				return slot
			}
		}
	case api.BackupPeriodWeekly:
		back := (int(local.Weekday()) - entry.GetDay() + 7) % 7
		for week := 0; week <= 2; week++ {
			slot := firstOccurrence(time.Date(local.Year(), local.Month(), local.Day()-back-7*week, hour, minute, 0, 0, tz))
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

// scheduleTime is spec.time. tz nil means UTC. An invalid time gives the zero time.
func NextSlot(entry api.BackupScheduleEntry, scheduleTime string, tz *time.Location, now time.Time) time.Time {
	if tz == nil {
		tz = time.UTC
	}
	hour, minute, err := api.ParseScheduleTime(scheduleTime)
	if err != nil {
		return time.Time{}
	}
	local := now.In(tz)
	switch entry.Period {
	case api.BackupPeriodMinute:
		slot := now.Truncate(time.Minute).Add(time.Minute)
		if start, delta, repeated := secondOccurrence(slot.In(tz)); repeated {
			// Skip the repeated hour: its minutes already ran in the first occurrence.
			slot = start.Add(delta)
		}
		return slot
	case api.BackupPeriodHour:
		if latest := LatestSlot(entry, scheduleTime, tz, now); !latest.IsZero() {
			slot := latest.Add(time.Hour)
			for {
				if _, _, repeated := secondOccurrence(slot); !repeated {
					break
				}
				slot = slot.Add(time.Hour)
			}
			return slot
		}
	case api.BackupPeriod6Hours:
		for day := 0; day <= 1; day++ {
			for k := 0; k <= 3; k++ {
				slot := firstOccurrence(time.Date(local.Year(), local.Month(), local.Day()+day, hour%6+6*k, minute, 0, 0, tz))
				if slot.After(now) {
					return slot
				}
			}
		}
	case api.BackupPeriodDaily:
		for day := 0; day <= 2; day++ {
			slot := firstOccurrence(time.Date(local.Year(), local.Month(), local.Day()+day, hour, minute, 0, 0, tz))
			if slot.After(now) {
				return slot
			}
		}
	case api.BackupPeriodWeekly:
		ahead := (entry.GetDay() - int(local.Weekday()) + 7) % 7
		for week := 0; week <= 2; week++ {
			slot := firstOccurrence(time.Date(local.Year(), local.Month(), local.Day()+ahead+7*week, hour, minute, 0, 0, tz))
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

// A day past the month end becomes the last day.
func monthlySlot(year int, month time.Month, day, hour, minute int, tz *time.Location) time.Time {
	first := time.Date(year, month, 1, 0, 0, 0, 0, tz)
	last := time.Date(first.Year(), first.Month()+1, 0, 0, 0, 0, 0, tz).Day()
	if day > last {
		day = last
	}
	return firstOccurrence(time.Date(first.Year(), first.Month(), day, hour, minute, 0, 0, tz))
}

// Zero time means none.
func NextScheduleTime(entries []api.BackupScheduleEntry, scheduleTime string, tz *time.Location, now time.Time) time.Time {
	var next time.Time
	for _, entry := range entries {
		if slot := NextSlot(entry, scheduleTime, tz, now); !slot.IsZero() && (next.IsZero() || slot.Before(next)) {
			next = slot
		}
	}
	return next
}

// A change of this string is a schedule change.
func retentionRules(entries []api.BackupScheduleEntry, scheduleTime, timeZone string) string {
	var formatted []string
	for _, entry := range entries {
		formatted = append(formatted, fmt.Sprintf("%s %d %d", entry.Period, entry.GetDay(), entry.Keep))
	}
	slices.Sort(formatted)
	return strings.Join(formatted, ";") + "|" + cmp.Or(scheduleTime, "00:00") + "|" + timeZone
}

// With onScheduleChange Hold, a change saves the old entries, so retention keeps their backups.
// A schedule seen for the first time holds nothing.
func TrackScheduleRetention(chbs *api.ClickHouseBackupSchedule, now time.Time) string {
	status := chbs.Status.Retention
	if status == nil {
		status = &api.ScheduleRetentionStatus{}
		chbs.Status.Retention = status
	}
	held := ""
	spec := retentionRules(chbs.Spec.Schedules, chbs.Spec.Time, chbs.Spec.TimeZone)
	switch {
	case len(status.AppliedSchedules) == 0:
	case retentionRules(status.AppliedSchedules, status.AppliedTime, status.AppliedTimeZone) == spec:
	case chbs.Spec.Retention.GetOnScheduleChange() == api.RetentionHold:
		// A changeID is a time, always later than earlier changeIDs, so releasing an earlier change never releases it.
		changed := now.UTC().Truncate(time.Second)
		if latest, err := time.Parse(time.RFC3339, status.ChangeID); (err == nil) && !changed.After(latest) {
			changed = latest.Add(time.Second)
		}
		held = changed.Format(time.RFC3339)
		change := api.RetiredScheduleRules{ChangeID: held, Schedules: status.AppliedSchedules, Time: status.AppliedTime,
			TimeZone: status.AppliedTimeZone}
		change.Expires = formatHeldUntil(entriesExpiry(changed, change.Schedules))
		status.Previous = append(status.Previous, change)
	}
	status.AppliedSchedules = slices.Clone(chbs.Spec.DeepCopy().Schedules)
	status.AppliedTime = chbs.Spec.Time
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

func entriesExpiry(changed time.Time, entries []api.BackupScheduleEntry) time.Time {
	latest := changed
	for _, entry := range entries {
		if until := entry.Period.Span(changed, int(entry.Keep)); until.After(latest) {
			latest = until
		}
	}
	return latest
}

func formatHeldUntil(until time.Time) string {
	if until.IsZero() {
		return ""
	}
	return until.UTC().Format(time.RFC3339)
}

var pinTagRegexp = regexp.MustCompile(`[0-9]{8}T?[0-9]{6}`)

// Such a pin ends with the hold. If the change is dropped, the pin would never end, so the change must stay.
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
