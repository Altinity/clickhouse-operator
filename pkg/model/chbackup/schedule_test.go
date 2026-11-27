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
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func entry(period api.BackupPeriod, day ...int32) api.BackupScheduleEntry {
	e := api.BackupScheduleEntry{Period: period, Keep: 1}
	if len(day) > 0 {
		e.Day = ptr.To(day[0])
	}
	return e
}

func at(year int, month time.Month, day, hour, minute int) time.Time {
	return time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
}

func periodsOf(entries []api.BackupScheduleEntry) []api.BackupPeriod {
	var periods []api.BackupPeriod
	for _, e := range entries {
		periods = append(periods, e.Period)
	}
	return periods
}

func TestMatchingEntries(t *testing.T) {
	all := []api.BackupScheduleEntry{
		entry(api.BackupPeriodHour),
		entry(api.BackupPeriod6Hours),
		entry(api.BackupPeriodDaily),
		entry(api.BackupPeriodWeekly, 0),
		entry(api.BackupPeriodMonthly, 31),
	}
	// 2026-11-01 is a Sunday, 2026-11-12 a Thursday
	for name, tc := range map[string]struct {
		entries []api.BackupScheduleEntry
		at      string
		t       time.Time
		want    []api.BackupPeriod
	}{
		"not the minute":                   {all, "02:30", at(2026, 11, 12, 2, 0), nil},
		"minute at any minute":             {[]api.BackupScheduleEntry{entry(api.BackupPeriodMinute)}, "", at(2026, 11, 12, 13, 47), []api.BackupPeriod{api.BackupPeriodMinute}},
		"minute ignores its time":          {[]api.BackupScheduleEntry{entry(api.BackupPeriodMinute)}, "02:30", at(2026, 11, 12, 13, 47), []api.BackupPeriod{api.BackupPeriodMinute}},
		"minute among all":                 {append([]api.BackupScheduleEntry{entry(api.BackupPeriodMinute)}, all...), "02:30", at(2026, 11, 12, 3, 30), []api.BackupPeriod{api.BackupPeriodMinute, api.BackupPeriodHour}},
		"hour only":                        {all, "02:30", at(2026, 11, 12, 3, 30), []api.BackupPeriod{api.BackupPeriodHour}},
		"hour and 6hours":                  {all, "02:30", at(2026, 11, 12, 8, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours}},
		"daily on a Thursday":              {all, "02:30", at(2026, 11, 12, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily}},
		"weekly on Sunday":                 {all, "02:30", at(2026, 11, 1, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodWeekly}},
		"monthly 31 on the 31st":           {all, "02:30", at(2026, 10, 31, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodMonthly}},
		"monthly 31 not on the 30th of 31": {all, "02:30", at(2026, 10, 30, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily}},
		"monthly 31 on the 30th of April":  {all, "02:30", at(2026, 4, 30, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodMonthly}},
		"monthly 31 on February 28":        {all, "02:30", at(2026, 2, 28, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodMonthly}},
		"monthly 31 not on Feb 28, leap":   {all, "02:30", at(2028, 2, 28, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily}},
		"monthly 31 on February 29, leap":  {all, "02:30", at(2028, 2, 29, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodMonthly}},
		"monthly 30 on the 30th of March":  {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, 30)}, "02:30", at(2026, 3, 30, 2, 30), []api.BackupPeriod{api.BackupPeriodMonthly}},
		"monthly 30 not on the 31st":       {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, 30)}, "02:30", at(2026, 3, 31, 2, 30), nil},
		"monthly 15 not on the 16th":       {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, 15)}, "02:30", at(2026, 3, 16, 2, 30), nil},
		"6hours, hour modulo 6":            {[]api.BackupScheduleEntry{entry(api.BackupPeriod6Hours)}, "13:45", at(2026, 11, 12, 19, 45), []api.BackupPeriod{api.BackupPeriod6Hours}},
		"6hours, another hour":             {[]api.BackupScheduleEntry{entry(api.BackupPeriod6Hours)}, "13:45", at(2026, 11, 12, 18, 45), nil},
		"hour, its own minute":             {[]api.BackupScheduleEntry{entry(api.BackupPeriodHour)}, "17:05", at(2026, 11, 12, 0, 5), []api.BackupPeriod{api.BackupPeriodHour}},
		"hour without time, at :00":        {[]api.BackupScheduleEntry{entry(api.BackupPeriodHour)}, "", at(2026, 11, 12, 7, 0), []api.BackupPeriod{api.BackupPeriodHour}},
		"weekly on Saturday":               {[]api.BackupScheduleEntry{entry(api.BackupPeriodWeekly, 6)}, "23:00", at(2026, 10, 31, 23, 0), []api.BackupPeriod{api.BackupPeriodWeekly}},
		"weekly, another weekday":          {[]api.BackupScheduleEntry{entry(api.BackupPeriodWeekly, 6)}, "23:00", at(2026, 11, 1, 23, 0), nil},
		"seconds do not count":             {[]api.BackupScheduleEntry{entry(api.BackupPeriodDaily)}, "02:30", at(2026, 11, 12, 2, 30).Add(59 * time.Second), []api.BackupPeriod{api.BackupPeriodDaily}},
		"a time that does not validate":    {[]api.BackupScheduleEntry{entry(api.BackupPeriodDaily)}, "2:30", at(2026, 11, 12, 2, 30), nil},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, periodsOf(MatchingEntries(tc.entries, tc.at, tc.t)))
		})
	}
}

// The time matched is the wall-clock time in the schedule's time zone, the time the CronJob, given the same zone,
// started the run at: across a DST change the entries still match by the wall clock.
func TestMatchingEntriesInATimeZone(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	entries := []api.BackupScheduleEntry{entry(api.BackupPeriodHour), entry(api.BackupPeriodDaily)}
	for name, tc := range map[string]struct {
		utc  time.Time
		want []api.BackupPeriod
	}{
		"winter time":               {at(2026, 1, 15, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriodDaily}},
		"summer time":               {at(2026, 7, 15, 1, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriodDaily}},
		"the day of spring-forward": {at(2026, 3, 29, 1, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriodDaily}},
		"UTC 03:30 in summer":       {at(2026, 7, 15, 3, 30), []api.BackupPeriod{api.BackupPeriodHour}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, periodsOf(MatchingEntries(entries, "03:30", tc.utc.In(berlin))))
		})
	}
	// A zone half an hour off UTC: the minute is the wall clock's
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)
	require.Equal(t, []api.BackupPeriod{api.BackupPeriodHour},
		periodsOf(MatchingEntries([]api.BackupScheduleEntry{entry(api.BackupPeriodHour)}, "", at(2026, 11, 12, 10, 30).In(kolkata))))
}

func TestBackupPeriodWeight(t *testing.T) {
	require.InDelta(t, 1.0/24, api.BackupPeriodHour.Weight(), 1e-9)
	require.InDelta(t, 1.0/1440, api.BackupPeriodMinute.Weight(), 1e-12)
	require.InDelta(t, 0.25, api.BackupPeriod6Hours.Weight(), 1e-9)
	require.InDelta(t, 1, api.BackupPeriodDaily.Weight(), 1e-9)
	require.InDelta(t, 7, api.BackupPeriodWeekly.Weight(), 1e-9)
	require.InDelta(t, 30, api.BackupPeriodMonthly.Weight(), 1e-9)
	require.Zero(t, api.BackupPeriod("yearly").Weight())
	for i := 1; i < len(api.BackupPeriods); i++ {
		require.Greater(t, api.BackupPeriods[i].Weight(), api.BackupPeriods[i-1].Weight())
	}
}

func mustLocation(t *testing.T, name string) *time.Location {
	t.Helper()
	location, err := time.LoadLocation(name)
	require.NoError(t, err)
	return location
}

func utc(year int, month time.Month, day, hour, minute, second int) time.Time {
	return time.Date(year, month, day, hour, minute, second, 0, time.UTC)
}

func TestLatestSlot(t *testing.T) {
	berlin := mustLocation(t, "Europe/Berlin")
	kolkata := mustLocation(t, "Asia/Kolkata")
	newYork := mustLocation(t, "America/New_York")
	for name, tc := range map[string]struct {
		entry api.BackupScheduleEntry
		at    string
		tz    *time.Location
		now   time.Time
		want  time.Time
	}{
		// hour: only the minute of the time counts
		"minute, on the minute":             {entry(api.BackupPeriodMinute), "", nil, utc(2026, 11, 12, 10, 7, 0), utc(2026, 11, 12, 10, 7, 0)},
		"minute, within the minute":         {entry(api.BackupPeriodMinute), "", kolkata, utc(2026, 11, 12, 10, 7, 59), utc(2026, 11, 12, 10, 7, 0)},
		"hour, no time, on the hour":        {entry(api.BackupPeriodHour), "", nil, utc(2026, 11, 12, 10, 0, 0), utc(2026, 11, 12, 10, 0, 0)},
		"hour, no time, past the hour":      {entry(api.BackupPeriodHour), "", nil, utc(2026, 11, 12, 10, 59, 59), utc(2026, 11, 12, 10, 0, 0)},
		"hour at :30, just before":          {entry(api.BackupPeriodHour), "02:30", nil, utc(2026, 11, 12, 10, 29, 59), utc(2026, 11, 12, 9, 30, 0)},
		"hour at :30, at it":                {entry(api.BackupPeriodHour), "02:30", nil, utc(2026, 11, 12, 10, 30, 0), utc(2026, 11, 12, 10, 30, 0)},
		"hour at :30 across midnight":       {entry(api.BackupPeriodHour), "17:30", nil, utc(2026, 11, 12, 0, 10, 0), utc(2026, 11, 11, 23, 30, 0)},
		"hour at :00 in a half-hour zone":   {entry(api.BackupPeriodHour), "00:00", kolkata, utc(2026, 11, 12, 10, 45, 0), utc(2026, 11, 12, 10, 30, 0)},
		"hour across a spring-forward gap":  {entry(api.BackupPeriodHour), "00:15", berlin, utc(2026, 3, 29, 1, 20, 0), utc(2026, 3, 29, 1, 15, 0)},
		"hour across a fall-back overlap":   {entry(api.BackupPeriodHour), "00:15", berlin, utc(2026, 10, 25, 1, 10, 0), utc(2026, 10, 25, 0, 15, 0)},
		"hour, sub-second now is truncated": {entry(api.BackupPeriodHour), "00:05", nil, utc(2026, 11, 12, 10, 5, 0).Add(999 * time.Millisecond), utc(2026, 11, 12, 10, 5, 0)},

		// 6hours: the hour modulo 6 and the minute
		"6hours at 02:15, at 08:15":           {entry(api.BackupPeriod6Hours), "02:15", nil, utc(2026, 11, 12, 8, 15, 0), utc(2026, 11, 12, 8, 15, 0)},
		"6hours at 02:15, at 08:14":           {entry(api.BackupPeriod6Hours), "02:15", nil, utc(2026, 11, 12, 8, 14, 0), utc(2026, 11, 12, 2, 15, 0)},
		"6hours at 02:15, before the first":   {entry(api.BackupPeriod6Hours), "02:15", nil, utc(2026, 11, 12, 1, 0, 0), utc(2026, 11, 11, 20, 15, 0)},
		"6hours at 13:00 is 01, 07, 13, 19":   {entry(api.BackupPeriod6Hours), "13:00", nil, utc(2026, 11, 12, 6, 59, 0), utc(2026, 11, 12, 1, 0, 0)},
		"6hours at 13:00, late in the day":    {entry(api.BackupPeriod6Hours), "13:00", nil, utc(2026, 11, 12, 23, 59, 0), utc(2026, 11, 12, 19, 0, 0)},
		"6hours, no time":                     {entry(api.BackupPeriod6Hours), "", nil, utc(2026, 11, 12, 17, 0, 0), utc(2026, 11, 12, 12, 0, 0)},
		"6hours in a zone, wall clock":        {entry(api.BackupPeriod6Hours), "00:00", berlin, utc(2026, 11, 12, 5, 30, 0), utc(2026, 11, 12, 5, 0, 0)},
		"6hours in summer time, wall clock":   {entry(api.BackupPeriod6Hours), "00:00", berlin, utc(2026, 7, 12, 4, 30, 0), utc(2026, 7, 12, 4, 0, 0)},
		"6hours on the day of spring-forward": {entry(api.BackupPeriod6Hours), "00:00", berlin, utc(2026, 3, 29, 4, 30, 0), utc(2026, 3, 29, 4, 0, 0)},

		// daily
		"daily, before the time":        {entry(api.BackupPeriodDaily), "02:00", nil, utc(2026, 11, 12, 1, 59, 0), utc(2026, 11, 11, 2, 0, 0)},
		"daily, at the time":            {entry(api.BackupPeriodDaily), "02:00", nil, utc(2026, 11, 12, 2, 0, 0), utc(2026, 11, 12, 2, 0, 0)},
		"daily in winter time":          {entry(api.BackupPeriodDaily), "02:00", berlin, utc(2026, 1, 15, 12, 0, 0), utc(2026, 1, 15, 1, 0, 0)},
		"daily in summer time":          {entry(api.BackupPeriodDaily), "02:00", berlin, utc(2026, 7, 15, 12, 0, 0), utc(2026, 7, 15, 0, 0, 0)},
		"daily, the day before the gap": {entry(api.BackupPeriodDaily), "02:30", berlin, utc(2026, 3, 29, 0, 59, 0), utc(2026, 3, 28, 1, 30, 0)},
		// 02:30 does not exist on 2026-03-29 in Berlin: it is moved forward by the hour the clocks skip, 03:30 CEST
		"daily in the spring-forward gap": {entry(api.BackupPeriodDaily), "02:30", berlin, utc(2026, 3, 29, 2, 0, 0), utc(2026, 3, 29, 1, 30, 0)},
		"daily, the day after the gap":    {entry(api.BackupPeriodDaily), "02:30", berlin, utc(2026, 3, 30, 1, 0, 0), utc(2026, 3, 30, 0, 30, 0)},
		"daily in New York":               {entry(api.BackupPeriodDaily), "23:00", newYork, utc(2026, 11, 12, 3, 0, 0), utc(2026, 11, 12, 4, 0, 0).AddDate(0, 0, -1)},

		// weekly: 0 is Sunday; 2026-11-08 is a Sunday, 2026-11-12 a Thursday
		"weekly on Sunday, from Thursday":     {entry(api.BackupPeriodWeekly, 0), "02:00", nil, utc(2026, 11, 12, 12, 0, 0), utc(2026, 11, 8, 2, 0, 0)},
		"weekly on Sunday, before its time":   {entry(api.BackupPeriodWeekly, 0), "02:00", nil, utc(2026, 11, 15, 1, 59, 0), utc(2026, 11, 8, 2, 0, 0)},
		"weekly on Sunday, at its time":       {entry(api.BackupPeriodWeekly, 0), "02:00", nil, utc(2026, 11, 15, 2, 0, 0), utc(2026, 11, 15, 2, 0, 0)},
		"weekly on Saturday, across a month":  {entry(api.BackupPeriodWeekly, 6), "23:00", nil, utc(2026, 11, 2, 0, 0, 0), utc(2026, 10, 31, 23, 0, 0)},
		"weekly on Thursday, today":           {entry(api.BackupPeriodWeekly, 4), "10:00", nil, utc(2026, 11, 12, 12, 0, 0), utc(2026, 11, 12, 10, 0, 0)},
		"weekly in a zone across fall-back":   {entry(api.BackupPeriodWeekly, 1), "02:00", berlin, utc(2026, 10, 27, 0, 0, 0), utc(2026, 10, 26, 1, 0, 0)},
		"weekly in a zone, before fall-back ": {entry(api.BackupPeriodWeekly, 1), "02:00", berlin, utc(2026, 10, 25, 0, 0, 0), utc(2026, 10, 19, 0, 0, 0)},

		// monthly: day 31 is the last day of a shorter month
		"monthly 31 in February":           {entry(api.BackupPeriodMonthly, 31), "02:00", nil, utc(2026, 3, 5, 0, 0, 0), utc(2026, 2, 28, 2, 0, 0)},
		"monthly 31 in a leap February":    {entry(api.BackupPeriodMonthly, 31), "02:00", nil, utc(2028, 3, 5, 0, 0, 0), utc(2028, 2, 29, 2, 0, 0)},
		"monthly 31, February not yet due": {entry(api.BackupPeriodMonthly, 31), "02:00", nil, utc(2026, 2, 28, 1, 0, 0), utc(2026, 1, 31, 2, 0, 0)},
		"monthly 31 in April":              {entry(api.BackupPeriodMonthly, 31), "02:00", nil, utc(2026, 4, 30, 3, 0, 0), utc(2026, 4, 30, 2, 0, 0)},
		"monthly 31 in May, before it":     {entry(api.BackupPeriodMonthly, 31), "02:00", nil, utc(2026, 5, 15, 0, 0, 0), utc(2026, 4, 30, 2, 0, 0)},
		"monthly 30 in March":              {entry(api.BackupPeriodMonthly, 30), "02:00", nil, utc(2026, 3, 31, 0, 0, 0), utc(2026, 3, 30, 2, 0, 0)},
		"monthly 1, across a year":         {entry(api.BackupPeriodMonthly, 1), "00:00", nil, utc(2026, 12, 31, 23, 59, 0), utc(2026, 12, 1, 0, 0, 0)},
		"monthly 1, January":               {entry(api.BackupPeriodMonthly, 1), "00:00", nil, utc(2027, 1, 1, 0, 0, 0), utc(2027, 1, 1, 0, 0, 0)},
		"monthly in a zone":                {entry(api.BackupPeriodMonthly, 1), "00:30", berlin, utc(2026, 11, 30, 23, 0, 0), utc(2026, 10, 31, 23, 30, 0)},

		// an entry or a time that does not validate has no slot
		"a time that is no time": {entry(api.BackupPeriodDaily), "25:00", nil, utc(2026, 11, 12, 0, 0, 0), time.Time{}},
		"an unknown period":      {entry("yearly"), "00:00", nil, utc(2026, 11, 12, 0, 0, 0), time.Time{}},
	} {
		t.Run(name, func(t *testing.T) {
			got := LatestSlot(tc.entry, tc.at, tc.tz, tc.now)
			require.True(t, tc.want.Equal(got), "want %s, got %s", tc.want.UTC(), got.UTC())
			if !got.IsZero() {
				require.False(t, got.After(tc.now))
				require.True(t, got.Equal(LatestSlot(tc.entry, tc.at, tc.tz, got)), "a slot is its own latest slot")
			}
		})
	}
}

// 02:30 happens twice on the night clocks fall back: the slot is taken once, at the wall-clock time of the entry.
func TestLatestSlotInTheFallBackOverlap(t *testing.T) {
	berlin := mustLocation(t, "Europe/Berlin")
	daily := entry(api.BackupPeriodDaily)
	slot := LatestSlot(daily, "02:30", berlin, utc(2026, 10, 25, 3, 0, 0))
	require.Equal(t, "2026-10-25 02:30", slot.In(berlin).Format("2006-01-02 15:04"))
	for now := slot; now.Before(utc(2026, 10, 25, 23, 0, 0)); now = now.Add(10 * time.Minute) {
		require.True(t, slot.Equal(LatestSlot(daily, "02:30", berlin, now)), "the slot of the day stays the same at %s", now)
	}
}

func TestNextSlot(t *testing.T) {
	berlin := mustLocation(t, "Europe/Berlin")
	for name, tc := range map[string]struct {
		entry api.BackupScheduleEntry
		at    string
		tz    *time.Location
		now   time.Time
		want  time.Time
	}{
		"hour at :30":                  {entry(api.BackupPeriodHour), "00:30", nil, utc(2026, 11, 12, 10, 30, 0), utc(2026, 11, 12, 11, 30, 0)},
		"minute":                       {entry(api.BackupPeriodMinute), "", nil, utc(2026, 11, 12, 10, 30, 0), utc(2026, 11, 12, 10, 31, 0)},
		"minute, within the minute":    {entry(api.BackupPeriodMinute), "", nil, utc(2026, 11, 12, 10, 30, 30), utc(2026, 11, 12, 10, 31, 0)},
		"hour at :30, just before":     {entry(api.BackupPeriodHour), "00:30", nil, utc(2026, 11, 12, 10, 29, 0), utc(2026, 11, 12, 10, 30, 0)},
		"6hours":                       {entry(api.BackupPeriod6Hours), "05:00", nil, utc(2026, 11, 12, 23, 0, 0), utc(2026, 11, 13, 5, 0, 0)},
		"daily, at its slot":           {entry(api.BackupPeriodDaily), "02:00", nil, utc(2026, 11, 12, 2, 0, 0), utc(2026, 11, 13, 2, 0, 0)},
		"daily into the gap":           {entry(api.BackupPeriodDaily), "02:30", berlin, utc(2026, 3, 28, 12, 0, 0), utc(2026, 3, 29, 1, 30, 0)},
		"weekly on Sunday":             {entry(api.BackupPeriodWeekly, 0), "02:00", nil, utc(2026, 11, 12, 0, 0, 0), utc(2026, 11, 15, 2, 0, 0)},
		"weekly on Sunday, at its own": {entry(api.BackupPeriodWeekly, 0), "02:00", nil, utc(2026, 11, 15, 2, 0, 0), utc(2026, 11, 22, 2, 0, 0)},
		"monthly 31 into February":     {entry(api.BackupPeriodMonthly, 31), "02:00", nil, utc(2026, 1, 31, 2, 0, 0), utc(2026, 2, 28, 2, 0, 0)},
		"monthly 31 into March":        {entry(api.BackupPeriodMonthly, 31), "02:00", nil, utc(2026, 2, 28, 2, 0, 0), utc(2026, 3, 31, 2, 0, 0)},
		"a time that is no time":       {entry(api.BackupPeriodDaily), "2:00", nil, utc(2026, 11, 12, 0, 0, 0), time.Time{}},
	} {
		t.Run(name, func(t *testing.T) {
			got := NextSlot(tc.entry, tc.at, tc.tz, tc.now)
			require.True(t, tc.want.Equal(got), "want %s, got %s", tc.want.UTC(), got.UTC())
			if !got.IsZero() {
				require.True(t, got.Equal(LatestSlot(tc.entry, tc.at, tc.tz, got)), "the next slot is a slot")
				require.True(t, LatestSlot(tc.entry, tc.at, tc.tz, got.Add(-time.Second)).Before(got))
			}
		})
	}
}

func TestNextScheduleTime(t *testing.T) {
	entries := []api.BackupScheduleEntry{entry(api.BackupPeriodDaily), entry(api.BackupPeriod6Hours)}
	require.True(t, utc(2026, 11, 12, 8, 0, 0).Equal(NextScheduleTime(entries, "02:00", nil, utc(2026, 11, 12, 2, 0, 0))))
	require.True(t, utc(2026, 11, 13, 2, 0, 0).Equal(NextScheduleTime(entries[:1], "02:00", nil, utc(2026, 11, 12, 2, 0, 0))))
	require.True(t, NextScheduleTime(nil, "02:00", nil, utc(2026, 11, 12, 2, 0, 0)).IsZero())
	// Weekly on Sunday and monthly on the 1st: Tuesday 2026-12-01 runs too, not only Sundays.
	weeklyMonthly := []api.BackupScheduleEntry{entry(api.BackupPeriodWeekly, 0), entry(api.BackupPeriodMonthly, 1)}
	require.True(t, utc(2026, 11, 29, 2, 0, 0).Equal(NextScheduleTime(weeklyMonthly, "02:00", nil, utc(2026, 11, 28, 2, 0, 0))))
	require.True(t, utc(2026, 12, 1, 2, 0, 0).Equal(NextScheduleTime(weeklyMonthly, "02:00", nil, utc(2026, 11, 29, 2, 0, 0))))
}

// A change of the entries or the time zone of a schedule is held: the status keeps the entries and the zone before
// it, under a changeID later than that of any change before, until releaseHeld names it or a later change; Prune
// holds none. A schedule seen for the first time holds nothing, and a change of no rule - incremental - neither.
func TestTrackScheduleRetention(t *testing.T) {
	chbs := testSchedule("s1")
	at := time.Date(2026, time.November, 12, 12, 0, 0, 0, time.UTC)
	require.Empty(t, TrackScheduleRetention(chbs, at))
	require.Equal(t, chbs.Spec.Schedules, chbs.Status.Retention.AppliedSchedules)
	require.Equal(t, "03:00", chbs.Status.Retention.AppliedTime)
	require.Empty(t, chbs.Status.Retention.Previous)

	chbs.Spec.Schedules[0].Incremental = types.NewStringBool(true)
	require.Empty(t, TrackScheduleRetention(chbs, at), "incremental keeps the same backups")

	before := chbs.DeepCopy().Spec.Schedules
	chbs.Spec.Time = "04:00"
	require.Equal(t, "2026-11-12T12:00:00Z", TrackScheduleRetention(chbs, at))
	require.Equal(t, []api.RetiredScheduleRules{{ChangeID: "2026-11-12T12:00:00Z", Schedules: before, Time: "03:00", Expires: "2026-11-13T12:00:00Z"}},
		chbs.Status.Retention.Previous, "the daily backups the entry kept expire a day after the change at the latest")
	require.Equal(t, "2026-11-12T12:00:00Z", chbs.Status.Retention.ChangeID)

	// A second change in the same second gets a later changeID, which a release of the first does not release
	chbs.Spec.TimeZone = "Europe/Berlin"
	require.Equal(t, "2026-11-12T12:00:01Z", TrackScheduleRetention(chbs, at))
	chbs.Spec.Retention = &api.BackupRetention{ReleaseHeld: "2026-11-12T12:00:00Z"}
	require.Empty(t, TrackScheduleRetention(chbs, at.Add(time.Hour)))
	require.Len(t, chbs.Status.Retention.Previous, 1)
	require.Equal(t, "2026-11-12T12:00:01Z", chbs.Status.Retention.ChangeID)
	chbs.Spec.Retention.ReleaseHeld = "2026-11-12T12:00:01Z"
	TrackScheduleRetention(chbs, at.Add(time.Hour))
	require.Empty(t, chbs.Status.Retention.Previous)
	require.Empty(t, chbs.Status.Retention.ChangeID)

	// Prune holds nothing
	chbs.Spec.Retention.OnScheduleChange = api.RetentionPrune
	chbs.Spec.Schedules[0].Keep = 5
	require.Empty(t, TrackScheduleRetention(chbs, at.Add(2*time.Hour)))
	require.Empty(t, chbs.Status.Retention.Previous)
	require.Equal(t, int32(5), chbs.Status.Retention.AppliedSchedules[0].Keep)

	// Beyond the most changes kept, the oldest two merge into one that holds every backup made before the newer
	chbs.Spec.Retention.OnScheduleChange = ""
	for i := 0; i < api.ScheduleRetentionPreviousMaxItems+1; i++ {
		chbs.Spec.Schedules[0].Keep = int32(10 + i)
		TrackScheduleRetention(chbs, at.Add(time.Duration(3+i)*time.Hour))
	}
	previous := chbs.Status.Retention.Previous
	require.Len(t, previous, api.ScheduleRetentionPreviousMaxItems)
	require.True(t, previous[0].KeepAll)
	require.Equal(t, at.Add(4*time.Hour).Format(time.RFC3339), previous[0].ChangeID)
	require.Equal(t, at.Add(4*time.Hour).AddDate(0, 0, 10).Format(time.RFC3339), previous[0].Expires,
		"the later expiry of the two merged, daily keep 5 and daily keep 10")
}

// A change is dropped once every backup it holds has expired, unless a pin without until may name a scheduled backup
// made before it; held backups whose hold ended leave the status.
func TestExpireScheduleRetention(t *testing.T) {
	chbs := testSchedule("s1")
	weekly := []api.BackupScheduleEntry{{Period: api.BackupPeriodWeekly, Day: ptr.To(int32(0)), Keep: 4}}
	chbs.Status.Retention = &api.ScheduleRetentionStatus{Previous: []api.RetiredScheduleRules{
		{ChangeID: "2026-01-01T00:00:00Z", KeepAll: true, Expires: "2026-12-31T00:00:00Z"},
		{ChangeID: "2026-11-01T00:00:00Z", Schedules: weekly, Expires: "2026-11-29T00:00:00Z"},
		{ChangeID: "2026-11-10T00:00:00Z", Schedules: weekly, Expires: "2026-12-08T00:00:00Z"},
	}, ChangeID: "2026-11-10T00:00:00Z"}
	chbs.Status.Clusters = []api.ScheduleClusterStatus{{Name: "main", HeldBackups: []api.HeldBackup{
		{Name: "a", Expires: "2026-11-20T00:00:00Z"}, {Name: "b", Expires: "2026-12-06T00:00:00Z"}, {Name: "c"},
	}}}
	require.Empty(t, ExpireScheduleRetention(chbs, time.Date(2026, time.November, 19, 0, 0, 0, 0, time.UTC)))
	require.Len(t, chbs.Status.Clusters[0].HeldBackups, 3)

	// A pin without until of a backup made before the change keeps the change: its pin ends with the hold
	chbs.Spec.Retention = &api.BackupRetention{KeepBackups: []api.BackupRetentionPin{{Name: "s1-20261025020000"}}}
	require.Empty(t, ExpireScheduleRetention(chbs, time.Date(2026, time.November, 30, 0, 0, 0, 0, time.UTC)))
	// One made at once, or with an until, or after the change, does not
	chbs.Spec.Retention.KeepBackups = []api.BackupRetentionPin{{Name: "s1-20261025T020000"},
		{Name: "s1-20261025020000", Until: "2027-01-01T00:00:00Z"}, {Name: "s1-20261102020000"}}
	require.Equal(t, []string{"2026-11-01T00:00:00Z"}, ExpireScheduleRetention(chbs, time.Date(2026, time.November, 30, 0, 0, 0, 0, time.UTC)))
	require.Equal(t, "2026-11-10T00:00:00Z", chbs.Status.Retention.ChangeID)
	require.Equal(t, []api.HeldBackup{{Name: "b", Expires: "2026-12-06T00:00:00Z"}, {Name: "c"}}, chbs.Status.Clusters[0].HeldBackups)

	// The pin of a backup made before a change keeps it past its expires until the pin is gone
	require.Empty(t, ExpireScheduleRetention(chbs, time.Date(2026, time.December, 8, 0, 0, 0, 0, time.UTC)))
	chbs.Spec.Retention.KeepBackups = nil
	require.Equal(t, []string{"2026-11-10T00:00:00Z"}, ExpireScheduleRetention(chbs, time.Date(2026, time.December, 8, 0, 0, 0, 0, time.UTC)))
	require.Len(t, chbs.Status.Retention.Previous, 1)
	require.Equal(t, "2026-01-01T00:00:00Z", chbs.Status.Retention.ChangeID)
	require.Equal(t, []string{"2026-01-01T00:00:00Z"}, ExpireScheduleRetention(chbs, time.Date(2026, time.December, 31, 0, 0, 0, 0, time.UTC)))
	require.Nil(t, chbs.Status.Retention.Previous)
}

// The slot of a daily, weekly or monthly entry whose time does not exist the day the clocks go forward, 02:30 in
// Berlin on 2026-03-29, is the time the zone moves it to, 03:30, which the entries still recognize it by.
func TestMatchingEntriesInTheSpringForwardGap(t *testing.T) {
	berlin := mustLocation(t, "Europe/Berlin")
	entries := []api.BackupScheduleEntry{entry(api.BackupPeriodDaily), entry(api.BackupPeriodWeekly, 0)}
	slot := LatestSlot(entries[1], "02:30", berlin, utc(2026, 3, 29, 12, 0, 0))
	require.Equal(t, "20260329033000", SlotTag(slot, "Europe/Berlin"), "Go normalizes the time that does not exist")
	require.Equal(t, []api.BackupPeriod{api.BackupPeriodDaily, api.BackupPeriodWeekly}, periodsOf(MatchingEntries(entries, "02:30", slot.In(berlin))))
	// The other days are as they were: 03:30 is no slot of the entries
	require.Empty(t, MatchingEntries(entries, "02:30", utc(2026, 3, 30, 1, 30, 0).In(berlin)))
	require.Equal(t, []api.BackupPeriod{api.BackupPeriodDaily}, periodsOf(MatchingEntries(entries, "02:30", utc(2026, 3, 30, 0, 30, 0).In(berlin))))
	// Not 03:30 of the next day, nor an hour that is no slot
	require.Empty(t, MatchingEntries(entries, "02:30", utc(2026, 3, 29, 2, 30, 0).In(berlin)))
}

// The second occurrence of a wall-clock time the zone repeats as it sets the clock back is no slot: the backup of a slot
// is named after its wall-clock time, and two slots would make two of a name. The slots of every entry are the first
// occurrences, on 2026-10-25 in Berlin, where 02:00 to 03:00 happens twice.
func TestSlotsSkipTheRepeatedWallClockTime(t *testing.T) {
	berlin := mustLocation(t, "Europe/Berlin")
	// 2026-10-25 00:00Z is 02:00 CEST, 01:00Z is 02:00 CET, 02:00Z is 03:00 CET
	hourly := entry(api.BackupPeriodHour)
	require.True(t, utc(2026, 10, 25, 0, 30, 0).Equal(LatestSlot(hourly, "00:30", berlin, utc(2026, 10, 25, 1, 45, 0))), "the second 02:30 is no slot")
	require.True(t, utc(2026, 10, 25, 1, 30, 0).Equal(NextSlot(hourly, "00:30", berlin, utc(2026, 10, 25, 0, 45, 0)).Add(-time.Hour)),
		"the slot after the first 02:30 is 03:30")
	minutely := entry(api.BackupPeriodMinute)
	require.True(t, utc(2026, 10, 25, 0, 59, 0).Equal(LatestSlot(minutely, "", berlin, utc(2026, 10, 25, 1, 15, 0))))
	require.True(t, utc(2026, 10, 25, 2, 0, 0).Equal(NextSlot(minutely, "", berlin, utc(2026, 10, 25, 0, 59, 30))))
	daily := entry(api.BackupPeriodDaily)
	// Go takes 02:30 of the repeated hour for its second occurrence: the slot is the first
	require.True(t, utc(2026, 10, 25, 0, 30, 0).Equal(LatestSlot(daily, "02:30", berlin, utc(2026, 10, 25, 1, 35, 0))))
	require.True(t, utc(2026, 10, 26, 1, 30, 0).Equal(NextSlot(daily, "02:30", berlin, utc(2026, 10, 25, 0, 40, 0))), "it ran at 00:30Z, the next is tomorrow's")

	for name, tc := range map[string]struct {
		e  api.BackupScheduleEntry
		at string
	}{"minute": {minutely, ""}, "hour": {hourly, "00:30"}, "daily": {daily, "02:30"}} {
		t.Run(name, func(t *testing.T) {
			seen := map[string]bool{}
			for slot := NextSlot(tc.e, tc.at, berlin, utc(2026, 10, 24, 21, 0, 0)); slot.Before(utc(2026, 10, 25, 4, 0, 0)); slot = NextSlot(tc.e, tc.at, berlin, slot) {
				tag := SlotTag(slot, "Europe/Berlin")
				require.False(t, seen[tag], "two slots are named %s", tag)
				seen[tag] = true
			}
			require.NotEmpty(t, seen)
		})
	}
}
