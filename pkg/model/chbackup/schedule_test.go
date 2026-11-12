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
	"strconv"
	"strings"
	"testing"
	"time"
	_ "time/tzdata"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func entry(period api.BackupPeriod, at string, day ...int32) api.BackupScheduleEntry {
	e := api.BackupScheduleEntry{Period: period, Time: at, Keep: 1}
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

// cronFires reports whether a standard five-field cron expression, of the forms the CronJob controller takes and
// FinestCron writes (numbers, comma lists, ranges, *), fires at the minute of t. It refuses anything else.
func cronFires(t *testing.T, expression string, at time.Time) bool {
	t.Helper()
	fields := strings.Fields(expression)
	require.Len(t, fields, 5, expression)
	field := func(spec string, value, low, high int) bool {
		if spec == "*" {
			return true
		}
		for _, part := range strings.Split(spec, ",") {
			from, to, isRange := strings.Cut(part, "-")
			first, err := strconv.Atoi(from)
			require.NoError(t, err, expression)
			last := first
			if isRange {
				last, err = strconv.Atoi(to)
				require.NoError(t, err, expression)
			}
			require.True(t, (low <= first) && (first <= last) && (last <= high), expression)
			if (first <= value) && (value <= last) {
				return true
			}
		}
		return false
	}
	return field(fields[0], at.Minute(), 0, 59) && field(fields[1], at.Hour(), 0, 23) &&
		field(fields[2], at.Day(), 1, 31) && field(fields[3], int(at.Month()), 1, 12) &&
		field(fields[4], int(at.Weekday()), 0, 6)
}

func TestFinestCron(t *testing.T) {
	for name, tc := range map[string]struct {
		entries []api.BackupScheduleEntry
		want    string
	}{
		"none":                         {nil, ""},
		"minute":                       {[]api.BackupScheduleEntry{entry(api.BackupPeriodMinute, "")}, "* * * * *"},
		"minute under daily, monthly":  {[]api.BackupScheduleEntry{entry(api.BackupPeriodDaily, "02:30"), entry(api.BackupPeriodMonthly, "02:30", 31), entry(api.BackupPeriodMinute, "")}, "* * * * *"},
		"hour, no time":                {[]api.BackupScheduleEntry{entry(api.BackupPeriodHour, "")}, "0 * * * *"},
		"hour at the shared minute":    {[]api.BackupScheduleEntry{entry(api.BackupPeriodHour, ""), entry(api.BackupPeriodDaily, "02:30")}, "30 * * * *"},
		"hour with its own time":       {[]api.BackupScheduleEntry{entry(api.BackupPeriodHour, "17:05")}, "5 * * * *"},
		"6hours, hour modulo 6":        {[]api.BackupScheduleEntry{entry(api.BackupPeriod6Hours, "13:45")}, "45 1,7,13,19 * * *"},
		"6hours at the shared time":    {[]api.BackupScheduleEntry{entry(api.BackupPeriodWeekly, "02:15", 0), entry(api.BackupPeriod6Hours, "")}, "15 2,8,14,20 * * *"},
		"6hours, no time":              {[]api.BackupScheduleEntry{entry(api.BackupPeriod6Hours, "")}, "0 0,6,12,18 * * *"},
		"daily":                        {[]api.BackupScheduleEntry{entry(api.BackupPeriodDaily, "02:00")}, "0 2 * * *"},
		"daily under weekly, monthly":  {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "23:59", 1), entry(api.BackupPeriodWeekly, "23:59", 6), entry(api.BackupPeriodDaily, "23:59")}, "59 23 * * *"},
		"weekly":                       {[]api.BackupScheduleEntry{entry(api.BackupPeriodWeekly, "02:00", 0)}, "0 2 * * 0"},
		"weekly under monthly":         {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "04:10", 15), entry(api.BackupPeriodWeekly, "04:10", 3)}, "10 4 * * 3"},
		"monthly on the 1st":           {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "00:00", 1)}, "0 0 1 * *"},
		"monthly on the 28th":          {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "03:00", 28)}, "0 3 28 * *"},
		"monthly on the 29th":          {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "03:00", 29)}, "0 3 28-31 * *"},
		"monthly on the 31st":          {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "03:00", 31)}, "0 3 28-31 * *"},
		"a time that is no time":       {[]api.BackupScheduleEntry{entry(api.BackupPeriodDaily, "2:00")}, ""},
		"the order of the spec counts": {[]api.BackupScheduleEntry{entry(api.BackupPeriodDaily, "06:00"), entry(api.BackupPeriodHour, "")}, "0 * * * *"},
	} {
		t.Run(name, func(t *testing.T) {
			got := FinestCron(tc.entries)
			require.Equal(t, tc.want, got)
			if got != "" {
				cronFires(t, got, at(2026, 1, 1, 0, 0))
			}
		})
	}
}

func TestMatchingEntries(t *testing.T) {
	all := []api.BackupScheduleEntry{
		entry(api.BackupPeriodHour, ""),
		entry(api.BackupPeriod6Hours, ""),
		entry(api.BackupPeriodDaily, "02:30"),
		entry(api.BackupPeriodWeekly, "02:30", 0),
		entry(api.BackupPeriodMonthly, "02:30", 31),
	}
	// 2026-11-01 is a Sunday, 2026-11-12 a Thursday
	for name, tc := range map[string]struct {
		entries []api.BackupScheduleEntry
		t       time.Time
		want    []api.BackupPeriod
	}{
		"not the minute":                   {all, at(2026, 11, 12, 2, 0), nil},
		"minute at any minute":             {[]api.BackupScheduleEntry{entry(api.BackupPeriodMinute, "")}, at(2026, 11, 12, 13, 47), []api.BackupPeriod{api.BackupPeriodMinute}},
		"minute ignores its time":          {[]api.BackupScheduleEntry{entry(api.BackupPeriodMinute, "02:30")}, at(2026, 11, 12, 13, 47), []api.BackupPeriod{api.BackupPeriodMinute}},
		"minute among all":                 {append([]api.BackupScheduleEntry{entry(api.BackupPeriodMinute, "")}, all...), at(2026, 11, 12, 3, 30), []api.BackupPeriod{api.BackupPeriodMinute, api.BackupPeriodHour}},
		"hour only":                        {all, at(2026, 11, 12, 3, 30), []api.BackupPeriod{api.BackupPeriodHour}},
		"hour and 6hours":                  {all, at(2026, 11, 12, 8, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours}},
		"daily on a Thursday":              {all, at(2026, 11, 12, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily}},
		"weekly on Sunday":                 {all, at(2026, 11, 1, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodWeekly}},
		"monthly 31 on the 31st":           {all, at(2026, 10, 31, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodMonthly}},
		"monthly 31 not on the 30th of 31": {all, at(2026, 10, 30, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily}},
		"monthly 31 on the 30th of April":  {all, at(2026, 4, 30, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodMonthly}},
		"monthly 31 on February 28":        {all, at(2026, 2, 28, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodMonthly}},
		"monthly 31 not on Feb 28, leap":   {all, at(2028, 2, 28, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily}},
		"monthly 31 on February 29, leap":  {all, at(2028, 2, 29, 2, 30), []api.BackupPeriod{api.BackupPeriodHour, api.BackupPeriod6Hours, api.BackupPeriodDaily, api.BackupPeriodMonthly}},
		"monthly 30 on the 30th of March":  {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "02:30", 30)}, at(2026, 3, 30, 2, 30), []api.BackupPeriod{api.BackupPeriodMonthly}},
		"monthly 30 not on the 31st":       {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "02:30", 30)}, at(2026, 3, 31, 2, 30), nil},
		"monthly 15 not on the 16th":       {[]api.BackupScheduleEntry{entry(api.BackupPeriodMonthly, "02:30", 15)}, at(2026, 3, 16, 2, 30), nil},
		"6hours, hour modulo 6":            {[]api.BackupScheduleEntry{entry(api.BackupPeriod6Hours, "13:45")}, at(2026, 11, 12, 19, 45), []api.BackupPeriod{api.BackupPeriod6Hours}},
		"6hours, another hour":             {[]api.BackupScheduleEntry{entry(api.BackupPeriod6Hours, "13:45")}, at(2026, 11, 12, 18, 45), nil},
		"hour, its own minute":             {[]api.BackupScheduleEntry{entry(api.BackupPeriodHour, "17:05")}, at(2026, 11, 12, 0, 5), []api.BackupPeriod{api.BackupPeriodHour}},
		"hour without time, at :00":        {[]api.BackupScheduleEntry{entry(api.BackupPeriodHour, "")}, at(2026, 11, 12, 7, 0), []api.BackupPeriod{api.BackupPeriodHour}},
		"weekly on Saturday":               {[]api.BackupScheduleEntry{entry(api.BackupPeriodWeekly, "23:00", 6)}, at(2026, 10, 31, 23, 0), []api.BackupPeriod{api.BackupPeriodWeekly}},
		"weekly, another weekday":          {[]api.BackupScheduleEntry{entry(api.BackupPeriodWeekly, "23:00", 6)}, at(2026, 11, 1, 23, 0), nil},
		"seconds do not count":             {[]api.BackupScheduleEntry{entry(api.BackupPeriodDaily, "02:30")}, at(2026, 11, 12, 2, 30).Add(59 * time.Second), []api.BackupPeriod{api.BackupPeriodDaily}},
		"an entry that does not validate":  {[]api.BackupScheduleEntry{entry(api.BackupPeriodDaily, "2:30")}, at(2026, 11, 12, 2, 30), nil},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, periodsOf(MatchingEntries(tc.entries, tc.t)))
		})
	}
}

// The time matched is the wall-clock time in the schedule's time zone, the time the CronJob, given the same zone,
// started the run at: across a DST change the entries still match by the wall clock.
func TestMatchingEntriesInATimeZone(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	entries := []api.BackupScheduleEntry{entry(api.BackupPeriodHour, ""), entry(api.BackupPeriodDaily, "03:30")}
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
			require.Equal(t, tc.want, periodsOf(MatchingEntries(entries, tc.utc.In(berlin))))
		})
	}
	// A zone half an hour off UTC: the minute is the wall clock's
	kolkata, err := time.LoadLocation("Asia/Kolkata")
	require.NoError(t, err)
	require.Equal(t, []api.BackupPeriod{api.BackupPeriodHour},
		periodsOf(MatchingEntries([]api.BackupScheduleEntry{entry(api.BackupPeriodHour, "")}, at(2026, 11, 12, 10, 30).In(kolkata))))
}

// Every slot of the cron schedule FinestCron gives is matched by the finest entry, and every slot of a coarser entry
// is one of the cron schedule's: a month of minutes is walked for several schedules.
func TestFinestCronFiresAtEverySlotOfEveryEntry(t *testing.T) {
	for name, entries := range map[string][]api.BackupScheduleEntry{
		"hour, daily, weekly, monthly 31": {entry(api.BackupPeriodHour, ""), entry(api.BackupPeriodDaily, "02:30"),
			entry(api.BackupPeriodWeekly, "02:30", 0), entry(api.BackupPeriodMonthly, "02:30", 31)},
		"6hours, monthly 30": {entry(api.BackupPeriod6Hours, ""), entry(api.BackupPeriodMonthly, "13:15", 30)},
		"weekly, monthly 29, hour": {entry(api.BackupPeriodWeekly, "00:00", 3), entry(api.BackupPeriodMonthly, "00:00", 29),
			entry(api.BackupPeriodHour, "")},
		"weekly alone":         {entry(api.BackupPeriodWeekly, "06:00", 1)},
		"monthly 31 alone":     {entry(api.BackupPeriodMonthly, "23:59", 31)},
		"minute, monthly 31":   {entry(api.BackupPeriodMinute, ""), entry(api.BackupPeriodMonthly, "23:59", 31)},
		"daily and 6hours mod": {entry(api.BackupPeriodDaily, "17:40"), entry(api.BackupPeriod6Hours, "")},
	} {
		t.Run(name, func(t *testing.T) {
			expression := FinestCron(entries)
			finestPeriod := finest(entries).Period
			// February of a non-leap and a leap year, and a 30-day month
			for _, month := range []time.Time{at(2026, 2, 1, 0, 0), at(2028, 2, 1, 0, 0), at(2026, 4, 1, 0, 0)} {
				for minute := month; minute.Month() == month.Month(); minute = minute.Add(time.Minute) {
					matched := MatchingEntries(entries, minute)
					if len(matched) > 0 {
						require.True(t, cronFires(t, expression, minute), "%s matches %v but the cron schedule does not fire", minute, periodsOf(matched))
					}
					if cronFires(t, expression, minute) && (finestPeriod != api.BackupPeriodMonthly) {
						require.Contains(t, periodsOf(matched), finestPeriod, "%s fires, the finest entry must match", minute)
					}
				}
			}
		})
	}
}

func TestWinner(t *testing.T) {
	require.Nil(t, Winner(nil))
	matched := []api.BackupScheduleEntry{
		entry(api.BackupPeriodHour, "02:30"), entry(api.BackupPeriodWeekly, "02:30", 0), entry(api.BackupPeriodDaily, "02:30"),
	}
	require.Equal(t, api.BackupPeriodWeekly, Winner(matched).Period)
	require.Equal(t, api.BackupPeriodDaily, Winner(matched[2:]).Period)
	require.Equal(t, api.BackupPeriodMonthly, Winner(append(matched, entry(api.BackupPeriodMonthly, "02:30", 1))).Period)
	require.Equal(t, api.BackupPeriod6Hours, Winner([]api.BackupScheduleEntry{entry(api.BackupPeriodHour, ""), entry(api.BackupPeriod6Hours, "")}).Period)
	require.Equal(t, api.BackupPeriodHour, Winner([]api.BackupScheduleEntry{entry(api.BackupPeriodMinute, ""), entry(api.BackupPeriodHour, "")}).Period)
	require.Equal(t, api.BackupPeriodMinute, Winner([]api.BackupScheduleEntry{entry(api.BackupPeriodMinute, "")}).Period)
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

func TestEntriesEnv(t *testing.T) {
	require.Equal(t, "", EntriesEnv(nil))
	require.Equal(t, "period=hour time=00:00 keep=1", EntriesEnv([]api.BackupScheduleEntry{entry(api.BackupPeriodHour, "")}))
	require.Equal(t, "period=minute keep=1;period=daily time=02:30 keep=1",
		EntriesEnv([]api.BackupScheduleEntry{entry(api.BackupPeriodMinute, ""), entry(api.BackupPeriodDaily, "02:30")}))
	hourly := entry(api.BackupPeriodHour, "")
	hourly.Keep = 24
	hourly.Incremental = types.NewStringBool(true)
	weekly := entry(api.BackupPeriodWeekly, "02:30", 0)
	weekly.Keep = 4
	monthly := entry(api.BackupPeriodMonthly, "02:30", 31)
	monthly.Keep = 12
	monthly.Incremental = types.NewStringBool(false)
	require.Equal(t,
		"period=hour time=02:30 keep=24 incremental=true;period=weekly time=02:30 day=0 keep=4;period=monthly time=02:30 day=31 keep=12",
		EntriesEnv([]api.BackupScheduleEntry{hourly, weekly, monthly}))
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
		tz    *time.Location
		now   time.Time
		want  time.Time
	}{
		// hour: only the minute of the time counts
		"minute, on the minute":             {entry(api.BackupPeriodMinute, ""), nil, utc(2026, 11, 12, 10, 7, 0), utc(2026, 11, 12, 10, 7, 0)},
		"minute, within the minute":         {entry(api.BackupPeriodMinute, ""), kolkata, utc(2026, 11, 12, 10, 7, 59), utc(2026, 11, 12, 10, 7, 0)},
		"hour, no time, on the hour":        {entry(api.BackupPeriodHour, ""), nil, utc(2026, 11, 12, 10, 0, 0), utc(2026, 11, 12, 10, 0, 0)},
		"hour, no time, past the hour":      {entry(api.BackupPeriodHour, ""), nil, utc(2026, 11, 12, 10, 59, 59), utc(2026, 11, 12, 10, 0, 0)},
		"hour at :30, just before":          {entry(api.BackupPeriodHour, "02:30"), nil, utc(2026, 11, 12, 10, 29, 59), utc(2026, 11, 12, 9, 30, 0)},
		"hour at :30, at it":                {entry(api.BackupPeriodHour, "02:30"), nil, utc(2026, 11, 12, 10, 30, 0), utc(2026, 11, 12, 10, 30, 0)},
		"hour at :30 across midnight":       {entry(api.BackupPeriodHour, "17:30"), nil, utc(2026, 11, 12, 0, 10, 0), utc(2026, 11, 11, 23, 30, 0)},
		"hour at :00 in a half-hour zone":   {entry(api.BackupPeriodHour, "00:00"), kolkata, utc(2026, 11, 12, 10, 45, 0), utc(2026, 11, 12, 10, 30, 0)},
		"hour across a spring-forward gap":  {entry(api.BackupPeriodHour, "00:15"), berlin, utc(2026, 3, 29, 1, 20, 0), utc(2026, 3, 29, 1, 15, 0)},
		"hour across a fall-back overlap":   {entry(api.BackupPeriodHour, "00:15"), berlin, utc(2026, 10, 25, 1, 10, 0), utc(2026, 10, 25, 0, 15, 0)},
		"hour, sub-second now is truncated": {entry(api.BackupPeriodHour, "00:05"), nil, utc(2026, 11, 12, 10, 5, 0).Add(999 * time.Millisecond), utc(2026, 11, 12, 10, 5, 0)},

		// 6hours: the hour modulo 6 and the minute
		"6hours at 02:15, at 08:15":           {entry(api.BackupPeriod6Hours, "02:15"), nil, utc(2026, 11, 12, 8, 15, 0), utc(2026, 11, 12, 8, 15, 0)},
		"6hours at 02:15, at 08:14":           {entry(api.BackupPeriod6Hours, "02:15"), nil, utc(2026, 11, 12, 8, 14, 0), utc(2026, 11, 12, 2, 15, 0)},
		"6hours at 02:15, before the first":   {entry(api.BackupPeriod6Hours, "02:15"), nil, utc(2026, 11, 12, 1, 0, 0), utc(2026, 11, 11, 20, 15, 0)},
		"6hours at 13:00 is 01, 07, 13, 19":   {entry(api.BackupPeriod6Hours, "13:00"), nil, utc(2026, 11, 12, 6, 59, 0), utc(2026, 11, 12, 1, 0, 0)},
		"6hours at 13:00, late in the day":    {entry(api.BackupPeriod6Hours, "13:00"), nil, utc(2026, 11, 12, 23, 59, 0), utc(2026, 11, 12, 19, 0, 0)},
		"6hours, no time":                     {entry(api.BackupPeriod6Hours, ""), nil, utc(2026, 11, 12, 17, 0, 0), utc(2026, 11, 12, 12, 0, 0)},
		"6hours in a zone, wall clock":        {entry(api.BackupPeriod6Hours, "00:00"), berlin, utc(2026, 11, 12, 5, 30, 0), utc(2026, 11, 12, 5, 0, 0)},
		"6hours in summer time, wall clock":   {entry(api.BackupPeriod6Hours, "00:00"), berlin, utc(2026, 7, 12, 4, 30, 0), utc(2026, 7, 12, 4, 0, 0)},
		"6hours on the day of spring-forward": {entry(api.BackupPeriod6Hours, "00:00"), berlin, utc(2026, 3, 29, 4, 30, 0), utc(2026, 3, 29, 4, 0, 0)},

		// daily
		"daily, before the time":        {entry(api.BackupPeriodDaily, "02:00"), nil, utc(2026, 11, 12, 1, 59, 0), utc(2026, 11, 11, 2, 0, 0)},
		"daily, at the time":            {entry(api.BackupPeriodDaily, "02:00"), nil, utc(2026, 11, 12, 2, 0, 0), utc(2026, 11, 12, 2, 0, 0)},
		"daily in winter time":          {entry(api.BackupPeriodDaily, "02:00"), berlin, utc(2026, 1, 15, 12, 0, 0), utc(2026, 1, 15, 1, 0, 0)},
		"daily in summer time":          {entry(api.BackupPeriodDaily, "02:00"), berlin, utc(2026, 7, 15, 12, 0, 0), utc(2026, 7, 15, 0, 0, 0)},
		"daily, the day before the gap": {entry(api.BackupPeriodDaily, "02:30"), berlin, utc(2026, 3, 29, 0, 59, 0), utc(2026, 3, 28, 1, 30, 0)},
		// 02:30 does not exist on 2026-03-29 in Berlin: it is moved forward by the hour the clocks skip, 03:30 CEST
		"daily in the spring-forward gap": {entry(api.BackupPeriodDaily, "02:30"), berlin, utc(2026, 3, 29, 2, 0, 0), utc(2026, 3, 29, 1, 30, 0)},
		"daily, the day after the gap":    {entry(api.BackupPeriodDaily, "02:30"), berlin, utc(2026, 3, 30, 1, 0, 0), utc(2026, 3, 30, 0, 30, 0)},
		"daily in New York":               {entry(api.BackupPeriodDaily, "23:00"), newYork, utc(2026, 11, 12, 3, 0, 0), utc(2026, 11, 12, 4, 0, 0).AddDate(0, 0, -1)},

		// weekly: 0 is Sunday; 2026-11-08 is a Sunday, 2026-11-12 a Thursday
		"weekly on Sunday, from Thursday":     {entry(api.BackupPeriodWeekly, "02:00", 0), nil, utc(2026, 11, 12, 12, 0, 0), utc(2026, 11, 8, 2, 0, 0)},
		"weekly on Sunday, before its time":   {entry(api.BackupPeriodWeekly, "02:00", 0), nil, utc(2026, 11, 15, 1, 59, 0), utc(2026, 11, 8, 2, 0, 0)},
		"weekly on Sunday, at its time":       {entry(api.BackupPeriodWeekly, "02:00", 0), nil, utc(2026, 11, 15, 2, 0, 0), utc(2026, 11, 15, 2, 0, 0)},
		"weekly on Saturday, across a month":  {entry(api.BackupPeriodWeekly, "23:00", 6), nil, utc(2026, 11, 2, 0, 0, 0), utc(2026, 10, 31, 23, 0, 0)},
		"weekly on Thursday, today":           {entry(api.BackupPeriodWeekly, "10:00", 4), nil, utc(2026, 11, 12, 12, 0, 0), utc(2026, 11, 12, 10, 0, 0)},
		"weekly in a zone across fall-back":   {entry(api.BackupPeriodWeekly, "02:00", 1), berlin, utc(2026, 10, 27, 0, 0, 0), utc(2026, 10, 26, 1, 0, 0)},
		"weekly in a zone, before fall-back ": {entry(api.BackupPeriodWeekly, "02:00", 1), berlin, utc(2026, 10, 25, 0, 0, 0), utc(2026, 10, 19, 0, 0, 0)},

		// monthly: day 31 is the last day of a shorter month
		"monthly 31 in February":           {entry(api.BackupPeriodMonthly, "02:00", 31), nil, utc(2026, 3, 5, 0, 0, 0), utc(2026, 2, 28, 2, 0, 0)},
		"monthly 31 in a leap February":    {entry(api.BackupPeriodMonthly, "02:00", 31), nil, utc(2028, 3, 5, 0, 0, 0), utc(2028, 2, 29, 2, 0, 0)},
		"monthly 31, February not yet due": {entry(api.BackupPeriodMonthly, "02:00", 31), nil, utc(2026, 2, 28, 1, 0, 0), utc(2026, 1, 31, 2, 0, 0)},
		"monthly 31 in April":              {entry(api.BackupPeriodMonthly, "02:00", 31), nil, utc(2026, 4, 30, 3, 0, 0), utc(2026, 4, 30, 2, 0, 0)},
		"monthly 31 in May, before it":     {entry(api.BackupPeriodMonthly, "02:00", 31), nil, utc(2026, 5, 15, 0, 0, 0), utc(2026, 4, 30, 2, 0, 0)},
		"monthly 30 in March":              {entry(api.BackupPeriodMonthly, "02:00", 30), nil, utc(2026, 3, 31, 0, 0, 0), utc(2026, 3, 30, 2, 0, 0)},
		"monthly 1, across a year":         {entry(api.BackupPeriodMonthly, "00:00", 1), nil, utc(2026, 12, 31, 23, 59, 0), utc(2026, 12, 1, 0, 0, 0)},
		"monthly 1, January":               {entry(api.BackupPeriodMonthly, "00:00", 1), nil, utc(2027, 1, 1, 0, 0, 0), utc(2027, 1, 1, 0, 0, 0)},
		"monthly in a zone":                {entry(api.BackupPeriodMonthly, "00:30", 1), berlin, utc(2026, 11, 30, 23, 0, 0), utc(2026, 10, 31, 23, 30, 0)},

		// an entry that does not validate has no slot
		"a time that is no time": {entry(api.BackupPeriodDaily, "25:00"), nil, utc(2026, 11, 12, 0, 0, 0), time.Time{}},
		"an unknown period":      {entry("yearly", "00:00"), nil, utc(2026, 11, 12, 0, 0, 0), time.Time{}},
	} {
		t.Run(name, func(t *testing.T) {
			got := LatestSlot(tc.entry, tc.tz, tc.now)
			require.True(t, tc.want.Equal(got), "want %s, got %s", tc.want.UTC(), got.UTC())
			if !got.IsZero() {
				require.False(t, got.After(tc.now))
				require.True(t, got.Equal(LatestSlot(tc.entry, tc.tz, got)), "a slot is its own latest slot")
			}
		})
	}
}

// 02:30 happens twice on the night clocks fall back: the slot is taken once, at the wall-clock time of the entry.
func TestLatestSlotInTheFallBackOverlap(t *testing.T) {
	berlin := mustLocation(t, "Europe/Berlin")
	daily := entry(api.BackupPeriodDaily, "02:30")
	slot := LatestSlot(daily, berlin, utc(2026, 10, 25, 3, 0, 0))
	require.Equal(t, "2026-10-25 02:30", slot.In(berlin).Format("2006-01-02 15:04"))
	for now := slot; now.Before(utc(2026, 10, 25, 23, 0, 0)); now = now.Add(10 * time.Minute) {
		require.True(t, slot.Equal(LatestSlot(daily, berlin, now)), "the slot of the day stays the same at %s", now)
	}
	due := DueEntries([]api.BackupScheduleEntry{daily}, berlin, slot.Add(time.Hour), map[api.BackupPeriod]time.Time{api.BackupPeriodDaily: slot}, 0)
	require.Empty(t, due, "the repeated wall-clock time does not fire the entry twice")
}

func TestNextSlot(t *testing.T) {
	berlin := mustLocation(t, "Europe/Berlin")
	for name, tc := range map[string]struct {
		entry api.BackupScheduleEntry
		tz    *time.Location
		now   time.Time
		want  time.Time
	}{
		"hour at :30":                  {entry(api.BackupPeriodHour, "00:30"), nil, utc(2026, 11, 12, 10, 30, 0), utc(2026, 11, 12, 11, 30, 0)},
		"minute":                       {entry(api.BackupPeriodMinute, ""), nil, utc(2026, 11, 12, 10, 30, 0), utc(2026, 11, 12, 10, 31, 0)},
		"minute, within the minute":    {entry(api.BackupPeriodMinute, ""), nil, utc(2026, 11, 12, 10, 30, 30), utc(2026, 11, 12, 10, 31, 0)},
		"hour at :30, just before":     {entry(api.BackupPeriodHour, "00:30"), nil, utc(2026, 11, 12, 10, 29, 0), utc(2026, 11, 12, 10, 30, 0)},
		"6hours":                       {entry(api.BackupPeriod6Hours, "05:00"), nil, utc(2026, 11, 12, 23, 0, 0), utc(2026, 11, 13, 5, 0, 0)},
		"daily, at its slot":           {entry(api.BackupPeriodDaily, "02:00"), nil, utc(2026, 11, 12, 2, 0, 0), utc(2026, 11, 13, 2, 0, 0)},
		"daily into the gap":           {entry(api.BackupPeriodDaily, "02:30"), berlin, utc(2026, 3, 28, 12, 0, 0), utc(2026, 3, 29, 1, 30, 0)},
		"weekly on Sunday":             {entry(api.BackupPeriodWeekly, "02:00", 0), nil, utc(2026, 11, 12, 0, 0, 0), utc(2026, 11, 15, 2, 0, 0)},
		"weekly on Sunday, at its own": {entry(api.BackupPeriodWeekly, "02:00", 0), nil, utc(2026, 11, 15, 2, 0, 0), utc(2026, 11, 22, 2, 0, 0)},
		"monthly 31 into February":     {entry(api.BackupPeriodMonthly, "02:00", 31), nil, utc(2026, 1, 31, 2, 0, 0), utc(2026, 2, 28, 2, 0, 0)},
		"monthly 31 into March":        {entry(api.BackupPeriodMonthly, "02:00", 31), nil, utc(2026, 2, 28, 2, 0, 0), utc(2026, 3, 31, 2, 0, 0)},
		"a time that is no time":       {entry(api.BackupPeriodDaily, "2:00"), nil, utc(2026, 11, 12, 0, 0, 0), time.Time{}},
	} {
		t.Run(name, func(t *testing.T) {
			got := NextSlot(tc.entry, tc.tz, tc.now)
			require.True(t, tc.want.Equal(got), "want %s, got %s", tc.want.UTC(), got.UTC())
			if !got.IsZero() {
				require.True(t, got.Equal(LatestSlot(tc.entry, tc.tz, got)), "the next slot is a slot")
				require.True(t, LatestSlot(tc.entry, tc.tz, got.Add(-time.Second)).Before(got))
			}
		})
	}
}

func TestDueEntries(t *testing.T) {
	// 2026-11-01 is a Sunday and the first of a month: every entry below is due at 02:00
	sunday := utc(2026, 11, 1, 2, 0, 0)
	entries := []api.BackupScheduleEntry{
		entry(api.BackupPeriodHour, "02:00"),
		entry(api.BackupPeriodDaily, "02:00"),
		entry(api.BackupPeriodMonthly, "02:00", 1),
		entry(api.BackupPeriodWeekly, "02:00", 0),
		entry(api.BackupPeriod6Hours, "02:00"),
	}
	type fired = map[api.BackupPeriod]time.Time
	for name, tc := range map[string]struct {
		now       time.Time
		lastFired fired
		deadline  time.Duration
		want      []api.BackupPeriod
	}{
		"all due at once, the longest period first": {
			now:  sunday,
			want: []api.BackupPeriod{api.BackupPeriodMonthly, api.BackupPeriodWeekly, api.BackupPeriodDaily, api.BackupPeriod6Hours, api.BackupPeriodHour},
		},
		"the slot already fired for some": {
			now:       sunday.Add(time.Minute),
			lastFired: fired{api.BackupPeriodMonthly: sunday, api.BackupPeriodHour: sunday},
			want:      []api.BackupPeriod{api.BackupPeriodWeekly, api.BackupPeriodDaily, api.BackupPeriod6Hours},
		},
		"the slot fired for all": {
			now: sunday.Add(time.Minute),
			lastFired: fired{api.BackupPeriodMonthly: sunday, api.BackupPeriodWeekly: sunday, api.BackupPeriodDaily: sunday,
				api.BackupPeriod6Hours: sunday, api.BackupPeriodHour: sunday},
		},
		"a fired time later than the slot": {
			now:       sunday,
			lastFired: fired{api.BackupPeriodMonthly: sunday.Add(time.Hour)},
			want:      []api.BackupPeriod{api.BackupPeriodWeekly, api.BackupPeriodDaily, api.BackupPeriod6Hours, api.BackupPeriodHour},
		},
		"an hour later only the hour is new": {
			now: sunday.Add(time.Hour),
			lastFired: fired{api.BackupPeriodMonthly: sunday, api.BackupPeriodWeekly: sunday, api.BackupPeriodDaily: sunday,
				api.BackupPeriod6Hours: sunday, api.BackupPeriodHour: sunday},
			want: []api.BackupPeriod{api.BackupPeriodHour},
		},
		"past the deadline only the recent slots": {
			now:       sunday.Add(time.Hour + 10*time.Minute),
			lastFired: fired{api.BackupPeriodHour: sunday},
			deadline:  30 * time.Minute,
			want:      []api.BackupPeriod{api.BackupPeriodHour},
		},
		"no deadline, a missed slot still fires": {
			now:       sunday.Add(5 * time.Hour),
			lastFired: fired{api.BackupPeriodHour: sunday.Add(5 * time.Hour), api.BackupPeriodDaily: sunday, api.BackupPeriod6Hours: sunday},
			want:      []api.BackupPeriod{api.BackupPeriodMonthly, api.BackupPeriodWeekly},
		},
		"at the deadline exactly": {
			now:       sunday.Add(30 * time.Minute),
			lastFired: fired{api.BackupPeriodMonthly: sunday, api.BackupPeriodWeekly: sunday, api.BackupPeriodHour: sunday},
			deadline:  30 * time.Minute,
			want:      []api.BackupPeriod{api.BackupPeriodDaily, api.BackupPeriod6Hours},
		},
	} {
		t.Run(name, func(t *testing.T) {
			due := DueEntries(entries, nil, tc.now, tc.lastFired, tc.deadline)
			require.Equal(t, tc.want, periodsOfDue(due))
			for _, d := range due {
				require.True(t, d.Slot.Equal(LatestSlot(d.Entry, nil, tc.now)))
			}
		})
	}
}

// Entries due at different slots are told apart by their slots: the run is that of the largest weight among the due.
func TestDueEntriesOfDifferentSlots(t *testing.T) {
	berlin := mustLocation(t, "Europe/Berlin")
	entries := []api.BackupScheduleEntry{entry(api.BackupPeriodHour, ""), entry(api.BackupPeriodDaily, "03:00")}
	now := time.Date(2026, 11, 12, 3, 20, 0, 0, berlin)
	due := DueEntries(entries, berlin, now, nil, 30*time.Minute)
	require.Equal(t, []api.BackupPeriod{api.BackupPeriodDaily, api.BackupPeriodHour}, periodsOfDue(due))
	require.True(t, due[0].Slot.Equal(due[1].Slot), "03:00 is a slot of both")

	now = time.Date(2026, 11, 12, 4, 20, 0, 0, berlin)
	due = DueEntries(entries, berlin, now, map[api.BackupPeriod]time.Time{api.BackupPeriodDaily: due[0].Slot}, 30*time.Minute)
	require.Equal(t, []api.BackupPeriod{api.BackupPeriodHour}, periodsOfDue(due))
	require.Equal(t, "04:00", due[0].Slot.In(berlin).Format("15:04"))
}

func TestNextScheduleTime(t *testing.T) {
	entries := []api.BackupScheduleEntry{entry(api.BackupPeriodDaily, "02:00"), entry(api.BackupPeriod6Hours, "02:00")}
	require.True(t, utc(2026, 11, 12, 8, 0, 0).Equal(NextScheduleTime(entries, nil, utc(2026, 11, 12, 2, 0, 0))))
	require.True(t, utc(2026, 11, 13, 2, 0, 0).Equal(NextScheduleTime(entries[:1], nil, utc(2026, 11, 12, 2, 0, 0))))
	require.True(t, NextScheduleTime(nil, nil, utc(2026, 11, 12, 2, 0, 0)).IsZero())
}

func periodsOfDue(due []DueEntry) []api.BackupPeriod {
	var periods []api.BackupPeriod
	for _, d := range due {
		periods = append(periods, d.Entry.Period)
	}
	return periods
}
