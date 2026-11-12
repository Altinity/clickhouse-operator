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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// The entries of a schedule: at least one, one per period, a valid time required where the period needs one and shared
// by every entry that sets one, a day exactly where the period needs one and in its range, keep at least 1, at most one
// incremental entry.
func TestBackupScheduleEntriesValidate(t *testing.T) {
	hour := BackupScheduleEntry{Period: BackupPeriodHour, Keep: 24}
	daily := BackupScheduleEntry{Period: BackupPeriodDaily, Time: "02:30", Keep: 7}
	weekly := BackupScheduleEntry{Period: BackupPeriodWeekly, Time: "02:30", Day: ptr.To(int32(0)), Keep: 4}
	monthly := BackupScheduleEntry{Period: BackupPeriodMonthly, Time: "02:30", Day: ptr.To(int32(31)), Keep: 12}
	with := func(entry BackupScheduleEntry, edit func(*BackupScheduleEntry)) BackupScheduleEntry {
		edit(&entry)
		return entry
	}
	for name, tc := range map[string]struct {
		entries []BackupScheduleEntry
		valid   bool
	}{
		"none":                          {nil, false},
		"minute alone":                  {[]BackupScheduleEntry{{Period: BackupPeriodMinute, Keep: 60}}, true},
		"minute with the others":        {[]BackupScheduleEntry{{Period: BackupPeriodMinute, Keep: 60}, hour, daily, weekly, monthly}, true},
		"minute twice":                  {[]BackupScheduleEntry{{Period: BackupPeriodMinute, Keep: 1}, {Period: BackupPeriodMinute, Keep: 2}}, false},
		"minute with a day":             {[]BackupScheduleEntry{{Period: BackupPeriodMinute, Keep: 1, Day: ptr.To(int32(1))}}, false},
		"weekly and monthly and minute": {[]BackupScheduleEntry{weekly, monthly, {Period: BackupPeriodMinute, Keep: 1}}, true},
		"weekly and monthly alone":      {[]BackupScheduleEntry{weekly, monthly}, false},
		"weekly and monthly and daily":  {[]BackupScheduleEntry{weekly, monthly, daily}, true},
		"weekly and monthly and hour":   {[]BackupScheduleEntry{weekly, monthly, hour}, true},
		"monthly alone":                 {[]BackupScheduleEntry{monthly}, true},
		"one of each":                   {[]BackupScheduleEntry{hour, daily, weekly, monthly, {Period: BackupPeriod6Hours, Keep: 4}}, true},
		"hour alone, no time":           {[]BackupScheduleEntry{hour}, true},
		"hour with a time":              {[]BackupScheduleEntry{with(hour, func(e *BackupScheduleEntry) { e.Time = "00:15" })}, true},
		"6hours with a time":            {[]BackupScheduleEntry{{Period: BackupPeriod6Hours, Time: "13:45", Keep: 1}}, true},
		"an unknown period":             {[]BackupScheduleEntry{{Period: "yearly", Time: "00:00", Keep: 1}}, false},
		"a period twice":                {[]BackupScheduleEntry{daily, daily}, false},
		"daily without a time":          {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Time = "" })}, false},
		"weekly without a time":         {[]BackupScheduleEntry{with(weekly, func(e *BackupScheduleEntry) { e.Time = "" })}, false},
		"monthly without a time":        {[]BackupScheduleEntry{with(monthly, func(e *BackupScheduleEntry) { e.Time = "" })}, false},
		"a time of one digit":           {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Time = "2:30" })}, false},
		"a time past midnight":          {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Time = "24:00" })}, false},
		"a time with seconds":           {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Time = "02:30:00" })}, false},
		"a minute past the hour":        {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Time = "02:60" })}, false},
		"23:59":                         {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Time = "23:59" })}, true},
		"times that differ":             {[]BackupScheduleEntry{daily, with(weekly, func(e *BackupScheduleEntry) { e.Time = "03:00" })}, false},
		"hour with another time":        {[]BackupScheduleEntry{with(hour, func(e *BackupScheduleEntry) { e.Time = "00:30" }), daily}, false},
		"hour without a time":           {[]BackupScheduleEntry{hour, daily}, true},
		"weekly without a day":          {[]BackupScheduleEntry{with(weekly, func(e *BackupScheduleEntry) { e.Day = nil })}, false},
		"weekly on Saturday":            {[]BackupScheduleEntry{with(weekly, func(e *BackupScheduleEntry) { e.Day = ptr.To(int32(6)) })}, true},
		"weekly on day 7":               {[]BackupScheduleEntry{with(weekly, func(e *BackupScheduleEntry) { e.Day = ptr.To(int32(7)) })}, false},
		"weekly on day -1":              {[]BackupScheduleEntry{with(weekly, func(e *BackupScheduleEntry) { e.Day = ptr.To(int32(-1)) })}, false},
		"monthly without a day":         {[]BackupScheduleEntry{with(monthly, func(e *BackupScheduleEntry) { e.Day = nil })}, false},
		"monthly on day 0":              {[]BackupScheduleEntry{with(monthly, func(e *BackupScheduleEntry) { e.Day = ptr.To(int32(0)) })}, false},
		"monthly on day 1":              {[]BackupScheduleEntry{with(monthly, func(e *BackupScheduleEntry) { e.Day = ptr.To(int32(1)) })}, true},
		"monthly on day 32":             {[]BackupScheduleEntry{with(monthly, func(e *BackupScheduleEntry) { e.Day = ptr.To(int32(32)) })}, false},
		"daily with a day":              {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Day = ptr.To(int32(1)) })}, false},
		"hour with a day":               {[]BackupScheduleEntry{with(hour, func(e *BackupScheduleEntry) { e.Day = ptr.To(int32(1)) })}, false},
		"keep 0":                        {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Keep = 0 })}, false},
		"keep -1":                       {[]BackupScheduleEntry{with(daily, func(e *BackupScheduleEntry) { e.Keep = -1 })}, false},
		"one incremental":               {[]BackupScheduleEntry{with(hour, func(e *BackupScheduleEntry) { e.Incremental = types.NewStringBool(true) }), daily}, true},
		"an incremental entry set off":  {[]BackupScheduleEntry{with(hour, func(e *BackupScheduleEntry) { e.Incremental = types.NewStringBool(false) }), with(daily, func(e *BackupScheduleEntry) { e.Incremental = types.NewStringBool(true) })}, true},
		"two incremental": {[]BackupScheduleEntry{
			with(hour, func(e *BackupScheduleEntry) { e.Incremental = types.NewStringBool(true) }),
			with(daily, func(e *BackupScheduleEntry) { e.Incremental = types.NewStringBool(true) }),
		}, false},
		"incremental that is no boolean": {[]BackupScheduleEntry{with(hour, func(e *BackupScheduleEntry) { e.Incremental = ptr.To(types.StringBool("maybe")) })}, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: tc.entries}).Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrInvalidBackupSpec)
			}
		})
	}
}

func TestBackupScheduleSpecHelpers(t *testing.T) {
	spec := &ClickHouseBackupScheduleSpec{}
	location, err := spec.GetLocation()
	require.NoError(t, err)
	require.Equal(t, time.UTC, location)
	require.Nil(t, spec.GetIncrementalEntry())
	require.Equal(t, "", spec.SharedTime())

	spec.TimeZone = "Europe/Berlin"
	spec.Schedules = []BackupScheduleEntry{
		{Period: BackupPeriodHour, Keep: 24},
		{Period: BackupPeriodDaily, Time: "02:30", Keep: 7, Incremental: types.NewStringBool(true)},
	}
	location, err = spec.GetLocation()
	require.NoError(t, err)
	require.Equal(t, "Europe/Berlin", location.String())
	require.Equal(t, BackupPeriodDaily, spec.GetIncrementalEntry().Period)
	require.Equal(t, int32(24), spec.GetEntry(BackupPeriodHour).Keep)
	require.Nil(t, spec.GetEntry(BackupPeriodWeekly))
	require.Equal(t, "02:30", spec.SharedTime())

	var nilSpec *ClickHouseBackupScheduleSpec
	require.Nil(t, nilSpec.GetEntry(BackupPeriodHour))
	require.Equal(t, "", nilSpec.SharedTime())

	status := &ClickHouseBackupScheduleStatus{Entries: []ScheduleEntryStatus{{Period: BackupPeriodDaily, LastBackupName: "b"}}}
	require.Equal(t, "b", status.GetEntry(BackupPeriodDaily).LastBackupName)
	require.Nil(t, status.GetEntry(BackupPeriodHour))
}

// An hour or 6hours entry that sets no time takes the time the others share, so every slot of a coarser entry is a
// slot of the finest one; with no time set anywhere, 00:00. The entries given are left as they are.
func TestResolveScheduleTimes(t *testing.T) {
	entries := []BackupScheduleEntry{
		{Period: BackupPeriodHour, Keep: 24},
		{Period: BackupPeriod6Hours, Keep: 4},
		{Period: BackupPeriodWeekly, Time: "13:45", Day: ptr.To(int32(0)), Keep: 4},
	}
	resolved := ResolveScheduleTimes(entries)
	require.Equal(t, []string{"13:45", "13:45", "13:45"}, []string{resolved[0].Time, resolved[1].Time, resolved[2].Time})
	require.Equal(t, "", entries[0].Time, "the entries given are left alone")
	*resolved[2].Day = 3
	require.Equal(t, int32(0), *entries[2].Day, "the copy owns its fields")

	resolved = ResolveScheduleTimes([]BackupScheduleEntry{{Period: BackupPeriodHour, Keep: 1}})
	require.Equal(t, "00:00", resolved[0].Time)
	require.Empty(t, ResolveScheduleTimes(nil))
}

func TestOperationPhaseAbortedIsTerminal(t *testing.T) {
	require.True(t, OperationPhaseAborted.IsTerminal())
	require.True(t, OperationPhaseFailed.IsTerminal())
	require.True(t, OperationPhaseCompleted.IsTerminal())
	require.False(t, OperationPhaseRunning.IsTerminal())
	require.False(t, OperationPhasePending.IsTerminal())
}
