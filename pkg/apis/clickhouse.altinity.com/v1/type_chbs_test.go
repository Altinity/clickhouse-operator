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

// The entries of a schedule: at least one, one per period, a day exactly where the period needs one and in its range,
// keep at least 1, at most one incremental entry.
func TestBackupScheduleEntriesValidate(t *testing.T) {
	hour := BackupScheduleEntry{Period: BackupPeriodHour, Keep: 24}
	daily := BackupScheduleEntry{Period: BackupPeriodDaily, Keep: 7}
	weekly := BackupScheduleEntry{Period: BackupPeriodWeekly, Day: ptr.To(int32(0)), Keep: 4}
	monthly := BackupScheduleEntry{Period: BackupPeriodMonthly, Day: ptr.To(int32(31)), Keep: 12}
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
		"weekly and monthly alone":      {[]BackupScheduleEntry{weekly, monthly}, true},
		"weekly and monthly and daily":  {[]BackupScheduleEntry{weekly, monthly, daily}, true},
		"weekly and monthly and hour":   {[]BackupScheduleEntry{weekly, monthly, hour}, true},
		"monthly alone":                 {[]BackupScheduleEntry{monthly}, true},
		"one of each":                   {[]BackupScheduleEntry{hour, daily, weekly, monthly, {Period: BackupPeriod6Hours, Keep: 4}}, true},
		"hour alone, no time":           {[]BackupScheduleEntry{hour}, true},
		"hour with daily":               {[]BackupScheduleEntry{hour, daily}, true},
		"an unknown period":             {[]BackupScheduleEntry{{Period: "yearly", Keep: 1}}, false},
		"a period twice":                {[]BackupScheduleEntry{daily, daily}, false},
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
			err := (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: tc.entries, Time: "02:30"}).Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrInvalidBackupSpec)
			}
		})
	}
}

// The schedule time: one for all entries, required for daily, weekly and monthly, always HH:MM.
func TestBackupScheduleTimeValidate(t *testing.T) {
	hour := BackupScheduleEntry{Period: BackupPeriodHour, Keep: 24}
	daily := BackupScheduleEntry{Period: BackupPeriodDaily, Keep: 7}
	weekly := BackupScheduleEntry{Period: BackupPeriodWeekly, Day: ptr.To(int32(0)), Keep: 4}
	monthly := BackupScheduleEntry{Period: BackupPeriodMonthly, Day: ptr.To(int32(31)), Keep: 12}
	for name, tc := range map[string]struct {
		entries []BackupScheduleEntry
		at      string
		valid   bool
	}{
		"hour alone, no time":     {[]BackupScheduleEntry{hour}, "", true},
		"hour with a time":        {[]BackupScheduleEntry{hour}, "00:15", true},
		"6hours with a time":      {[]BackupScheduleEntry{{Period: BackupPeriod6Hours, Keep: 1}}, "13:45", true},
		"minute with no time":     {[]BackupScheduleEntry{{Period: BackupPeriodMinute, Keep: 1}}, "", true},
		"daily without a time":    {[]BackupScheduleEntry{daily}, "", false},
		"weekly without a time":   {[]BackupScheduleEntry{weekly}, "", false},
		"monthly without a time":  {[]BackupScheduleEntry{monthly}, "", false},
		"hour and daily, no time": {[]BackupScheduleEntry{hour, daily}, "", false},
		"a time of one digit":     {[]BackupScheduleEntry{daily}, "2:30", false},
		"a time past midnight":    {[]BackupScheduleEntry{daily}, "24:00", false},
		"a time with seconds":     {[]BackupScheduleEntry{daily}, "02:30:00", false},
		"a minute past the hour":  {[]BackupScheduleEntry{daily}, "02:60", false},
		"an invalid hour time":    {[]BackupScheduleEntry{hour}, "2:30", false},
		"23:59":                   {[]BackupScheduleEntry{daily, weekly, monthly}, "23:59", true},
	} {
		t.Run(name, func(t *testing.T) {
			err := (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: tc.entries, Time: tc.at}).Validate()
			if tc.valid {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, ErrInvalidBackupSpec)
			}
		})
	}
	hourOf, minute, err := ParseScheduleTime("")
	require.NoError(t, err)
	require.Equal(t, []int{0, 0}, []int{hourOf, minute})
	hourOf, minute, err = ParseScheduleTime("13:45")
	require.NoError(t, err)
	require.Equal(t, []int{13, 45}, []int{hourOf, minute})
}

func TestBackupScheduleSpecHelpers(t *testing.T) {
	spec := &ClickHouseBackupScheduleSpec{}
	location, err := spec.GetLocation()
	require.NoError(t, err)
	require.Equal(t, time.UTC, location)

	spec.TimeZone = "Europe/Berlin"
	spec.Schedules = []BackupScheduleEntry{
		{Period: BackupPeriodHour, Keep: 24},
		{Period: BackupPeriodDaily, Keep: 7, Incremental: types.NewStringBool(true)},
	}
	location, err = spec.GetLocation()
	require.NoError(t, err)
	require.Equal(t, "Europe/Berlin", location.String())

	status := &ClickHouseBackupScheduleStatus{Entries: []ScheduleEntryStatus{{Period: BackupPeriodDaily, LastBackupName: "b"}}}
	require.Equal(t, "b", status.GetEntry(BackupPeriodDaily).LastBackupName)
	require.Nil(t, status.GetEntry(BackupPeriodHour))
}

func TestOperationPhaseAbortedIsTerminal(t *testing.T) {
	require.True(t, OperationPhaseAborted.IsTerminal())
	require.True(t, OperationPhaseFailed.IsTerminal())
	require.True(t, OperationPhaseCompleted.IsTerminal())
	require.False(t, OperationPhaseRunning.IsTerminal())
	require.False(t, OperationPhasePending.IsTerminal())
}

// The retention of a schedule holds a change, or prunes at once, pins backups by name, at most 512, each until a time
// as RFC 3339 or without one, and releases a change by its changeID, a time.
func TestValidateBackupRetention(t *testing.T) {
	many := make([]BackupRetentionPin, BackupRetentionKeepBackupsMaxItems+1)
	for i := range many {
		many[i] = BackupRetentionPin{Name: "b"}
	}
	pins := func(pins ...BackupRetentionPin) *BackupRetention { return &BackupRetention{KeepBackups: pins} }
	for name, tc := range map[string]struct {
		retention *BackupRetention
		valid     bool
	}{
		"none":                 {nil, true},
		"hold":                 {&BackupRetention{OnScheduleChange: RetentionHold}, true},
		"prune":                {&BackupRetention{OnScheduleChange: RetentionPrune}, true},
		"another policy":       {&BackupRetention{OnScheduleChange: "Keep"}, false},
		"pins":                 {pins(BackupRetentionPin{Name: "nightly-20261101020000"}, BackupRetentionPin{Name: "20261101020000"}), true},
		"a pin until a time":   {pins(BackupRetentionPin{Name: "nightly-20261101020000", Until: "2026-12-31T00:00:00+01:00"}), true},
		"a pin until a day":    {pins(BackupRetentionPin{Name: "nightly-20261101020000", Until: "2026-12-31"}), false},
		"a pin until no time":  {pins(BackupRetentionPin{Name: "nightly-20261101020000", Until: "soon"}), false},
		"an empty pin":         {pins(BackupRetentionPin{}), false},
		"a pin of no name":     {pins(BackupRetentionPin{Name: "a b"}), false},
		"too many pins":        {&BackupRetention{KeepBackups: many}, false},
		"a release":            {&BackupRetention{ReleaseHeld: "2026-11-12T12:00:00Z"}, true},
		"a release of no time": {&BackupRetention{ReleaseHeld: "yesterday"}, false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.valid, tc.retention.Validate() == nil)
		})
	}
	require.Equal(t, RetentionHold, (*BackupRetention)(nil).GetOnScheduleChange())
	require.True(t, (&BackupRetentionPin{Name: "b"}).GetUntil().IsZero())
	require.Equal(t, time.Date(2026, time.December, 31, 0, 0, 0, 0, time.UTC),
		(&BackupRetentionPin{Name: "b", Until: "2026-12-31T00:00:00Z"}).GetUntil().UTC())
}

// A period spans its length, a month by the calendar; a change holds its backups until its expires.
func TestBackupRetentionSpans(t *testing.T) {
	at := time.Date(2026, time.January, 31, 2, 0, 0, 0, time.UTC)
	require.Equal(t, at.Add(3*time.Minute), BackupPeriodMinute.Span(at, 3))
	require.Equal(t, at.Add(18*time.Hour), BackupPeriod6Hours.Span(at, 3))
	require.Equal(t, time.Date(2026, time.February, 28, 2, 0, 0, 0, time.UTC), BackupPeriodWeekly.Span(at, 4))
	require.Equal(t, time.Date(2026, time.March, 3, 2, 0, 0, 0, time.UTC), BackupPeriodMonthly.Span(at, 1), "Go normalizes February 31")
	require.Equal(t, time.Date(2026, time.April, 30, 2, 0, 0, 0, time.UTC), BackupPeriodDaily.Span(at, 89))

	change := RetiredScheduleRules{ChangeID: "2026-11-12T12:00:00Z", Schedules: []BackupScheduleEntry{
		{Period: BackupPeriodDaily, Keep: 7}, {Period: BackupPeriodWeekly, Keep: 4},
	}, Expires: "2027-01-01T00:00:00Z"}
	require.Equal(t, time.Date(2027, time.January, 1, 0, 0, 0, 0, time.UTC), change.HeldUntil())
}
