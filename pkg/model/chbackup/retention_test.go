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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// entriesOf parses entries written "period=daily time=02:00 day=0 keep=3 incremental=true", separated by ";".
func entriesOf(t *testing.T, text string) []api.BackupScheduleEntry {
	t.Helper()
	var entries []api.BackupScheduleEntry
	for _, item := range strings.Split(text, ";") {
		if strings.TrimSpace(item) == "" {
			continue
		}
		var entry api.BackupScheduleEntry
		for _, field := range strings.Fields(item) {
			key, value, _ := strings.Cut(field, "=")
			switch key {
			case "period":
				entry.Period = api.BackupPeriod(value)
			case "time":
				entry.Time = value
			case "day":
				day, err := strconv.Atoi(value)
				require.NoError(t, err)
				entry.Day = ptrInt32(int32(day))
			case "keep":
				keep, err := strconv.Atoi(value)
				require.NoError(t, err)
				entry.Keep = int32(keep)
			case "incremental":
				entry.Incremental = types.NewStringBool(value == "true")
			}
		}
		require.NoError(t, entry.Validate(), item)
		entries = append(entries, entry)
	}
	return entries
}

func ptrInt32(v int32) *int32 {
	return &v
}

func TestSlotMatches(t *testing.T) {
	for _, tc := range []struct {
		entry string
		tag   string
		match bool
	}{
		{"period=minute keep=1", "20260101031759", true},
		{"period=hour time=07:15 keep=1", "20260101031500", true},
		{"period=hour time=07:15 keep=1", "20260101031600", false},
		{"period=hour keep=1", "20260101090059", true},
		{"period=6hours time=08:30 keep=1", "20260101023000", true},
		{"period=6hours time=08:30 keep=1", "20260101203000", true},
		{"period=6hours time=08:30 keep=1", "20260101033000", false},
		{"period=6hours time=08:30 keep=1", "20260101080000", false},
		{"period=daily time=23:45 keep=1", "20260505234500", true},
		{"period=daily time=23:45 keep=1", "20260505224500", false},
		// 2026-11-15 is a Sunday
		{"period=weekly day=0 time=01:00 keep=1", "20261115010000", true},
		{"period=weekly day=0 time=01:00 keep=1", "20261116010000", false},
		{"period=weekly day=6 time=01:00 keep=1", "20261114010000", true},
		{"period=weekly day=4 time=00:00 keep=1", "19700101000000", true},
		{"period=weekly day=1 time=00:00 keep=1", "20000228000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20260131000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20260228000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20280229000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20280228000000", false},
		{"period=monthly day=31 time=00:00 keep=1", "20260430000000", true},
		{"period=monthly day=31 time=00:00 keep=1", "20260429000000", false},
		{"period=monthly day=29 time=00:00 keep=1", "21000228000000", true},
		{"period=monthly day=15 time=00:00 keep=1", "20260215000000", true},
		{"period=monthly day=15 time=00:00 keep=1", "20260228000000", false},
	} {
		entry := SlotEntries(entriesOf(t, tc.entry), "")[0]
		require.Equal(t, tc.match, SlotMatches(entry, tc.tag, 0), "%s at %s", tc.entry, tc.tag)
	}
}

// With a naming template a backup belongs to the slot of a schedule entry up to 30 minutes after the slot, as the
// Altinity usual backup workflow names a scheduled backup after the time its run started, its seconds included.
func TestSlotTolerance(t *testing.T) {
	daily := SlotEntries(entriesOf(t, "period=daily time=02:00 keep=1"), "")[0]
	for tag, match := range map[string]bool{
		"20261112020000": true, "20261112022959": true, "20261112023000": false, "20261112015959": false,
		// Past midnight, of a slot at 23:50
	} {
		require.Equal(t, match, SlotMatches(daily, tag, SlotTolerance), tag)
	}
	late := SlotEntries(entriesOf(t, "period=daily time=23:50 keep=1"), "")[0]
	require.True(t, SlotMatches(late, "20261113001959", SlotTolerance), "a run past midnight counts for the slot of the day before")
	require.False(t, SlotMatches(late, "20261113002000", SlotTolerance))
}

// The heaviest entry of a slot wins: minute < hour < 6hours < daily < weekly < monthly; the operator agrees.
func TestWinnerEntry(t *testing.T) {
	entries := entriesOf(t, "period=minute keep=60;period=hour time=00:30 keep=24;period=6hours time=00:30 keep=4;"+
		"period=daily time=06:30 keep=7;period=weekly day=0 time=06:30 keep=4;period=monthly day=15 time=06:30 keep=12")
	for tag, want := range map[string]string{
		"20261112071700": "minute 0", "20261112073000": "hour 0 1", "20261112003000": "6hours 0 1 2",
		"20261112063000": "daily 0 1 2 3", "20261122063000": "weekly 0 1 2 3 4", "20261215063000": "monthly 0 1 2 3 5",
	} {
		winner, matched := WinnerEntry(SlotEntries(entries, ""), tag, 0)
		got := string(entries[winner].Period)
		for _, i := range matched {
			got += " " + strconv.Itoa(i)
		}
		require.Equal(t, want, got, tag)
		at, err := time.Parse(tagLayout, tag)
		require.NoError(t, err)
		require.Equal(t, entries[winner].Period, Winner(MatchingEntries(entries, at)).Period, tag)
	}
}

// retentionOf runs the retention of the default naming over keys of the prefix d, every one of them complete and of a
// size but those named.
func retentionOf(t *testing.T, schedules, made string, names, incomplete, empty, sizeless, broken []string,
	previous []api.RetiredScheduleRules, keepBackups []string) ([]string, []string, map[string]bool) {
	t.Helper()
	sets := NewBackupSets()
	for _, name := range names {
		sets.All[name], sets.Complete[name], sets.Sized[name] = true, true, true
	}
	for _, name := range incomplete {
		delete(sets.Complete, name)
	}
	for _, name := range empty {
		sets.Empty[name] = true
	}
	for _, name := range sizeless {
		delete(sets.Sized, name)
	}
	for _, name := range broken {
		sets.Broken[name] = true
	}
	var pins []api.BackupRetentionPin
	for _, name := range keepBackups {
		pins = append(pins, api.BackupRetentionPin{Name: name})
	}
	retention := Retention{Prefix: "d", Entries: SlotEntries(entriesOf(t, schedules), ""), Made: made,
		Pinned: PinnedKeys(Naming{}, "d", pins), Previous: previous}
	plan := retention.Plan(names, sets)
	return plan.Obsolete, heldNames(plan), plan.Keep
}

// heldNames returns the backups a plan holds.
func heldNames(plan RetentionPlan) []string {
	var names []string
	for _, held := range plan.Held {
		names = append(names, held.Key)
	}
	return names
}

func TestRetentionPlan(t *testing.T) {
	all := func(names ...string) []string { return names }
	for name, tc := range map[string]struct {
		schedules                           string
		names                               []string
		incomplete, empty, sizeless, broken []string
		made                                string
		keep, obsolete                      []string
	}{
		"the newest of a daily entry": {
			schedules: "period=daily time=00:00 keep=2",
			names:     all("d-20200101000000", "d-20200104000000", "d-20200102000000", "d-20200103000000"),
			made:      "d-20200104000000",
			keep:      all("d-20200103000000", "d-20200104000000"),
			obsolete:  all("d-20200102000000", "d-20200101000000"),
		},
		"a backup kept by several entries": {
			schedules: "period=weekly day=0 time=00:00 keep=2;period=daily time=00:00 keep=2",
			names:     all("d-20200105000000", "d-20200111000000", "d-20200112000000", "d-20200113000000", "d-20200110000000"),
			keep:      all("d-20200105000000", "d-20200112000000", "d-20200113000000"),
			obsolete:  all("d-20200111000000", "d-20200110000000"),
		},
		"backups made at once from the schedule": {
			schedules: "period=daily time=00:00 keep=2",
			names:     all("d-20200101000000", "d-20200102000000", "d-20200103000000", "d-20200101T120000", "d-20200102T120000", "d-20200103T120000"),
			keep:      all("d-20200102000000", "d-20200102T120000", "d-20200103000000", "d-20200103T120000"),
			obsolete:  all("d-20200101T120000", "d-20200101000000"),
		},
		"backups made at once without a scheduled one": {
			schedules: "period=daily time=00:00 keep=1",
			names:     all("d-20200101T120000", "d-20200102T120000"),
			keep:      all("d-20200101T120000", "d-20200102T120000"),
		},
		"backups of no slot": {
			schedules: "period=daily time=00:00 keep=5",
			names:     all("d-20200101000000", "d-20200101060000", "d-20200102000000"),
			keep:      all("d-20200101000000", "d-20200102000000"),
			obsolete:  all("d-20200101060000"),
		},
		"incomplete, empty and invalid backups": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200103000000", "d-20200104000000", "d-20201399000000"),
			incomplete: all("d-20200104000000"),
			empty:      all("d-20200101000000"),
			sizeless:   all("d-20200101000000"),
			keep:       all("d-20200103000000"),
			obsolete:   all("d-20200102000000"),
		},
		"incomplete backups older than the newest complete one": {
			schedules:  "period=daily time=00:00 keep=2",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200103000000", "d-20200104000000", "d-20200102T120000"),
			incomplete: all("d-20200102000000", "d-20200102T120000"),
			keep:       all("d-20200103000000", "d-20200104000000"),
			obsolete:   all("d-20200102T120000", "d-20200102000000", "d-20200101000000"),
		},
		"an incomplete backup of size 0": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000"),
			incomplete: all("d-20200101000000"),
			sizeless:   all("d-20200101000000"),
			keep:       all("d-20200102000000"),
		},
		"a broken backup of size 0": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200103000000"),
			incomplete: all("d-20200101000000", "d-20200103000000"),
			sizeless:   all("d-20200101000000", "d-20200103000000"),
			broken:     all("d-20200101000000", "d-20200103000000"),
			keep:       all("d-20200102000000"),
			obsolete:   all("d-20200101000000"),
		},
		"an incomplete backup newer than every complete one": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000", "d-20200102T120000"),
			incomplete: all("d-20200102000000", "d-20200102T120000"),
			keep:       all("d-20200101000000"),
		},
		"the run just made, incomplete": {
			schedules:  "period=daily time=00:00 keep=1",
			names:      all("d-20200101000000", "d-20200102000000", "d-29990101000000"),
			incomplete: all("d-20200101000000"),
			made:       "d-20200101000000",
			keep:       all("d-29990101000000"),
			obsolete:   all("d-20200102000000"),
		},
		"the run just made, past newer ones": {
			schedules: "period=daily time=00:00 keep=1",
			names:     all("d-20200101000000", "d-20200102000000", "d-29990101000000"),
			made:      "d-20200101000000",
			keep:      all("d-20200101000000", "d-29990101000000"),
			obsolete:  all("d-20200102000000"),
		},
		"no backups": {schedules: "period=daily time=00:00 keep=1"},
	} {
		t.Run(name, func(t *testing.T) {
			obsolete, held, keep := retentionOf(t, tc.schedules, tc.made, tc.names, tc.incomplete, tc.empty, tc.sizeless, tc.broken, nil, nil)
			require.Empty(t, held)
			require.Equal(t, tc.obsolete, obsolete)
			require.ElementsMatch(t, tc.keep, SortedKeys(keep))
		})
	}
}

// The day by day simulations of a backup at midnight every day and one made at once at noon every 10 days, retention
// after every scheduled backup, leave every entry's backups, and the backups made at once in its window.
func TestRetentionSimulation(t *testing.T) {
	for name, tc := range map[string]struct {
		schedules string
		days      int
		left      []string
	}{
		"4 weekly + 7 daily incremental, after 90 days": {
			schedules: "period=weekly day=0 time=00:00 keep=4;period=daily time=00:00 keep=7 incremental=true",
			days:      90,
			left: []string{
				"20260308000000", "20260315000000", "20260317T120000", "20260322000000", "20260325000000", "20260326000000",
				"20260327000000", "20260327T120000", "20260328000000", "20260329000000", "20260330000000", "20260331000000",
			},
		},
		"1 monthly + 2 weekly + 3 daily incremental, after 120 days": {
			schedules: "period=monthly day=1 time=00:00 keep=1;period=weekly day=0 time=00:00 keep=2;period=daily time=00:00 keep=3 incremental=true",
			days:      120,
			left: []string{
				"20260401000000", "20260406T120000", "20260416T120000", "20260419000000", "20260426000000", "20260426T120000",
				"20260428000000", "20260429000000", "20260430000000",
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			held := map[string]bool{}
			start := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
			for day := 0; day < tc.days; day++ {
				date := start.AddDate(0, 0, day).Format("20060102")
				made := "d-" + date + "000000"
				held[made] = true
				obsolete, _, _ := retentionOf(t, tc.schedules, made, SortedKeys(held), nil, nil, nil, nil, nil, nil)
				for _, name := range obsolete {
					delete(held, name)
				}
				if day%10 == 5 {
					held["d-"+date+"T120000"] = true
				}
			}
			var left []string
			for _, name := range SortedKeys(held) {
				left = append(left, strings.TrimPrefix(name, "d-"))
			}
			require.Equal(t, tc.left, left)
		})
	}
}

// changeAt returns a change of the schedule at the time, RFC 3339, from entries in zone
func changeAt(t *testing.T, at, zone, entries string) api.RetiredScheduleRules {
	if entries == "*" {
		return api.RetiredScheduleRules{ChangeID: at, TimeZone: zone, KeepAll: true}
	}
	return api.RetiredScheduleRules{ChangeID: at, TimeZone: zone, Schedules: entriesOf(t, entries)}
}

// dailyBackups returns the names of the backups of prefix d made every day at hhmmss from the first day of November
// 2026, days of them
func dailyBackups(days int, hhmmss string) []string {
	var names []string
	for day := 1; day <= days; day++ {
		names = append(names, fmt.Sprintf("d-202611%02d%s", day, hhmmss))
	}
	return names
}

// A change of the schedule never deletes the backups the schedule kept before it while the change is held: retention
// holds every backup the entries and the time zone before the change kept of those made before it, and tells those the
// schedule as it is would delete. A backup pinned is kept whatever the change.
func TestRetentionHoldsWhatAChangeKept(t *testing.T) {
	for name, tc := range map[string]struct {
		before, zone, after string
		names               []string
		held                []string
	}{
		"the time shifted": {
			before: "period=daily time=02:00 keep=3", after: "period=daily time=03:00 keep=3",
			names: append(dailyBackups(12, "020000"), "d-20261113030000"),
			held:  []string{"d-20261112020000", "d-20261111020000", "d-20261110020000"},
		},
		"an entry removed": {
			before: "period=weekly day=0 time=02:00 keep=2;period=daily time=02:00 keep=2", after: "period=daily time=02:00 keep=2",
			names: append(dailyBackups(12, "020000"), "d-20261113020000"),
			held:  []string{"d-20261111020000", "d-20261108020000", "d-20261101020000"},
		},
		"keep lowered": {
			before: "period=daily time=02:00 keep=5", after: "period=daily time=02:00 keep=2",
			names: append(dailyBackups(12, "020000"), "d-20261113020000"),
			held:  []string{"d-20261111020000", "d-20261110020000", "d-20261109020000", "d-20261108020000"},
		},
		"the period changed": {
			before: "period=daily time=02:00 keep=3", after: "period=weekly day=0 time=02:00 keep=3",
			names: append(dailyBackups(12, "020000"), "d-20261113020000"),
			held:  []string{"d-20261112020000", "d-20261111020000", "d-20261110020000"},
		},
		"the zone changed back, from a zone of its own": {
			before: "period=daily time=03:00 keep=3", zone: "Europe/Berlin", after: "period=daily time=04:00 keep=3",
			names: append(dailyBackups(12, "030000"), "d-20261113040000"),
			held:  []string{"d-20261112030000", "d-20261111030000", "d-20261110030000"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			obsolete, held, _ := retentionOf(t, tc.after, "", tc.names, nil, nil, nil, nil, nil, nil)
			require.Empty(t, held)
			for _, name := range tc.held {
				require.Contains(t, obsolete, name, "the schedule as it is deletes it")
			}
			previous := []api.RetiredScheduleRules{changeAt(t, "2026-11-12T12:00:00Z", tc.zone, tc.before)}
			obsolete, held, _ = retentionOf(t, tc.after, "", tc.names, nil, nil, nil, nil, previous, nil)
			require.Equal(t, tc.held, held)
			for _, name := range tc.held {
				require.NotContains(t, obsolete, name, "held")
			}
			_, held, _ = retentionOf(t, tc.after, "", tc.names, nil, nil, nil, nil, previous, []string{tc.held[0]})
			require.NotContains(t, held, tc.held[0], "a pinned backup is kept, not held")
		})
	}
}

// A schedule changed again holds the backups of each change, a change that holds all of them every backup made before
// it, and a backup made after a change is the schedule's as it is.
func TestRetentionHoldsEveryChange(t *testing.T) {
	names := append(dailyBackups(12, "020000"), "d-20261113030000", "d-20261114040000", "d-20261115040000")
	previous := []api.RetiredScheduleRules{
		changeAt(t, "2026-11-12T12:00:00Z", "", "period=daily time=02:00 keep=2"),
		changeAt(t, "2026-11-13T12:00:00Z", "", "period=daily time=03:00 keep=2"),
	}
	obsolete, held, _ := retentionOf(t, "period=daily time=04:00 keep=1", "", names, nil, nil, nil, nil, previous, nil)
	require.Equal(t, []string{"d-20261113030000", "d-20261112020000", "d-20261111020000"}, held)
	require.Equal(t, []string{"d-20261114040000", "d-20261110020000", "d-20261109020000", "d-20261108020000", "d-20261107020000",
		"d-20261106020000", "d-20261105020000", "d-20261104020000", "d-20261103020000", "d-20261102020000", "d-20261101020000"}, obsolete)

	previous = []api.RetiredScheduleRules{changeAt(t, "2026-11-13T12:00:00Z", "", "*")}
	obsolete, held, _ = retentionOf(t, "period=daily time=04:00 keep=1", "", names, nil, nil, nil, nil, previous, nil)
	require.Len(t, held, 13)
	require.Equal(t, []string{"d-20261114040000"}, obsolete)
}

// Backups named by the template of the Altinity usual backup workflow, after the time their run started, count for the
// slots of the entries with its tolerance; one pinned by the name of the backup of any shard stays.
func TestRetentionOfTemplateNamedBackups(t *testing.T) {
	naming := Naming{Template: api.BackupNamingShardTime, Cluster: "main", Shards: []string{"0", "1"}, ShardOf: map[string]int{"h0": 0, "h1": 1}}
	keys := []string{"20261108020412", "20261109021733", "20261110020059", "20261111020502", "20261111143000", "20261112020911"}
	sets := NewBackupSets()
	for _, key := range keys {
		sets.All[key], sets.Complete[key], sets.Sized[key] = true, true, true
	}
	retention := Retention{Naming: naming, Prefix: "nightly", Entries: SlotEntries(entriesOf(t, "period=daily time=02:00 keep=3"), ""),
		Pinned: PinnedKeys(naming, "nightly", []api.BackupRetentionPin{{Name: "shard2-20261111143000"}})}
	require.Equal(t, []string{"20261109021733", "20261108020412"}, retention.Plan(keys, sets).Obsolete)
}

// An incremental backup is made from the newest backup of the series every host holds, until the chain of incremental
// backups that ends with it is as long as the entry keeps; a backup made at once is a base as a scheduled one.
func TestIncrementalBase(t *testing.T) {
	const (
		d1 = "d-20260101000000"
		d2 = "d-20260102000000"
		d3 = "d-20260103000000"
		r1 = "d-20260101T120000"
	)
	type series map[string]map[string]string
	for name, tc := range map[string]struct {
		series series
		keep   int
		base   string
		chain  int
	}{
		"no backup":           {series: series{}, keep: 3},
		"after a full backup": {series: series{"h0": {d1: ""}, "h1": {d1: ""}}, keep: 3, base: d1},
		"a chain shorter than keep": {series: series{"h0": {d1: "", d2: d1}, "h1": {d1: "", d2: d1}}, keep: 2, base: d2,
			chain: 1},
		"a chain of keep": {series: series{"h0": {d1: "", d2: d1, d3: d2}, "h1": {d1: "", d2: d1, d3: d2}}, keep: 2, chain: 2},
		"a newest backup one lacks": {series: series{"h0": {d1: "", d2: d1, d3: d2}, "h1": {d1: "", d2: d1}}, keep: 2,
			base: d2, chain: 1},
		"the longest chain of hosts": {series: series{"h0": {d1: "", d2: ""}, "h1": {d1: "", d2: d1}}, keep: 1, chain: 1},
		"a host without the series":  {series: series{"h0": {d1: ""}}, keep: 1},
		"a backup made at once, newest": {series: series{"h0": {d1: "", r1: d1}, "h1": {d1: "", r1: d1}}, keep: 3,
			base: r1, chain: 1},
		"a backup made at once, older than a scheduled one": {series: series{"h0": {d1: "", r1: d1, d2: r1},
			"h1": {d1: "", r1: d1, d2: r1}}, keep: 3, base: d2, chain: 2},
	} {
		t.Run(name, func(t *testing.T) {
			base, chain := IncrementalBase(Naming{}, "d", tc.series, []string{"h0", "h1"}, tc.keep)
			require.Equal(t, tc.base, base)
			require.Equal(t, tc.chain, chain)
		})
	}
}

// A name a template makes tells its shard, its prefix and its time, a T before the hour for one made at once; a name
// of another cluster, shard or time of no time is none of them.
func TestNaming(t *testing.T) {
	naming := Naming{Template: "{cluster}-{prefix}-shard{shardNum}-{YYYYMMDDHHIISS}", Cluster: "main",
		Shards: []string{"0", "1"}, ShardOf: map[string]int{"h0": 0, "h1": 1}}
	require.True(t, naming.HoldsPrefix())
	require.Equal(t, SlotTolerance, naming.Tolerance())
	name, err := naming.HostName("h1", "20261112020000", "nightly")
	require.NoError(t, err)
	require.Equal(t, "main-nightly-shard2-20261112020000", name)
	name, err = naming.HostName("h0", "20261112T020000", "nightly")
	require.NoError(t, err)
	require.Equal(t, "main-nightly-shard1-20261112T020000", name)
	_, err = naming.HostName("h9", "20261112020000", "nightly")
	require.Error(t, err)

	parsed, ok := naming.Parse("main-nightly-shard2-20261112T020000", "")
	require.True(t, ok)
	require.Equal(t, api.ParsedBackupName{ShardNum: 2, Prefix: "nightly", Tag: "20261112T020000"}, parsed)
	for _, other := range []string{"cold-nightly-shard2-20261112020000", "main-nightly-shard2-20261332020000", "main-nightly-2-20261112020000"} {
		_, ok := naming.Parse(other, "")
		require.False(t, ok, other)
	}
	key, prefix, err := naming.Key("backupName", "main-nightly-shard1-20261112020000", "")
	require.NoError(t, err)
	require.Equal(t, "20261112020000", key)
	require.Equal(t, "nightly", prefix)
	_, _, err = naming.Key("backupName", "20261112020000", "")
	require.ErrorContains(t, err, "holds {prefix}")
	key, _, err = naming.Key("backupName", "20261112020000", "nightly")
	require.NoError(t, err)
	require.Equal(t, "20261112020000", key)

	key, ok = naming.SeriesKey("h1", "main-nightly-shard2-20261112020000", "nightly")
	require.True(t, ok)
	require.Equal(t, "20261112020000", key)
	_, ok = naming.SeriesKey("h0", "main-nightly-shard2-20261112020000", "nightly")
	require.False(t, ok, "the backup of another shard")
	_, ok = naming.SeriesKey("h1", "main-weekly-shard2-20261112020000", "nightly")
	require.False(t, ok, "the backup of another series")

	plain := Naming{}
	key, ok = plain.SeriesKey("h0", "nightly-20261112T020000", "nightly")
	require.True(t, ok)
	require.Equal(t, "nightly-20261112T020000", key)
	_, ok = plain.SeriesKey("h0", "nightly-manual", "nightly")
	require.False(t, ok)
	tag, atOnce := plain.KeyTime("nightly-20261112T020000", "nightly")
	require.Equal(t, "20261112020000", tag)
	require.True(t, atOnce)
}

// {shard} names the backup of a shard by the value of its {shard} macro, the name of the shard, from 0 by default or as
// the layout names it, and {shardNum} by its number from 1: a name tells the shard by either, and a backup of the series
// of a host is that of its own shard alone.
func TestNamingByShardNameAndNumber(t *testing.T) {
	byIndex := Naming{Template: "{prefix}-s{shard}-{YYYYMMDDHHIISS}", Cluster: "main", Shards: []string{"0", "1"},
		ShardOf: map[string]int{"h0": 0, "h1": 1}}
	name, err := byIndex.HostName("h0", "20261112020000", "nightly")
	require.NoError(t, err)
	require.Equal(t, "nightly-s0-20261112020000", name)
	parsed, ok := byIndex.Parse("nightly-s1-20261112020000", "nightly")
	require.True(t, ok)
	require.Equal(t, api.ParsedBackupName{Shard: "1", Prefix: "nightly", Tag: "20261112020000"}, parsed)
	_, ok = byIndex.Parse("nightly-s2-20261112020000", "nightly")
	require.False(t, ok, "no shard of the cluster")

	custom := Naming{Template: "{cluster}-{shard}-n{shardNum}-{YYYYMMDDHHIISS}", Cluster: "main",
		Shards: []string{"east", "east-2", "west"}, ShardOf: map[string]int{"h0": 0, "h1": 1, "h2": 2}}
	for host, want := range map[string]string{
		"h0": "main-east-n1-20261112020000", "h1": "main-east-2-n2-20261112020000", "h2": "main-west-n3-20261112020000",
	} {
		name, err := custom.HostName(host, "20261112020000", "")
		require.NoError(t, err)
		require.Equal(t, want, name)
		key, ok := custom.SeriesKey(host, want, "")
		require.True(t, ok, want)
		require.Equal(t, "20261112020000", key)
	}
	parsed, ok = custom.Parse("main-east-2-n2-20261112T020000", "")
	require.True(t, ok)
	require.Equal(t, api.ParsedBackupName{Shard: "east-2", ShardNum: 2, Tag: "20261112T020000"}, parsed)
	_, ok = custom.SeriesKey("h0", "main-east-2-n2-20261112020000", "")
	require.False(t, ok, "the backup of another shard")
	_, ok = custom.SeriesKey("h1", "main-east-2-n1-20261112020000", "")
	require.False(t, ok, "the name and the number of different shards")
	key, _, err := custom.Key("backupName", "main-west-n3-20261112020000", "")
	require.NoError(t, err)
	require.Equal(t, "20261112020000", key)
	first, err := custom.FirstName("20261112020000", "")
	require.NoError(t, err)
	require.Equal(t, "main-east-n1-20261112020000", first)
	scheduled, err := ScheduledBackupName("nightly", custom, "20261112020000")
	require.NoError(t, err)
	require.Equal(t, first, scheduled)
}

// planOf runs the retention of the default naming over complete keys of the prefix d, but those incomplete, at the time.
func planOf(t *testing.T, schedules string, names, incomplete []string, previous []api.RetiredScheduleRules,
	pins []api.BackupRetentionPin, now time.Time, writing map[string]bool) RetentionPlan {
	t.Helper()
	sets := NewBackupSets()
	for _, name := range names {
		sets.All[name], sets.Complete[name], sets.Sized[name] = true, true, true
	}
	for _, name := range incomplete {
		delete(sets.Complete, name)
	}
	retention := Retention{Prefix: "d", Entries: SlotEntries(entriesOf(t, schedules), ""), Pinned: PinnedKeys(Naming{}, "d", pins),
		Previous: previous, Now: now, Writing: writing}
	return retention.Plan(names, sets)
}

// A backup held for a change expires as the schedule before the change would have deleted it: its time and keep
// periods of the entry of the longest period that kept it, by the calendar for a monthly one; retention deletes it then
// without a release, and tells it expired.
func TestRetentionHoldExpires(t *testing.T) {
	const after = "period=daily time=02:00 keep=2"
	t.Run("weekly", func(t *testing.T) {
		names := append(dailyBackups(12, "020000"), "d-20261113020000")
		previous := []api.RetiredScheduleRules{changeAt(t, "2026-11-12T12:00:00Z", "",
			"period=weekly day=0 time=02:00 keep=4;period=daily time=02:00 keep=2")}
		plan := planOf(t, after, names, nil, previous, nil, utc(2026, time.November, 12, 13, 0, 0), nil)
		require.Equal(t, []HeldKey{
			{Key: "d-20261111020000", Expires: utc(2026, time.November, 13, 2, 0, 0)},
			{Key: "d-20261108020000", Expires: utc(2026, time.December, 6, 2, 0, 0)},
			{Key: "d-20261101020000", Expires: utc(2026, time.November, 29, 2, 0, 0)},
		}, plan.Held)
		require.Empty(t, plan.Expired)

		plan = planOf(t, after, names, nil, previous, nil, utc(2026, time.November, 20, 0, 0, 0), nil)
		require.Equal(t, []string{"d-20261108020000", "d-20261101020000"}, heldNames(plan))
		require.Contains(t, plan.Obsolete, "d-20261111020000")
		require.Equal(t, []string{"d-20261111020000"}, plan.Expired)

		plan = planOf(t, after, names, nil, previous, nil, utc(2026, time.November, 30, 0, 0, 0), nil)
		require.Equal(t, []string{"d-20261108020000"}, heldNames(plan))
		require.Equal(t, []string{"d-20261111020000", "d-20261101020000"}, plan.Expired)

		// Without a time, nothing expires
		plan = planOf(t, after, names, nil, previous, nil, time.Time{}, nil)
		require.Len(t, plan.Held, 3)
	})
	t.Run("monthly", func(t *testing.T) {
		names := append(append([]string{"d-20260901020000", "d-20261001020000"}, dailyBackups(12, "020000")...), "d-20261113020000")
		previous := []api.RetiredScheduleRules{changeAt(t, "2026-11-12T12:00:00Z", "",
			"period=monthly day=1 time=02:00 keep=2;period=daily time=02:00 keep=2")}
		plan := planOf(t, after, names, nil, previous, nil, utc(2026, time.November, 12, 13, 0, 0), nil)
		require.Equal(t, []HeldKey{
			{Key: "d-20261111020000", Expires: utc(2026, time.November, 13, 2, 0, 0)},
			{Key: "d-20261101020000", Expires: utc(2027, time.January, 1, 2, 0, 0)},
			{Key: "d-20261001020000", Expires: utc(2026, time.December, 1, 2, 0, 0)},
		}, plan.Held)
		require.Contains(t, plan.Obsolete, "d-20260901020000", "the monthly entry kept two")

		plan = planOf(t, after, names, nil, previous, nil, utc(2026, time.December, 2, 0, 0, 0), nil)
		require.Equal(t, []string{"d-20261101020000"}, heldNames(plan))
		require.Equal(t, []string{"d-20261111020000", "d-20261001020000"}, plan.Expired)
	})
	t.Run("several changes, the latest expiry", func(t *testing.T) {
		names := append(dailyBackups(12, "020000"), "d-20261113020000")
		previous := []api.RetiredScheduleRules{
			changeAt(t, "2026-11-12T12:00:00Z", "", "period=daily time=02:00 keep=3"),
			changeAt(t, "2026-11-12T13:00:00Z", "", "period=daily time=02:00 keep=5"),
		}
		plan := planOf(t, "period=daily time=02:00 keep=1", names, nil, previous, nil, utc(2026, time.November, 12, 14, 0, 0), nil)
		require.Equal(t, HeldKey{Key: "d-20261112020000", Expires: utc(2026, time.November, 17, 2, 0, 0)}, plan.Held[0])
	})
	t.Run("every backup, until the change expires", func(t *testing.T) {
		names := append(dailyBackups(12, "020000"), "d-20261113020000")
		change := changeAt(t, "2026-11-12T12:00:00Z", "", "*")
		change.Expires = "2026-11-30T00:00:00Z"
		plan := planOf(t, "period=daily time=02:00 keep=1", names, nil, []api.RetiredScheduleRules{change}, nil,
			utc(2026, time.November, 20, 0, 0, 0), nil)
		require.Len(t, plan.Held, 12)
		require.Equal(t, utc(2026, time.November, 30, 0, 0, 0), plan.Held[0].Expires)
	})
}

// A pin with an until keeps its backup until then; without one, a backup an entry before a change kept until it
// expires as that schedule would have deleted it, and any other backup as long as it is pinned.
func TestRetentionPinsEnd(t *testing.T) {
	const after = "period=daily time=02:00 keep=2"
	names := append(dailyBackups(12, "020000"), "d-20261113020000")
	previous := []api.RetiredScheduleRules{changeAt(t, "2026-11-12T12:00:00Z", "",
		"period=weekly day=0 time=02:00 keep=4;period=daily time=02:00 keep=2")}
	pin := func(name, until string) []api.BackupRetentionPin {
		return []api.BackupRetentionPin{{Name: name, Until: until}}
	}

	plan := planOf(t, after, names, nil, nil, pin("d-20261105020000", "2026-12-31T00:00:00Z"), utc(2026, time.December, 1, 0, 0, 0), nil)
	require.True(t, plan.Keep["d-20261105020000"], "until in the future keeps")
	plan = planOf(t, after, names, nil, nil, pin("d-20261105020000", "2026-11-20T00:00:00Z"), utc(2026, time.December, 1, 0, 0, 0), nil)
	require.Contains(t, plan.Obsolete, "d-20261105020000", "until past releases")
	require.Equal(t, []string{"d-20261105020000"}, plan.Expired)

	plan = planOf(t, after, names, nil, nil, pin("d-20261105020000", ""), utc(2030, time.January, 1, 0, 0, 0), nil)
	require.True(t, plan.Keep["d-20261105020000"], "a pin of a backup no change holds lasts")
	require.Empty(t, plan.Expired)

	// Without until, a backup the weekly entry kept: until 4 weeks after it
	plan = planOf(t, after, names, nil, previous, pin("d-20261101020000", ""), utc(2026, time.November, 28, 0, 0, 0), nil)
	require.True(t, plan.Keep["d-20261101020000"])
	require.NotContains(t, heldNames(plan), "d-20261101020000", "pinned, not held")
	plan = planOf(t, after, names, nil, previous, pin("d-20261101020000", ""), utc(2026, time.November, 30, 0, 0, 0), nil)
	require.Contains(t, plan.Obsolete, "d-20261101020000")
	require.Contains(t, plan.Expired, "d-20261101020000")

	// An until past leaves a backup still held to its hold, a later until outlasts the hold
	plan = planOf(t, after, names, nil, previous, pin("d-20261101020000", "2026-11-15T00:00:00Z"), utc(2026, time.November, 20, 0, 0, 0), nil)
	require.Contains(t, heldNames(plan), "d-20261101020000")
	plan = planOf(t, after, names, nil, previous, pin("d-20261101020000", "2026-12-31T00:00:00Z"), utc(2026, time.December, 20, 0, 0, 0), nil)
	require.True(t, plan.Keep["d-20261101020000"])

	// A backup made at once is kept as long as it is pinned without until, even one a change holds
	atOnce := append(append([]string{}, names...), "d-20261110T120000")
	plan = planOf(t, after, atOnce, nil, previous, pin("d-20261110T120000", ""), utc(2030, time.January, 1, 0, 0, 0), nil)
	require.True(t, plan.Keep["d-20261110T120000"])
}

// An incomplete backup older than the newest complete one is deleted, unless a create_remote or an upload still writes
// it, or a host cannot tell, or it is pinned.
func TestRetentionOfIncompleteBackups(t *testing.T) {
	const schedules = "period=daily time=02:00 keep=1"
	names := []string{"d-20261101020000", "d-20261102020000", "d-20261103020000"}
	incomplete := []string{"d-20261102020000"}
	now := utc(2026, time.November, 4, 0, 0, 0)
	plan := planOf(t, schedules, names, incomplete, nil, nil, now, nil)
	require.Equal(t, []string{"d-20261102020000", "d-20261101020000"}, plan.Obsolete)

	plan = planOf(t, schedules, names, incomplete, nil, nil, now, map[string]bool{"d-20261102020000": true})
	require.Equal(t, []string{"d-20261101020000"}, plan.Obsolete)
	require.Equal(t, []string{"d-20261102020000"}, plan.Writing)

	plan = planOf(t, schedules, names, incomplete, nil, []api.BackupRetentionPin{{Name: "d-20261102020000"}}, now, nil)
	require.Equal(t, []string{"d-20261101020000"}, plan.Obsolete)
	require.Empty(t, plan.Writing)

	plan = planOf(t, schedules, names, incomplete, nil, []api.BackupRetentionPin{{Name: "d-20261102020000", Until: "2026-11-03T00:00:00Z"}}, now, nil)
	require.Equal(t, []string{"d-20261102020000", "d-20261101020000"}, plan.Obsolete)
	require.Equal(t, []string{"d-20261102020000"}, plan.Expired)
}

// The backups made before a shard was added lack the new shard, so none is complete for the new topology: once a
// backup that is complete is newer they go, whatever keep says, unless keepBackups pins them.
func TestRetentionOfBackupsBeforeAShardWasAdded(t *testing.T) {
	const schedules = "period=daily time=02:00 keep=7"
	before := []string{"d-20261101020000", "d-20261102020000", "d-20261103020000"}
	names := append(slices.Clone(before), "d-20261104020000", "d-20261105020000")
	now := utc(2026, time.November, 6, 0, 0, 0)

	// Before the first backup of the new topology is complete, nothing goes: no complete backup is newer
	plan := planOf(t, schedules, before, before, nil, nil, now, nil)
	require.Empty(t, plan.Obsolete)

	plan = planOf(t, schedules, names, before, nil, nil, now, nil)
	require.Equal(t, []string{"d-20261103020000", "d-20261102020000", "d-20261101020000"}, plan.Obsolete)
	require.Equal(t, map[string]bool{"d-20261104020000": true, "d-20261105020000": true}, plan.Keep)

	pins := []api.BackupRetentionPin{{Name: "d-20261101020000"}, {Name: "d-20261102020000", Until: "2027-01-01T00:00:00Z"}}
	plan = planOf(t, schedules, names, before, nil, pins, now, nil)
	require.Equal(t, []string{"d-20261103020000"}, plan.Obsolete)
	require.True(t, plan.Keep["d-20261101020000"])
	require.True(t, plan.Keep["d-20261102020000"])
}

// A backup named after a slot whose time does not exist the day the clocks go forward, 02:30 in Berlin on 2026-03-29,
// whose name tells 03:30, is a backup of the slot to the entries and to retention, which keeps it as it keeps any.
func TestSlotMatchesInTheSpringForwardGap(t *testing.T) {
	entries := entriesOf(t, "period=daily time=02:30 keep=2;period=weekly day=0 time=02:30 keep=1")
	berlin := SlotEntries(entries, "Europe/Berlin")
	for tag, want := range map[string]string{
		"20260329033000": "weekly", "20260330023000": "daily", "20260328023000": "daily",
		"20260329033059": "weekly", "20260329033100": "", "20260329043000": "", "20260330033000": "",
	} {
		winner, _ := WinnerEntry(berlin, tag, 0)
		got := ""
		if winner >= 0 {
			got = string(berlin[winner].Period)
		}
		require.Equal(t, want, got, tag)
	}
	winner, matched := WinnerEntry(berlin, "20260329033000", 0)
	require.Equal(t, api.BackupPeriodWeekly, berlin[winner].Period)
	require.Equal(t, []int{0, 1}, matched, "the slot is a slot of both entries")
	// In UTC the zone has no gap: the slot is no slot of the entries
	winner, _ = WinnerEntry(SlotEntries(entries, ""), "20260329033000", 0)
	require.Equal(t, -1, winner)
	// With the tolerance of a naming template, seconds after the slot as it is
	require.True(t, SlotMatches(berlin[0], "20260329034959", SlotTolerance))
	require.False(t, SlotMatches(berlin[0], "20260329040000", SlotTolerance))

	names := []string{"d-20260328023000", "d-20260329033000", "d-20260330023000", "d-20260331023000"}
	sets := NewBackupSets()
	for _, name := range names {
		sets.All[name], sets.Complete[name], sets.Sized[name] = true, true, true
	}
	retention := Retention{Prefix: "d", Entries: berlin, Made: "d-20260331023000"}
	plan := retention.Plan(names, sets)
	// The daily entry keeps two, the weekly one the Sunday: the backup of the gap is the Sunday's
	require.Equal(t, []string{"d-20260328023000"}, plan.Obsolete)
	require.True(t, plan.Keep["d-20260329033000"], "the backup of the weekly slot of the gap is kept")
}
