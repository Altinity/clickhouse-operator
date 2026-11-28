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
	"sort"
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

type SlotEntry struct {
	Period api.BackupPeriod
	Hour   int
	Minute int
	// Day is -1 for periods other than weekly and monthly.
	Day             int
	Keep            int
	Incremental     bool
	ConfigOverrides []string
	// Zone is nil for UTC. A scheduled time in a spring-forward gap moves to the Go-normalized time, as the backup name does.
	Zone *time.Location
}

// "" and an unknown zone mean UTC.
func SlotEntries(entries []api.BackupScheduleEntry, scheduleTime, zone string) []SlotEntry {
	location := loadZone(zone)
	var resolved []SlotEntry
	for _, entry := range entries {
		hour, minute, _ := api.ParseScheduleTime(scheduleTime)
		if entry.Period == api.BackupPeriodMinute {
			hour, minute = 0, 0
		}
		resolved = append(resolved, SlotEntry{
			Period: entry.Period, Hour: hour, Minute: minute, Day: entry.GetDay(), Keep: int(entry.Keep),
			Incremental: entry.IsIncremental(), ConfigOverrides: api.ConfigOverrides(entry.ConfigOverrides), Zone: location,
		})
	}
	return resolved
}

// The tag may be up to tolerance seconds after the scheduled time. For a scheduled time in a DST gap, see inGapSlot.
func SlotMatches(entry SlotEntry, tag string, tolerance int) bool {
	if entry.Period == api.BackupPeriodMinute {
		return true
	}
	t, err := time.Parse(api.BackupTagLayout, tag)
	if err != nil {
		return false
	}
	if inGapSlot(entry.Period, entry.Hour, entry.Minute, entry.Day, entry.Zone, t, tolerance) {
		return true
	}
	if tolerance > 0 {
		back := ((t.Minute()-entry.Minute+60)%60)*60 + t.Second()
		if back >= tolerance {
			return false
		}
		t = t.Add(-time.Duration(back) * time.Second)
	}
	if t.Minute() != entry.Minute {
		return false
	}
	switch entry.Period {
	case api.BackupPeriodHour:
		return true
	case api.BackupPeriod6Hours:
		return t.Hour()%6 == entry.Hour%6
	}
	if t.Hour() != entry.Hour {
		return false
	}
	switch entry.Period {
	case api.BackupPeriodDaily:
		return true
	case api.BackupPeriodWeekly:
		return int(t.Weekday()) == entry.Day
	case api.BackupPeriodMonthly:
		last := time.Date(t.Year(), t.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
		return t.Day() == min(entry.Day, last)
	}
	return false
}

func WinnerEntry(entries []SlotEntry, tag string, tolerance int) (int, []int) {
	winner, weight := -1, 0.0
	var matched []int
	for i, entry := range entries {
		if !SlotMatches(entry, tag, tolerance) {
			continue
		}
		matched = append(matched, i)
		if entry.Period.Weight() > weight {
			winner, weight = i, entry.Period.Weight()
		}
	}
	return winner, matched
}

func IncrementalEntry(entries []SlotEntry) int {
	for i, entry := range entries {
		if entry.Incremental {
			return i
		}
	}
	return -1
}

func SlotTag(t time.Time, zone string) string {
	location := time.UTC
	if (zone != "") && (zone != "UTC") {
		var err error
		if location, err = time.LoadLocation(zone); err != nil {
			return ""
		}
	}
	return t.In(location).Format(api.BackupTagLayout)
}

// loadZone returns nil for UTC and for an unknown zone.
func loadZone(zone string) *time.Location {
	if (zone == "") || (zone == "UTC") {
		return nil
	}
	location, err := time.LoadLocation(zone)
	if err != nil {
		return nil
	}
	return location
}

type BackupSets struct {
	// Complete holds backups that each shard that must hold them lists on a replica, not broken.
	Complete map[string]bool
	WithData map[string]bool
	Empty    map[string]bool
	Sized    map[string]bool
	Broken   map[string]bool
	All      map[string]bool
	// Stored describes each backup of All as the hosts list it.
	Stored map[string]*StoredDetail
}

// StoredDetail is what the hosts list of a backup.
type StoredDetail struct {
	// Created is the earliest creation time that a host lists, zero when none tells one.
	Created time.Time
	// Size is the sum of the sizes that the hosts list.
	Size uint64
	// Base is the backup the first host that lists the backup names as required, as listed.
	Base string
	// BaseKey is the key of Base, empty when Base is not a backup of the listing.
	BaseKey string
}

func NewBackupSets() BackupSets {
	return BackupSets{Complete: map[string]bool{}, WithData: map[string]bool{}, Empty: map[string]bool{}, Sized: map[string]bool{},
		Broken: map[string]bool{}, All: map[string]bool{}, Stored: map[string]*StoredDetail{}}
}

type Retention struct {
	Naming  Naming
	Prefix  string
	Entries []SlotEntry
	// Made is always kept.
	Made string
	// Zero pin end means no end.
	Pinned   map[string]time.Time
	Previous []api.RetiredScheduleRules
	// Zero Now expires nothing.
	Now time.Time
	// Writing are incomplete backups that a create_remote or upload may still write. Never delete them.
	Writing map[string]bool
}

// Zero Until means no end.
type HeldKey struct {
	Key     string
	Expires time.Time
}

// All lists are newest first.
type RetentionPlan struct {
	Obsolete []string
	Held     []HeldKey
	Keep     map[string]bool
	Expired  []string
	Writing  []string
}

type keyed struct {
	key, name string
}

func (r Retention) alive(end time.Time) bool {
	return end.IsZero() || r.Now.IsZero() || r.Now.Before(end)
}

// A backup empty on every host is never deleted. An incomplete one is deleted only after a newer complete one exists.
func (r Retention) Plan(names []string, sets BackupSets) RetentionPlan {
	var ordered, incomplete []keyed
	for _, name := range names {
		key, _ := r.Naming.KeyTime(name, r.Prefix)
		if !api.BackupTagValid(key) {
			continue
		}
		switch {
		case !sets.Complete[name]:
			if (sets.Sized[name] || sets.Broken[name]) && (name != r.Made) {
				incomplete = append(incomplete, keyed{key, name})
			}
		case !sets.Empty[name]:
			ordered = append(ordered, keyed{key, name})
		}
	}
	sortNewestFirst(ordered)
	sortNewestFirst(incomplete)
	newest := ""
	if len(ordered) > 0 {
		newest = ordered[0].key
	}
	plan := RetentionPlan{Keep: map[string]bool{}}
	for name := range r.rulesKept(r.Entries, ordered) {
		plan.Keep[name] = true
	}
	for _, b := range ordered {
		if b.name == r.Made {
			plan.Keep[b.name] = true
		}
	}
	held, byEntry := r.heldExpiries(ordered)
	pinned, unpinned := map[string]bool{}, map[string]bool{}
	for name, until := range r.Pinned {
		end, expires := until, true
		if until.IsZero() {
			// A pin without until on a backup held by a schedule change ends with the hold. Other such pins never end.
			end, expires = byEntry[name]
		}
		if !expires || r.alive(end) {
			pinned[name], plan.Keep[name] = true, true
		} else {
			unpinned[name] = true
		}
	}
	var gone []keyed
	for _, b := range ordered {
		expires, isHeld := held[b.name]
		switch {
		case plan.Keep[b.name]:
		case isHeld && r.alive(expires):
			plan.Held = append(plan.Held, HeldKey{Key: b.name, Expires: expires})
		default:
			gone = append(gone, b)
			if isHeld || unpinned[b.name] {
				plan.Expired = append(plan.Expired, b.name)
			}
		}
	}
	for _, b := range incomplete {
		switch {
		case pinned[b.name], (newest == "") || !(b.key < newest):
		case r.Writing[b.name]:
			plan.Writing = append(plan.Writing, b.name)
		default:
			gone = append(gone, b)
			if unpinned[b.name] {
				plan.Expired = append(plan.Expired, b.name)
			}
		}
	}
	sortNewestFirst(gone)
	for _, b := range gone {
		plan.Obsolete = append(plan.Obsolete, b.name)
	}
	return plan
}

func sortNewestFirst(backups []keyed) {
	sort.SliceStable(backups, func(i, j int) bool {
		if backups[i].key != backups[j].key {
			return backups[i].key > backups[j].key
		}
		return backups[i].name > backups[j].name
	})
}

func (r Retention) rulesKept(entries []SlotEntry, ordered []keyed) map[string]int {
	kept := map[string]int{}
	counts := make([]int, len(entries))
	oldest := ""
	var manual []keyed
	for i, b := range ordered {
		if i == 0 {
			kept[b.name] = -1
		}
		if _, atOnce := r.Naming.KeyTime(b.name, r.Prefix); atOnce {
			manual = append(manual, b)
			continue
		}
		for j, entry := range entries {
			if SlotMatches(entry, b.key, r.Naming.Tolerance()) {
				counts[j]++
				if counts[j] <= entry.Keep {
					if by, found := kept[b.name]; !found || (by < 0) || (entry.Period.Weight() > entries[by].Period.Weight()) {
						kept[b.name] = j
					}
					oldest = b.key
				}
			}
		}
	}
	for _, b := range manual {
		if (oldest == "") || !(b.key < oldest) {
			kept[b.name] = -1
		}
	}
	return kept
}

func longestEntry(entries []SlotEntry) int {
	longest := -1
	for i, entry := range entries {
		if (longest < 0) || (entry.Period.Weight() > entries[longest].Period.Weight()) {
			longest = i
		}
	}
	return longest
}

// Zero time means no end, so it wins.
func later(a, b time.Time) time.Time {
	if a.IsZero() || b.IsZero() {
		return time.Time{}
	}
	if a.After(b) {
		return a
	}
	return b
}

// A hold expires Keep periods of the keeping entry after the backup time. Zero time means no end.
func (r Retention) heldExpiries(ordered []keyed) (held, byEntry map[string]time.Time) {
	held, byEntry = map[string]time.Time{}, map[string]time.Time{}
	hold := func(set map[string]time.Time, name string, expires time.Time) {
		if previous, found := set[name]; found {
			expires = later(previous, expires)
		}
		set[name] = expires
	}
	for i := range r.Previous {
		change := &r.Previous[i]
		changed, err := time.Parse(time.RFC3339, change.ChangeID)
		if err != nil {
			continue
		}
		tag := SlotTag(changed, change.TimeZone)
		var before []keyed
		for _, b := range ordered {
			if b.key < tag {
				before = append(before, b)
			}
		}
		if change.KeepAll || (len(change.Schedules) == 0) {
			for _, b := range before {
				hold(held, b.name, change.HeldUntil())
			}
			continue
		}
		location := cmp.Or(loadZone(change.TimeZone), time.UTC)
		entries := SlotEntries(change.Schedules, change.Time, change.TimeZone)
		longest := longestEntry(entries)
		keys := map[string]string{}
		for _, b := range before {
			keys[b.name] = b.key
		}
		for name, by := range r.rulesKept(entries, before) {
			made, err := time.ParseInLocation(api.BackupTagLayout, keys[name], location)
			if err != nil {
				hold(held, name, time.Time{})
				continue
			}
			entry := by
			if entry < 0 {
				entry = longest
			}
			expires := entries[entry].Period.Span(made, entries[entry].Keep)
			hold(held, name, expires)
			if by >= 0 {
				hold(byEntry, name, expires)
			}
		}
	}
	return held, byEntry
}

// The base is "" (a full backup) when the chain reaches keep.
func IncrementalBase(naming Naming, prefix string, required map[string]map[string]string, hosts []string, keep int) (string, int) {
	counts := map[string]int{}
	for _, host := range hosts {
		for key := range required[host] {
			counts[key]++
		}
	}
	newest, newestTime := "", ""
	for key, n := range counts {
		t, _ := naming.KeyTime(key, prefix)
		if (n == len(hosts)) && ((t > newestTime) || ((t == newestTime) && (key > newest))) {
			newest, newestTime = key, t
		}
	}
	if newest == "" {
		return "", 0
	}
	chain := 0
	for _, host := range hosts {
		var keys []keyed
		for key := range required[host] {
			t, _ := naming.KeyTime(key, prefix)
			keys = append(keys, keyed{t, key})
		}
		sortNewestFirst(keys)
		count := 0
		for _, b := range keys {
			if b.key > newestTime {
				continue
			}
			if required[host][b.name] == "" {
				break
			}
			count++
		}
		chain = max(chain, count)
	}
	if chain < keep {
		return newest, chain
	}
	return "", chain
}

// A pin without until (zero time) wins over other pins of the same backup.
func PinnedKeys(naming Naming, prefix string, keepBackups []api.BackupRetentionPin) map[string]time.Time {
	pinned := map[string]time.Time{}
	if naming.HoldsPrefix() {
		// the names carry the prefix of the series
	} else {
		prefix = ""
	}
	pin := func(key string, until time.Time) {
		if previous, found := pinned[key]; found {
			until = later(previous, until)
		}
		pinned[key] = until
	}
	for i := range keepBackups {
		item := keepBackups[i].Name
		until := keepBackups[i].GetUntil()
		if !naming.IsTemplate() || api.BackupTagRegexp.MatchString(item) {
			pin(item, until)
			continue
		}
		if parsed, ok := naming.Parse(item, prefix); ok {
			pin(parsed.Tag, until)
		}
	}
	return pinned
}
