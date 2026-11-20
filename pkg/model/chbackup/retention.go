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
	"slices"
	"sort"
	"time"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// tagLayout is the layout of a tag, the wall time a backup is named after.
const tagLayout = "20060102150405"

// SlotEntry is an entry of a schedule as the slots of the backups are matched against it: its time resolved.
type SlotEntry struct {
	Period api.BackupPeriod
	Hour   int
	Minute int
	// Day is the day of a weekly or a monthly entry, -1 for none
	Day         int
	Keep        int
	Incremental bool
	// ConfigOverrides are the config overrides of the runs of the entry
	ConfigOverrides []string
	// Zone is the time zone of the schedule, nil for UTC: a daily, weekly or monthly slot whose time does not exist in it
	// on the day of a clock change that sets the clock forward, as 02:30 in Europe/Berlin on the last Sunday of March, is
	// the time Go normalizes it to, 03:30, which the backup of the slot is named after
	Zone *time.Location
}

// SlotEntries returns the entries of a schedule as the slots of the backups are matched against them, in its time zone
// ("" for UTC, as for a zone that is unknown).
func SlotEntries(entries []api.BackupScheduleEntry, zone string) []SlotEntry {
	location := loadZone(zone)
	var resolved []SlotEntry
	for _, entry := range api.ResolveScheduleTimes(entries) {
		hour, minute, _ := entry.ParseTime()
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

// SlotMatches reports whether the wall time tag, YYYYMMDDHHMMSS, is a slot of the entry, to the minute, or no more
// than tolerance seconds after one. A slot whose time does not exist in the zone of the entry that day is the time the
// zone moves it to, see inGapSlot.
func SlotMatches(entry SlotEntry, tag string, tolerance int) bool {
	if entry.Period == api.BackupPeriodMinute {
		return true
	}
	t, err := time.Parse(tagLayout, tag)
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

// WinnerEntry returns the index of the heaviest entry the tag is a slot of, -1 for none, and the indexes of every
// entry it is a slot of.
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

// IncrementalEntry returns the index of the incremental entry, -1 for none.
func IncrementalEntry(entries []SlotEntry) int {
	for i, entry := range entries {
		if entry.Incremental {
			return i
		}
	}
	return -1
}

// SlotTag returns the wall time of t in the zone, YYYYMMDDHHMMSS; "" for a zone that is unknown.
func SlotTag(t time.Time, zone string) string {
	location := time.UTC
	if (zone != "") && (zone != "UTC") {
		var err error
		if location, err = time.LoadLocation(zone); err != nil {
			return ""
		}
	}
	return t.In(location).Format(tagLayout)
}

// loadZone returns the time zone of the name, nil for UTC and for a zone that is unknown.
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

// BackupSets classifies the keys of the backups the hosts of a series list.
type BackupSets struct {
	// Complete are the keys every host lists, not broken
	Complete map[string]bool
	// WithData are the keys of backups that hold data on a host
	WithData map[string]bool
	// Empty are the keys of backups that hold nothing on a host, metadata included
	Empty map[string]bool
	// Sized are the keys of backups of a size on a host
	Sized map[string]bool
	// Broken are the keys of backups broken on a host
	Broken map[string]bool
	// All are the keys of every backup
	All map[string]bool
}

// NewBackupSets returns empty sets.
func NewBackupSets() BackupSets {
	return BackupSets{Complete: map[string]bool{}, WithData: map[string]bool{}, Empty: map[string]bool{}, Sized: map[string]bool{},
		Broken: map[string]bool{}, All: map[string]bool{}}
}

// Retention is what retention keeps of the backups of a series beyond the newest one.
type Retention struct {
	Naming Naming
	// Prefix is the backup name prefix of the series
	Prefix  string
	Entries []SlotEntry
	// Made is the key of the backup the run just made, kept whatever the entries
	Made string
	// Pinned are the keys of the backups retention.keepBackups names, with when each pin ends, the zero time for a pin
	// that sets no until
	Pinned map[string]time.Time
	// Previous are the changes of the schedule whose backups retention holds
	Previous []api.RetiredScheduleRules
	// Now is the time pins and holds expire by; the zero time expires none
	Now time.Time
	// Writing are the keys of incomplete backups retention leaves: a create_remote or an upload of them still runs on a
	// host, or a host cannot tell whether one does
	Writing map[string]bool
}

// HeldKey is a backup retention holds for a change of the schedule, and when the hold ends, the zero time for never.
type HeldKey struct {
	Key     string
	Expires time.Time
}

// RetentionPlan is what retention does with the backups of a series.
type RetentionPlan struct {
	// Obsolete are the backups retention deletes, newest first
	Obsolete []string
	// Held are the backups held for a change of the schedule that the schedule as it is would delete, newest first
	Held []HeldKey
	// Keep are the backups the entries, the run just made and the pins keep
	Keep map[string]bool
	// Expired are the backups of Obsolete that a hold or a pin kept until it expired, newest first
	Expired []string
	// Writing are the incomplete backups retention would delete but leaves, as Retention.Writing tells, newest first
	Writing []string
}

type keyed struct {
	key, name string
}

// alive reports whether a hold or a pin that ends at the time, the zero time for never, still holds.
func (r Retention) alive(end time.Time) bool {
	return end.IsZero() || r.Now.IsZero() || r.Now.Before(end)
}

// Plan returns which backups retention deletes, newest first, which it holds for a change of the schedule that the
// schedule as it is would delete, and which it keeps, as the cleanup of the Altinity usual backup workflow decides:
// every entry keeps the newest keep backups of its slots, the newest backup is kept, backups made at once are kept as
// long as they are no older than the oldest backup an entry keeps, a backup that holds nothing on every host is left
// alone, and one that is not complete is deleted once a newer one is, unless a command still writes it. A pin keeps a
// backup until it ends; a hold for a change until the backup expires as the schedule before the change would have
// deleted it.
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
			// A pin of a backup an entry before a change kept lasts as long as the hold; of any other, as it is named
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

// rulesKept returns the backups, newest first, the entries keep: the newest, the newest keep of the slots of every
// entry, and the backups made at once no older than the oldest of those. Each tells the index of the entry of the
// longest period that keeps it, -1 for a backup kept as the newest or as one made at once.
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

// longestEntry returns the index of the entry of the longest period, -1 for none.
func longestEntry(entries []SlotEntry) int {
	longest := -1
	for i, entry := range entries {
		if (longest < 0) || (entry.Period.Weight() > entries[longest].Period.Weight()) {
			longest = i
		}
	}
	return longest
}

// later returns the later of two ends of a hold, the zero time being never.
func later(a, b time.Time) time.Time {
	if a.IsZero() || b.IsZero() {
		return time.Time{}
	}
	if a.After(b) {
		return a
	}
	return b
}

// heldExpiries returns the backups retention holds for the changes of the schedule, every backup the entries and the
// time zone before a change kept of those made before it, with when each expires: the time of the backup and keep
// periods of the entry of the longest period that kept it, or of the longest entry for one kept as the newest or made
// at once, the latest of the changes that hold it; the zero time for never. byEntry are those an entry kept, with the
// latest expiry of the changes whose entries kept them.
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
		location := time.UTC
		if (change.TimeZone != "") && (change.TimeZone != "UTC") {
			if location, err = time.LoadLocation(change.TimeZone); err != nil {
				location = time.UTC
			}
		}
		entries := SlotEntries(change.Schedules, change.TimeZone)
		longest := longestEntry(entries)
		keys := map[string]string{}
		for _, b := range before {
			keys[b.name] = b.key
		}
		for name, by := range r.rulesKept(entries, before) {
			made, err := time.ParseInLocation(tagLayout, keys[name], location)
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

// IncrementalBase returns the backup an incremental backup on every host is made from, "" for a full one, and the
// length of the chain of incremental backups it ends: the newest backup of the series every host holds, as long as the
// chain is shorter than keep. required tells, by host and key, the backup the backup of the key is incremental from,
// "" for a full one.
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

// PinnedKeys returns the keys of the backups retention.keepBackups names, names, or with a template the name of the
// backup of any shard or the time alone, with when each pin ends: the zero time for one that sets no until, which wins
// over any other pin of the key.
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

// SortedKeys returns the keys of a set, sorted.
func SortedKeys(set map[string]bool) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
