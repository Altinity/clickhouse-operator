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
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// dailyBackups returns the names of the backups of prefix d made every day at hhmmss from the first day of November
// 2026, days of them
func dailyBackups(days int, hhmmss string) []string {
	var names []string
	for day := 1; day <= days; day++ {
		names = append(names, fmt.Sprintf("d-202611%02d%s", day, hhmmss))
	}
	return names
}

// retentionPlanOf runs retention_plan over the complete backups names with the entries schedules, and returns what it
// deletes and what it holds, newest first
func retentionPlanOf(t *testing.T, env map[string]string, names []string) (obsolete, held []string) {
	t.Helper()
	out := sourcedOutput(t, withEnv(map[string]string{envBackupNamePrefix: "d", "ALL": strings.Join(names, " ")}, env), `
		naming_init
		parse_schedules
		read -r -a NAMES <<<"${ALL}"
		declare -A COMPLETE=() SIZED=()
		for name in "${NAMES[@]}"; do COMPLETE[${name}]=1; SIZED[${name}]=1; done
		retention_plan
		echo "${OBSOLETE[*]}"
		printf '%s\n' "${HELD_OBSOLETE[@]}" | sort -r | tr '\n' ' '; echo`)
	lines := strings.Split(out, "\n")
	return strings.Fields(lines[0]), strings.Fields(lines[1])
}

// changeAt returns the PREVIOUS_SCHEDULES record of a change at the time, RFC 3339, from entries in zone
func changeAt(at, zone, entries string) string {
	changed, err := time.Parse(time.RFC3339, at)
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("%d|%s|%s", changed.Unix(), zone, entries)
}

// A change of the schedule never deletes the backups the schedule kept before it while the change is held: retention
// holds every backup the entries and the time zone before the change kept of those made before it, and tells those the
// schedule as it is would delete. Released, the change no longer holds them. Every change of table (b) of the design
// is a change of the entries or of the zone.
func TestJobScriptRetentionHoldsWhatAChangeKept(t *testing.T) {
	for name, tc := range map[string]struct {
		// before are the entries and the zone before the change at 2026-11-12T12:00:00Z, after those after it
		before, zone, after, afterZone string
		names                          []string
		// held are the backups the change holds that the schedule as it is would delete
		held []string
	}{
		"the time shifted": {
			before: "period=daily time=02:00 keep=3", after: "period=daily time=03:00 keep=3",
			names: append(dailyBackups(12, "020000"), "d-20261113030000"),
			held:  []string{"d-20261112020000", "d-20261111020000", "d-20261110020000"},
		},
		"an entry removed": {
			// 2026-11-01 and 2026-11-08 are Sundays
			before: "period=weekly day=0 time=02:00 keep=2;period=daily time=02:00 keep=2", after: "period=daily time=02:00 keep=2",
			names: append(dailyBackups(12, "020000"), "d-20261113020000"),
			held:  []string{"d-20261111020000", "d-20261108020000", "d-20261101020000"},
		},
		"keep lowered": {
			before: "period=daily time=02:00 keep=5", after: "period=daily time=02:00 keep=2",
			names: append(dailyBackups(12, "020000"), "d-20261113020000"),
			held:  []string{"d-20261111020000", "d-20261110020000", "d-20261109020000", "d-20261108020000"},
		},
		"the day of a weekly entry changed": {
			before: "period=weekly day=0 time=02:00 keep=2;period=daily time=02:00 keep=1",
			after:  "period=weekly day=1 time=02:00 keep=2;period=daily time=02:00 keep=1",
			names:  append(dailyBackups(12, "020000"), "d-20261113020000"),
			held:   []string{"d-20261112020000", "d-20261108020000", "d-20261101020000"},
		},
		"the period changed": {
			before: "period=daily time=02:00 keep=3", after: "period=weekly day=0 time=02:00 keep=3",
			names: append(dailyBackups(12, "020000"), "d-20261113020000"),
			held:  []string{"d-20261112020000", "d-20261111020000", "d-20261110020000"},
		},
		"the zone changed, at the same instant": {
			before: "period=daily time=02:00 keep=3", zone: "UTC", after: "period=daily time=03:00 keep=3", afterZone: "Europe/Berlin",
			names: append(dailyBackups(12, "020000"), "d-20261113030000"),
			held:  []string{"d-20261112020000", "d-20261111020000", "d-20261110020000"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			env := map[string]string{envSchedules: tc.after, envScheduleTimezone: tc.afterZone}
			// Without the change, the schedule as it is decides alone
			obsolete, held := retentionPlanOf(t, env, tc.names)
			require.Empty(t, held)
			for _, name := range tc.held {
				require.Contains(t, obsolete, name, "the schedule as it is deletes it")
			}
			env[envPreviousSchedules] = changeAt("2026-11-12T12:00:00Z", tc.zone, tc.before)
			obsolete, held = retentionPlanOf(t, env, tc.names)
			require.Equal(t, tc.held, held)
			for _, name := range tc.held {
				require.NotContains(t, obsolete, name, "held")
			}
			// What the schedule before the change would have deleted, and what neither keeps, still goes
			before, _ := retentionPlanOf(t, map[string]string{envSchedules: tc.before, envScheduleTimezone: tc.zone}, tc.names[:12])
			for _, name := range obsolete {
				require.True(t, slices.Contains(before, name) || !slices.Contains(tc.names[:12], name), "%s is deleted though the schedule before kept it", name)
			}
			// A backup pinned is kept whatever the change
			env[envKeepBackups] = tc.held[0]
			_, held = retentionPlanOf(t, env, tc.names)
			require.NotContains(t, held, tc.held[0])
		})
	}
}

// A schedule changed again holds the backups of each change, a change that holds all of them every backup made before
// it, and a backup made after a change is the schedule's as it is.
func TestJobScriptRetentionHoldsEveryChange(t *testing.T) {
	names := append(dailyBackups(12, "020000"), "d-20261113030000", "d-20261114040000", "d-20261115040000")
	env := map[string]string{envSchedules: "period=daily time=04:00 keep=1", envPreviousSchedules: strings.Join([]string{
		changeAt("2026-11-12T12:00:00Z", "", "period=daily time=02:00 keep=2"),
		changeAt("2026-11-13T12:00:00Z", "", "period=daily time=03:00 keep=2"),
	}, "#")}
	obsolete, held := retentionPlanOf(t, env, names)
	require.Equal(t, []string{"d-20261113030000", "d-20261112020000", "d-20261111020000"}, held)
	require.Equal(t, []string{"d-20261114040000", "d-20261110020000", "d-20261109020000", "d-20261108020000", "d-20261107020000",
		"d-20261106020000", "d-20261105020000", "d-20261104020000", "d-20261103020000", "d-20261102020000", "d-20261101020000"}, obsolete)

	env[envPreviousSchedules] = changeAt("2026-11-13T12:00:00Z", "", "*")
	obsolete, held = retentionPlanOf(t, env, names)
	require.Len(t, held, 13)
	require.Equal(t, []string{"d-20261114040000"}, obsolete)
}

// Backups the Altinity Cloud Manager made, named after the time their run started, count for the slots of the entries
// with its tolerance; one it made by hand, pinned, stays, as the dialog of a schedule change of the Altinity Cloud
// Manager keeps it.
func TestJobScriptRetentionOfBackupsTheAltinityCloudManagerMade(t *testing.T) {
	keys := []string{"20261108020412", "20261109021733", "20261110020059", "20261111020502", "20261111143000", "20261112020911"}
	out := sourcedOutput(t, scriptNamingEnv(api.BackupNamingACM, map[string]string{
		envSchedules: "period=daily time=02:00 keep=3", envBackupNamePrefix: "nightly", "ALL": strings.Join(keys, " "),
		envKeepBackups: "shard2-20261111143000",
	}), `
		naming_init
		parse_schedules
		read -r -a NAMES <<<"${ALL}"
		declare -A COMPLETE=() SIZED=()
		for name in "${NAMES[@]}"; do COMPLETE[${name}]=1; SIZED[${name}]=1; done
		retention_plan
		echo "${OBSOLETE[*]}"`)
	require.Equal(t, "20261109021733 20261108020412\n", out)
}

// The scheduled run of a schedule holding backups for a change tells them in its termination message, which the
// operator records in the status of the schedule, by the names of the backups of their first shard.
func TestJobScriptTellsTheBackupsItHolds(t *testing.T) {
	var backups []stubBackup
	for _, name := range dailyBackups(5, "020000") {
		backups = append(backups, remote(name, ""))
	}
	s := newSidecarStub(t, map[string]*stubHost{"h0": {remote: backups}})
	termination := filepath.Join(t.TempDir(), "termination-log")
	env := scheduledEnv("h0", "period=daily time=03:00 keep=2", "2026-11-06T03:00:00Z")
	env[envBackupNamePrefix] = "d"
	env[envPreviousSchedules] = changeAt("2026-11-05T12:00:00Z", "", "period=daily time=02:00 keep=2")
	env[envHeldBackupsMessage] = HeldBackupsMessage
	env[envTerminationMessagePath] = termination
	code, out := s.run(env)
	require.Zero(t, code, out)
	require.Contains(t, out, "retention holds 2 backup(s) the schedule kept before a change of it")
	message, err := os.ReadFile(termination)
	require.NoError(t, err)
	pod := &core.Pod{Status: core.PodStatus{ContainerStatuses: []core.ContainerStatus{{Name: jobContainerName,
		State: core.ContainerState{Terminated: &core.ContainerStateTerminated{Message: string(message)}}}}}}
	require.Equal(t, []string{"d-20261105020000", "d-20261104020000"}, JobHeldBackups(pod))
	names := s.remoteNames("h0")
	slices.Sort(names)
	require.Equal(t, []string{"d-20261104020000", "d-20261105020000", "d-20261106030000"}, names)
}
