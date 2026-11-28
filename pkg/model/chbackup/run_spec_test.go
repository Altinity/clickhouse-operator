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

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
)

// withOperatorTimeouts sets backup.timeouts of the operator configuration for the test.
func withOperatorTimeouts(t *testing.T, timeouts api.BackupTimeouts) {
	t.Helper()
	saved := chop.Config().Backup.Timeouts
	chop.Config().Backup.Timeouts = timeouts
	t.Cleanup(func() { chop.Config().Backup.Timeouts = saved })
}

// The timeouts of a run are those of the resource, then of the schedule a backup runs, then of the operator
// configuration, field by field; a field none of them sets is left to the built-in default of the orchestrator.
func TestRunTimeoutsPrecedence(t *testing.T) {
	withOperatorTimeouts(t, api.BackupTimeouts{
		ActionTimeout:      dur(1 * time.Hour),
		UnreachableTimeout: dur(1 * time.Minute),
		ErrorResponsesMax:  ptr.To(int32(7)),
	})

	// A backup that runs a schedule: its own field, then the schedule's, then the operator's
	chb := testBackup("now")
	chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "s1"}
	chb.Spec.Timeouts = &api.BackupTimeouts{ActionTimeout: dur(100 * time.Second)}
	chb.Status.Schedule = &api.BackupScheduleRecord{Name: "s1", BackupNamePrefix: "nightly",
		Timeouts: &api.BackupTimeouts{ActionTimeout: dur(200 * time.Second), UnreachableTimeout: dur(5 * time.Minute)}}
	timeouts := BackupRunSpec(chb, testTopology()).Timeouts
	require.Equal(t, 100*time.Second, timeouts.GetActionTimeout(), "the backup's own")
	require.Equal(t, 300*time.Second, timeouts.GetUnreachableTimeout(), "the schedule's")
	require.Equal(t, 7, timeouts.GetErrorResponsesMax(), "the operator's")
	require.Zero(t, timeouts.GetHTTPReceiveTimeout(), "none: the built-in default")

	// Without timeouts of its own, the schedule's
	chb.Spec.Timeouts = nil
	require.Equal(t, 200*time.Second, BackupRunSpec(chb, testTopology()).Timeouts.GetActionTimeout())

	// A run of the schedule: the schedule's, then the operator's
	chbs := testSchedule("s1")
	chbs.Spec.Timeouts = &api.BackupTimeouts{HTTPReceiveTimeout: dur(15 * time.Minute)}
	timeouts = ScheduledRunSpec(chbs, testTopology(), time.Date(2026, time.November, 12, 3, 0, 0, 0, time.UTC)).Timeouts
	require.Equal(t, 900*time.Second, timeouts.GetHTTPReceiveTimeout())
	require.Equal(t, time.Hour, timeouts.GetActionTimeout())

	// A restore: its own, then the operator's
	chr := testRestore("r1")
	chr.Spec.Timeouts = &api.BackupTimeouts{ErrorResponsesMax: ptr.To(int32(2))}
	timeouts = RestoreRunSpec(chr, testTopology()).Timeouts
	require.Equal(t, 2, timeouts.GetErrorResponsesMax())
	require.Equal(t, time.Minute, timeouts.GetUnreachableTimeout())

	// Nothing set anywhere leaves every field to the built-in default
	withOperatorTimeouts(t, api.BackupTimeouts{})
	require.Equal(t, &api.BackupTimeouts{}, RestoreRunSpec(testRestore("r2"), testTopology()).Timeouts)
}

// dur returns a duration of a spec.
func dur(d time.Duration) *meta.Duration {
	return &meta.Duration{Duration: d}
}

// A backup runs on the preferred replica of every shard, falling back to the first one, and a restore restores the schema on every replica and the data on the first of every
// shard, every one of them bounded by its deadline.
func TestRunSpecsTargetTheHostsOfTheirOperation(t *testing.T) {
	topology := testTopology()
	topology.clusters[0].preferredReplica = 1
	topology.enabled = topology.clusters
	chb := testBackup("b1")
	chb.Spec.BackupName = "manual"
	chb.Spec.DiffFromRemote = "base"
	chb.Spec.ConfigOverrides = []string{"--env=S3_STORAGE_CLASS=GLACIER"}
	chb.Spec.ActiveDeadlineSeconds = ptrInt64(3600)
	backup := BackupRunSpec(chb, topology)
	require.Equal(t, api.BackupRunOperationBackup, backup.Operation)
	require.Equal(t, "main", backup.Cluster)
	require.Equal(t, []string{"chi-demo-main-0-1", "chi-demo-main-1-1"}, backup.BackupHosts)
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, backup.FallbackHosts)
	require.Equal(t, "manual", backup.BackupName)
	require.Equal(t, "base", backup.DiffFromRemote)
	require.Equal(t, []string{"S3_STORAGE_CLASS=GLACIER"}, backup.ConfigOverrides)
	require.Equal(t, time.Hour, backup.Deadline)
	require.Empty(t, backup.Entries, "a one-off backup runs no retention")
	require.Equal(t, backup.BackupHosts, backup.Hosts())

	chb.Spec.ReplicaSelection = api.ReplicaSelectionAllReplicas
	require.Equal(t, topology.AllReplicas(), BackupRunSpec(chb, topology).BackupHosts)
	require.Empty(t, BackupRunSpec(chb, topology).FallbackHosts)

	chr := testRestore("r1")
	chr.Spec.Overwrite = types.NewStringBool(true)
	chr.Spec.DatabaseMapping = map[string]string{"b": "c", "a": "d"}
	restore := RestoreRunSpec(chr, topology)
	require.Equal(t, topology.AllReplicas(), restore.SchemaHosts)
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, restore.DataHosts)
	require.True(t, restore.Overwrite)
	require.True(t, restore.ValidateTopology)
	require.Equal(t, "a:d,b:c", restore.DatabaseMapping)
	require.Equal(t, 24*time.Hour, restore.Deadline)
	require.True(t, restore.ReplicatedCopyToDetached, "the replicas copy the data to detached by default")
	chr.Spec.ReplicatedCopyToDetached = types.NewStringBool(false)
	require.False(t, RestoreRunSpec(chr, topology).ReplicatedCopyToDetached)
}

// A scheduled run names its backup after its scheduled time, in the zone of the schedule, and prunes on every host it backs up
func TestScheduledRunSpec(t *testing.T) {
	chbs := testSchedule("nightly")
	chbs.Spec.TimeZone = "Europe/Berlin"
	chbs.Spec.Retention = &api.BackupRetention{KeepBackups: []api.BackupRetentionPin{{Name: "x", Until: "2026-12-31T00:00:00Z"}}}
	slot := time.Date(2026, time.November, 12, 2, 0, 0, 0, time.UTC)
	spec := ScheduledRunSpec(chbs, testTopology(), slot)
	require.Equal(t, BackupNamePrefix(chbs), spec.Prefix)
	require.Equal(t, slot, spec.Slot)
	require.Equal(t, "Europe/Berlin", spec.TimeZone)
	require.Equal(t, []api.BackupRetentionPin{{Name: "x", Until: "2026-12-31T00:00:00Z"}}, spec.KeepBackups)
	require.Len(t, spec.Entries, 1)
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, spec.RetentionHosts)
	require.Equal(t, "20261112030000", SlotTag(slot, chbs.Spec.TimeZone))
}

// A backup run now from a schedule runs its retention with the backups the schedule pins and holds, as recorded,
// and stays out of its series by its name.
func TestRunNowCarriesTheRetentionOfItsSchedule(t *testing.T) {
	chb := testBackup("now")
	chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "s1"}
	chb.Status.BackupName = "nightly-20260101T120000"
	previous := []api.RetiredScheduleRules{{ChangeID: "2026-01-01T00:00:00Z", Schedules: testSchedule("s1").Spec.Schedules}}
	chb.Status.Schedule = &api.BackupScheduleRecord{
		Name: "s1", BackupNamePrefix: "nightly", Schedules: testSchedule("s1").Spec.Schedules,
		KeepBackups:       []api.BackupRetentionPin{{Name: "nightly-20251201000000"}, {Name: "nightly-20251202000000"}},
		PreviousSchedules: previous,
	}
	spec := BackupRunSpec(chb, testTopology())
	require.True(t, spec.RunsSchedule)
	require.Equal(t, "nightly", spec.Prefix)
	require.Equal(t, []api.BackupRetentionPin{{Name: "nightly-20251201000000"}, {Name: "nightly-20251202000000"}}, spec.KeepBackups)
	require.Equal(t, previous, spec.Previous)
	require.Equal(t, "nightly-20260101T120000", spec.BackupName)
	_, inSeries := Naming{}.SeriesKey("h", spec.BackupName, "nightly")
	require.True(t, inSeries, "the series of the schedule holds it, which retention keeps as made at once")
	_, atOnce := Naming{}.KeyTime(spec.BackupName, "nightly")
	require.True(t, atOnce)
}

func ptrInt64(v int64) *int64 {
	return &v
}
