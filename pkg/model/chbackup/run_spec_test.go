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
