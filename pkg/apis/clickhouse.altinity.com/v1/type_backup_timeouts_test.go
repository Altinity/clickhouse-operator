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
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

// Every timeout is positive and bounded, on every resource that takes it; a backup that runs a schedule may set its
// own.
func TestBackupTimeoutsValidate(t *testing.T) {
	require.NoError(t, (*BackupTimeouts)(nil).Validate())
	require.NoError(t, (&BackupTimeouts{ActionTimeout: dur(1 * time.Second), UnreachableTimeout: dur(168 * time.Hour),
		ErrorResponsesMax: ptr.To(int32(1000)), HTTPReceiveTimeout: dur(4 * time.Hour)}).Validate())
	for name, timeouts := range map[string]*BackupTimeouts{
		"action zero":         {ActionTimeout: dur(0)},
		"action sub-second":   {ActionTimeout: dur(500 * time.Millisecond)},
		"action too long":     {ActionTimeout: dur(BackupActionTimeoutMax + time.Second)},
		"unreachable":         {UnreachableTimeout: dur(-1 * time.Second)},
		"http receive":        {HTTPReceiveTimeout: dur(BackupHTTPReceiveTimeoutMax + time.Second)},
		"error responses":     {ErrorResponsesMax: ptr.To(int32(0))},
		"error responses max": {ErrorResponsesMax: ptr.To(int32(BackupErrorResponsesMaxMax + 1))},
	} {
		require.ErrorIs(t, timeouts.Validate(), ErrInvalidBackupSpec, name)
		require.ErrorIs(t, (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", Timeouts: timeouts}).Validate(), ErrInvalidBackupSpec, name)
		require.ErrorIs(t, (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "b1", Timeouts: timeouts}).Validate(),
			ErrInvalidBackupSpec, name)
		require.Equal(t, &BackupTimeouts{}, timeouts.Sanitized(), "the operator configuration drops it: %s", name)
	}
	require.NoError(t, (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "s1"},
		Timeouts: &BackupTimeouts{ActionTimeout: dur(1 * time.Minute)}}).Validate())
}

// A field the timeouts leave unset is that of the first layer that sets it.
func TestBackupTimeoutsMergeFrom(t *testing.T) {
	own := &BackupTimeouts{ActionTimeout: dur(10 * time.Second)}
	merged := own.MergeFrom(nil, &BackupTimeouts{ActionTimeout: dur(20 * time.Second), ErrorResponsesMax: ptr.To(int32(3))},
		&BackupTimeouts{ErrorResponsesMax: ptr.To(int32(4)), HTTPReceiveTimeout: dur(30 * time.Second)})
	require.Equal(t, 10*time.Second, merged.GetActionTimeout())
	require.Equal(t, 3, merged.GetErrorResponsesMax())
	require.Equal(t, 30*time.Second, merged.GetHTTPReceiveTimeout())
	require.Zero(t, merged.GetUnreachableTimeout())
	require.Equal(t, 10*time.Second, own.ActionTimeout.Duration, "the timeouts merged into are left alone")
	require.Zero(t, (*BackupTimeouts)(nil).GetActionTimeout())
}

// The timeouts are Go durations in the CRD schema of every resource that takes them, bounded by the spec
// validation; errorResponsesMax is an integer bounded by both.
func TestBackupCRDDeclaresTheTimeouts(t *testing.T) {
	for _, name := range []string{"clickhousebackups.clickhouse.altinity.com", "clickhousebackupschedules.clickhouse.altinity.com",
		"clickhouserestores.clickhouse.altinity.com"} {
		schema := loadCRDSchema(t, backupCRDTemplatePath, name)
		for _, field := range []string{"actionTimeout", "unreachableTimeout", "httpReceiveTimeout"} {
			property := schemaProperty(schema, "spec.timeouts."+field)
			require.NotNil(t, property, "%s: %s", name, field)
			require.Equal(t, "string", property["type"], "%s: %s", name, field)
			require.Equal(t, "duration", property["format"], "%s: %s", name, field)
		}
		property := schemaProperty(schema, "spec.timeouts.errorResponsesMax")
		require.NotNil(t, property, name)
		require.Equal(t, 1, property["minimum"], name)
		require.Equal(t, BackupErrorResponsesMaxMax, property["maximum"], name)
	}
}

// The timeouts read as Go durations, in a resource and in the operator configuration alike.
func TestBackupTimeoutsAreGoDurations(t *testing.T) {
	var timeouts BackupTimeouts
	require.NoError(t, json.Unmarshal([]byte(`{"actionTimeout":"12h","unreachableTimeout":"1h30m","httpReceiveTimeout":"90s",`+
		`"errorResponsesMax":5}`), &timeouts))
	require.Equal(t, 12*time.Hour, timeouts.GetActionTimeout())
	require.Equal(t, 90*time.Minute, timeouts.GetUnreachableTimeout())
	require.Equal(t, 90*time.Second, timeouts.GetHTTPReceiveTimeout())
	require.Error(t, json.Unmarshal([]byte(`{"actionTimeout":"twelve hours"}`), &timeouts))
}

// dur returns a duration of a spec.
func dur(d time.Duration) *meta.Duration {
	return &meta.Duration{Duration: d}
}
