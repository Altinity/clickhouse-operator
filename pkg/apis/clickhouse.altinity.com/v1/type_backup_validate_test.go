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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/ptr"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// Backup names become storage paths and clickhouse-backup command lines, so the spec is
// checked in Go as well as by the CRD schema: an installation may still run CRDs without the
// patterns.
func TestBackupSpecValidate(t *testing.T) {
	longName := strings.Repeat("a", BackupNameMaxLength+1)
	longPrefix := strings.Repeat("a", BackupNamePrefixMaxLength+1)
	daily := []BackupScheduleEntry{{Period: BackupPeriodDaily, Time: "02:00", Keep: 7}}
	for name, tc := range map[string]struct {
		validate func() error
		valid    bool
	}{
		"a plain backup":  {func() error { return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi"}).Validate() }, true},
		"no installation": {func() error { return (&ClickHouseBackupSpec{}).Validate() }, false},
		"a backup that runs a schedule": {func() error {
			return (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "nightly"}, Cluster: "main"}).Validate()
		}, true},
		"an installation and a schedule": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", ScheduleRef: &BackupScheduleRef{Name: "nightly"}}).Validate()
		}, false},
		"a schedule of no name": {func() error {
			return (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{}}).Validate()
		}, false},
		"a schedule whose name is a path": {func() error {
			return (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "../nightly"}}).Validate()
		}, false},
		// The CRD schema cannot type schemaOnly to refuse it there
		"a schedule with options of its own": {func() error {
			return (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "nightly"},
				BackupOptions: BackupOptions{SchemaOnly: types.NewStringBool(true)}}).Validate()
		}, false},
		"a schedule with an empty list of partitions": {func() error {
			return (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "nightly"},
				BackupOptions: BackupOptions{Partitions: []string{}}}).Validate()
		}, false},
		// The schedule's backup name prefix names it, which keeps it recognizable as the schedule's
		"a schedule with a name of its own": {func() error {
			return (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "nightly"}, BackupName: "nightly-once"}).Validate()
		}, false},
		// A schedule's retention would take it for one of its own backups
		"a name of a schedule's series": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupName: "nightly-20261012020000"}).Validate()
		}, false},
		"a name with a date and a time apart": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupName: "nightly-20261012-020000"}).Validate()
		}, true},
		"a name of a backup that runs a schedule": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupName: "nightly-20261012T020000"}).Validate()
		}, true},
		"a scheduled time with its offset": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", ScheduledTime: "2026-10-20T02:00:00+02:00",
				StartingDeadlineSeconds: ptr.To(int64(3600))}).Validate()
		}, true},
		"a scheduled time without its offset": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", ScheduledTime: "2026-10-20T02:00:00"}).Validate()
		}, false},
		"a scheduled date": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", ScheduledTime: "2026-10-20"}).Validate()
		}, false},
		"a starting deadline past any time": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", StartingDeadlineSeconds: ptr.To(int64(BackupStartingDeadlineMaxSeconds) + 1)}).Validate()
		}, false},
		"the longest starting deadline": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", StartingDeadlineSeconds: ptr.To(int64(BackupStartingDeadlineMaxSeconds))}).Validate()
		}, true},
		"a schedule starting deadline past any time": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: []BackupScheduleEntry{{Period: BackupPeriodHour, Keep: 1}},
				StartingDeadlineSeconds: ptr.To(int64(BackupStartingDeadlineMaxSeconds) + 1)}).Validate()
		}, false},
		"a negative starting deadline": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", StartingDeadlineSeconds: ptr.To(int64(-1))}).Validate()
		}, false},
		"a dotted name": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupName: "daily.2026-10-04"}).Validate()
		}, true},
		// clickhouse-backup strips "..", so the backup would land under another name.
		"two dots in a row": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupName: "a..b"}).Validate()
		}, false},
		"a name that is a command": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupName: "x$(touch F)"}).Validate()
		}, false},
		"a name too long": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupName: longName}).Validate()
		}, false},
		"an incremental base that is a command": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", DiffFromRemote: "base;rm"}).Validate()
		}, false},
		"table patterns": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: []string{"db.*", "other.t?"}}}).Validate()
		}, true},
		"a table pattern with a quote": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: []string{"db.t'"}}}).Validate()
		}, false},
		"a table pattern with a comma": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: []string{"db.*,other.*"}}}).Validate()
		}, false},
		"an empty table pattern": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: []string{""}}}).Validate()
		}, false},
		"too many table patterns": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi",
				BackupOptions: BackupOptions{Tables: make([]string, BackupTablesMaxItems+1)}}).Validate()
		}, false},
		"table patterns too long joined": {func() error {
			half := strings.Repeat("t", backupTablesMaxLength/2)
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: []string{half, half}}}).Validate()
		}, false},
		"tables left out of a backup": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{
				SkipTables: []string{"db.tmp*", "logs.*"}, SkipTableEngines: []string{"Kafka"}}}).Validate()
		}, true},
		"tables left out of a backup, reading a flag": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{SkipTables: []string{"--rbac"}}}).Validate()
		}, false},
		"tables left out of a backup, with a comma": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{SkipTables: []string{"a.*,b.*"}}}).Validate()
		}, false},
		"an engine left out of a backup, with a comma": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{SkipTableEngines: []string{"Kafka,Null"}}}).Validate()
		}, false},
		"tables left out of a backup that runs a schedule": {func() error {
			return (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "s1"}, BackupOptions: BackupOptions{SkipTables: []string{"db.*"}}}).Validate()
		}, false},
		"a partition with a space": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Partitions: []string{"2026 10"}}}).Validate()
		}, false},
		"an unknown replica selection": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{ReplicaSelection: "Some"}}).Validate()
		}, false},
		"a zero deadline": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{ActiveDeadlineSeconds: ptr.To(int64(0))}}).Validate()
		}, false},
		"a schedule": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily}).Validate()
		}, true},
		"a schedule without a schedule": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi"}).Validate()
		}, false},
		"a schedule in a time zone": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily, TimeZone: "Europe/Berlin"}).Validate()
		}, true},
		"a schedule in an unknown time zone": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily, TimeZone: "Mars/Olympus"}).Validate()
		}, false},
		"a negative starting deadline of a schedule": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				StartingDeadlineSeconds: ptr.To(int64(-1))}).Validate()
		}, false},
		// The "-YYYYMMDDHHMMSS" suffix of a scheduled run must still fit the name.
		"a prefix too long for the timestamp": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupNamePrefix: longPrefix}).Validate()
		}, false},
		"the longest prefix": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupNamePrefix: strings.Repeat("a", BackupNamePrefixMaxLength)}).Validate()
		}, true},
		"a dotted prefix": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupNamePrefix: "nightly.eu"}).Validate()
		}, true},
		"a prefix that is a command": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupNamePrefix: "x$(touch F)"}).Validate()
		}, false},
		"a prefix that reads as a flag": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupNamePrefix: "-nightly"}).Validate()
		}, false},
		"options of a schedule": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupOptions: BackupOptions{SchemaOnly: types.NewStringBool(true), ReplicaSelection: ReplicaSelectionAllReplicas,
					Tables: []string{"db.*"}, SkipTables: []string{"db.tmp*"}, SkipTableEngines: []string{"Kafka"}, Partitions: []string{"202610"}, ActiveDeadlineSeconds: ptr.To(int64(3600))}}).Validate()
		}, true},
		"an unknown replica selection of a schedule": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupOptions: BackupOptions{ReplicaSelection: "Some"}}).Validate()
		}, false},
		"a table pattern of a schedule that reads as a flag": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupOptions: BackupOptions{Tables: []string{"-db.*"}}}).Validate()
		}, false},
		"a partition of a schedule with a space": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				BackupOptions: BackupOptions{Partitions: []string{"2026 10"}}}).Validate()
		}, false},
		// status.recentRuns keeps at least one run of each outcome
		"no successful run kept": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				SuccessfulRunsHistoryLimit: ptr.To(int32(0))}).Validate()
		}, false},
		"no failed run kept": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				FailedRunsHistoryLimit: ptr.To(int32(0))}).Validate()
		}, false},
		"one run of each kept": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: daily,
				SuccessfulRunsHistoryLimit: ptr.To(int32(1)), FailedRunsHistoryLimit: ptr.To(int32(1))}).Validate()
		}, true},
		"a restore": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000"}).Validate()
		}, true},
		"a restore without a backup": {func() error { return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi"}).Validate() }, false},
		"a restore of tables": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000", Tables: []string{"db.*", "other-db.t?"}}).Validate()
		}, true},
		"a restore of tables with a quote": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000", Tables: []string{"db.t'"}}).Validate()
		}, false},
		// A Job passes the pattern as an argument, which must never read as a flag
		"a restore of tables that reads as a flag": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000", Tables: []string{"--rbac"}}).Validate()
		}, false},
		"a table pattern that reads as a flag": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: []string{"-db.*"}}}).Validate()
		}, false},
		"a restore of tables too long": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				Tables: []string{strings.Repeat("t", backupTablesMaxLength+1)}}).Validate()
		}, false},
		"a restore of a backup another installation made": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				SourceInstallation: "prod-eu", SourceNamespace: "team-a"}).Validate()
		}, true},
		// The source becomes a storage path on the command line of clickhouse-backup actions
		"a source installation that is no Kubernetes name": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				SourceInstallation: "prod eu"}).Validate()
		}, false},
		"a source namespace that is no Kubernetes name": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				SourceNamespace: "Team_A"}).Validate()
		}, false},
		"a source installation too long": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				SourceInstallation: strings.Repeat("s", BackupSourceInstallationMaxLength+1)}).Validate()
		}, false},
		"a source installation of the longest name": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				SourceInstallation: strings.Repeat("s", BackupSourceInstallationMaxLength)}).Validate()
		}, true},
		"a source namespace of the longest name": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				SourceNamespace: strings.Repeat("n", BackupSourceNamespaceMaxLength)}).Validate()
		}, true},
		"a source namespace too long": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				SourceNamespace: strings.Repeat("n", BackupSourceNamespaceMaxLength+1)}).Validate()
		}, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.validate()
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrInvalidBackupSpec)
		})
	}
}

// Backups, schedules and restores bound their Jobs alike: a deadline that is not positive is
// refused, with the same error for each, and a spec without one gets the default.
func TestActiveDeadlineSeconds(t *testing.T) {
	for name, tc := range map[string]struct {
		deadline *int64
		expected int64
		valid    bool
	}{
		"unset":    {deadline: nil, expected: defaultBackupActiveDeadlineSeconds, valid: true},
		"positive": {deadline: ptr.To(int64(600)), expected: 600, valid: true},
		"longest":  {deadline: ptr.To(int64(BackupActiveDeadlineMaxSeconds)), expected: BackupActiveDeadlineMaxSeconds, valid: true},
		"zero":     {deadline: ptr.To(int64(0)), expected: defaultBackupActiveDeadlineSeconds},
		"negative": {deadline: ptr.To(int64(-1)), expected: defaultBackupActiveDeadlineSeconds},
	} {
		t.Run(name, func(t *testing.T) {
			options := BackupOptions{ActiveDeadlineSeconds: tc.deadline}
			backup := &ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: options}
			schedule := &ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: []BackupScheduleEntry{{Period: BackupPeriodHour, Keep: 1}}, BackupOptions: options}
			restore := &ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				ActiveDeadlineSeconds: tc.deadline}

			require.Equal(t, tc.expected, backup.GetActiveDeadlineSeconds())
			require.Equal(t, tc.expected, schedule.GetActiveDeadlineSeconds())
			require.Equal(t, tc.expected, restore.GetActiveDeadlineSeconds())
			for kind, err := range map[string]error{"backup": backup.Validate(), "schedule": schedule.Validate(), "restore": restore.Validate()} {
				if tc.valid {
					require.NoError(t, err, kind)
					continue
				}
				require.ErrorIs(t, err, ErrActiveDeadlineNotPositive, kind)
				require.ErrorIs(t, err, ErrInvalidBackupSpec, kind)
				require.EqualError(t, err, "invalid backup spec: activeDeadlineSeconds must be positive", kind)
			}
		})
	}
	// Past the maximum, which the CRD schema and the validation refuse, seconds would overflow a time.Duration
	tooLong := ptr.To(int64(BackupActiveDeadlineMaxSeconds) + 1)
	require.EqualValues(t, BackupActiveDeadlineMaxSeconds, (&BackupOptions{ActiveDeadlineSeconds: tooLong}).GetActiveDeadlineSeconds())
	require.ErrorIs(t, (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{ActiveDeadlineSeconds: tooLong}}).Validate(), ErrInvalidBackupSpec)
	require.ErrorIs(t, (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedules: []BackupScheduleEntry{{Period: BackupPeriodHour, Keep: 1}}, BackupOptions: BackupOptions{ActiveDeadlineSeconds: tooLong}}).Validate(), ErrInvalidBackupSpec)
	require.ErrorIs(t, (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000", ActiveDeadlineSeconds: tooLong}).Validate(), ErrInvalidBackupSpec)
	require.Equal(t, defaultBackupActiveDeadlineSeconds, (*BackupOptions)(nil).GetActiveDeadlineSeconds())
	require.Equal(t, defaultBackupActiveDeadlineSeconds, (*ClickHouseRestoreSpec)(nil).GetActiveDeadlineSeconds())
}

// The filters of a restore, as the Altinity usual backup workflow has them, become single tokens of clickhouse-backup commands:
// partition ids, a table pattern, table engines, and a mapping of databases, each strictly bounded.
func TestValidateRestoreFilters(t *testing.T) {
	restore := func(set func(*ClickHouseRestoreSpec)) error {
		spec := &ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000"}
		set(spec)
		return spec.Validate()
	}
	for name, tc := range map[string]struct {
		set   func(*ClickHouseRestoreSpec)
		valid bool
	}{
		"partitions":               {func(s *ClickHouseRestoreSpec) { s.Partitions = []string{"202601", "all"} }, true},
		"a partition with a quote": {func(s *ClickHouseRestoreSpec) { s.Partitions = []string{"'a'"} }, false},
		"an empty partition":       {func(s *ClickHouseRestoreSpec) { s.Partitions = []string{""} }, false},
		"partitions of the schema alone": {func(s *ClickHouseRestoreSpec) {
			s.Partitions, s.SchemaOnly = []string{"202601"}, types.NewStringBool(true)
		}, false},
		"partitions with access entities": {func(s *ClickHouseRestoreSpec) {
			s.Partitions, s.RBAC = []string{"202601"}, types.NewStringBool(true)
		}, false},
		"partitions over existing tables": {func(s *ClickHouseRestoreSpec) {
			s.Partitions, s.Overwrite = []string{"202601"}, types.NewStringBool(true)
		}, true},
		"tables left out":                {func(s *ClickHouseRestoreSpec) { s.SkipTables = []string{"db.tmp*", "logs.*"} }, true},
		"tables left out reading a flag": {func(s *ClickHouseRestoreSpec) { s.SkipTables = []string{"--rbac"} }, false},
		"engines left out":               {func(s *ClickHouseRestoreSpec) { s.SkipTableEngines = []string{"Kafka", "RabbitMQ"} }, true},
		"an engine with a blank":         {func(s *ClickHouseRestoreSpec) { s.SkipTableEngines = []string{"Kafka x"} }, false},
		"an engine with a comma":         {func(s *ClickHouseRestoreSpec) { s.SkipTableEngines = []string{"Kafka,Null"} }, false},
		"a database mapping": {func(s *ClickHouseRestoreSpec) {
			s.DatabaseMapping = map[string]string{"db1": "db1_restored", "db2": "db-2"}
		}, true},
		"a mapping to itself":         {func(s *ClickHouseRestoreSpec) { s.DatabaseMapping = map[string]string{"db1": "db1"} }, false},
		"a mapping of two to one":     {func(s *ClickHouseRestoreSpec) { s.DatabaseMapping = map[string]string{"a": "c", "b": "c"} }, false},
		"a chain of mappings":         {func(s *ClickHouseRestoreSpec) { s.DatabaseMapping = map[string]string{"a": "b", "b": "c"} }, false},
		"a mapping with a colon":      {func(s *ClickHouseRestoreSpec) { s.DatabaseMapping = map[string]string{"a:b": "c"} }, false},
		"a mapping with a comma":      {func(s *ClickHouseRestoreSpec) { s.DatabaseMapping = map[string]string{"a": "b,c:d"} }, false},
		"a mapping into system":       {func(s *ClickHouseRestoreSpec) { s.DatabaseMapping = map[string]string{"a": "system"} }, false},
		"a mapping of the system one": {func(s *ClickHouseRestoreSpec) { s.DatabaseMapping = map[string]string{"INFORMATION_SCHEMA": "a"} }, false},
	} {
		t.Run(name, func(t *testing.T) {
			err := restore(tc.set)
			if tc.valid {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrInvalidBackupSpec)
		})
	}
	require.Equal(t, "a:b,c:d", (&ClickHouseRestoreSpec{DatabaseMapping: map[string]string{"c": "d", "a": "b"}}).GetDatabaseMapping())
	require.Equal(t, "", (*ClickHouseRestoreSpec)(nil).GetDatabaseMapping())
}
