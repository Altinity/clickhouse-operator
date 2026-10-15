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
	for name, tc := range map[string]struct {
		validate func() error
		valid    bool
	}{
		"a plain backup":  {func() error { return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi"}).Validate() }, true},
		"no installation": {func() error { return (&ClickHouseBackupSpec{}).Validate() }, false},
		"a backup that runs a schedule": {func() error {
			return (&ClickHouseBackupSpec{ScheduleRef: &BackupScheduleRef{Name: "nightly"}, Cluster: "main", Verify: types.NewStringBool(true)}).Validate()
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
		// A schedule's retention would take it for one of its own backups
		"a name of a schedule's series": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupName: "nightly-20261012-020000"}).Validate()
		}, false},
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
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: "db.*,other.t?"}}).Validate()
		}, true},
		"a table pattern with a quote": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: "db.t'"}}).Validate()
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
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedule: "0 2 * * *", ConcurrencyPolicy: "Forbid"}).Validate()
		}, true},
		"a schedule without a schedule": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi"}).Validate()
		}, false},
		"an unknown concurrency policy": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedule: "0 2 * * *", ConcurrencyPolicy: "Sometimes"}).Validate()
		}, false},
		// The "-YYYYMMDD-HHMMSS" suffix of a scheduled run must still fit the name.
		"a prefix too long for the timestamp": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedule: "0 2 * * *",
				BackupTemplate: ClickHouseBackupTemplateSpec{BackupNamePrefix: longPrefix}}).Validate()
		}, false},
		// The operator learns how a run went from its Job, which must outlive the run.
		"no successful Job kept": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedule: "0 2 * * *",
				SuccessfulJobsHistoryLimit: ptr.To(int32(0))}).Validate()
		}, false},
		"no failed Job kept": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedule: "0 2 * * *",
				FailedJobsHistoryLimit: ptr.To(int32(0))}).Validate()
		}, false},
		"one Job of each kept": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedule: "0 2 * * *",
				SuccessfulJobsHistoryLimit: ptr.To(int32(1)), FailedJobsHistoryLimit: ptr.To(int32(1))}).Validate()
		}, true},
		"a negative retention": {func() error {
			return (&ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedule: "0 2 * * *",
				Retention: BackupRetention{KeepLast: ptr.To(int32(-1))}}).Validate()
		}, false},
		"a restore": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000"}).Validate()
		}, true},
		"a restore without a backup": {func() error { return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi"}).Validate() }, false},
		"a restore of tables": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000", Tables: "db.*,other-db.t?"}).Validate()
		}, true},
		"a restore of tables with a quote": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000", Tables: "db.t'"}).Validate()
		}, false},
		// A Job passes the pattern as an argument, which must never read as a flag
		"a restore of tables that reads as a flag": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000", Tables: "--rbac"}).Validate()
		}, false},
		"a table pattern that reads as a flag": {func() error {
			return (&ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: BackupOptions{Tables: "-db.*"}}).Validate()
		}, false},
		"a restore of tables too long": {func() error {
			return (&ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				Tables: strings.Repeat("t", backupTablesMaxLength+1)}).Validate()
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
		"zero":     {deadline: ptr.To(int64(0)), expected: defaultBackupActiveDeadlineSeconds},
		"negative": {deadline: ptr.To(int64(-1)), expected: defaultBackupActiveDeadlineSeconds},
	} {
		t.Run(name, func(t *testing.T) {
			options := BackupOptions{ActiveDeadlineSeconds: tc.deadline}
			backup := &ClickHouseBackupSpec{ClickHouseInstallation: "chi", BackupOptions: options}
			schedule := &ClickHouseBackupScheduleSpec{ClickHouseInstallation: "chi", Schedule: "0 2 * * *",
				BackupTemplate: ClickHouseBackupTemplateSpec{BackupOptions: options}}
			restore := &ClickHouseRestoreSpec{ClickHouseInstallation: "chi", BackupName: "daily-20261004-020000",
				ActiveDeadlineSeconds: tc.deadline}

			require.Equal(t, tc.expected, backup.GetActiveDeadlineSeconds())
			require.Equal(t, tc.expected, schedule.BackupTemplate.GetActiveDeadlineSeconds())
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
	require.Equal(t, defaultBackupActiveDeadlineSeconds, (*BackupOptions)(nil).GetActiveDeadlineSeconds())
	require.Equal(t, defaultBackupActiveDeadlineSeconds, (*ClickHouseRestoreSpec)(nil).GetActiveDeadlineSeconds())
}
