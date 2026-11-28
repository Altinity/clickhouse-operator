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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// Only a backup that runs a schedule is named with the prefix it recorded of it: where the API server does not
// enforce the immutable scheduleRef, a backup edited to name its installation is named as any other backup, by
// its explicit name or after its own name, whatever record of a schedule its status still holds.
func TestBackupNamedWithTheSchedulePrefixOnlyWithScheduleRef(t *testing.T) {
	chb := testBackup("b1")
	chb.Spec.BackupName = "explicit"
	chb.Status.Schedule = &api.BackupScheduleRecord{Name: "nightly", BackupNamePrefix: "ns-demo-nightly"}
	require.Equal(t, "explicit", ResolveBackupName(chb))
	chb.Spec.BackupName = ""
	require.Equal(t, "b1-1700000000", ResolveBackupName(chb))
}

// A plain backup with a scheduled time is named after that time. A backup without one is named after its creation.
func TestBackupDueLaterIsNamedAfterItsScheduledTime(t *testing.T) {
	chb := testBackup("b1")
	chb.Spec.ScheduledTime = "2026-10-20T02:00:00+02:00"
	require.Equal(t, "b1-1792454400", ResolveBackupName(chb))
}

func TestResolvedBackupNamesPassValidation(t *testing.T) {
	short := testBackup("b1")
	require.Equal(t, "b1-1700000000", ResolveBackupName(short))
	require.Equal(t, ResolveBackupName(short), ResolveBackupName(short.DeepCopy()), "the name must not change between reconciles")

	for _, chb := range []*api.ClickHouseBackup{short, testBackup(strings.Repeat("a", 250))} {
		named := chb.DeepCopy()
		named.Spec.BackupName = ResolveBackupName(chb)
		require.NoError(t, named.Spec.Validate(), named.Spec.BackupName)
	}

	chbs := testSchedule(strings.Repeat("s", 250))
	prefix := BackupNamePrefix(chbs)
	require.LessOrEqual(t, len(prefix)+api.BackupNameScheduleSuffixLength, api.BackupNameMaxLength, "a scheduled backup name must fit the backup name limit")
	chbs.Spec.BackupNamePrefix = prefix
	require.NoError(t, chbs.Spec.Validate())
}

// The longest prefix the API admits must still give a backup name it admits.
func TestScheduledBackupNameSuffixFitsTheBackupNameLimit(t *testing.T) {
	require.Equal(t, api.BackupNameScheduleSuffixLength, len("-"+api.BackupTagLayout))

	chbs := testSchedule("nightly")
	chbs.Spec.BackupNamePrefix = strings.Repeat("p", api.BackupNamePrefixMaxLength)
	require.NoError(t, chbs.Spec.Validate())
	name, err := ScheduledBackupName(BackupNamePrefix(chbs), Naming{}, SlotTag(time.Date(2026, time.January, 2, 3, 4, 0, 0, time.UTC), ""))
	require.NoError(t, err)
	require.Len(t, name, api.BackupNameMaxLength-1, "one character is left for the \"T\" of a backup that runs the schedule once")

	chr := testRestore("r1")
	chr.Spec.BackupName = name
	require.NoError(t, chr.Spec.Validate(), "a restore must be able to name the backup a scheduled run made")
}

// Retention prunes by prefix. A restored installation typically shares the storage path of
// the installation its backup came from, often under the same name in another namespace, so a
// schedule of the same name on each must not get the same default prefix: each would prune the
// backups of the other.
func TestBackupNamePrefixDefaultsToNamespaceInstallationAndSchedule(t *testing.T) {
	source := testSchedule("nightly")
	clone := testSchedule("nightly")
	clone.Spec.ClickHouseInstallation = "demo-restored"
	sameName := testSchedule("nightly")
	sameName.Namespace = "staging"
	require.Equal(t, "ns-demo-nightly", BackupNamePrefix(source))
	require.Equal(t, "ns-demo-restored-nightly", BackupNamePrefix(clone))
	require.Equal(t, "staging-demo-nightly", BackupNamePrefix(sameName))

	long := testSchedule(strings.Repeat("s", 250))
	long.Spec.ClickHouseInstallation = strings.Repeat("c", 250)
	longToo := long.DeepCopy()
	longToo.Spec.ClickHouseInstallation = strings.Repeat("c", 249) + "d"
	for _, chbs := range []*api.ClickHouseBackupSchedule{source, clone, sameName, long, longToo} {
		prefix := BackupNamePrefix(chbs)
		require.LessOrEqual(t, len(prefix), api.BackupNamePrefixMaxLength, prefix)
		chbs.Spec.BackupNamePrefix = prefix
		require.NoError(t, chbs.Spec.Validate(), prefix)
		chbs.Spec.BackupNamePrefix = ""
	}
	require.Equal(t, BackupNamePrefix(long), BackupNamePrefix(long.DeepCopy()), "the prefix must not change between reconciles")
	require.NotEqual(t, BackupNamePrefix(long), BackupNamePrefix(longToo), "shortened prefixes must stay distinct")

	source.Spec.BackupNamePrefix = "custom"
	require.Equal(t, "custom", BackupNamePrefix(source))
}

// Every schedule of an installation that leaves its backup name prefix to the default names its backups
// after the namespace and the installation, whatever the schedule, shortened or not: a backup carries
// the default prefix of the installation exactly when it starts with what every such prefix starts
// with.
func TestHasDefaultBackupNamePrefix(t *testing.T) {
	for name, tc := range map[string]struct {
		namespace, chi string
	}{
		"short names":                          {namespace: testNamespace, chi: "demo"},
		"names a shortened prefix keeps whole": {namespace: testNamespace, chi: strings.Repeat("c", 90)},
		"names a shortened prefix cuts":        {namespace: testNamespace, chi: strings.Repeat("c", 120)},
		"names only a prefix left whole keeps": {namespace: testNamespace, chi: strings.Repeat("c", 104)},
	} {
		t.Run(name, func(t *testing.T) {
			for _, schedule := range []string{"s", "nightly", strings.Repeat("s", 30), strings.Repeat("s", 200)} {
				chbs := testSchedule(schedule)
				chbs.Namespace, chbs.Spec.ClickHouseInstallation = tc.namespace, tc.chi
				backup := BackupNamePrefix(chbs) + "-20261004020000"
				require.True(t, HasDefaultBackupNamePrefix(tc.namespace, tc.chi, backup), backup)

				other := chbs.DeepCopy()
				other.Spec.ClickHouseInstallation = "x" + tc.chi
				require.False(t, HasDefaultBackupNamePrefix(tc.namespace, tc.chi, BackupNamePrefix(other)+"-20261004020000"))
			}
			require.False(t, HasDefaultBackupNamePrefix(tc.namespace, tc.chi, "custom-20261004020000"))
			// An installation whose name extends this one's is another one, the shortened prefix included
			installation := joinName(tc.namespace, tc.chi)
			for _, root := range []string{installation, boundedHead(installation, "", api.BackupNamePrefixMaxLength)} {
				require.False(t, HasDefaultBackupNamePrefix(tc.namespace, tc.chi, root+"2-nightly-20261004020000"), root)
			}
		})
	}
}
