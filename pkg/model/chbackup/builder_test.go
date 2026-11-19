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
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

func testTopology() *Topology {
	main := topologyCluster{
		name: "main",
		shards: []topologyShard{
			{name: "0", hosts: []string{"chi-demo-main-0-0", "chi-demo-main-0-1"}},
			{name: "1", hosts: []string{"chi-demo-main-1-0", "chi-demo-main-1-1"}},
		},
	}
	return &Topology{installation: "demo", namespace: testNamespace, clusters: []topologyCluster{main}, enabled: []topologyCluster{main}}
}

func testInstallation() *api.ClickHouseInstallation {
	return newCHI("demo")
}

func testScheme(t *testing.T) *apiMachinery.Scheme {
	t.Helper()
	scheme := apiMachinery.NewScheme()
	require.NoError(t, api.AddToScheme(scheme))
	return scheme
}

func testBackup(name string) *api.ClickHouseBackup {
	chb := &api.ClickHouseBackup{}
	chb.Name = name
	chb.Namespace = testNamespace
	chb.CreationTimestamp = meta.NewTime(time.Unix(1700000000, 0))
	chb.Spec.ClickHouseInstallation = "demo"
	return chb
}

func testRestore(name string) *api.ClickHouseRestore {
	chr := &api.ClickHouseRestore{}
	chr.Name = name
	chr.Namespace = testNamespace
	chr.Spec.ClickHouseInstallation = "demo"
	chr.Spec.BackupName = "nightly-20260101-000000"
	return chr
}

func testSchedule(name string) *api.ClickHouseBackupSchedule {
	chbs := &api.ClickHouseBackupSchedule{}
	chbs.Name = name
	chbs.Namespace = testNamespace
	chbs.Spec.ClickHouseInstallation = "demo"
	chbs.Spec.Schedules = []api.BackupScheduleEntry{{Period: api.BackupPeriodDaily, Time: "03:00", Keep: 1}}
	return chbs
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

// A scheduled run names its backup after its slot, in the zone of the schedule, and prunes on every host it backs up
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

var _ = controllerutil.SetControllerReference
var _ = strings.Repeat

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

// A plain backup due at a scheduled time is named after that time, as one due when created is after its creation.
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

// The suffix a scheduled run appends to its backup name prefix is spelled twice: the API bounds prefixes by its
// length, the operator derives it with scheduleTimestampLayout. The longest prefix the API admits must still give a
// backup name it admits.
func TestScheduledBackupNameSuffixFitsTheBackupNameLimit(t *testing.T) {
	require.Equal(t, api.BackupNameScheduleSuffixLength, len("-"+scheduleTimestampLayout))

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

// A restore keeps to the cluster recorded when it was validated, whatever its spec names later.
func TestRestoresRBACFollowsThePinnedCluster(t *testing.T) {
	chr := testRestore("r1")
	chr.Spec.RBAC = types.NewStringBool(true)
	require.True(t, RestoresRBAC(chr), "a restore of every cluster restores access entities")
	chr.Spec.Cluster = "main"
	require.False(t, RestoresRBAC(chr), "a restore of one cluster does not")

	chr.Status.Clusters = []api.OperationClusterStatus{{Name: "main"}}
	chr.Status.Cluster = ""
	require.True(t, RestoresRBAC(chr), "validated as a restore of every cluster, it stays one")
	chr.Spec.Cluster, chr.Status.Cluster = "", "main"
	require.False(t, RestoresRBAC(chr), "validated as a restore of one cluster, it stays one")
}

// A recovery restores one cluster of a new installation, its access entities included.
func TestRecoveryRestore(t *testing.T) {
	chi := testInstallation()
	restore := BuildRecoveryRestore(chi, ClusterRecovery{Cluster: "cold", BackupName: "nightly-20260101-000000"})

	require.Equal(t, "demo-cold-recovery", restore.Name)
	require.Equal(t, chi.Namespace, restore.Namespace)
	require.Equal(t, "demo", restore.Spec.ClickHouseInstallation)
	require.Equal(t, "cold", restore.Spec.Cluster)
	require.Equal(t, "nightly-20260101-000000", restore.Spec.BackupName)
	require.True(t, restore.Spec.RBAC.IsTrue())
	require.NoError(t, restore.Spec.Validate())
}

// A recovery takes the installation that made its backup from the recovery it inherits, and hands it to the restore
// it creates.
func TestRecoveryRestoresFromItsSource(t *testing.T) {
	chi := newCHI("demo", &api.Cluster{Name: "hot", Backup: enabledBackup()}, &api.Cluster{Name: "cold", Backup: enabledBackup()})
	chi.Spec.Configuration.Clusters[1].Backup.Recovery = &api.BackupRecovery{SourceInstallation: types.NewString("cold-src")}
	chi.Spec.Backup = &api.ClusterBackup{Recovery: &api.BackupRecovery{
		BackupName:         types.NewString("nightly-20261004-020000"),
		SourceInstallation: types.NewString("prod"),
		SourceNamespace:    types.NewString("team-a"),
	}}
	_, stored := normalize(t, chi)

	recoveries := Recoveries(stored)
	require.Equal(t, []ClusterRecovery{
		{Cluster: "hot", BackupName: "nightly-20261004-020000", SourceInstallation: "prod", SourceNamespace: "team-a"},
		{Cluster: "cold", BackupName: "nightly-20261004-020000", SourceInstallation: "cold-src", SourceNamespace: "team-a"},
	}, recoveries)
	restore := BuildRecoveryRestore(stored, recoveries[1])
	require.Equal(t, "cold-src", restore.Spec.SourceInstallation)
	require.Equal(t, "team-a", restore.Spec.SourceNamespace)
	require.NoError(t, restore.Spec.Validate())
}

// Every backup-enabled cluster that inherits a non-empty backup name is recovered; a cluster that
// opts out with "", or has no sidecar to restore through, is not. A cluster that turns backup on without a
// storage is told apart, as its recovery waits for one.
func TestRecoveries(t *testing.T) {
	chi := newCHI("demo",
		&api.Cluster{Name: "hot", Backup: enabledBackup()},
		&api.Cluster{Name: "cold", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), Recovery: &api.BackupRecovery{BackupName: types.NewString("cold-20261001-030000")},
		}},
		&api.Cluster{Name: "scratch", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), Recovery: &api.BackupRecovery{BackupName: types.NewString("")},
		}},
		&api.Cluster{Name: "plain", Backup: &api.ClusterBackup{Enabled: types.NewStringBool(false)}},
		&api.Cluster{Name: "lacking", Backup: &api.ClusterBackup{
			Enabled: types.NewStringBool(true), StorageConfig: api.NewStorageConfig("", ""),
		}},
	)
	chi.Spec.Backup = &api.ClusterBackup{
		StorageConfig: api.NewStorageConfig(testStorageSecret, "config.yml"),
		Recovery:      &api.BackupRecovery{BackupName: types.NewString("nightly-20261004-020000")},
	}
	_, stored := normalize(t, chi)

	require.Equal(t, []ClusterRecovery{
		{Cluster: "hot", BackupName: "nightly-20261004-020000"},
		{Cluster: "cold", BackupName: "cold-20261001-030000"},
	}, Recoveries(stored))
	require.Equal(t, []ClusterRecovery{{Cluster: "lacking", BackupName: "nightly-20261004-020000"}}, RecoveriesWithoutStorage(stored),
		"a cluster whose backup a missing storage leaves off is not recovered, and its recovery is told apart")
	require.Empty(t, Recoveries(nil))
}

// The record of a recovery names the backup, or, when the clusters were recovered from different
// backups, each cluster with its backup; either reads back as the backups it names.
func TestRecoveredFrom(t *testing.T) {
	same := []ClusterRecovery{{Cluster: "hot", BackupName: "nightly"}, {Cluster: "cold", BackupName: "nightly"}}
	require.Equal(t, "nightly", RecoveredFrom(same))
	require.Equal(t, []string{"nightly"}, RecoveredBackupNames(RecoveredFrom(same)))

	different := []ClusterRecovery{{Cluster: "hot", BackupName: "nightly"}, {Cluster: "cold", BackupName: "weekly"}}
	require.Equal(t, "cold:weekly,hot:nightly", RecoveredFrom(different), "sorted by cluster")
	require.Equal(t, []string{"weekly", "nightly"}, RecoveredBackupNames(RecoveredFrom(different)))

	require.Equal(t, []string{"by-hand"}, RecoveredBackupNames("by-hand"))
	require.Empty(t, RecoveredBackupNames(""))
}
