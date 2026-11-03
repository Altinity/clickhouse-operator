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
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// lastSuccesses keeps the last-success series the controllers export, by cluster of the test
// installation, as the metrics package does: set by every report, dropped for the clusters a retain
// does not list.
type lastSuccesses map[string]time.Time

// captureLastSuccesses keeps the last-success series the controllers export for the duration of the test.
func captureLastSuccesses(t *testing.T) lastSuccesses {
	t.Helper()
	series := lastSuccesses{}
	savedSuccess, savedFailure, savedRetain := reportSuccess, reportFailure, retainClusters
	reportSuccess = func(_ context.Context, namespace, chi, cluster string, at time.Time) {
		require.Equal(t, testNamespace, namespace)
		require.Equal(t, testCHI, chi)
		series[cluster] = at
	}
	reportFailure = func(context.Context, string, string, string, time.Time) {}
	retainClusters = func(namespace, chi string, clusters []string) {
		require.Equal(t, testNamespace, namespace)
		require.Equal(t, testCHI, chi)
		for cluster := range series {
			if !slices.Contains(clusters, cluster) {
				delete(series, cluster)
			}
		}
	}
	t.Cleanup(func() {
		reportSuccess, reportFailure, retainClusters = savedSuccess, savedFailure, savedRetain
	})
	return series
}

func (s lastSuccesses) clusters() []string {
	var clusters []string
	for cluster := range s {
		clusters = append(clusters, cluster)
	}
	slices.Sort(clusters)
	return clusters
}

// A cluster that leaves the installation makes no backup any more: the series of its last backup goes,
// or an alert on the age of the last backup would fire on it for good. Its status entry may stay, for the
// hosts retention no longer prunes, and an operator restart, which sets the series again from every
// status, must not bring the series back from it.
func TestScheduleDropsTheLastOutcomeOfAClusterThatLeft(t *testing.T) {
	series := captureLastSuccesses(t)
	succeeded := meta.NewTime(time.Unix(1700000000, 0))
	chbs := newScheduleCR("s1")
	chbs.Status.Clusters = []api.ScheduleClusterStatus{
		{Name: "hot", CronJobName: "s1-hot-backup", LastSuccessfulTime: &succeeded},
		{Name: "cold", CronJobName: "s1-cold-backup", LastSuccessfulTime: &succeeded},
	}
	cr := newFakeCR(twoClusters(t, api.StatusCompleted))
	f := newScheduleFixture(t, cr, nil, chbs)
	f.reconcile("s1")
	require.Equal(t, []string{"cold", "hot"}, series.clusters())

	cr.set(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("plain", 1, false)))
	f.reconcile("s1")
	require.Equal(t, []string{"hot"}, series.clusters(), "the series of the cluster that left goes")

	// After a restart, from a status that keeps the entry of the cluster that left for its unpruned hosts
	restarted := captureLastSuccesses(t)
	left := f.schedule("s1")
	left.Status.Clusters = append(left.Status.Clusters,
		api.ScheduleClusterStatus{Name: "cold", LastSuccessfulTime: &succeeded, UnprunedHosts: []string{"chi-demo-cold-0-0"}})
	require.NoError(t, f.client.Status().Update(context.Background(), left))
	reportScheduleOutcome(f.schedule("s1"))
	require.Equal(t, []string{"hot"}, restarted.clusters(), "the entry of a cluster that left brings back no series")
}

// The series of a cluster goes when the installation no longer backs it up, whichever controller
// resolves the installation: a schedule or a backup of one cluster keeps the series of the others.
func TestControllersRetainTheSeriesOfEveryBackupEnabledCluster(t *testing.T) {
	series := captureLastSuccesses(t)
	at := time.Unix(1700000000, 0)
	for _, cluster := range []string{"hot", "cold", "gone"} {
		series[cluster] = at
	}
	chb := newBackupCR("b1")
	chb.Spec.Cluster = "hot"
	f := newBackupFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, chb)
	f.reconcile("b1")
	require.Equal(t, []string{"cold", "hot"}, series.clusters())

	series["gone"] = at
	chbs := newScheduleCR("s1")
	chbs.Spec.Cluster = "cold"
	s := newScheduleFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, chbs)
	s.reconcile("s1")
	require.Equal(t, []string{"cold", "hot"}, series.clusters())
}

// A finished backup is read again only after an operator restart, or a resync, by when a cluster it
// backed up may have left the installation, which may have no schedule to tell: the series of that cluster
// stays gone, as do those of every other cluster that left. An installation that has not completed a
// reconcile yet tells nothing of its clusters, so nothing is set, nor dropped.
func TestFinishedBackupReportsTheClustersTheInstallationStillBacksUp(t *testing.T) {
	at := meta.NewTime(time.Unix(1700000000, 0))
	chb := newBackupCR("b1")
	chb.Status.Phase = api.OperationPhaseCompleted
	chb.Status.Clusters = []api.OperationClusterStatus{
		{Name: "hot", Phase: api.OperationPhaseCompleted, CompletionTime: &at},
		{Name: "cold", Phase: api.OperationPhaseCompleted, CompletionTime: &at},
	}

	series := captureLastSuccesses(t)
	series["gone"] = at.Time
	cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, false)))
	newBackupFixture(t, cr, nil, chb).reconcile("b1")
	require.Equal(t, []string{"hot"}, series.clusters(), "cold no longer backs up, gone left")

	unknown := captureLastSuccesses(t)
	unknown["cold"] = at.Time
	newBackupFixture(t, newFakeCR(installationSpec(backupCluster("hot", 2, true))), nil, chb).reconcile("b1")
	require.Equal(t, []string{"cold"}, unknown.clusters(), "an installation without a completed reconcile tells nothing")
}

// An installation that is gone makes no backup any more: its series go once a schedule of it finds it
// gone, and a finished backup of it read again after an operator restart brings none of them back.
func TestSeriesGoWithTheInstallation(t *testing.T) {
	series := captureLastSuccesses(t)
	succeeded := meta.NewTime(time.Unix(1700000000, 0))
	chbs := newScheduleCR("s1")
	chbs.Status.Clusters = []api.ScheduleClusterStatus{
		{Name: "hot", CronJobName: "s1-hot-backup", LastSuccessfulTime: &succeeded},
		{Name: "cold", CronJobName: "s1-cold-backup", LastSuccessfulTime: &succeeded},
	}
	cr := newFakeCR(twoClusters(t, api.StatusCompleted))
	f := newScheduleFixture(t, cr, nil, chbs)
	f.reconcile("s1")
	require.Equal(t, []string{"cold", "hot"}, series.clusters())

	delete(cr.chis, key(testCHI).String())
	f.reconcile("s1")
	require.Empty(t, series.clusters(), "the schedule finds the installation gone")
	f.reconcile("s1")
	require.Empty(t, series.clusters(), "the status of the schedule brings no series back")

	restarted := captureLastSuccesses(t)
	chb := newBackupCR("b1")
	chb.Status.Phase = api.OperationPhaseCompleted
	chb.Status.Clusters = []api.OperationClusterStatus{{Name: "hot", Phase: api.OperationPhaseCompleted, CompletionTime: &succeeded}}
	newBackupFixture(t, cr, nil, chb).reconcile("b1")
	require.Empty(t, restarted.clusters(), "a finished backup brings no series of a gone installation back")
}

// A schedule whose clusters the installation completed its reconcile without, or without backup on, is
// held, its CronJobs kept suspended for when the clusters back up again, but those clusters make no backup
// meanwhile: their series go, or an alert on the age of their last backup would fire for good, and the
// status the schedule keeps brings none back, after an operator restart neither. The series of the clusters
// the installation still backs up stay.
func TestScheduleDropsTheSeriesOfTheClustersTheInstallationNoLongerBacksUp(t *testing.T) {
	succeeded := meta.NewTime(time.Unix(1700000000, 0))
	for name, tc := range map[string]struct {
		// cluster is the cluster the schedule names, scheduled the one it runs on
		cluster, scheduled string
		before, after      []*api.Cluster
		reason             string
		kept               []string
	}{
		"backup turned off on the only cluster": {
			scheduled: "hot",
			before:    []*api.Cluster{backupCluster("hot", 2, true)},
			after:     []*api.Cluster{backupCluster("hot", 2, false)},
			reason:    api.BackupReasonBackupDisabled,
		},
		"backup turned off on the cluster the schedule names": {
			cluster:   "cold",
			scheduled: "cold",
			before:    []*api.Cluster{backupCluster("hot", 2, true), backupCluster("cold", 1, true)},
			after:     []*api.Cluster{backupCluster("hot", 2, true), backupCluster("cold", 1, false)},
			reason:    api.BackupReasonBackupDisabled,
			kept:      []string{"hot"},
		},
		"the cluster the schedule names removed": {
			cluster:   "cold",
			scheduled: "cold",
			before:    []*api.Cluster{backupCluster("hot", 2, true), backupCluster("cold", 1, true)},
			after:     []*api.Cluster{backupCluster("hot", 2, true)},
			reason:    api.BackupReasonClusterNotFound,
			kept:      []string{"hot"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			series := captureLastSuccesses(t)
			chbs := newScheduleCR("s1")
			chbs.Spec.Cluster = tc.cluster
			cronJob := model.CronJobName(chbs, tc.scheduled)
			chbs.Status.Clusters = []api.ScheduleClusterStatus{{Name: tc.scheduled, CronJobName: cronJob, LastSuccessfulTime: &succeeded}}
			cr := newFakeCR(installation(t, api.StatusCompleted, tc.before...))
			f := newScheduleFixture(t, cr, nil, chbs)
			f.reconcile("s1")
			require.Equal(t, []string{tc.scheduled}, series.clusters())

			for _, cluster := range tc.kept {
				series[cluster] = succeeded.Time
			}
			cr.set(installation(t, api.StatusCompleted, tc.after...))
			f.reconcile("s1")
			require.Equal(t, tc.reason, readyReason(t, f.schedule("s1")))
			require.True(t, ptr.Deref(f.cronJob(cronJob).Spec.Suspend, false), "the CronJob is kept, suspended")
			require.Equal(t, tc.kept, series.clusters(), "the series of %s goes", tc.scheduled)

			restarted := captureLastSuccesses(t)
			f.reconcile("s1")
			require.Empty(t, restarted.clusters(), "the status of the schedule brings no series back")
		})
	}
}

// A cluster that loses its storage asks for backups all the same, which it makes no more: its series stay,
// after an operator restart as well, so that an alert on the age of its last backup fires. The schedule holds its
// CronJob, telling why.
func TestScheduleKeepsTheSeriesOfAClusterWithoutStorage(t *testing.T) {
	succeeded := meta.NewTime(time.Unix(1700000000, 0))
	for name, cluster := range map[string]string{"the schedule names the cluster": "hot", "a schedule of every cluster": ""} {
		t.Run(name, func(t *testing.T) {
			series := captureLastSuccesses(t)
			chbs := newScheduleCR("s1")
			chbs.Spec.Cluster = cluster
			cronJob := model.CronJobName(chbs, "hot")
			chbs.Status.Clusters = []api.ScheduleClusterStatus{{Name: "hot", CronJobName: cronJob, LastSuccessfulTime: &succeeded}}
			cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("hot", 2, true)))
			f := newScheduleFixture(t, cr, nil, chbs)
			f.reconcile("s1")
			require.Equal(t, []string{"hot"}, series.clusters())

			lacking := lackingCluster("hot", 2)
			cr.set(installation(t, api.StatusCompleted, lacking))
			f.reconcile("s1")
			require.Equal(t, api.BackupReasonNoBackupStorage, readyReason(t, f.schedule("s1")))
			require.True(t, ptr.Deref(f.cronJob(cronJob).Spec.Suspend, false), "the CronJob is kept, suspended")
			require.Equal(t, []string{"hot"}, series.clusters())

			restarted := captureLastSuccesses(t)
			f.reconcile("s1")
			require.Equal(t, []string{"hot"}, restarted.clusters(), "the status of the schedule brings the series back")
		})
	}

	t.Run("a schedule of every cluster, running on another one", func(t *testing.T) {
		series := captureLastSuccesses(t)
		chbs := newScheduleCR("s1")
		hotCronJob := model.CronJobName(chbs, "hot")
		chbs.Status.Clusters = []api.ScheduleClusterStatus{
			{Name: "hot", CronJobName: hotCronJob, LastSuccessfulTime: &succeeded},
			{Name: "cold", CronJobName: model.CronJobName(chbs, "cold"), LastSuccessfulTime: &succeeded},
		}
		cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, true)))
		f := newScheduleFixture(t, cr, nil, chbs)
		f.reconcile("s1")
		require.Equal(t, []string{"cold", "hot"}, series.clusters())

		cr.set(installation(t, api.StatusCompleted, lackingCluster("hot", 2), backupCluster("cold", 1, true)))
		f.reconcile("s1")
		require.True(t, ptr.Deref(f.cronJob(hotCronJob).Spec.Suspend, false), "the CronJob is kept, suspended")
		require.Equal(t, api.BackupReasonScheduled, readyReason(t, f.schedule("s1")), "the schedule runs on the other cluster")
		require.Equal(t, []string{"cold", "hot"}, series.clusters())

		restarted := captureLastSuccesses(t)
		f.reconcile("s1")
		require.Equal(t, []string{"cold", "hot"}, restarted.clusters(), "the status of the schedule brings the series back")

		cr.set(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, true)))
		f.reconcile("s1")
		require.False(t, ptr.Deref(f.cronJob(hotCronJob).Spec.Suspend, true), "resumed once the cluster has a storage")
	})
	t.Run("a finished backup read again after a restart", func(t *testing.T) {
		series := captureLastSuccesses(t)
		chb := newBackupCR("b1")
		chb.Status.Phase = api.OperationPhaseCompleted
		chb.Status.Clusters = []api.OperationClusterStatus{{Name: "hot", Phase: api.OperationPhaseCompleted, CompletionTime: &succeeded}}
		lacking := lackingCluster("hot", 2)
		newBackupFixture(t, newFakeCR(installation(t, api.StatusCompleted, lacking)), nil, chb).reconcile("b1")
		require.Equal(t, []string{"hot"}, series.clusters())
	})
}

// A schedule whose spec an edit made invalid holds the CronJobs of its last valid spec, which back up its
// clusters as ever once the spec is fixed: their series stay, and come back after an operator restart.
func TestInvalidScheduleReportsTheSeriesOfItsClusters(t *testing.T) {
	series := captureLastSuccesses(t)
	succeeded := meta.NewTime(time.Unix(1700000000, 0))
	chbs := newScheduleCR("s1")
	chbs.Spec.Schedule = ""
	chbs.Status.Clusters = []api.ScheduleClusterStatus{{Name: "main", CronJobName: "s1-main-backup", LastSuccessfulTime: &succeeded}}
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs)

	f.reconcile("s1")
	require.Equal(t, api.BackupReasonInvalidSpec, readyReason(t, f.schedule("s1")))
	require.Equal(t, []string{"main"}, series.clusters())
}

// A finished backup read again reports the clusters the normalized state of the last completed reconcile
// backs up, never those of the raw spec: with backup enabled for the whole installation in spec.backup, its
// clusters back up, though none of them sets backup of its own.
func TestFinishedBackupReportsTheClustersOfTheNormalizedInstallation(t *testing.T) {
	at := meta.NewTime(time.Unix(1700000000, 0))
	chb := newBackupCR("b1")
	chb.Status.Phase = api.OperationPhaseCompleted
	chb.Status.Clusters = []api.OperationClusterStatus{{Name: "hot", Phase: api.OperationPhaseCompleted, CompletionTime: &at}}
	spec := installationSpec(&api.Cluster{Name: "hot", Layout: &api.ChiClusterLayout{ShardsCount: 1, ReplicasCount: 1}})
	spec.Spec.Backup = &api.ClusterBackup{
		Enabled:  types.NewStringBool(true),
		Storages: []api.BackupStorage{{Name: testStorage, SecretRef: &core.LocalObjectReference{Name: testStorageSecret}}},
	}

	series := captureLastSuccesses(t)
	newBackupFixture(t, newFakeCR(installationFrom(t, spec, api.StatusCompleted)), nil, chb).reconcile("b1")
	require.Equal(t, []string{"hot"}, series.clusters())
}
