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

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// A successful run replaces the backups of its cluster in status.backups with those retention listed, and tells when;
// a run whose retention did not list leaves them; a cluster that leaves the status takes its backups along.
func TestScheduleStatusListsTheBackupsOfEachCluster(t *testing.T) {
	c := &ScheduleController{reconciler: &reconciler{}}
	at := func(d int) *meta.Time {
		return &meta.Time{Time: time.Date(2026, time.November, d, 3, 0, 0, 0, time.UTC)}
	}
	chbs := &api.ClickHouseBackupSchedule{}
	chbs.Status.Backups = []api.StoredBackup{
		{Cluster: "a", Name: "old", Created: at(1)},
		{Cluster: "b", Name: "b1", Created: at(2)},
	}
	run := func(cluster string, outcome runOutcome) {
		scheduled := &api.ScheduledRun{Slot: *at(10)}
		scheduled.Name = cluster
		scheduled.StartTime = at(10)
		var out announcements
		c.finishRun(chbs, scheduled, outcome, &out)
	}

	run("a", runOutcome{backupName: "a2", series: []api.StoredBackup{{Name: "a2", Created: at(3)}, {Name: "a1", Created: at(1)}}})
	require.Equal(t, []api.StoredBackup{
		{Cluster: "a", Name: "a2", Created: at(3)},
		{Cluster: "b", Name: "b1", Created: at(2)},
		{Cluster: "a", Name: "a1", Created: at(1)},
	}, chbs.Status.Backups)
	require.NotNil(t, findScheduleCluster(chbs, "a").BackupsListTime)

	run("b", runOutcome{backupName: "b2"})
	require.Len(t, chbs.Status.Backups, 3, "retention did not list")
	require.Nil(t, findScheduleCluster(chbs, "b").BackupsListTime)

	run("a", runOutcome{failed: true, message: "failed", series: []api.StoredBackup{}})
	require.Len(t, chbs.Status.Backups, 3, "a failed run lists nothing")

	var out announcements
	c.dropLeavingClusters(chbs, []string{"b"}, &out)
	require.Equal(t, []api.StoredBackup{{Cluster: "b", Name: "b1", Created: at(2)}}, chbs.Status.Backups)
}

// status.backups is newest first, and holds at most StoredBackupsMax backups.
func TestReplaceStoredBackupsKeepsTheNewest(t *testing.T) {
	var backups []api.StoredBackup
	for i := 0; i < api.StoredBackupsMax+10; i++ {
		backups = append(backups, api.StoredBackup{Name: "b", Created: &meta.Time{Time: time.Unix(int64(i), 0)}})
	}
	list := api.ReplaceStoredBackups([]api.StoredBackup{{Cluster: "x", Name: "undated"}}, "a", backups)
	require.Len(t, list, api.StoredBackupsMax)
	require.Equal(t, int64(api.StoredBackupsMax+9), list[0].Created.Unix())
	require.Equal(t, "a", list[0].Cluster)
	require.NotContains(t, list, api.StoredBackup{Cluster: "x", Name: "undated"}, "undated sorts last and is cut")
}
