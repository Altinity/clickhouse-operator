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
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

const (
	copyCommand = "restore --data=1 --ignore-dependencies --replicated-copy-to-detached b1"
	dataCommand = "restore --data=1 --ignore-dependencies b1"
)

// copySpec is the spec of a restore of a shard of three replicas, h0 h1 h2, and of a shard of one, h3.
func copySpec(copies bool) *model.RunSpec {
	return &model.RunSpec{Operation: api.BackupRunOperationRestore, Cluster: "main", BackupName: "b1",
		SchemaHosts: []string{"h0", "h1", "h2", "h3"}, DataHosts: []string{"h0", "h3"}, ValidateTopology: true,
		ReplicatedCopyToDetached: copies, ResumeDownload: true}
}

func copySidecars() *fakeSidecars {
	sc := newFakeSidecars("h0", "h1", "h2", "h3")
	for _, host := range []string{"h0", "h1", "h2", "h3"} {
		sc.hosts[host].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
	}
	return sc
}

// rowOf returns the row of the command on the host.
func rowOf(t *testing.T, sc *fakeSidecars, host, command string) actionRow {
	t.Helper()
	for _, row := range sc.hosts[host].actions {
		if row.Command == command {
			return row
		}
	}
	require.Failf(t, "no row", "%s: %s", host, command)
	return actionRow{}
}

// The replicas of a shard but the first copy the data to detached all at once, then the first one restores it; a shard
// of one replica restores its data as before.
func TestRestoreCopiesTheDataToDetachedBeforeTheFirstReplicaRestoresIt(t *testing.T) {
	sc := copySidecars()
	sc.slow = map[string]int{"h1|--replicated-copy-to-detached": 5}
	outcome, status, _ := runOf(t, sc, copySpec(true), nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h0: download --hardlink-exists-files b1", "h1: download --hardlink-exists-files b1",
		"h2: download --hardlink-exists-files b1", "h3: download --hardlink-exists-files b1",
		"h0: restore --schema=1 --ignore-dependencies --env=RESTORE_SCHEMA_ON_CLUSTER={cluster} b1",
		"h1: restore --schema=1 --ignore-dependencies b1",
		"h2: restore --schema=1 --ignore-dependencies b1",
		"h3: restore --schema=1 --ignore-dependencies b1",
		"h1: " + copyCommand, "h2: " + copyCommand,
		"h0: " + dataCommand, "h3: " + dataCommand,
		"h0: delete local b1", "h1: delete local b1", "h2: delete local b1", "h3: delete local b1",
	}, sc.commands())
	slowCopy := rowOf(t, sc, "h1", copyCommand)
	require.Less(t, rowOf(t, sc, "h2", copyCommand).Start, slowCopy.Finish, "the replicas copy at once")
	require.Less(t, slowCopy.Finish, rowOf(t, sc, "h0", dataCommand).Start, "the first replica restores once they copied")
	require.Equal(t, "h0 h3", status.Plan[planDataHosts])
	require.Equal(t, "h1 h2", status.Plan[planCopyHosts])
	var copied []string
	for _, action := range status.Actions {
		if action.Step == stepCopy {
			require.Equal(t, actionSuccess, action.Status)
			copied = append(copied, action.Host+": "+action.Command)
		}
	}
	require.ElementsMatch(t, []string{"h1: " + copyCommand, "h2: " + copyCommand}, copied, "the status records every copy")

	// shards of one replica copy nothing
	single := copySidecars()
	spec := copySpec(true)
	spec.DataHosts = spec.SchemaHosts
	outcome, _, _ = runOf(t, single, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	for _, command := range single.commands() {
		require.NotContains(t, command, "--replicated-copy-to-detached")
	}
	require.Equal(t, []string{"h0: " + dataCommand, "h1: " + dataCommand, "h2: " + dataCommand, "h3: " + dataCommand},
		slices.DeleteFunc(single.commands(), func(command string) bool { return !strings.Contains(command, "--data=1") }))
}

// Without replicatedCopyToDetached, the replicas of a shard but the first download the schema alone and restore no data.
func TestRestoreWithoutCopyDownloadsTheSchemaAloneOnTheOtherReplicas(t *testing.T) {
	sc := copySidecars()
	spec := copySpec(false)
	spec.Tables = "db.*"
	outcome, status, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h0: download --hardlink-exists-files --tables=db.* b1", "h1: download --schema --tables=db.* b1",
		"h2: download --schema --tables=db.* b1", "h3: download --hardlink-exists-files --tables=db.* b1",
		"h0: restore --schema=1 --ignore-dependencies --tables=db.* --env=RESTORE_SCHEMA_ON_CLUSTER={cluster} b1",
		"h1: restore --schema=1 --ignore-dependencies --tables=db.* b1",
		"h2: restore --schema=1 --ignore-dependencies --tables=db.* b1",
		"h3: restore --schema=1 --ignore-dependencies --tables=db.* b1",
		"h0: restore --data=1 --ignore-dependencies --tables=db.* b1", "h3: restore --data=1 --ignore-dependencies --tables=db.* b1",
		"h0: delete local b1", "h1: delete local b1", "h2: delete local b1", "h3: delete local b1",
	}, sc.commands())
	require.Empty(t, status.Plan[planCopyToDetached])
	require.Empty(t, status.Plan[planDataHosts])
}

// A first replica that does not answer as the restore comes to copy the data leaves the restore of the data of its
// shard to the next replica, which copies nothing, and takes the data by replication once it answers.
func TestRestoreFallsBackToTheNextReplicaWhenTheFirstDoesNotAnswer(t *testing.T) {
	sc := copySidecars()
	sc.hosts["h0"].notReady = errors.New("connection refused")
	status := &api.BackupRunStatus{Operation: api.BackupRunOperationRestore, Step: stepCopy,
		Plan: map[string]string{planCopyToDetached: "true"}}
	outcome, status, events := runOf(t, sc, copySpec(true), status)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h2: " + copyCommand, "h1: " + dataCommand, "h3: " + dataCommand}, sc.commands())
	require.Equal(t, "h1 h3", status.Plan[planDataHosts])
	require.Equal(t, "h2", status.Plan[planCopyHosts])
	require.Contains(t, reasonsOf(events), "OrchestratorReplicaFallback")
}

// A first replica the restore of the data cannot be submitted to hands it to the next replica of its shard, which
// restores the data over its copy of it to detached.
func TestRestoreFallsBackToAReplicaThatCopiedTheData(t *testing.T) {
	sc := copySidecars()
	sc.lose = map[string]error{"h0|" + dataCommand: errors.New("connection reset")}
	outcome, status, events := runOf(t, sc, copySpec(true), nil)
	require.False(t, outcome.failed, outcome.message)
	restores := slices.DeleteFunc(sc.commands(), func(command string) bool { return !strings.Contains(command, "--data=1") })
	require.Equal(t, []string{"h1: " + copyCommand, "h2: " + copyCommand, "h1: " + dataCommand, "h3: " + dataCommand}, restores)
	require.Equal(t, "h1 h3", status.Plan[planDataHosts])
	require.Contains(t, reasonsOf(events), "OrchestratorReplicaFallback")

	// with no replica that copied the data, the restore fails
	sc = copySidecars()
	sc.lose = map[string]error{"h3|" + dataCommand: errors.New("connection reset")}
	outcome, _, _ = runOf(t, sc, copySpec(true), nil)
	require.True(t, outcome.failed)
	require.Contains(t, outcome.message, "data restore of b1 failed on h3")
}

// A restore the operator restarts in the middle of the copy to detached waits for the copies it submitted, submits
// those it did not, and restores the data on the replicas it chose before.
func TestRestoreResumesInTheMiddleOfTheCopyToDetached(t *testing.T) {
	sc := copySidecars()
	sc.hosts["h1"].actions = []actionRow{{Command: copyCommand, Start: "2026-11-12 00:00:01", Finish: "2026-11-12 00:00:09",
		Status: actionSuccess}}
	sc.seconds = 10
	status := &api.BackupRunStatus{Operation: api.BackupRunOperationRestore, Step: stepCopy,
		Plan: map[string]string{planCopyToDetached: "true", planDataHosts: "h0 h3", planCopyHosts: "h1 h2"},
		Actions: []api.BackupRunAction{{Step: stepCopy, Host: "h1", Command: copyCommand, TimeFrom: "2026-11-12 00:00:01",
			Status: actionInProgress}}}
	outcome, status, _ := runOf(t, sc, copySpec(true), status)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h2: " + copyCommand, "h0: " + dataCommand, "h3: " + dataCommand}, sc.commands())
	require.True(t, slices.ContainsFunc(status.Actions, func(action api.BackupRunAction) bool {
		return (action.Step == stepCopy) && (action.Host == "h1") && (action.Status == actionSuccess)
	}))
	require.Equal(t, stepCleanup, status.Step)
}

// A replica whose local copy an earlier run of the restore downloaded the schema of alone, before spec.taskID reran it
// with the replicas copying the data, deletes it and downloads the backup whole.
func TestRestoreDownloadsWholeTheSchemaCopiesOfAnEarlierRun(t *testing.T) {
	sc := copySidecars()
	sc.hosts["h0"].local = []backupRow{{Name: "b1", Location: "local", DataSize: 1, MetadataSize: 1}}
	sc.hosts["h1"].local = []backupRow{{Name: "b1", Location: "local", MetadataSize: 1}}
	spec := copySpec(true)
	spec.PreviousActions = []api.BackupRunAction{
		{Step: stepDownload, Host: "h0", Command: "download --hardlink-exists-files b1", TimeFrom: "2026-11-11 00:00:00"},
		{Step: stepDownload, Host: "h1", Command: "download --schema b1", TimeFrom: "2026-11-11 00:00:00"},
	}
	outcome, status, _ := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{
		"h1: delete local b1", "h1: download --hardlink-exists-files b1",
		"h2: download --hardlink-exists-files b1", "h3: download --hardlink-exists-files b1",
	}, sc.commands()[:4])
	require.Equal(t, "h1", status.Plan[planSchemaCopies])
	require.Equal(t, "h0", status.Plan[planHadLocal])
}
