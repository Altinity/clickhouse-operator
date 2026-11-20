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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// eventsOf returns the messages of the Events of the reason.
func eventsOf(events []postedEvent, reason string) []string {
	var messages []string
	for _, e := range events {
		if e.reason == reason {
			messages = append(messages, e.message)
		}
	}
	return messages
}

// commandsHolding returns the commands inserted whose text holds the text.
func commandsHolding(sc *fakeSidecars, text string) []string {
	var out []string
	for _, command := range sc.commands() {
		if strings.Contains(command, text) {
			out = append(out, command)
		}
	}
	return out
}

// A failed backup deletes the local copy it left on the hosts it failed on, and nothing remote: the broken remote
// copy is left to retention.
func TestOrchestratorDropsOnlyTheLocalCopyOfAFailedBackup(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.leave = true
	sc.fail["h1|create_remote"] = "upload failed"
	outcome, _, events := runOf(t, sc, backupSpec("b1", "h0", "h1"), nil)
	require.True(t, outcome.failed)
	require.Equal(t, []string{"h0: create_remote --rbac b1", "h1: create_remote --rbac b1", "h1: delete local b1"}, sc.commands())
	require.Empty(t, sc.hosts["h1"].local)
	require.Len(t, sc.hosts["h1"].remote, 1, "the broken remote copy is left to retention")
	require.Len(t, eventsOf(events, "OrchestratorFailedBackupDeleted"), 1)
	require.Len(t, eventsOf(events, "OrchestratorFailedBackupRemoteLeft"), 1)
	require.Contains(t, eventsOf(events, "OrchestratorFailedBackupRemoteLeft")[0], "retention")
}

// A failed backup whose sidecar refuses the delete of its local copy as busy leaves it at once, with a Warning that
// tells how to delete it, and the run ends.
func TestOrchestratorLeavesTheFailedBackupOfABusySidecar(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.leave = true
	sc.fail["h1|create_remote"] = "upload failed"
	sc.refuse = []string{"delete local"}
	done := make(chan struct{})
	var outcome runOutcome
	var events []postedEvent
	go func() {
		defer close(done)
		outcome, _, events = runOf(t, sc, backupSpec("b1", "h0", "h1"), nil)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the run waits for the busy sidecar")
	}
	require.True(t, outcome.failed)
	require.Empty(t, commandsHolding(sc, "delete"))
	left := eventsOf(events, "OrchestratorFailedBackupLeft")
	require.Len(t, left, 1)
	require.Contains(t, left[0], "h1: the local copy of the failed backup b1 is left: the sidecar runs another command")
	require.Contains(t, left[0], "INSERT INTO system.backup_actions(command) VALUES ('delete local b1')")
}

// A host whose upload outlives the action timeout fails, and its sidecar, still busy with it, gets no delete: the run
// ends at once, and the backup the upload may yet complete is not deleted from the remote storage.
func TestOrchestratorEndsAfterTheActionTimeoutOfABusySidecar(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h1"].hang = "create_remote"
	spec := withTimeouts(backupSpec("b1", "h0", "h1"), api.BackupTimeouts{ActionTimeout: dur(1 * time.Second)})
	done := make(chan struct{})
	var outcome runOutcome
	var events []postedEvent
	go func() {
		defer close(done)
		outcome, _, events = runOf(t, sc, spec, nil)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("the run waits for the busy sidecar")
	}
	require.True(t, outcome.failed)
	require.Contains(t, outcome.message, "Action Timeout")
	require.Equal(t, []string{"h0: create_remote --rbac b1", "h1: create_remote --rbac b1"}, sc.commands(),
		"no delete reaches the busy sidecar, and no delete remote any")
	left := eventsOf(events, "OrchestratorFailedBackupLeft")
	require.Len(t, left, 1)
	require.Contains(t, left[0], "the sidecar runs another command")
}

// A restore that fails at its download, its schema or its data deletes nothing it downloaded, and tells the copies it
// keeps and how to delete them.
func TestOrchestratorKeepsTheDownloadsOfAFailedRestore(t *testing.T) {
	for _, failing := range []string{"h1|download", "h1|restore --schema", "h0|restore --data"} {
		t.Run(failing, func(t *testing.T) {
			sc := newFakeSidecars("h0", "h1")
			sc.leave = true
			for _, host := range []string{"h0", "h1"} {
				sc.hosts[host].remote = []backupRow{{Name: "b1", DataSize: 1, MetadataSize: 1}}
			}
			sc.fail[failing] = "failed"
			outcome, _, events := runOf(t, sc, restoreSpec("h0", "h1"), nil)
			require.True(t, outcome.failed)
			require.Empty(t, commandsHolding(sc, "delete"), "a failed restore deletes nothing")
			for _, host := range []string{"h0", "h1"} {
				require.Len(t, sc.hosts[host].local, 1, host)
			}
			kept := eventsOf(events, "OrchestratorRestoreDownloadsKept")
			require.Len(t, kept, 1)
			require.Contains(t, kept[0], "h0=b1 h1=b1")
			require.Contains(t, kept[0], "INSERT INTO system.backup_actions(command) VALUES ('delete local <backup>')")
		})
	}
}

// A restore from the storage paths of another installation refuses a host that holds a local copy of the backup,
// unless the status of this restore records that it downloaded that copy there, whose download it then resumes.
func TestOrchestratorRefusesALocalCopyItDidNotDownload(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h0"].local = []backupRow{{Name: "b1", Location: "local", Desc: "broken (can't stat metadata.json)"}}
	sc.hosts["h1"].local = []backupRow{{Name: "b1", Location: "local"}}
	o := testOrchestrator(t, sc, restoreSpec("h0", "h1"))
	o.backupNames = map[string]string{"h0": "b1", "h1": "b1"}
	err := o.refuseLocalCopy(context.Background(), "h0")
	require.ErrorContains(t, err, "preflight failed: h0 holds a local copy of b1")

	o = testOrchestrator(t, sc, restoreSpec("h0", "h1"))
	o.backupNames = map[string]string{"h0": "b1", "h1": "b1"}
	o.status.Actions = []api.BackupRunAction{
		{Step: stepDownload, Host: "h0", Command: "download --hardlink-exists-files --env=S3_PATH=other b1", TimeFrom: "2026-11-12 00:00:01",
			Status: "error"},
		{Step: stepSource, Host: "h1", Command: "download --schema --rbac --env=S3_PATH=other b2", TimeFrom: "2026-11-12 00:00:01",
			Status: actionSuccess},
	}
	require.NoError(t, o.refuseLocalCopy(context.Background(), "h0"), "this restore downloaded the copy, whatever its status")
	require.ErrorContains(t, o.refuseLocalCopy(context.Background(), "h1"), "preflight failed: h1 holds a local copy of b1",
		"a download of another backup is no download of this one")
}
