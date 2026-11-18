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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// The backup hosts of the ready installation of the tests: the first replica of each of its two shards.
var backupHosts = []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}

// gone reports whether the resource of the name is gone.
func gone(t *testing.T, c client.Client, obj client.Object, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), key(name), obj)
	if apiErrors.IsNotFound(err) {
		return true
	}
	require.NoError(t, err)
	return false
}

// finishHanging finishes, successfully, every command a host keeps in progress, as an upload or a download that ends.
func finishHanging(sc *fakeSidecars) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	for _, host := range sc.hosts {
		host.hang = ""
		for i := range host.actions {
			row := &host.actions[i]
			if row.Status != actionInProgress {
				continue
			}
			row.Status, row.Finish = actionSuccess, sc.now()
			fields := strings.Fields(row.Command)
			backup := fields[len(fields)-1]
			switch {
			case strings.HasPrefix(row.Command, "create_remote"):
				host.remote = append(host.remote, backupRow{Name: backup, Location: "remote", DataSize: 100, MetadataSize: 10})
			case strings.HasPrefix(row.Command, "download"):
				host.local = append(host.local, backupRow{Name: backup, Location: "local", DataSize: 100, MetadataSize: 10})
			}
		}
	}
}

// runningBackup returns a backup whose create_remote runs on both backup hosts, which keep it in progress, and the
// controller that runs it.
func runningBackup(t *testing.T, sc *fakeSidecars, chb *api.ClickHouseBackup, chi *api.ClickHouseInstallation) (client.Client,
	*BackupController, func() (ctrl.Result, error)) {
	t.Helper()
	for _, host := range backupHosts {
		sc.hosts[host] = &fakeHost{hang: "create_remote"}
	}
	c := newClient(t, nil, chb)
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(chi))
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request(chb.Name)) }
	reconcileUntil(t, reconcile, func() bool { return len(commandsHolding(sc, "create_remote")) == 2 })
	reconcileUntil(t, reconcile, func() bool {
		clusters := getBackup(t, c, chb.Name).Status.Clusters
		return (len(clusters) == 1) && (clusters[0].Run != nil) && (len(clusters[0].Run.Hosts) == 2)
	})
	got := getBackup(t, c, chb.Name)
	require.Contains(t, got.Finalizers, backupFinalizer)
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
	return c, controller, reconcile
}

// A backup deleted while it runs has its runs stopped, the create_remote of it the sidecars run killed, its local
// copies and its partly uploaded remote copy deleted, the latter once per shard, then goes.
func TestDeletedRunningBackupIsKilledAndDeleted(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	c, _, reconcile := runningBackup(t, sc, chb, readyInstallation(t))
	sc.mu.Lock()
	sc.hosts["chi-demo-main-0-0"].remote = []backupRow{{Name: "manual", Location: "remote", Desc: "broken (can't stat metadata.json)"}}
	sc.mu.Unlock()

	require.NoError(t, c.Delete(context.Background(), getBackup(t, c, "b1")))
	reconcileUntil(t, reconcile, func() bool { return gone(t, c, &api.ClickHouseBackup{}, "b1") })
	commands := sc.commands()
	for _, host := range backupHosts {
		require.Contains(t, commands, host+`: kill "create_remote --rbac manual"`)
		require.Contains(t, commands, host+": delete local manual")
	}
	require.Equal(t, []string{"chi-demo-main-0-0: delete remote manual"}, commandsHolding(sc, "delete remote"))
	require.Len(t, eventMessages(t, c, reasonDeletionCleanup), 1)
	require.Len(t, eventMessages(t, c, reasonDeletionCleanedUp), 1)
	require.Empty(t, eventMessages(t, c, reasonDeletionLeftover))
	require.Len(t, eventMessages(t, c, "OrchestratorDeletionKilled"), 2)
}

// A backup deleted once it completed keeps its remote backup, which retention owns: no command is sent, and it goes.
func TestDeletedCompletedBackupKeepsItsRemoteBackup(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	c := newClient(t, nil, chb)
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(readyInstallation(t)))
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("b1")) }
	reconcileUntil(t, reconcile, func() bool { return getBackup(t, c, "b1").Status.Phase == api.OperationPhaseCompleted })
	made := sc.commands()

	require.NoError(t, c.Delete(context.Background(), getBackup(t, c, "b1")))
	_, err := reconcile()
	require.NoError(t, err)
	require.True(t, gone(t, c, &api.ClickHouseBackup{}, "b1"))
	require.Equal(t, made, sc.commands(), "the deletion sends no command")
	kept := eventMessages(t, c, reasonDeletionRemoteBackupKept)
	require.Len(t, kept, 1)
	require.Contains(t, kept[0], "keeps the remote backup")
}

// The deletion of a backup never kills a command of anything else the sidecar runs: the delete of the local copy the
// sidecar then refuses as busy is left, told in a Warning with how to delete it by hand, and the backup goes.
func TestDeletedBackupNeverKillsAnUnrelatedCommand(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	other := "create_remote --rbac other"
	sc.hosts["chi-demo-main-0-0"] = &fakeHost{
		actions: []actionRow{{Command: other, Start: "2026-11-11 00:00:00", Status: actionInProgress}},
		local:   []backupRow{{Name: "manual", Location: "local"}},
	}
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	c := newClient(t, nil, chb)
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(readyInstallation(t)))
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("b1")) }
	// The backup waits for the sidecar of the first host, busy with the other command
	reconcileUntil(t, reconcile, func() bool {
		clusters := getBackup(t, c, "b1").Status.Clusters
		return (len(clusters) == 1) && (clusters[0].Run != nil) && (len(clusters[0].Run.Hosts) == 2)
	})

	require.NoError(t, c.Delete(context.Background(), getBackup(t, c, "b1")))
	reconcileUntil(t, reconcile, func() bool { return gone(t, c, &api.ClickHouseBackup{}, "b1") })
	require.Empty(t, commandsHolding(sc, "kill"), "the other command is not the backup's")
	require.Empty(t, commandsHolding(sc, "delete"), "the busy sidecar refuses the delete")
	sc.mu.Lock()
	require.True(t, sc.hosts["chi-demo-main-0-0"].actions[0].running(), "the other command still runs")
	sc.mu.Unlock()
	left := eventMessagesOfType(t, c, reasonDeletionLeftover, core.EventTypeWarning)
	require.Len(t, left, 1)
	require.Contains(t, left[0], "chi-demo-main-0-0=manual")
	require.Contains(t, left[0], "INSERT INTO system.backup_actions(command) VALUES ('delete local manual')")
	require.Contains(t, left[0], "INSERT INTO system.backup_actions(command) VALUES ('delete remote manual')")
	hostLeft := eventMessages(t, c, "OrchestratorDeletionLeft")
	require.Len(t, hostLeft, 1)
	require.Contains(t, hostLeft[0], "the sidecar runs another command")
}

// A host that does not answer is retried for the unreachable timeout of the backup, then what the backup left there is
// told in a Warning and the backup goes; the other hosts are cleaned up all the same.
func TestDeletedBackupGivesUpOnAnUnreachableHost(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	chb.Spec.Timeouts = &api.BackupTimeouts{UnreachableTimeout: dur(time.Second)}
	c, _, reconcile := runningBackup(t, sc, chb, readyInstallation(t))
	sc.mu.Lock()
	sc.hosts["chi-demo-main-1-0"].runningError = errors.New("connection refused")
	sc.mu.Unlock()

	started := time.Now()
	require.NoError(t, c.Delete(context.Background(), getBackup(t, c, "b1")))
	reconcileUntil(t, reconcile, func() bool { return gone(t, c, &api.ClickHouseBackup{}, "b1") })
	require.GreaterOrEqual(t, time.Since(started), time.Second, "the host is retried for the unreachable timeout")
	require.Contains(t, sc.commands(), `chi-demo-main-0-0: kill "create_remote --rbac manual"`)
	require.Contains(t, sc.commands(), "chi-demo-main-0-0: delete local manual")
	require.Empty(t, commandsHolding(sc, "chi-demo-main-1-0: kill"))
	left := eventMessagesOfType(t, c, reasonDeletionLeftover, core.EventTypeWarning)
	require.Len(t, left, 1)
	require.Contains(t, left[0], "chi-demo-main-1-0=manual")
	require.Contains(t, left[0], "connection refused")
}

// A backup whose installation is being deleted, or is gone, gives up at once: a host that does not answer is not
// waited for, and what is left is told in a Warning.
func TestDeletedBackupGivesUpAtOnceWithItsInstallation(t *testing.T) {
	for _, chiGone := range []bool{false, true} {
		t.Run(map[bool]string{false: "installation deleting", true: "installation gone"}[chiGone], func(t *testing.T) {
			sc := newFakeSidecars()
			sc.any = true
			withSidecars(t, sc)
			chb := newBackupCR("b1")
			chb.Spec.BackupName = "manual"
			chi := readyInstallation(t)
			c, controller, reconcile := runningBackup(t, sc, chb, chi)
			sc.mu.Lock()
			for _, host := range backupHosts {
				sc.hosts[host].runningError = errors.New("connection refused")
			}
			sc.mu.Unlock()
			if chiGone {
				controller.cr = newFakeCR()
			} else {
				now := meta.Now()
				chi.DeletionTimestamp = &now
				controller.cr = newFakeCR(chi)
			}

			started := time.Now()
			require.NoError(t, c.Delete(context.Background(), getBackup(t, c, "b1")))
			reconcileUntil(t, reconcile, func() bool { return gone(t, c, &api.ClickHouseBackup{}, "b1") })
			require.Less(t, time.Since(started), 5*time.Second, "the default unreachable timeout of 10m is not waited for")
			require.Empty(t, commandsHolding(sc, "kill"))
			left := eventMessagesOfType(t, c, reasonDeletionLeftover, core.EventTypeWarning)
			require.Len(t, left, 1)
			if chiGone {
				require.Contains(t, left[0], "left on chi-demo-main-0-0=manual chi-demo-main-1-0=manual: ClickHouseInstallation demo not found")
			} else {
				require.Contains(t, left[0], "chi-demo-main-0-0=manual (chi-demo-main-0-0: cannot read the commands in progress: connection refused)")
				require.Contains(t, left[0], "chi-demo-main-1-0=manual (chi-demo-main-1-0: cannot read the commands in progress: connection refused)")
			}
		})
	}
}

// A restore deleted while it runs keeps its finalizer until its run ends, which is never killed, then goes; nothing it
// downloaded is deleted.
func TestDeletedRestoreWaitsForItsRun(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	for _, host := range []string{"chi-demo-main-0-0", "chi-demo-main-0-1"} {
		sc.hosts[host] = &fakeHost{remote: []backupRow{{Name: "nightly-20260101-000000", DataSize: 1, MetadataSize: 1}}, hang: "download"}
	}
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, newScheme(t), newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 1, true))))
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("r1")) }
	reconcileUntil(t, reconcile, func() bool { return len(commandsHolding(sc, "download")) > 0 })
	require.Contains(t, getRestore(t, c, "r1").Finalizers, restoreFinalizer)

	require.NoError(t, c.Delete(context.Background(), getRestore(t, c, "r1")))
	for i := 0; i < 3; i++ {
		_, err := reconcile()
		require.NoError(t, err)
	}
	got := getRestore(t, c, "r1")
	require.NotNil(t, got.DeletionTimestamp)
	require.Contains(t, got.Finalizers, restoreFinalizer, "the run in progress is waited for")
	waiting := eventMessages(t, c, reasonDeletionWaitingForRun)
	require.Len(t, waiting, 1, "told once")
	require.Contains(t, waiting[0], "waiting for the restore run to finish before deletion")

	finishHanging(sc)
	reconcileUntil(t, reconcile, func() bool { return gone(t, c, &api.ClickHouseRestore{}, "r1") })
	require.Empty(t, commandsHolding(sc, "kill"))
	require.NotEmpty(t, commandsHolding(sc, "restore"), "the run went on to its end, which deletes its downloads as ever")
	require.Len(t, eventMessages(t, c, reasonDeletionRunFinished), 1)
}

// A schedule deleted starts no slot from then on; one whose run is in progress keeps its finalizer until the run, its
// retention included, ends, then goes.
func TestDeletedScheduleStartsNoSlotAndWaitsForItsRun(t *testing.T) {
	newSchedule := func(t *testing.T, sc *fakeSidecars) (client.Client, *clocktesting.FakePassiveClock, func() (ctrl.Result, error)) {
		chbs := newScheduleCR("nightly")
		chbs.Spec.BackupNamePrefix = "nightly"
		chbs.CreationTimestamp = meta.NewTime(time.Date(2026, time.November, 12, 2, 0, 0, 0, time.UTC))
		c := newClient(t, nil, chbs)
		controller := NewScheduleController(c, c, newScheme(t), newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 1, true))))
		clock := clocktesting.NewFakePassiveClock(time.Date(2026, time.November, 12, 2, 30, 0, 0, time.UTC))
		controller.clock = clock
		return c, clock, func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("nightly")) }
	}

	t.Run("no slot", func(t *testing.T) {
		sc := newFakeSidecars()
		sc.any = true
		withSidecars(t, sc)
		c, clock, reconcile := newSchedule(t, sc)
		_, err := reconcile()
		require.NoError(t, err)
		require.Contains(t, getSchedule(t, c, "nightly").Finalizers, scheduleFinalizer)
		require.NoError(t, c.Delete(context.Background(), getSchedule(t, c, "nightly")))
		clock.SetTime(time.Date(2026, time.November, 12, 3, 0, 5, 0, time.UTC))
		_, err = reconcile()
		require.NoError(t, err)
		require.True(t, gone(t, c, &api.ClickHouseBackupSchedule{}, "nightly"))
		require.Empty(t, sc.commands(), "the slot due after the deletion never starts")
	})

	t.Run("run in progress", func(t *testing.T) {
		sc := newFakeSidecars()
		sc.any = true
		withSidecars(t, sc)
		sc.hosts["chi-demo-main-0-0"] = &fakeHost{hang: "create_remote"}
		c, clock, reconcile := newSchedule(t, sc)
		clock.SetTime(time.Date(2026, time.November, 12, 3, 0, 5, 0, time.UTC))
		reconcileUntil(t, reconcile, func() bool { return len(sc.commands()) == 1 })
		require.Len(t, getSchedule(t, c, "nightly").Status.Active, 1)

		require.NoError(t, c.Delete(context.Background(), getSchedule(t, c, "nightly")))
		clock.SetTime(time.Date(2026, time.November, 13, 3, 0, 5, 0, time.UTC))
		for i := 0; i < 3; i++ {
			_, err := reconcile()
			require.NoError(t, err)
		}
		got := getSchedule(t, c, "nightly")
		require.Contains(t, got.Finalizers, scheduleFinalizer, "the run in progress is waited for")
		require.Len(t, got.Status.Active, 1)
		require.Len(t, eventMessages(t, c, reasonDeletionWaitingForRun), 1)

		finishHanging(sc)
		reconcileUntil(t, reconcile, func() bool { return gone(t, c, &api.ClickHouseBackupSchedule{}, "nightly") })
		require.Equal(t, []string{"chi-demo-main-0-0: create_remote --rbac nightly-20261112030000"}, sc.commands(),
			"the slot of the next day never starts, the run in progress is never killed")
		require.Len(t, eventMessages(t, c, reasonDeletionRunFinished), 1)
		require.Len(t, eventMessages(t, c, reasonScheduledBackupSucceeded), 1, "the outcome of the run is recorded")
	})
}

// The command that kills a command quotes it as one argument, as the sidecar splits it.
func TestKillCommandQuotesTheCommand(t *testing.T) {
	require.Equal(t, `kill "create_remote --rbac b1"`, killCommand("create_remote --rbac b1"))
	require.Equal(t, `kill "upload --env=X=\"a\\b\" b1"`, killCommand(`upload --env=X="a\b" b1`))
	require.True(t, namesBackup("create_remote --rbac --diff-from-remote=b0 b1", "b1"))
	require.False(t, namesBackup("create_remote --rbac --diff-from-remote=b1 b2", "b1"), "a base is no backup of the deletion")
	require.False(t, namesBackup("b1", "b1"))
}
