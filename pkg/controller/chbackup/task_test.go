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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// taskID returns a spec.taskID of the value.
func taskID(value string) *types.Id {
	return (*types.Id)(types.NewString(value))
}

// setTaskID sets the spec.taskID of the resource of the name, as an edit of the user does.
func setTaskID(t *testing.T, c client.Client, obj client.Object, name, value string) {
	t.Helper()
	require.NoError(t, c.Get(context.Background(), key(name), obj))
	switch typed := obj.(type) {
	case *api.ClickHouseBackup:
		typed.Spec.TaskID = taskID(value)
	case *api.ClickHouseRestore:
		typed.Spec.TaskID = taskID(value)
	case *api.ClickHouseBackupSchedule:
		typed.Spec.TaskID = taskID(value)
	}
	require.NoError(t, c.Update(context.Background(), obj))
}

// reconcileTimes reconciles the given number of times.
func reconcileTimes(t *testing.T, reconcile func() (ctrl.Result, error), times int) {
	t.Helper()
	for i := 0; i < times; i++ {
		_, err := reconcile()
		require.NoError(t, err)
	}
}

// A new spec.taskID of a completed backup makes a new backup, named at the time of the rerun: the backup it made stays
// in the storage and in status.previousBackupNames, and the backup, unfinished again, gets its finalizer back. The
// same taskID again does nothing.
func TestTaskIDRerunsACompletedBackupUnderANewName(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	created := time.Date(2026, time.November, 12, 1, 0, 0, 0, time.UTC)
	chb := newBackupCR("b1")
	chb.CreationTimestamp = meta.NewTime(created)
	c := newClient(t, nil, chb)
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(readyInstallation(t)))
	clock := clocktesting.NewFakePassiveClock(created.Add(time.Minute))
	controller.clock = clock
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("b1")) }
	reconcileUntil(t, reconcile, func() bool { return getBackup(t, c, "b1").Status.Phase == api.OperationPhaseCompleted })
	first := getBackup(t, c, "b1").Status.BackupName
	require.Equal(t, fmt.Sprintf("b1-%d", created.Unix()), first)

	rerun := created.Add(2 * time.Hour)
	clock.SetTime(rerun)
	setTaskID(t, c, &api.ClickHouseBackup{}, "b1", "t1")
	_, err := reconcile()
	require.NoError(t, err)
	got := getBackup(t, c, "b1")
	require.Contains(t, got.Finalizers, backupFinalizer, "the rerun backup is unfinished again")
	require.NotEqual(t, api.OperationPhaseCompleted, got.Status.Phase)
	require.Equal(t, []string{first}, got.Status.PreviousBackupNames)

	reconcileUntil(t, reconcile, func() bool { return getBackup(t, c, "b1").Status.Phase == api.OperationPhaseCompleted })
	got = getBackup(t, c, "b1")
	second := fmt.Sprintf("b1-%d", rerun.Unix())
	require.Equal(t, second, got.Status.BackupName)
	require.Equal(t, []string{first}, got.Status.PreviousBackupNames)
	require.Equal(t, "t1", got.Status.TaskID)
	require.Equal(t, []string{"t1"}, got.Status.TaskIDsStarted)
	require.Equal(t, []string{"t1"}, got.Status.TaskIDsCompleted)
	require.Equal(t, rerun, got.Status.RerunTime.UTC())
	require.Equal(t, []string{
		"chi-demo-main-0-0: create_remote --rbac " + first, "chi-demo-main-1-0: create_remote --rbac " + first,
		"chi-demo-main-0-0: create_remote --rbac " + second, "chi-demo-main-1-0: create_remote --rbac " + second,
	}, commandsHolding(sc, "create_remote"))
	require.Empty(t, commandsHolding(sc, "delete"), "the backup made first stays in the storage")
	rerunEvents := eventMessages(t, c, reasonTaskIDRerun)
	require.Len(t, rerunEvents, 1)
	require.Contains(t, rerunEvents[0], "spec.taskID t1 reruns the completed backup "+first)

	// The same taskID again does nothing
	made := sc.commands()
	reconcileTimes(t, reconcile, 3)
	require.Equal(t, made, sc.commands())
	require.Len(t, eventMessages(t, c, reasonTaskIDRerun), 1)
}

// A completed backup named by spec.backupName is not rerun under that name: the taskID is told ignored.
func TestTaskIDDoesNotRerunACompletedBackupUnderItsExplicitName(t *testing.T) {
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

	setTaskID(t, c, &api.ClickHouseBackup{}, "b1", "t1")
	reconcileTimes(t, reconcile, 2)
	got := getBackup(t, c, "b1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
	require.Equal(t, []string{"t1"}, got.Status.TaskIDsCompleted)
	require.Empty(t, got.Status.TaskIDsStarted)
	require.Equal(t, made, sc.commands())
	ignored := eventMessagesOfType(t, c, reasonTaskIDIgnored, core.EventTypeWarning)
	require.Len(t, ignored, 1)
	require.Contains(t, ignored[0], "set another spec.backupName")

	// Another name with another taskID makes the new backup
	got.Spec.BackupName = "manual2"
	got.Spec.TaskID = taskID("t2")
	require.NoError(t, c.Update(context.Background(), got))
	reconcileUntil(t, reconcile, func() bool {
		got := getBackup(t, c, "b1")
		return (got.Status.Phase == api.OperationPhaseCompleted) && (got.Status.BackupName == "manual2")
	})
	require.Equal(t, []string{"manual"}, getBackup(t, c, "b1").Status.PreviousBackupNames)
}

// A new spec.taskID of a backup that failed on one of its clusters makes the backup again under its name on that
// cluster alone, replacing the broken copy of it the failed run left there.
func TestTaskIDRerunsAFailedBackupUnderItsNameOnTheFailedClusters(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	sc.leave = true
	sc.fail["chi-demo-cold-0-0|create_remote"] = "upload failed"
	withSidecars(t, sc)
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	chi := twoClusters(t, api.StatusCompleted)
	paths := map[string]string{"REMOTE_STORAGE": "s3", "S3_BUCKET": "backups", "S3_PATH": "backup/{cluster}/{shard}"}
	c := newClient(t, nil, chb, storageSettings(t, chi, "hot", paths), storageSettings(t, chi, "cold", paths))
	controller := NewBackupController(c, c, newScheme(t), newFakeCR(chi))
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("b1")) }
	reconcileUntil(t, reconcile, func() bool { return getBackup(t, c, "b1").Status.Phase.IsTerminal() })
	got := getBackup(t, c, "b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	sc.mu.Lock()
	require.Equal(t, []backupRow{{Name: "manual", Location: "remote", Desc: "broken (can't stat metadata.json)"}},
		sc.hosts["chi-demo-cold-0-0"].remote, "the failed upload left a broken remote copy")
	delete(sc.fail, "chi-demo-cold-0-0|create_remote")
	sc.mu.Unlock()
	before := len(sc.commands())

	setTaskID(t, c, &api.ClickHouseBackup{}, "b1", "t1")
	reconcileUntil(t, reconcile, func() bool { return getBackup(t, c, "b1").Status.Phase == api.OperationPhaseCompleted })
	got = getBackup(t, c, "b1")
	require.Equal(t, "manual", got.Status.BackupName)
	require.Empty(t, got.Status.PreviousBackupNames)
	require.Equal(t, []string{
		"chi-demo-cold-0-0: delete remote manual",
		"chi-demo-cold-0-0: create_remote --rbac manual",
	}, sc.commands()[before:], "only the failed cluster reruns, replacing what it left")
	require.Len(t, got.Status.PreviousRuns, 1)
	require.Equal(t, "cold", got.Status.PreviousRuns[0].Cluster)
	require.Equal(t, []api.BackupRunHost{{Host: "chi-demo-cold-0-0", Backup: "manual"}}, got.Status.PreviousRuns[0].Hosts)
	require.Equal(t, "create_remote --rbac manual", got.Status.PreviousRuns[0].Actions[0].Command)
	rerun := eventMessages(t, c, reasonTaskIDRerun)
	require.Len(t, rerun, 1)
	require.Contains(t, rerun[0], "reruns backup manual under its name on cluster cold")
	require.Len(t, eventMessages(t, c, "OrchestratorRerunReplaced"), 1)
	require.Equal(t, api.BackupReasonCompleted, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	require.Equal(t, []string{"t1"}, got.Status.TaskIDsCompleted)
}

// A spec.taskID given while the backup runs reruns nothing, then or once the backup finished: it is told ignored and
// recorded done at once. The taskID the backup was created with is the one of its run.
func TestTaskIDIsIgnoredWhileTheBackupRuns(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "manual"
	chb.Spec.TaskID = taskID("t1")
	c, _, reconcile := runningBackup(t, sc, chb, readyInstallation(t))
	got := getBackup(t, c, "b1")
	require.Equal(t, "t1", got.Status.TaskID)
	require.Equal(t, []string{"t1"}, got.Status.TaskIDsStarted)
	require.Empty(t, eventMessages(t, c, reasonTaskIDIgnored))

	setTaskID(t, c, &api.ClickHouseBackup{}, "b1", "t2")
	reconcileTimes(t, reconcile, 2)
	ignored := eventMessagesOfType(t, c, reasonTaskIDIgnored, core.EventTypeWarning)
	require.Len(t, ignored, 1)
	require.Contains(t, ignored[0], "the backup has not finished")
	got = getBackup(t, c, "b1")
	require.Equal(t, "t1", got.Status.TaskID)
	require.Equal(t, []string{"t2"}, got.Status.TaskIDsCompleted)

	finishHanging(sc)
	reconcileUntil(t, reconcile, func() bool { return getBackup(t, c, "b1").Status.Phase == api.OperationPhaseCompleted })
	reconcileTimes(t, reconcile, 2)
	got = getBackup(t, c, "b1")
	require.Equal(t, []string{"t1", "t2"}, got.Status.TaskIDsCompleted)
	require.Len(t, commandsHolding(sc, "create_remote"), 2, "nothing reruns once the backup finished")
	require.Empty(t, eventMessages(t, c, reasonTaskIDRerun))
}

// A new spec.taskID of a failed restore runs it again on its cluster: status.previousRuns keeps the downloads the
// failed run made, which the rerun resumes. A completed restore is not rerun.
func TestTaskIDRerunsAFailedRestoreKeepingItsDownloads(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	sc.leave = true
	withSidecars(t, sc)
	for _, host := range []string{"chi-demo-main-0-0", "chi-demo-main-0-1"} {
		sc.hosts[host] = &fakeHost{remote: []backupRow{{Name: "nightly-20260101-000000", DataSize: 1, MetadataSize: 1}}}
	}
	sc.fail["chi-demo-main-0-1|download"] = "download failed"
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, newScheme(t), newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 1, true))))
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("r1")) }
	reconcileUntil(t, reconcile, func() bool { return getRestore(t, c, "r1").Status.Phase.IsTerminal() })
	require.Equal(t, api.OperationPhaseFailed, getRestore(t, c, "r1").Status.Phase)
	download := "download --hardlink-exists-files nightly-20260101-000000"
	sc.mu.Lock()
	delete(sc.fail, "chi-demo-main-0-1|download")
	sc.mu.Unlock()

	setTaskID(t, c, &api.ClickHouseRestore{}, "r1", "t1")
	_, err := reconcile()
	require.NoError(t, err)
	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Contains(t, got.Finalizers, restoreFinalizer)
	require.Len(t, got.Status.PreviousRuns, 1)
	var downloaded []string
	for _, action := range got.Status.PreviousRuns[0].Actions {
		downloaded = append(downloaded, action.Host+": "+action.Command)
	}
	require.ElementsMatch(t, []string{"chi-demo-main-0-0: " + download, "chi-demo-main-0-1: " + download}, downloaded,
		"the downloads of the failed run are kept")

	reconcileUntil(t, reconcile, func() bool { return getRestore(t, c, "r1").Status.Phase.IsTerminal() })
	got = getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase, got.Status.Clusters[0].Message)
	require.Len(t, got.Status.PreviousRuns, 1, "the history of the downloads stays")
	require.Equal(t, []string{"t1"}, got.Status.TaskIDsCompleted)
	require.Len(t, eventMessages(t, c, reasonTaskIDRerun), 1)
	require.Len(t, commandsHolding(sc, "chi-demo-main-0-1: "+download), 2, "the download runs again, resuming")

	// A completed restore is not rerun
	made := sc.commands()
	setTaskID(t, c, &api.ClickHouseRestore{}, "r1", "t2")
	reconcileTimes(t, reconcile, 2)
	require.Equal(t, made, sc.commands())
	require.Equal(t, api.OperationPhaseCompleted, getRestore(t, c, "r1").Status.Phase)
	ignored := eventMessagesOfType(t, c, reasonTaskIDIgnored, core.EventTypeWarning)
	require.Len(t, ignored, 1)
	require.Contains(t, ignored[0], "create a new ClickHouseRestore")
}

// The local copy of the backup the run of a restore before spec.taskID reran it downloaded is no copy the restore from
// another installation refuses: its download resumes. A copy no run downloaded is refused all the same.
func TestTaskIDRerunRestoreResumesTheDownloadsOfItsEarlierRuns(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h0"].local = []backupRow{{Name: "b1", Location: "local", Desc: "broken (can't stat metadata.json)"}}
	sc.hosts["h1"].local = []backupRow{{Name: "b1", Location: "local"}}
	spec := restoreSpec("h0", "h1")
	spec.PreviousActions = []api.BackupRunAction{{Step: stepDownload, Host: "h0", Command: "download --hardlink-exists-files --env=S3_PATH=other b1",
		TimeFrom: "2026-11-12 00:00:01", Status: "error"}}
	o := testOrchestrator(t, sc, spec)
	o.backupNames = map[string]string{"h0": "b1", "h1": "b1"}
	require.NoError(t, o.refuseLocalCopy(context.Background(), "h0"), "an earlier run of this restore downloaded the copy")
	require.ErrorContains(t, o.refuseLocalCopy(context.Background(), "h1"), "preflight failed: h1 holds a local copy of b1")
}

// A rerun backup replaces the copies of its backup an earlier run of it left, local and remote; a copy no earlier run
// left refuses it as ever, before anything is deleted.
func TestOrchestratorRerunReplacesOnlyWhatEarlierRunsLeft(t *testing.T) {
	sc := newFakeSidecars("h0", "h1")
	sc.hosts["h0"].remote = []backupRow{{Name: "b1", Location: "remote", Desc: "broken (can't stat metadata.json)"}}
	sc.hosts["h0"].local = []backupRow{{Name: "b1", Location: "local"}}
	spec := backupSpec("b1", "h0", "h1")
	spec.PreviousActions = []api.BackupRunAction{{Step: stepCreate, Host: "h0", Command: "create_remote --rbac b1", TimeFrom: "2026-11-12 00:00:01"}}
	outcome, _, events := runOf(t, sc, spec, nil)
	require.False(t, outcome.failed, outcome.message)
	require.Equal(t, []string{"h0: delete local b1", "h0: delete remote b1", "h0: create_remote --rbac b1", "h1: create_remote --rbac b1"},
		sc.commands())
	require.Contains(t, reasonsOf(events), "OrchestratorRerunReplaced")

	foreign := newFakeSidecars("h0", "h1")
	foreign.hosts["h1"].remote = []backupRow{{Name: "b1", Location: "remote"}}
	outcome, _, _ = runOf(t, foreign, spec, nil)
	require.True(t, outcome.failed)
	require.Equal(t, "backup b1 already exists in the storage of h1", outcome.message)
	require.Empty(t, foreign.commands(), "nothing is deleted before the refusal")
}

// A new spec.taskID of an aborted schedule resets it: it is validated again at once, its abort, which holds still,
// told anew; once its spec is fixed it is active. It runs nothing by itself.
func TestTaskIDResetsAnAbortedSchedule(t *testing.T) {
	sc := newFakeSidecars()
	sc.any = true
	withSidecars(t, sc)
	chbs := newScheduleCR("nightly")
	chbs.Spec.ReplicaSelection = "Bogus"
	chbs.CreationTimestamp = meta.NewTime(time.Date(2026, time.November, 12, 2, 0, 0, 0, time.UTC))
	c := newClient(t, nil, chbs)
	controller := NewScheduleController(c, c, newScheme(t), newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 1, true))))
	controller.clock = clocktesting.NewFakePassiveClock(time.Date(2026, time.November, 12, 2, 30, 0, 0, time.UTC))
	reconcile := func() (ctrl.Result, error) { return controller.Reconcile(context.Background(), request("nightly")) }
	reconcileTimes(t, reconcile, 1)
	require.Equal(t, api.SchedulePhaseAborted, getSchedule(t, c, "nightly").Status.Phase)
	require.Len(t, eventMessages(t, c, reasonScheduleAborted), 1)

	setTaskID(t, c, &api.ClickHouseBackupSchedule{}, "nightly", "t1")
	reconcileTimes(t, reconcile, 2)
	got := getSchedule(t, c, "nightly")
	require.Equal(t, api.SchedulePhaseAborted, got.Status.Phase, "the spec is invalid still")
	require.Equal(t, api.BackupReasonInvalidSpec, got.Status.Reason)
	require.Len(t, eventMessages(t, c, reasonTaskIDReset), 1)
	require.Len(t, eventMessages(t, c, reasonScheduleAborted), 2, "the abort is told anew")
	require.Equal(t, "t1", got.Status.TaskID)
	require.Equal(t, []string{"t1"}, got.Status.TaskIDsStarted)
	require.Equal(t, []string{"t1"}, got.Status.TaskIDsCompleted)

	got.Spec.ReplicaSelection = ""
	got.Spec.TaskID = taskID("t2")
	require.NoError(t, c.Update(context.Background(), got))
	reconcileTimes(t, reconcile, 2)
	got = getSchedule(t, c, "nightly")
	require.Equal(t, api.SchedulePhaseActive, got.Status.Phase)
	require.Len(t, eventMessages(t, c, reasonTaskIDReset), 2)
	require.Equal(t, []string{"t2", "t1"}, got.Status.TaskIDsCompleted)
	require.Empty(t, sc.commands(), "the reset runs nothing")

	// A taskID of an active schedule is recorded, and resets nothing
	setTaskID(t, c, &api.ClickHouseBackupSchedule{}, "nightly", "t3")
	reconcileTimes(t, reconcile, 1)
	require.Len(t, eventMessages(t, c, reasonTaskIDReset), 2)
	require.Equal(t, "t3", getSchedule(t, c, "nightly").Status.TaskID)
}

// The task ids a status records are bounded, newest first, as those of an installation.
func TestBackupTaskStatusIsBounded(t *testing.T) {
	var task api.BackupTaskStatus
	for i := 0; i < 12; i++ {
		id := fmt.Sprint(i)
		require.False(t, task.Seen(id))
		task.Start(id)
		task.Complete()
		task.Complete()
		require.True(t, task.Seen(id))
	}
	require.Len(t, task.TaskIDsStarted, 10)
	require.Len(t, task.TaskIDsCompleted, 10)
	require.Equal(t, "11", task.TaskIDsStarted[0])
	require.Equal(t, "11", task.TaskIDsCompleted[0])
}

// A rerun records as the hosts of the earlier run only those it sent a command to. A backup of the same name on the
// other hosts belongs to another backup, and deleting this one must not delete it.
func TestRerunRecordsOnlyTheHostsTheEarlierRunSentACommandTo(t *testing.T) {
	chb := newBackupCR("b1")
	chb.Status.BackupName = "manual"
	cluster := &api.OperationClusterStatus{Name: "main", Hosts: []string{"h0", "h1"}, Run: &api.BackupRunStatus{}}
	require.Empty(t, runOfBackup(chb, cluster).Hosts, "a run that sent no command made no backup")

	cluster.Run.Actions = []api.BackupRunAction{{Step: stepCreate, Host: "h0", Command: "create_remote manual", TimeFrom: "t"}}
	require.Equal(t, []api.BackupRunHost{{Host: "h0", Backup: "manual"}}, runOfBackup(chb, cluster).Hosts)

	cluster.Run.Actions = nil
	cluster.Run.Mutated = true
	require.Equal(t, []api.BackupRunHost{{Host: "h0", Backup: "manual"}, {Host: "h1", Backup: "manual"}},
		runOfBackup(chb, cluster).Hosts, "a command that may have reached a sidecar without a record counts on every host")
}
