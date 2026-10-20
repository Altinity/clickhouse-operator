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
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	clocktesting "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// backupCreated is when the backups of the tests that tell the time were created
var backupCreated = time.Date(2026, time.October, 19, 22, 0, 0, 0, time.UTC)

// at makes the controller of the fixture tell the given time, and returns its clock.
func (f *backupFixture) at(now time.Time) *clocktesting.FakePassiveClock {
	clock := clocktesting.NewFakePassiveClock(now)
	f.controller.clock = clock
	return clock
}

// newBackupCreated returns a backup created at backupCreated.
func newBackupCreated(name string) *api.ClickHouseBackup {
	chb := newBackupCR(name)
	chb.CreationTimestamp = meta.NewTime(backupCreated)
	return chb
}

// A backup scheduled for a later time waits for it, rechecking just past it, and at least hourly, as the wall clock
// may jump. It is named after that time. The time can be moved until the backup starts.
func TestBackupWaitsForItsScheduledTime(t *testing.T) {
	chb := newBackupCreated("b1")
	chb.Spec.ScheduledTime = "2026-10-20T02:00:00+02:00"
	due := time.Date(2026, time.October, 20, 0, 0, 0, 0, time.UTC)
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
	clock := f.at(due.Add(-30 * time.Minute))

	require.Equal(t, 30*time.Minute+dueRecheckDelay, f.reconcile("b1").RequeueAfter)
	got := f.backup("b1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	validated := condition(t, got.Status.Conditions, api.BackupConditionValidated)
	require.Equal(t, api.BackupReasonWaitingForScheduledTime, validated.Reason)
	require.Equal(t, "the backup is due at 2026-10-20T00:00:00Z", validated.Message)
	require.Zero(t, jobCount(t, f.client))

	clock.SetTime(due.Add(-72 * time.Hour))
	require.Equal(t, dueRecheckCap, f.reconcile("b1").RequeueAfter)

	clock.SetTime(due)
	f.reconcile("b1")
	got = f.backup("b1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
	require.Equal(t, "b1-"+strconv.FormatInt(due.Unix(), 10), got.Status.BackupName)

	moved := newBackupCreated("b2")
	moved.Spec.ScheduledTime = "2026-10-20T02:00:00+02:00"
	f = newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, moved)
	f.at(due.Add(-30 * time.Minute))
	f.reconcile("b2")
	moved = f.backup("b2")
	moved.Spec.ScheduledTime = "2026-10-20T00:10:00Z"
	require.NoError(t, f.client.Update(context.Background(), moved))
	require.Equal(t, 40*time.Minute+dueRecheckDelay, f.reconcile("b2").RequeueAfter, "the backup waits for the time it names now")
}

// A backup that cannot be validated by its starting deadline fails, telling what it waited for; until then it
// rechecks no later than just past the deadline.
func TestBackupFailsPastItsStartingDeadlineBeforeItIsValidated(t *testing.T) {
	chb := newBackupCreated("b1")
	chb.Spec.StartingDeadlineSeconds = ptr.To(int64(60))
	f := newBackupFixture(t, newFakeCR(installation(t, api.StatusInProgress, backupCluster("main", 2, true))), nil, chb)
	clock := f.at(backupCreated.Add(30 * time.Second))

	require.Equal(t, pendingRequeueInterval, f.reconcile("b1").RequeueAfter)
	waited := condition(t, f.backup("b1").Status.Conditions, api.BackupConditionValidated).Message
	clock.SetTime(backupCreated.Add(50 * time.Second))
	require.Equal(t, 10*time.Second+dueRecheckDelay, f.reconcile("b1").RequeueAfter, "rechecked just past the deadline")

	clock.SetTime(backupCreated.Add(61 * time.Second))
	require.Equal(t, ctrl.Result{}, f.reconcile("b1"))
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonStartingDeadlineExceeded, ready.Reason)
	require.Equal(t, "not started by 2026-10-19T22:01:00Z, 60s after the backup was due: "+waited, ready.Message)
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonStartingDeadlineExceeded))
	require.Zero(t, jobCount(t, f.client))
}

// A cluster whose Job is not created by the starting deadline, as another operation runs there, fails.
func TestBackupFailsOnAClusterPastItsStartingDeadline(t *testing.T) {
	chb := newBackupCreated("b1")
	chb.Spec.StartingDeadlineSeconds = ptr.To(int64(60))
	running := jobOwnedBy(t, "other-job", newRestoreCR("other"), model.Labels(api.ClickHouseRestoreCRDResourceKind, "other", testCHI, "main", testStorage))
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb, running)
	clock := f.at(backupCreated.Add(50 * time.Second))

	require.Equal(t, 10*time.Second+dueRecheckDelay, f.reconcile("b1").RequeueAfter)
	got := f.backup("b1")
	require.Equal(t, api.BackupReasonOperationInProgress, got.Status.Clusters[0].Reason)
	waited := got.Status.Clusters[0].Message

	clock.SetTime(backupCreated.Add(61 * time.Second))
	f.reconcile("b1")
	got = f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.OperationPhaseFailed, got.Status.Clusters[0].Phase)
	require.Equal(t, api.BackupReasonStartingDeadlineExceeded, got.Status.Clusters[0].Reason)
	require.Equal(t, "not started by 2026-10-19T22:01:00Z, 60s after the backup was due: "+waited, got.Status.Clusters[0].Message)
	require.Equal(t, 1, jobCount(t, f.client), "no backup Job joins the operation running there")
}

// A backup that runs a schedule waits for the schedule to exist, then makes the backup the schedule makes, with the
// options it records of it, under a name of the schedule's prefix: whatever the schedule becomes afterwards.
func TestBackupRunsASchedule(t *testing.T) {
	chb := newBackupCreated("pre-upgrade")
	chb.Spec.ClickHouseInstallation = ""
	chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "nightly"}
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
	f.at(backupCreated)

	require.Equal(t, pendingRequeueInterval, f.reconcile("pre-upgrade").RequeueAfter)
	require.Equal(t, api.BackupReasonScheduleNotFound, condition(t, f.backup("pre-upgrade").Status.Conditions, api.BackupConditionValidated).Reason)
	require.Zero(t, jobCount(t, f.client))

	chbs := newScheduleCR("nightly")
	chbs.Spec.Tables = "db.*"
	require.NoError(t, f.client.Create(context.Background(), chbs))
	f.reconcile("pre-upgrade")
	got := f.backup("pre-upgrade")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
	require.Equal(t, &api.BackupScheduleRecord{
		Name:                   "nightly",
		Generation:             chbs.GetGeneration(),
		ClickHouseInstallation: testCHI,
		BackupNamePrefix:       model.BackupNamePrefix(chbs),
		BackupOptions:          api.BackupOptions{Tables: "db.*"},
	}, got.Status.Schedule)
	require.Equal(t, model.BackupNamePrefix(chbs)+"-20261019T220000", got.Status.BackupName)
	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("pre-upgrade-main-backup"), job))
	require.Contains(t, envValues(job.Spec.Template.Spec), "db.*")
	require.Equal(t, model.Labels(api.ClickHouseBackupCRDResourceKind, "pre-upgrade", testCHI, "main", testStorage), job.Labels)

	edited := &api.ClickHouseBackupSchedule{}
	require.NoError(t, f.client.Get(context.Background(), key("nightly"), edited))
	edited.Spec.Tables = "other.*"
	require.NoError(t, f.client.Update(context.Background(), edited))
	f.reconcile("pre-upgrade")
	require.Equal(t, "db.*", f.backup("pre-upgrade").Status.Schedule.Tables, "a validated backup keeps what it recorded")
}

// A backup that runs a schedule waits while the schedule's spec is invalid, and is refused for a cluster the
// schedule does not back up, as neither of them can change that.
func TestBackupRunsAScheduleOnlyAsTheScheduleAllows(t *testing.T) {
	invalid := newScheduleCR("nightly")
	invalid.Spec.ConcurrencyPolicy = "Sometimes"
	chb := newBackupCreated("b1")
	chb.Spec.ClickHouseInstallation = ""
	chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "nightly"}
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb, invalid)
	f.at(backupCreated)
	require.Equal(t, pendingRequeueInterval, f.reconcile("b1").RequeueAfter)
	require.Equal(t, api.BackupReasonInvalidSpec, condition(t, f.backup("b1").Status.Conditions, api.BackupConditionValidated).Reason)

	one := newScheduleCR("nightly")
	one.Spec.Cluster = "main"
	chb = newBackupCreated("b1")
	chb.Spec.ClickHouseInstallation = ""
	chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "nightly"}
	chb.Spec.Cluster = "cold"
	f = newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb, one)
	f.at(backupCreated)
	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonClusterNotFound, ready.Reason)
	require.Equal(t, "ClickHouseBackupSchedule nightly backs up cluster main only, not cold", ready.Message)
	require.Equal(t, ctrl.Result{}, f.reconcile("b1"), "a backup refused before it named an installation reports nothing, and is done")
}

// A backup that runs a schedule never found by its starting deadline fails, and is done with.
func TestBackupRunsAScheduleThatNeverCame(t *testing.T) {
	chb := newBackupCreated("b1")
	chb.Spec.ClickHouseInstallation = ""
	chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "nightly"}
	chb.Spec.StartingDeadlineSeconds = ptr.To(int64(60))
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
	clock := f.at(backupCreated.Add(30 * time.Second))
	require.Equal(t, pendingRequeueInterval, f.reconcile("b1").RequeueAfter)

	clock.SetTime(backupCreated.Add(61 * time.Second))
	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, "not started by 2026-10-19T22:01:00Z, 60s after the backup was due: ClickHouseBackupSchedule nightly not found",
		condition(t, got.Status.Conditions, api.BackupConditionReady).Message)
	require.Equal(t, ctrl.Result{}, f.reconcile("b1"))
}

// A backup whose spec is invalid waits for a change of it, and fails at its starting deadline like any backup that
// does not start by then.
func TestBackupWithAnInvalidSpecFailsAtItsStartingDeadline(t *testing.T) {
	chb := newBackupCreated("b1")
	chb.Spec.ClickHouseInstallation = ""
	chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "nightly"}
	chb.Spec.SchemaOnly = types.NewStringBool(true)
	chb.Spec.StartingDeadlineSeconds = ptr.To(int64(60))
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
	clock := f.at(backupCreated.Add(30 * time.Second))
	require.Equal(t, 30*time.Second+dueRecheckDelay, f.reconcile("b1").RequeueAfter)
	require.Equal(t, api.BackupReasonInvalidSpec, condition(t, f.backup("b1").Status.Conditions, api.BackupConditionValidated).Reason)

	clock.SetTime(backupCreated.Add(61 * time.Second))
	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.BackupReasonStartingDeadlineExceeded, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)

	unbounded := newBackupCreated("b2")
	unbounded.Spec.BackupName = "not a name"
	f = newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, unbounded)
	f.at(backupCreated)
	require.Equal(t, ctrl.Result{}, f.reconcile("b2"), "without a deadline only a change of the spec ends the wait")
}
