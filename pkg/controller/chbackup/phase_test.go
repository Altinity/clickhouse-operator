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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

// eventTypeInfo is the type of the events the operator emits that are no warning.
const eventTypeInfo = "Info"

// eventsOn returns the events with the given reason on the named object, of any type when eventType is "".
func eventsOn(t *testing.T, c client.Client, kind, name, reason, eventType string) []core.Event {
	t.Helper()
	events := &core.EventList{}
	require.NoError(t, c.List(context.Background(), events, client.InNamespace(testNamespace)))
	var found []core.Event
	for _, event := range events.Items {
		if (event.InvolvedObject.Kind == kind) && (event.InvolvedObject.Name == name) && (event.Reason == reason) &&
			((eventType == "") || (event.Type == eventType)) {
			found = append(found, event)
		}
	}
	return found
}

// A backup Job runs the script of the operator configuration from the ConfigMap the operator keeps of the cluster, as
// the orchestrator ServiceAccount, and posts its events on the backup.
func TestBackupJobRunsTheOrchestratorScript(t *testing.T) {
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
	f.reconcile("b1")

	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("b1-main-backup"), job))
	spec := job.Spec.Template.Spec
	configMap := model.OrchestratorConfigMapName(testCHI, "main")
	require.Equal(t, configMap, spec.Volumes[0].ConfigMap.Name)
	require.Equal(t, OrchestratorServiceAccountName(), spec.ServiceAccountName)
	require.True(t, ptr.Deref(spec.AutomountServiceAccountToken, false))
	require.Equal(t, model.ScriptSHA256(repositoryScript(t)), job.Spec.Template.Annotations[model.AnnotationOrchestratorScriptSHA256])
	values := map[string]string{}
	for _, e := range spec.Containers[0].Env {
		values[e.Name] = e.Value
	}
	require.Equal(t, api.ClickHouseBackupCRDResourceKind, values["EVENT_OBJECT_KIND"])
	require.Equal(t, "b1", values["EVENT_OBJECT_NAME"])
	require.Equal(t, "b1-uid", values["EVENT_OBJECT_UID"])
	require.Equal(t, "sidecar", values["BACKUP_TRANSPORT"])

	stored := &core.ConfigMap{}
	require.NoError(t, f.client.Get(context.Background(), key(configMap), stored))
	require.Equal(t, repositoryScript(t), stored.Data[model.OrchestratorScriptFile])

	require.Len(t, eventsOn(t, f.client, api.ClickHouseBackupCRDResourceKind, "b1", "BackupRunning", eventTypeInfo), 1,
		"the transition to Running is announced on the backup")
}

// Without the script the operator aborts the backup, naming the file it looked for, and runs no Job.
func TestBackupIsAbortedWithoutTheScript(t *testing.T) {
	dir := t.TempDir()
	withScriptDir(t, dir)
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
	f.reconcile("b1")
	f.reconcile("b1")

	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseAborted, got.Status.Phase)
	require.Equal(t, reasonOrchestratorScriptMissing, got.Status.Clusters[0].Reason)
	require.Equal(t, reasonOrchestratorScriptMissing, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	require.Zero(t, jobCount(t, f.client))
	aborted := eventsOn(t, f.client, api.ClickHouseBackupCRDResourceKind, "b1", "BackupAborted", core.EventTypeWarning)
	require.Len(t, aborted, 1)
	require.Contains(t, aborted[0].Message, filepath.Join(dir, model.OrchestratorScriptFile))
	require.Contains(t, aborted[0].Message, reasonOrchestratorScriptMissing)
}

// A Job that fails by itself fails the backup, and the event tells the Job and the tail of what its script wrote.
func TestBackupFailedByItsJobIsAnnounced(t *testing.T) {
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
	f.reconcile("b1")
	failPodOfJob(t, f.client, "b1-main-backup", "upload to s3 failed: access denied")
	finishJob(t, f.client, "b1-main-backup", batch.JobFailed, batch.JobReasonBackoffLimitExceeded)
	f.reconcile("b1")

	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	failed := eventsOn(t, f.client, api.ClickHouseBackupCRDResourceKind, "b1", "BackupFailed", core.EventTypeWarning)
	require.Len(t, failed, 1)
	require.Contains(t, failed[0].Message, "b1-main-backup")
	require.Contains(t, failed[0].Message, "upload to s3 failed: access denied")

	f.reconcile("b1")
	require.Len(t, eventsOn(t, f.client, api.ClickHouseBackupCRDResourceKind, "b1", "BackupFailed", ""), 1, "announced once")
}

// The Job the operator stops at its deadline aborts the backup.
func TestBackupPastItsDeadlineIsAborted(t *testing.T) {
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
	f.reconcile("b1")
	finishJob(t, f.client, "b1-main-backup", batch.JobFailed, batch.JobReasonDeadlineExceeded)
	f.reconcile("b1")

	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseAborted, got.Status.Phase)
	require.Equal(t, api.OperationPhaseAborted, got.Status.Clusters[0].Phase)
	require.Len(t, eventsOn(t, f.client, api.ClickHouseBackupCRDResourceKind, "b1", "BackupAborted", core.EventTypeWarning), 1)
}

// Without the script the operator aborts the restore before it changes anything.
func TestRestoreIsAbortedWithoutTheScript(t *testing.T) {
	withScriptDir(t, t.TempDir())
	c := newClient(t, nil, newRestoreCR("r1"))
	controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
	for range 3 {
		reconcileRestore(t, controller, "r1")
	}

	got := getRestore(t, c, "r1")
	require.Equal(t, api.OperationPhaseAborted, got.Status.Phase)
	require.Equal(t, reasonOrchestratorScriptMissing, got.Status.Clusters[0].Reason)
	require.Zero(t, jobCount(t, c))
	require.Len(t, eventsOn(t, c, api.ClickHouseRestoreCRDResourceKind, "r1", "RestoreAborted", core.EventTypeWarning), 1)
	require.False(t, holdsSchedules(got), "an aborted restore that ran no Job holds no schedule")
}

// A schedule is Active while its CronJobs run, Suspended while its spec suspends it, and its CronJob runs the script
// of the ConfigMap with the schedule as the event object.
func TestSchedulePhases(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")

	got := f.schedule("s1")
	require.Equal(t, api.SchedulePhaseActive, got.Status.Phase)
	require.Equal(t, api.BackupReasonScheduled, got.Status.Reason)
	require.NotNil(t, got.Status.NextScheduleTime)
	require.Equal(t, 3, got.Status.NextScheduleTime.UTC().Hour(), "the daily entry runs at 03:00")
	require.Len(t, eventsOn(t, f.client, api.ClickHouseBackupScheduleCRDResourceKind, "s1", reasonScheduleActive, eventTypeInfo), 1)
	created := eventsOn(t, f.client, api.ClickHouseBackupScheduleCRDResourceKind, "s1", reasonCronJobCreated, eventTypeInfo)
	require.Len(t, created, 1)
	require.Contains(t, created[0].Message, "s1-main-backup")

	cronJob := f.cronJob("s1-main-backup")
	spec := cronJob.Spec.JobTemplate.Spec.Template.Spec
	require.Equal(t, model.OrchestratorConfigMapName(testCHI, "main"), spec.Volumes[0].ConfigMap.Name)
	values := map[string]string{}
	for _, e := range spec.Containers[0].Env {
		values[e.Name] = e.Value
	}
	require.Equal(t, api.ClickHouseBackupScheduleCRDResourceKind, values["EVENT_OBJECT_KIND"])
	require.Equal(t, "false", values["RESTORE_IN_PROGRESS"])

	suspended := f.schedule("s1")
	suspended.Spec.Suspend = types.NewStringBool(true)
	require.NoError(t, f.client.Update(context.Background(), suspended))
	f.reconcile("s1")
	got = f.schedule("s1")
	require.Equal(t, api.SchedulePhaseSuspended, got.Status.Phase)
	require.Nil(t, got.Status.NextScheduleTime)
	require.Len(t, eventsOn(t, f.client, api.ClickHouseBackupScheduleCRDResourceKind, "s1", reasonScheduleSuspended, eventTypeInfo), 1)
	require.Len(t, eventsOn(t, f.client, api.ClickHouseBackupScheduleCRDResourceKind, "s1", reasonCronJobUpdated, eventTypeInfo), 1)
}

// A schedule that cannot run is Aborted, with why, until it can.
func TestScheduleIsAbortedWhileItCannotRun(t *testing.T) {
	t.Run("invalid spec", func(t *testing.T) {
		chbs := newScheduleCR("s1")
		chbs.Spec.Schedules = nil
		f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs)
		f.reconcile("s1")
		got := f.schedule("s1")
		require.Equal(t, api.SchedulePhaseAborted, got.Status.Phase)
		require.Equal(t, api.BackupReasonInvalidSpec, got.Status.Reason)
		require.Nil(t, got.Status.NextScheduleTime)
		require.Len(t, eventsOn(t, f.client, api.ClickHouseBackupScheduleCRDResourceKind, "s1", reasonScheduleAborted, core.EventTypeWarning), 1)
	})
	t.Run("installation missing", func(t *testing.T) {
		f := newScheduleFixture(t, newFakeCR(), nil, newScheduleCR("s1"))
		f.reconcile("s1")
		got := f.schedule("s1")
		require.Equal(t, api.SchedulePhaseAborted, got.Status.Phase)
		require.Equal(t, api.BackupReasonCHINotFound, got.Status.Reason)
	})
	t.Run("no script", func(t *testing.T) {
		dir := t.TempDir()
		withScriptDir(t, dir)
		f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
		f.reconcile("s1")
		got := f.schedule("s1")
		require.Equal(t, api.SchedulePhaseAborted, got.Status.Phase)
		require.Equal(t, reasonOrchestratorScriptMissing, got.Status.Reason)
		require.Contains(t, got.Status.Message, filepath.Join(dir, model.OrchestratorScriptFile))
		aborted := eventsOn(t, f.client, api.ClickHouseBackupScheduleCRDResourceKind, "s1", reasonScheduleAborted, core.EventTypeWarning)
		require.Len(t, aborted, 1)
		require.Contains(t, aborted[0].Message, filepath.Join(dir, model.OrchestratorScriptFile))

		// The script comes back: the schedule runs again
		withScriptDir(t, repositoryScriptDir)
		f.reconcile("s1")
		require.Equal(t, api.SchedulePhaseActive, f.schedule("s1").Status.Phase)
	})
}

// A schedule whose last run failed is Aborted until a later run succeeds; every run is announced with how it went.
func TestScheduleIsAbortedByAFailedRunUntilOneSucceeds(t *testing.T) {
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, newScheduleCR("s1"))
	f.reconcile("s1")
	due := time.Date(2026, time.January, 2, 3, 0, 0, 0, time.UTC)

	failed := f.scheduledRunAt(due)
	failPodOfJob(t, f.client, failed.Name, "no space left on device")
	finishJobAt(t, f.client, failed.Name, batch.JobFailed, batch.JobReasonBackoffLimitExceeded, due.Add(time.Minute))
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Equal(t, api.SchedulePhaseAborted, got.Status.Phase)
	require.Equal(t, api.BackupReasonLastBackupFailed, got.Status.Reason)
	require.NotNil(t, got.Status.NextScheduleTime, "the CronJobs still run")
	messages := eventMessagesOfType(t, f.client, reasonScheduledBackupFailed, core.EventTypeWarning)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], failed.Name)
	require.Contains(t, messages[0], "no space left on device")

	succeeded := f.scheduledRunAt(due.Add(24 * time.Hour))
	finishJobAt(t, f.client, succeeded.Name, batch.JobComplete, "", due.Add(24*time.Hour+time.Minute))
	f.reconcile("s1")
	got = f.schedule("s1")
	require.Equal(t, api.SchedulePhaseActive, got.Status.Phase)
	require.Len(t, eventMessagesOfType(t, f.client, reasonScheduledBackupSucceeded, eventTypeInfo), 1)
	require.Len(t, eventsOn(t, f.client, api.ClickHouseBackupScheduleCRDResourceKind, "s1", reasonScheduleActive, eventTypeInfo), 2,
		"active at first, and again after the failure")
}

// Every entry records the newest run whose slot matches it, and the backup of its newest successful one.
func TestScheduleRecordsItsEntries(t *testing.T) {
	chbs := newScheduleCR("s1")
	chbs.Spec.Schedules = []api.BackupScheduleEntry{
		{Period: api.BackupPeriodDaily, Time: "03:00", Keep: 7},
		{Period: api.BackupPeriodWeekly, Time: "03:00", Day: ptr.To(int32(5)), Keep: 4},
	}
	f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs)
	f.reconcile("s1")

	thursday := time.Date(2026, time.January, 1, 3, 0, 0, 0, time.UTC)
	friday := thursday.Add(24 * time.Hour)
	first := f.scheduledRunAt(thursday)
	finishJobAt(t, f.client, first.Name, batch.JobComplete, "", thursday.Add(time.Minute))
	f.reconcile("s1")
	got := f.schedule("s1")
	require.Len(t, got.Status.Entries, 2)
	daily, weekly := got.Status.GetEntry(api.BackupPeriodDaily), got.Status.GetEntry(api.BackupPeriodWeekly)
	require.True(t, daily.LastScheduleTime.Equal(&meta.Time{Time: thursday}))
	require.Equal(t, "ns-demo-s1-20260101030000", daily.LastBackupName)
	require.Nil(t, weekly.LastScheduleTime, "thursday is no slot of the friday entry")
	require.Empty(t, weekly.LastBackupName)

	second := f.scheduledRunAt(friday)
	f.reconcile("s1")
	got = f.schedule("s1")
	daily, weekly = got.Status.GetEntry(api.BackupPeriodDaily), got.Status.GetEntry(api.BackupPeriodWeekly)
	require.True(t, weekly.LastScheduleTime.Equal(&meta.Time{Time: friday}), "a running run is scheduled")
	require.True(t, daily.LastScheduleTime.Equal(&meta.Time{Time: friday}), "the weekly run is the daily one too")
	require.Equal(t, "ns-demo-s1-20260101030000", daily.LastBackupName, "until the run completes")
	require.Empty(t, weekly.LastBackupName)

	finishJobAt(t, f.client, second.Name, batch.JobComplete, "", friday.Add(time.Minute))
	f.reconcile("s1")
	got = f.schedule("s1")
	require.Equal(t, "ns-demo-s1-20260102030000", got.Status.GetEntry(api.BackupPeriodDaily).LastBackupName)
	require.Equal(t, "ns-demo-s1-20260102030000", got.Status.GetEntry(api.BackupPeriodWeekly).LastBackupName)

	// An entry the spec drops goes from the status
	edited := f.schedule("s1")
	edited.Spec.Schedules = edited.Spec.Schedules[:1]
	require.NoError(t, f.client.Update(context.Background(), edited))
	f.reconcile("s1")
	got = f.schedule("s1")
	require.Len(t, got.Status.Entries, 1)
	require.Equal(t, api.BackupPeriodDaily, got.Status.Entries[0].Period)
}

// scriptConfigMap returns a ConfigMap that holds an orchestrator script of the user's.
func scriptConfigMap(name, script string) *core.ConfigMap {
	return &core.ConfigMap{
		ObjectMeta: meta.ObjectMeta{Name: name, Namespace: testNamespace},
		Data:       map[string]string{api.BackupOrchestratorScriptKey: script},
	}
}

// jobConfigMap returns the ConfigMap of the orchestrator script the pod of a Job mounts.
func jobConfigMap(t *testing.T, c client.Client, name string) string {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(name), job))
	return job.Spec.Template.Spec.Volumes[0].ConfigMap.Name
}

// A backup, a backup that runs a schedule, a schedule and a restore run the script of the ConfigMap their spec names;
// a backup that runs a schedule takes the schedule's unless it names its own.
func TestOperationsRunTheOrchestratorTheyName(t *testing.T) {
	ref := func(name string) *core.LocalObjectReference { return &core.LocalObjectReference{Name: name} }

	t.Run("backup", func(t *testing.T) {
		chb := newBackupCR("b1")
		chb.Spec.OrchestratorConfigMapRef = ref("mine")
		f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb, scriptConfigMap("mine", "echo mine"))
		f.reconcile("b1")
		require.Equal(t, "mine", jobConfigMap(t, f.client, "b1-main-backup"))
		err := f.client.Get(context.Background(), key(model.OrchestratorConfigMapName(testCHI, "main")), &core.ConfigMap{})
		require.Error(t, err, "the operator keeps no ConfigMap for a backup that names its own")
	})

	for name, tc := range map[string]struct {
		own      *core.LocalObjectReference
		expected string
	}{
		"of the schedule": {nil, "schedules"},
		"of its own":      {ref("own"), "own"},
		"of the operator": {ref(""), model.OrchestratorConfigMapName(testCHI, "main")},
	} {
		t.Run("backup that runs a schedule, the script "+name, func(t *testing.T) {
			chbs := newScheduleCR("nightly")
			chbs.Spec.OrchestratorConfigMapRef = ref("schedules")
			chb := newBackupCR("pre-upgrade")
			chb.Spec.ClickHouseInstallation = ""
			chb.Spec.ScheduleRef = &api.BackupScheduleRef{Name: "nightly"}
			chb.Spec.OrchestratorConfigMapRef = tc.own
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb, chbs,
				scriptConfigMap("schedules", "echo schedules"), scriptConfigMap("own", "echo own"))
			f.reconcile("pre-upgrade")
			require.Equal(t, "schedules", f.backup("pre-upgrade").Status.Schedule.GetOrchestratorConfigMapName(), "recorded of the schedule")
			require.Equal(t, tc.expected, jobConfigMap(t, f.client, "pre-upgrade-main-backup"))
		})
	}

	t.Run("schedule", func(t *testing.T) {
		chbs := newScheduleCR("s1")
		chbs.Spec.OrchestratorConfigMapRef = ref("mine")
		f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs, scriptConfigMap("mine", "echo mine"))
		f.reconcile("s1")
		template := f.cronJob("s1-main-backup").Spec.JobTemplate.Spec.Template
		require.Equal(t, "mine", template.Spec.Volumes[0].ConfigMap.Name)
		require.Equal(t, model.ScriptSHA256("echo mine"), template.Annotations[model.AnnotationOrchestratorScriptSHA256])
	})

	t.Run("schedule without its ConfigMap", func(t *testing.T) {
		chbs := newScheduleCR("s1")
		chbs.Spec.OrchestratorConfigMapRef = ref("missing")
		f := newScheduleFixture(t, newFakeCR(readyInstallation(t)), nil, chbs)
		f.reconcile("s1")
		got := f.schedule("s1")
		require.Equal(t, api.SchedulePhaseAborted, got.Status.Phase)
		require.Equal(t, reasonOrchestratorScriptMissing, got.Status.Reason)
		require.Contains(t, got.Status.Message, "missing")
	})

	t.Run("restore without its ConfigMap's script", func(t *testing.T) {
		chr := newRestoreCR("r1")
		chr.Spec.OrchestratorConfigMapRef = ref("empty")
		empty := scriptConfigMap("empty", "")
		c := newClient(t, nil, chr, empty)
		controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
		for range 3 {
			reconcileRestore(t, controller, "r1")
		}
		got := getRestore(t, c, "r1")
		require.Equal(t, api.OperationPhaseAborted, got.Status.Phase)
		require.Equal(t, reasonOrchestratorScriptMissing, got.Status.Clusters[0].Reason)
		require.Contains(t, got.Status.Clusters[0].Message, "empty")
		aborted := eventsOn(t, c, api.ClickHouseRestoreCRDResourceKind, "r1", "RestoreAborted", core.EventTypeWarning)
		require.Len(t, aborted, 1)
		require.Contains(t, aborted[0].Message, api.BackupOrchestratorScriptKey)
	})

	t.Run("restore", func(t *testing.T) {
		chr := newRestoreCR("r1")
		chr.Spec.OrchestratorConfigMapRef = ref("mine")
		c := newClient(t, nil, chr, scriptConfigMap("mine", "echo mine"))
		controller := NewRestoreController(c, c, c.Scheme(), newFakeCR(readyInstallation(t)))
		startRestore(t, c, controller, "r1")
		require.Equal(t, "mine", jobConfigMap(t, c, "r1-main-restore"))
	})
}
