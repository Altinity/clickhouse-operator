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
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
)

type backupFixture struct {
	t          *testing.T
	client     client.Client
	cr         *fakeCR
	controller *BackupController
}

func newBackupFixture(t *testing.T, cr *fakeCR, funcs *interceptor.Funcs, objs ...client.Object) *backupFixture {
	t.Helper()
	c := newClient(t, funcs, objs...)
	return &backupFixture{t: t, client: c, cr: cr, controller: NewBackupController(c, c, c.Scheme(), cr)}
}

func (f *backupFixture) reconcile(name string) ctrl.Result {
	f.t.Helper()
	result, err := f.controller.Reconcile(context.Background(), request(name))
	require.NoError(f.t, err)
	return result
}

func (f *backupFixture) backup(name string) *api.ClickHouseBackup {
	f.t.Helper()
	chb := &api.ClickHouseBackup{}
	require.NoError(f.t, f.client.Get(context.Background(), key(name), chb))
	return chb
}

func TestBackupSkipsUnwatchedNamespaces(t *testing.T) {
	watchOnly(t, "elsewhere")
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))

	require.Equal(t, ctrl.Result{}, f.reconcile("b1"))
	require.Zero(t, jobCount(t, f.client), "an unwatched namespace must never get a Job")
	require.Empty(t, f.backup("b1").Status.Phase)

	require.False(t, WatchedNamespacePredicate().Create(event.CreateEvent{Object: newBackupCR("b1")}),
		"events from unwatched namespaces must not even be queued")
}

func TestBackupRejectsInvalidSpec(t *testing.T) {
	chb := newBackupCR("b1")
	chb.Spec.BackupName = "not a valid name"
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)

	require.Equal(t, ctrl.Result{}, f.reconcile("b1"), "only a spec change can fix the spec")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	validated := condition(t, got.Status.Conditions, api.BackupConditionValidated)
	require.Equal(t, meta.ConditionFalse, validated.Status)
	require.Equal(t, api.BackupReasonInvalidSpec, validated.Reason)
	require.Zero(t, jobCount(t, f.client))
}

func TestBackupWaitsForItsInstallation(t *testing.T) {
	for name, cr := range map[string]*fakeCR{
		api.BackupReasonCHINotFound: newFakeCR(),
		api.BackupReasonCHINotReady: newFakeCR(installation(t, api.StatusInProgress, backupCluster("main", 2, true))),
	} {
		t.Run(name, func(t *testing.T) {
			f := newBackupFixture(t, cr, nil, newBackupCR("b1"))

			require.NotZero(t, f.reconcile("b1").RequeueAfter)
			got := f.backup("b1")
			require.Equal(t, api.OperationPhasePending, got.Status.Phase)
			require.Equal(t, name, condition(t, got.Status.Conditions, api.BackupConditionValidated).Reason)
			require.Zero(t, jobCount(t, f.client))
		})
	}
}

// A one-off backup of a cluster the completed installation does not have, or does not back up, can
// never run: it fails for good, rather than wait for a change that may never come.
func TestBackupRefusesTargetsWithoutBackup(t *testing.T) {
	for name, tc := range map[string]struct {
		cluster string
		reason  string
	}{
		"a cluster without backup":             {cluster: "plain", reason: api.BackupReasonBackupDisabled},
		"a missing cluster":                    {cluster: "gone", reason: api.BackupReasonClusterNotFound},
		"a cluster with backup but no storage": {cluster: "lacking", reason: api.BackupReasonNoBackupStorage},
	} {
		t.Run(name, func(t *testing.T) {
			chb := newBackupCR("b1")
			chb.Spec.Cluster = tc.cluster
			lacking := lackingCluster("lacking", 1)
			cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 1, true), backupCluster("plain", 1, false), lacking))
			f := newBackupFixture(t, cr, nil, chb)

			require.Equal(t, ctrl.Result{}, f.reconcile("b1"))
			got := f.backup("b1")
			require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
			require.Equal(t, tc.reason, condition(t, got.Status.Conditions, api.BackupConditionValidated).Reason)
			require.Equal(t, tc.reason, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
			require.Empty(t, got.Status.Clusters)
			require.Len(t, eventMessagesOfType(t, f.client, tc.reason, core.EventTypeWarning), 1)
			require.Zero(t, jobCount(t, f.client))
		})
	}

	t.Run("an installation still reconciling", func(t *testing.T) {
		chb := newBackupCR("b1")
		chb.Spec.Cluster = "gone"
		f := newBackupFixture(t, newFakeCR(installation(t, api.StatusInProgress, backupCluster("main", 1, true))), nil, chb)

		require.NotZero(t, f.reconcile("b1").RequeueAfter, "the reconcile may add the cluster")
		require.Equal(t, api.OperationPhasePending, f.backup("b1").Status.Phase)
	})
}

// A cluster that turns backup on without a storage is refused telling how to turn backup on there, and a
// backup of every cluster leaves it out, warning about it: its spec asks for backups it would otherwise go without
// unnoticed.
func TestBackupOfEveryClusterLeavesOutAClusterWithoutStorage(t *testing.T) {
	lacking := lackingCluster("lacking", 1)
	cr := newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 1, true), lacking))
	f := newBackupFixture(t, cr, nil, newBackupCR("b1"))

	f.reconcile("b1")
	got := f.backup("b1")
	require.Len(t, got.Status.Clusters, 1)
	require.Equal(t, "main", got.Status.Clusters[0].Name)
	messages := eventMessagesOfType(t, f.client, api.BackupReasonNoBackupStorage, core.EventTypeWarning)
	require.Len(t, messages, 1)
	require.Contains(t, messages[0], "leaves out the clusters with backup enabled without a storage: lacking - ")
	require.Contains(t, messages[0], api.BackupStorageHint)

	f.reconcile("b1")
	require.Len(t, eventMessagesOfType(t, f.client, api.BackupReasonNoBackupStorage, core.EventTypeWarning), 1,
		"warned once, when the backup pins its clusters")

	chb := newBackupCR("b2")
	chb.Spec.Cluster = "lacking"
	f = newBackupFixture(t, cr, nil, chb)
	f.reconcile("b2")
	ready := condition(t, f.backup("b2").Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonNoBackupStorage, ready.Reason)
	require.Contains(t, ready.Message, api.BackupStorageHint)
}

// A stopped installation has no running hosts: a Job could only wait for them, holding every other
// operation on the installation meanwhile.
func TestBackupRefusesAStoppedInstallation(t *testing.T) {
	for name, chi := range stoppedInstallations(t) {
		t.Run(name, func(t *testing.T) {
			f := newBackupFixture(t, newFakeCR(chi), nil, newBackupCR("b1"))

			require.Equal(t, targetRequeueInterval, f.reconcile("b1").RequeueAfter)
			got := f.backup("b1")
			require.Equal(t, api.OperationPhasePending, got.Status.Phase)
			require.Equal(t, api.BackupReasonCHIStopped, condition(t, got.Status.Conditions, api.BackupConditionValidated).Reason)
			require.Zero(t, jobCount(t, f.client))
		})
	}
}

func TestBackupCreatesItsJob(t *testing.T) {
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))

	f.reconcile("b1")

	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
	require.Equal(t, "b1-main-backup", got.Status.Clusters[0].JobName)
	require.Equal(t, testStorage, got.Status.Clusters[0].Storage, "the storage the Job keeps the backup in")
	require.Equal(t, model.ResolveBackupName(got), got.Status.BackupName)
	require.NotNil(t, got.Status.StartTime)
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionValidated).Status)
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Status)
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonJobCreated))

	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("b1-main-backup"), job))
	require.True(t, meta.IsControlledBy(job, got))
	require.Equal(t, model.Labels(api.ClickHouseBackupCRDResourceKind, "b1", testCHI, "main", testStorage), job.Labels)
	require.Contains(t, envValues(job.Spec.Template.Spec), "chi-demo-main-0-0 chi-demo-main-1-0", "hosts come from the normalized installation")
}

// The installation may start another reconcile while the backup runs. The Job already exists, so
// the backup is tracked to its end rather than sent back to Pending.
func TestBackupReadinessGateAppliesOnlyBeforeTheJob(t *testing.T) {
	cr := newFakeCR(readyInstallation(t))
	f := newBackupFixture(t, cr, nil, newBackupCR("b1"))
	f.reconcile("b1")

	cr.set(installation(t, api.StatusInProgress, backupCluster("main", 2, true)))
	f.reconcile("b1")
	require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase)

	finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")
	f.reconcile("b1")
	require.Equal(t, api.OperationPhaseCompleted, f.backup("b1").Status.Phase)
}

func TestBackupCompletes(t *testing.T) {
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
	f.reconcile("b1")
	finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")

	require.Equal(t, ctrl.Result{}, f.reconcile("b1"))
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
	require.NotNil(t, got.Status.CompletionTime)
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionReady).Status)
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonCompleted))

	f.reconcile("b1")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonCompleted), "a finished backup is never touched again")
}

// A backup that holds no data, as its Job tells, completed all the same: it is ready, with a reason
// and a warning that tell why it restores no data, and counts as a success.
func TestBackupReportsABackupWithoutData(t *testing.T) {
	for name, tc := range map[string]struct {
		tables     string
		partitions []string
		cause      string
	}{
		"every table":     {cause: "there is no table to back up, or none of the tables holds data"},
		"a table pattern": {tables: "db.*", cause: `the tables pattern "db.*" matches no table, or none of the tables it matches holds data`},
		"partitions": {
			partitions: []string{"202601", "202602"},
			cause:      `there is no table to back up, or none of the tables holds data in the partitions "202601,202602"`,
		},
		"a table pattern and partitions": {
			tables:     "db.*",
			partitions: []string{"202601"},
			cause:      `the tables pattern "db.*" matches no table, or none of the tables it matches holds data in the partitions "202601"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			captured := captureOutcomes(t)
			chb := newBackupCR("b1")
			chb.Spec.Tables = tc.tables
			chb.Spec.Partitions = tc.partitions
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
			f.reconcile("b1")
			succeedPodOfJob(t, f.client, "b1-main-backup", model.NoDataMessage)
			finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")

			f.reconcile("b1")
			got := f.backup("b1")
			require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
			ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
			require.Equal(t, meta.ConditionTrue, ready.Status, "a backup without data succeeded")
			require.Equal(t, api.BackupReasonCompletedWithoutData, ready.Reason)
			require.Equal(t, "Job b1-main-backup completed, but the backup holds no data: "+tc.cause, ready.Message)
			warnings := eventMessagesOfType(t, f.client, api.BackupReasonCompletedWithoutData, core.EventTypeWarning)
			require.Len(t, warnings, 1)
			require.Contains(t, warnings[0], "backup "+got.Status.BackupName+" of cluster main completed in ")
			require.Contains(t, warnings[0], ", but holds no data: "+tc.cause)
			require.Zero(t, eventCount(t, f.client, api.BackupReasonCompleted))

			f.reconcile("b1")
			require.Len(t, eventMessages(t, f.client, api.BackupReasonCompletedWithoutData), 1, "a finished backup is never touched again")
			require.Equal(t, []outcome{{namespace: testNamespace, chi: testCHI, cluster: "main", at: got.Status.CompletionTime.Unix()}}, captured.successes)
		})
	}

	t.Run("a backup with data", func(t *testing.T) {
		f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
		f.reconcile("b1")
		succeedPodOfJob(t, f.client, "b1-main-backup", "")
		finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")

		f.reconcile("b1")
		require.Equal(t, api.BackupReasonCompleted, condition(t, f.backup("b1").Status.Conditions, api.BackupConditionReady).Reason)
		require.Zero(t, eventCount(t, f.client, api.BackupReasonCompletedWithoutData))
	})
}

func TestBackupFailureReasons(t *testing.T) {
	for jobReason, expected := range map[string]string{
		batch.JobReasonDeadlineExceeded:     api.BackupReasonDeadlineExceeded,
		batch.JobReasonBackoffLimitExceeded: api.BackupReasonJobFailed,
	} {
		t.Run(jobReason, func(t *testing.T) {
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
			f.reconcile("b1")
			finishJob(t, f.client, "b1-main-backup", batch.JobFailed, jobReason)

			f.reconcile("b1")
			got := f.backup("b1")
			require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
			ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
			require.Equal(t, meta.ConditionFalse, ready.Status)
			require.Equal(t, expected, ready.Reason)
			require.Equal(t, 1, eventCount(t, f.client, expected))
		})
	}
}

// A Job whose pod never starts, refused by admission or with an image that cannot be pulled, would
// keep its backup Running, and every other operation on the installation waiting, until its
// deadline. The backup fails instead, and the Job is stopped so that its pod cannot start late.
func TestBackupFailsAJobThatNeverStarts(t *testing.T) {
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"), newBackupCR("b2"))
	f.reconcile("b1")
	stallJob(t, f.client, "b1-main-backup", neverStarted)

	require.Equal(t, ctrl.Result{}, f.reconcile("b1"))
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, api.BackupReasonJobNotStarted, ready.Reason)
	require.Contains(t, ready.Message, "no pod of Job b1-main-backup started within")
	require.Contains(t, ready.Message, "kubectl get events -n "+testNamespace)
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonJobNotStarted))
	requireStopped(t, f.client, "b1-main-backup")

	f.reconcile("b2")
	require.Equal(t, api.OperationPhaseRunning, f.backup("b2").Status.Phase, "a Job that never started holds no other operation")
}

// Only a Job without any pod that ever ran counts as not started.
func TestBackupKeepsAJobWhosePodStarted(t *testing.T) {
	for name, status := range map[string]func(*batch.JobStatus){
		"running pod":  func(s *batch.JobStatus) { s.Active, s.Ready = 1, ptr.To(int32(1)) },
		"finished pod": func(s *batch.JobStatus) { s.Succeeded, s.Ready = 1, ptr.To(int32(0)) },
		"failed pod":   func(s *batch.JobStatus) { s.Failed, s.Ready = 1, ptr.To(int32(0)) },
	} {
		t.Run(name, func(t *testing.T) {
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
			f.reconcile("b1")
			stallJob(t, f.client, "b1-main-backup", status)

			f.reconcile("b1")
			require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase)
			requireRunning(t, f.client, "b1-main-backup")
		})
	}
}

// A cluster that does not report ready pods leaves Active as the only count, and Active counts
// pending pods too: once the start times out, the pods themselves tell whether one started.
func TestBackupAsksThePodsWhereReadyIsNotReported(t *testing.T) {
	for name, tc := range map[string]struct {
		pod     *core.PodStatus
		started bool
	}{
		"running pod": {pod: &core.PodStatus{Phase: core.PodRunning}, started: true},
		"pending pod": {pod: &core.PodStatus{Phase: core.PodPending}},
	} {
		t.Run(name, func(t *testing.T) {
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
			f.reconcile("b1")
			if tc.pod != nil {
				podOfJob(t, f.client, "b1-main-backup", "b1-pod", *tc.pod)
			}
			stallJob(t, f.client, "b1-main-backup", func(s *batch.JobStatus) { s.Active = 1 })

			f.reconcile("b1")
			if tc.started {
				require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase)
				requireRunning(t, f.client, "b1-main-backup")
				return
			}
			require.Equal(t, api.OperationPhaseFailed, f.backup("b1").Status.Phase, "a pending pod is not a started one")
			requireStopped(t, f.client, "b1-main-backup")
		})
	}
}

// A pod that runs for hours and stops being ready, on a node gone NotReady or behind a flapping
// probe, leaves its Job with no ready pod, as if it never started. Stopping it would kill the
// operation midway and report it as never started; the start seen earlier is recorded instead.
func TestBackupKeepsAJobWhosePodStoppedBeingReady(t *testing.T) {
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"), newBackupCR("b2"))
	f.reconcile("b1")
	setJobStatus(t, f.client, "b1-main-backup", runningReady)
	f.reconcile("b1")

	stallJob(t, f.client, "b1-main-backup", neverStarted)
	f.reconcile("b1")
	require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase)
	requireRunning(t, f.client, "b1-main-backup")
	require.Zero(t, eventCount(t, f.client, api.BackupReasonJobNotStarted))

	f.reconcile("b2")
	require.Equal(t, api.OperationPhasePending, f.backup("b2").Status.Phase, "the operation still runs")
}

// A pod may have started while no reconcile looked, the operator down or the Job's ready count
// already back to zero. Before a Job is stopped as never started, its pods are asked.
func TestBackupAsksThePodsBeforeStoppingAJob(t *testing.T) {
	waiting := core.ContainerState{Waiting: &core.ContainerStateWaiting{Reason: "ImagePullBackOff"}}
	terminated := core.ContainerState{Terminated: &core.ContainerStateTerminated{ExitCode: 137}}
	sidecar := core.ContainerStatus{Name: "istio-proxy", State: core.ContainerState{Running: &core.ContainerStateRunning{}}}
	for name, tc := range map[string]struct {
		pod     func(jobContainer string) core.PodStatus
		started bool
	}{
		"running pod": {
			pod:     func(string) core.PodStatus { return core.PodStatus{Phase: core.PodRunning} },
			started: true,
		},
		"pod that ran and waits to restart": {
			pod: func(jobContainer string) core.PodStatus {
				return core.PodStatus{Phase: core.PodPending, ContainerStatuses: []core.ContainerStatus{
					{Name: jobContainer, State: waiting, LastTerminationState: terminated},
				}}
			},
			started: true,
		},
		"pending pod": {
			pod: func(jobContainer string) core.PodStatus {
				return core.PodStatus{Phase: core.PodPending, ContainerStatuses: []core.ContainerStatus{{Name: jobContainer, State: waiting}}}
			},
		},
		"pending pod with a running sidecar": {
			pod: func(jobContainer string) core.PodStatus {
				return core.PodStatus{Phase: core.PodPending, ContainerStatuses: []core.ContainerStatus{sidecar, {Name: jobContainer, State: waiting}}}
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			other := jobOwnedBy(t, "other-job", newBackupCR("other"), model.Labels(api.ClickHouseBackupCRDResourceKind, "other", "another-chi", "main", testStorage))
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"), other)
			f.reconcile("b1")
			job := &batch.Job{}
			require.NoError(t, f.client.Get(context.Background(), key("b1-main-backup"), job))
			podOfJob(t, f.client, "other-job", "other-pod", core.PodStatus{Phase: core.PodRunning})
			podOfJob(t, f.client, "b1-main-backup", "b1-pod", tc.pod(job.Spec.Template.Spec.Containers[0].Name))
			stallJob(t, f.client, "b1-main-backup", neverStarted)

			f.reconcile("b1")
			if tc.started {
				require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase)
				requireRunning(t, f.client, "b1-main-backup")
				return
			}
			require.Equal(t, api.OperationPhaseFailed, f.backup("b1").Status.Phase, "pods of other Jobs tell nothing")
			requireStopped(t, f.client, "b1-main-backup")
		})
	}
}

// The stop of a Job whose pod never started applies only to the Job it was decided on. A pod that
// starts right after the read changes the Job, and the stop is decided again on the live Job.
func TestBackupNeverStopsAJobWhosePodJustStarted(t *testing.T) {
	startPod := true
	funcs := &interceptor.Funcs{
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, isJob := obj.(*batch.Job); isJob && startPod {
				startPod = false
				setJobStatus(t, c, obj.GetName(), runningReady)
			}
			return c.Patch(ctx, obj, patch, opts...)
		},
	}
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), funcs, newBackupCR("b1"))
	f.reconcile("b1")
	stallJob(t, f.client, "b1-main-backup", neverStarted)

	f.reconcile("b1")
	require.False(t, startPod, "the operator tried to stop the Job")
	require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase)
	requireRunning(t, f.client, "b1-main-backup")
}

// A Job past its start timeout that its owner has not stopped yet may still start its pod, so it
// keeps every other operation on the installation waiting until it is stopped.
func TestBackupWaitsForAJobNotStoppedYet(t *testing.T) {
	owner := newRestoreCR("other")
	pending := jobOwnedBy(t, "other-job", owner, model.Labels(api.ClickHouseRestoreCRDResourceKind, "other", testCHI, "main", testStorage))
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"), pending)
	stallJob(t, f.client, "other-job", neverStarted)

	f.reconcile("b1")
	require.Equal(t, api.OperationPhasePending, f.backup("b1").Status.Phase)
	require.Equal(t, api.BackupReasonOperationInProgress, condition(t, f.backup("b1").Status.Conditions, api.BackupConditionJobCreated).Reason)

	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("other-job"), job))
	markJobStopped(job)
	require.NoError(t, f.client.Update(context.Background(), job))
	f.reconcile("b1")
	require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase, "a stopped Job holds no other operation")
}

// A Job the operator stopped may fail through its deadline before the status write that reports
// it lands; it is still reported as not started.
func TestBackupReportsAStoppedJobAsNotStarted(t *testing.T) {
	chb := newBackupCR("b1")
	chb.Status.Phase = api.OperationPhaseRunning
	chb.Status.Clusters = runningOn("b1-main-backup")
	job := jobOwnedBy(t, "b1-main-backup", chb, model.Labels(api.ClickHouseBackupCRDResourceKind, "b1", testCHI, "main", testStorage))
	job.Annotations = map[string]string{model.AnnotationJobNotStarted: jobStartTimeout.String()}
	job.Status.Conditions = []batch.JobCondition{{Type: batch.JobFailed, Status: core.ConditionTrue, Reason: batch.JobReasonDeadlineExceeded}}
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb, job)

	f.reconcile("b1")
	require.Equal(t, api.BackupReasonJobNotStarted, condition(t, f.backup("b1").Status.Conditions, api.BackupConditionReady).Reason)
}

// A Job that vanished after its name was recorded is a lost operation, not one to start again.
func TestBackupReportsALostJob(t *testing.T) {
	chb := newBackupCR("b1")
	chb.Status.Phase = api.OperationPhaseRunning
	chb.Status.Clusters = runningOn("b1-main-backup")
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)

	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	require.Equal(t, api.BackupReasonJobLost, condition(t, got.Status.Conditions, api.BackupConditionReady).Reason)
	require.Zero(t, jobCount(t, f.client), "a lost backup must not be run again")
}

// A Job of the same name that another owner controls belongs to a deleted predecessor awaiting
// garbage collection. Adopting it would report someone else's backup as this one.
func TestBackupNeverAdoptsAStaleJob(t *testing.T) {
	predecessor := newBackupCR("b1")
	predecessor.UID = "predecessor-uid"
	stale := jobOwnedBy(t, "b1-main-backup", predecessor, model.Labels(api.ClickHouseBackupCRDResourceKind, "b1", testCHI, "main", testStorage))
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"), stale)

	require.NotZero(t, f.reconcile("b1").RequeueAfter)
	got := f.backup("b1")
	require.Equal(t, api.OperationPhasePending, got.Status.Phase)
	require.Empty(t, got.Status.Clusters[0].JobName)
	require.Equal(t, api.BackupReasonWaitingForGarbageCollection, condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Reason)

	job := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("b1-main-backup"), job))
	require.True(t, meta.IsControlledBy(job, predecessor), "the stale Job must keep its owner")

	finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")
	f.reconcile("b1")
	require.NotEqual(t, api.OperationPhaseCompleted, f.backup("b1").Status.Phase, "a stale Job's success is not this backup's")
}

// Two operations on one installation never overlap, scheduled backups included.
func TestBackupWaitsForAnotherOperationOnTheInstallation(t *testing.T) {
	for name, kind := range map[string]string{
		"restore":   api.ClickHouseRestoreCRDResourceKind,
		"scheduled": api.ClickHouseBackupScheduleCRDResourceKind,
		"backup":    api.ClickHouseBackupCRDResourceKind,
	} {
		t.Run(name, func(t *testing.T) {
			owner := newRestoreCR("other")
			running := jobOwnedBy(t, "other-job", owner, model.Labels(kind, "other", testCHI, "main", testStorage))
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"), running)

			require.NotZero(t, f.reconcile("b1").RequeueAfter)
			got := f.backup("b1")
			require.Equal(t, api.OperationPhasePending, got.Status.Phase)
			require.Equal(t, api.BackupReasonOperationInProgress, condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Reason)
			require.Equal(t, 1, jobCount(t, f.client))

			finishJob(t, f.client, "other-job", batch.JobComplete, "")
			f.reconcile("b1")
			require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase, "the backup starts once the other operation finished")
		})
	}

	otherCHI := jobOwnedBy(t, "other-job", newRestoreCR("other"), model.Labels(api.ClickHouseRestoreCRDResourceKind, "other", "another-chi", "main", testStorage))
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"), otherCHI)
	f.reconcile("b1")
	require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase, "operations on other installations do not interfere")
}

// A status write lost to a conflict is redone by the next reconcile; the transition it carried
// must be announced once, after the write that finally lands.
func TestBackupAnnouncesEachTransitionOnce(t *testing.T) {
	conflicts := 1
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), conflictingStatusUpdates(&conflicts), newBackupCR("b1"))

	require.True(t, f.reconcile("b1").Requeue)
	require.Equal(t, 1, jobCount(t, f.client), "the Job is created before the status write")
	require.Empty(t, f.backup("b1").Status.Phase)
	require.Zero(t, eventCount(t, f.client, api.BackupReasonJobCreated), "nothing is announced for a write that did not land")

	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase, "the Job created by the lost pass is picked up")
	require.Equal(t, "b1-main-backup", got.Status.Clusters[0].JobName)
	require.Equal(t, 1, jobCount(t, f.client))
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonJobCreated))

	f.reconcile("b1")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonJobCreated))

	finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")
	conflicts = 1
	require.True(t, f.reconcile("b1").Requeue)
	require.Zero(t, eventCount(t, f.client, api.BackupReasonCompleted))
	f.reconcile("b1")
	require.Equal(t, 1, eventCount(t, f.client, api.BackupReasonCompleted))
}

// A completed backup stays completed while it is verified, even when its Job is gone meanwhile -
// removed by hand, or by a policy that sets ttlSecondsAfterFinished: the backup exists either way.
func TestBackupCompletedStaysCompletedWhileVerified(t *testing.T) {
	chb := newBackupCR("b1")
	chb.Spec.Verify = types.NewStringBool(true)
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
	f.reconcile("b1")
	finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")
	f.reconcile("b1")

	backupJob := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("b1-main-backup"), backupJob))
	require.NoError(t, f.client.Delete(context.Background(), backupJob))
	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase, "the verification still runs")
	require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionReady).Status)
	require.Zero(t, eventCount(t, f.client, api.BackupReasonJobLost))

	finishJob(t, f.client, "b1-main-verify", batch.JobComplete, "")
	f.reconcile("b1")
	got = f.backup("b1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
	require.Equal(t, api.BackupReasonVerified, condition(t, got.Status.Conditions, api.BackupConditionVerified).Reason)
}

func TestBackupVerification(t *testing.T) {
	for name, tc := range map[string]struct {
		condition batch.JobConditionType
		reason    string
		verified  meta.ConditionStatus
		expected  string
	}{
		"verified":      {condition: batch.JobComplete, verified: meta.ConditionTrue, expected: api.BackupReasonVerified},
		"failed":        {condition: batch.JobFailed, reason: batch.JobReasonBackoffLimitExceeded, verified: meta.ConditionFalse, expected: api.BackupReasonVerificationFailed},
		"out of time":   {condition: batch.JobFailed, reason: batch.JobReasonDeadlineExceeded, verified: meta.ConditionFalse, expected: api.BackupReasonDeadlineExceeded},
		"verify absent": {verified: meta.ConditionFalse, expected: api.BackupReasonJobLost},
		"never started": {verified: meta.ConditionFalse, expected: api.BackupReasonJobNotStarted},
	} {
		t.Run(name, func(t *testing.T) {
			chb := newBackupCR("b1")
			chb.Spec.Verify = types.NewStringBool(true)
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
			f.reconcile("b1")
			finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")

			f.reconcile("b1")
			got := f.backup("b1")
			require.Equal(t, api.OperationPhaseRunning, got.Status.Phase, "the backup completes only once verified")
			require.Equal(t, api.BackupReasonVerifying, condition(t, got.Status.Conditions, api.BackupConditionVerified).Reason)
			verifyJob := &batch.Job{}
			require.NoError(t, f.client.Get(context.Background(), key("b1-main-verify"), verifyJob))
			require.True(t, meta.IsControlledBy(verifyJob, got))

			switch {
			case tc.expected == api.BackupReasonJobNotStarted:
				stallJob(t, f.client, "b1-main-verify", neverStarted)
			case tc.condition == "":
				require.NoError(t, f.client.Delete(context.Background(), verifyJob))
			default:
				finishJob(t, f.client, "b1-main-verify", tc.condition, tc.reason)
			}
			f.reconcile("b1")
			got = f.backup("b1")
			require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase, "verification is advisory, the backup exists either way")
			verified := condition(t, got.Status.Conditions, api.BackupConditionVerified)
			require.Equal(t, tc.verified, verified.Status)
			require.Equal(t, tc.expected, verified.Reason)
			require.Equal(t, meta.ConditionTrue, condition(t, got.Status.Conditions, api.BackupConditionReady).Status)
		})
	}
}

// The cache may lag behind a Job created moments ago. Its absence there must not be taken for a
// lost Job: it is confirmed against the API server first.
func TestBackupConfirmsAnAbsenceAgainstTheAPIServer(t *testing.T) {
	chb := newBackupCR("b1")
	chb.Status.Phase = api.OperationPhaseRunning
	chb.Status.Clusters = runningOn("b1-main-backup")
	job := jobOwnedBy(t, "b1-main-backup", chb, model.Labels(api.ClickHouseBackupCRDResourceKind, "b1", testCHI, "main", testStorage))

	live := newClient(t, nil, chb, job)
	lagging := newClient(t, nil, chb)
	controller := NewBackupController(lagging, live, lagging.Scheme(), newFakeCR(readyInstallation(t)))

	_, err := controller.Reconcile(context.Background(), request("b1"))
	require.NoError(t, err)
	got := &api.ClickHouseBackup{}
	require.NoError(t, lagging.Get(context.Background(), kubeTypes.NamespacedName{Namespace: testNamespace, Name: "b1"}, got))
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
}

// A verification checks the backup that was made, which the status records: the spec may name
// another backup by the time the verification starts.
func TestBackupVerifiesTheBackupItMade(t *testing.T) {
	chb := newBackupCR("b1")
	chb.Spec.Verify = types.NewStringBool(true)
	chb.Spec.BackupName = "made"
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
	f.reconcile("b1")
	finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")

	edited := f.backup("b1")
	edited.Spec.BackupName = "edited-later"
	require.NoError(t, f.client.Update(context.Background(), edited))
	f.reconcile("b1")

	verifyJob := &batch.Job{}
	require.NoError(t, f.client.Get(context.Background(), key("b1-main-verify"), verifyJob))
	require.Contains(t, envValues(verifyJob.Spec.Template.Spec), "made")
	require.NotContains(t, envValues(verifyJob.Spec.Template.Spec), "edited-later")
	require.Equal(t, "made", f.backup("b1").Status.BackupName)
}

// A Job found by a pass whose predecessor lost its status write makes the backup it was built
// with: that is what the status records, whatever the spec says by now.
func TestBackupRecordsTheBackupItsJobMakes(t *testing.T) {
	original := newBackupCR("b1")
	original.Spec.BackupName = "made"
	topology, err := model.NewTopology(readyInstallation(t).GetAncestorT(), "")
	require.NoError(t, err)
	job := model.BuildBackupJob(original, readyInstallation(t), topology)
	require.NoError(t, ctrl.SetControllerReference(original, job, newScheme(t)))
	edited := original.DeepCopy()
	edited.Spec.BackupName = "edited-later"
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, edited, job)

	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
	require.Equal(t, "made", got.Status.BackupName)
}

// The Job and its log may be gone by the time anybody looks: why it failed, as its script wrote it
// to the termination message of its pod, goes to the status and to the event.
func TestBackupReportsWhyItsJobFailed(t *testing.T) {
	const reported = "backup b1-1: h0: create_remote b1-1 ended with status 'error': can't upload: disk full"
	for name, tc := range map[string]struct {
		verify  bool
		job     string
		reason  string
		message string
	}{
		"backup":       {job: "b1-main-backup", reason: api.BackupReasonJobFailed},
		"verification": {verify: true, job: "b1-main-verify", reason: api.BackupReasonVerificationFailed},
	} {
		t.Run(name, func(t *testing.T) {
			chb := newBackupCR("b1")
			chb.Spec.Verify = types.NewStringBool(tc.verify)
			f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, chb)
			f.reconcile("b1")
			if tc.verify {
				finishJob(t, f.client, "b1-main-backup", batch.JobComplete, "")
				f.reconcile("b1")
			}
			failPodOfJob(t, f.client, tc.job, reported+"\n")
			finishJob(t, f.client, tc.job, batch.JobFailed, batch.JobReasonBackoffLimitExceeded)

			f.reconcile("b1")
			conditionType := api.BackupConditionReady
			if tc.verify {
				conditionType = api.BackupConditionVerified
			}
			got := condition(t, f.backup("b1").Status.Conditions, conditionType)
			require.Equal(t, tc.reason, got.Reason)
			require.Equal(t, "Job "+tc.job+" failed: "+reported, got.Message)
			messages := eventMessages(t, f.client, tc.reason)
			require.Len(t, messages, 1)
			require.Contains(t, messages[0], reported)
		})
	}

	t.Run("no pod left", func(t *testing.T) {
		f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, newBackupCR("b1"))
		f.reconcile("b1")
		finishJob(t, f.client, "b1-main-backup", batch.JobFailed, batch.JobReasonBackoffLimitExceeded)
		f.reconcile("b1")
		require.Equal(t, "Job b1-main-backup failed, see its logs", condition(t, f.backup("b1").Status.Conditions, api.BackupConditionReady).Message)
	})
}

// The last-outcome gauges live in the operator's memory: every reconcile sets them again from the
// status, the one of a finished backup included, so that they are back after a restart.
func TestBackupOutcomeGaugesComeBackFromTheStatus(t *testing.T) {
	captured := captureOutcomes(t)
	succeeded, failed := meta.NewTime(time.Unix(1700000000, 0)), meta.NewTime(time.Unix(1700003600, 0))
	done := newBackupCR("done")
	done.Status.Phase = api.OperationPhaseCompleted
	done.Status.CompletionTime = &succeeded
	done.Status.Clusters = []api.OperationClusterStatus{{Name: "main", Phase: api.OperationPhaseCompleted, CompletionTime: &succeeded}}
	newBackupOperation(done).setCondition(api.BackupConditionReady, meta.ConditionTrue, api.BackupReasonCompleted, "")
	broken := newBackupCR("broken")
	broken.Status.Phase = api.OperationPhaseFailed
	broken.Status.CompletionTime = &failed
	broken.Status.Clusters = []api.OperationClusterStatus{{Name: "main", Phase: api.OperationPhaseFailed, CompletionTime: &failed}}
	f := newBackupFixture(t, newFakeCR(readyInstallation(t)), nil, done, broken)

	f.reconcile("done")
	f.reconcile("broken")
	require.Equal(t, []outcome{{namespace: testNamespace, chi: testCHI, cluster: "main", at: succeeded.Unix()}}, captured.successes)
	require.Equal(t, []outcome{{namespace: testNamespace, chi: testCHI, cluster: "main", at: failed.Unix()}}, captured.failures)
}

// A backup of every cluster runs a Job per backup-enabled cluster, each driving the hosts of its own
// cluster, all making the backup under one name. It completes once every cluster completed, and each
// cluster tells its own outcome and gauge.
func TestBackupRunsAJobPerCluster(t *testing.T) {
	captured := captureOutcomes(t)
	f := newBackupFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newBackupCR("b1"))

	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
	require.Len(t, got.Status.Clusters, 2, "the cluster without backup is not targeted")
	for i, tc := range []struct {
		cluster string
		job     string
		hosts   []string
	}{
		{cluster: "hot", job: "b1-hot-backup", hosts: []string{"chi-demo-hot-0-0", "chi-demo-hot-1-0"}},
		{cluster: "cold", job: "b1-cold-backup", hosts: []string{"chi-demo-cold-0-0"}},
	} {
		cluster := got.Status.Clusters[i]
		require.Equal(t, tc.cluster, cluster.Name)
		require.Equal(t, api.OperationPhaseRunning, cluster.Phase)
		require.Equal(t, tc.job, cluster.JobName)
		require.Equal(t, tc.hosts, cluster.Hosts)
		require.NotNil(t, cluster.StartTime)
		job := &batch.Job{}
		require.NoError(t, f.client.Get(context.Background(), key(tc.job), job))
		require.Equal(t, model.Labels(api.ClickHouseBackupCRDResourceKind, "b1", testCHI, tc.cluster, testStorage), job.Labels)
		require.Equal(t, got.Status.BackupName, jobEnv(t, f.client, tc.job, "BACKUP_NAME"), "every cluster makes the one backup")
		require.Equal(t, strings.Join(tc.hosts, " "), jobEnv(t, f.client, tc.job, "BACKUP_HOSTS"))
	}
	require.Equal(t, "Jobs b1-hot-backup, b1-cold-backup created", condition(t, got.Status.Conditions, api.BackupConditionJobCreated).Message)
	require.Equal(t, 2, eventCount(t, f.client, api.BackupReasonJobCreated))

	finishJob(t, f.client, "b1-hot-backup", batch.JobComplete, "")
	f.reconcile("b1")
	got = f.backup("b1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase, "a backup completes once every cluster completed")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Clusters[0].Phase)
	require.Nil(t, apiMeta.FindStatusCondition(got.Status.Conditions, api.BackupConditionReady))

	finishJob(t, f.client, "b1-cold-backup", batch.JobComplete, "")
	f.reconcile("b1")
	got = f.backup("b1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, meta.ConditionTrue, ready.Status)
	require.Equal(t, api.BackupReasonCompleted, ready.Reason)
	require.Equal(t, "cluster hot: Job b1-hot-backup completed; cluster cold: Job b1-cold-backup completed", ready.Message)
	require.Equal(t, 2, eventCount(t, f.client, api.BackupReasonCompleted))
	require.NotNil(t, got.Status.CompletionTime)

	f.reconcile("b1")
	clusters := map[string]bool{}
	for _, success := range captured.successes {
		clusters[success.cluster] = true
	}
	require.Equal(t, map[string]bool{"hot": true, "cold": true}, clusters, "each cluster has a gauge of its own")
}

// A backup that fails on one cluster and completes on another made the backup of the one, which is
// kept: it fails, telling which cluster completed and why the other failed, once.
func TestBackupFailsOnOneClusterOnly(t *testing.T) {
	captured := captureOutcomes(t)
	f := newBackupFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, newBackupCR("b1"))
	f.reconcile("b1")
	finishJob(t, f.client, "b1-hot-backup", batch.JobComplete, "")
	failPodOfJob(t, f.client, "b1-cold-backup", "disk full")
	finishJob(t, f.client, "b1-cold-backup", batch.JobFailed, batch.JobReasonBackoffLimitExceeded)

	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseFailed, got.Status.Phase)
	ready := condition(t, got.Status.Conditions, api.BackupConditionReady)
	require.Equal(t, meta.ConditionFalse, ready.Status)
	require.Equal(t, api.BackupReasonPartiallyCompleted, ready.Reason)
	require.Equal(t, "completed on cluster hot; cluster cold: Job b1-cold-backup failed: disk full", ready.Message)
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Clusters[0].Phase)
	require.Equal(t, api.OperationPhaseFailed, got.Status.Clusters[1].Phase)
	require.Equal(t, api.BackupReasonJobFailed, got.Status.Clusters[1].Reason)
	require.Len(t, eventMessagesOfType(t, f.client, api.BackupReasonPartiallyCompleted, core.EventTypeWarning), 1)
	require.Len(t, eventMessages(t, f.client, api.BackupReasonJobFailed), 1)

	f.reconcile("b1")
	require.Len(t, eventMessages(t, f.client, api.BackupReasonPartiallyCompleted), 1, "a finished backup is never touched again")
	require.Equal(t, "hot", captured.successes[len(captured.successes)-1].cluster)
	require.Equal(t, "cold", captured.failures[len(captured.failures)-1].cluster)
}

// The clusters and the backup name of a backup are resolved once, when it is validated: a cluster
// whose Job waits for another operation starts later with the name the others made the backup under,
// whatever the spec names by then; a cluster added meanwhile is not targeted, and one that left fails
// the backup there while the others go on.
func TestBackupPinsItsClustersAndName(t *testing.T) {
	cr := newFakeCR(twoClusters(t, api.StatusCompleted))
	busy := otherOperationOn(t, "cold")
	f := newBackupFixture(t, cr, nil, newBackupCR("b1"), busy)

	f.reconcile("b1")
	got := f.backup("b1")
	name := got.Status.BackupName
	require.Equal(t, api.OperationPhaseRunning, got.Status.Clusters[0].Phase, "an operation on another cluster does not hold this one")
	require.Equal(t, api.OperationPhasePending, got.Status.Clusters[1].Phase)
	require.Equal(t, api.BackupReasonOperationInProgress, got.Status.Clusters[1].Reason)
	jobCreated := condition(t, got.Status.Conditions, api.BackupConditionJobCreated)
	require.Equal(t, meta.ConditionFalse, jobCreated.Status)
	require.Equal(t, operationInProgressMessage(testCHI, "cold"), jobCreated.Message)

	got.Spec.BackupName = "edited-later"
	require.NoError(t, f.client.Update(context.Background(), got))
	cr.set(installation(t, api.StatusCompleted, backupCluster("hot", 2, true), backupCluster("cold", 1, true), backupCluster("third", 1, true)))
	finishJob(t, f.client, busy.Name, batch.JobComplete, "")
	f.reconcile("b1")
	got = f.backup("b1")
	require.Len(t, got.Status.Clusters, 2, "a cluster added after the validation is not targeted")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Clusters[1].Phase)
	require.Equal(t, name, got.Status.BackupName)
	require.Equal(t, name, jobEnv(t, f.client, "b1-cold-backup", "BACKUP_NAME"), "every cluster makes the backup under one name")

	t.Run("a cluster that left", func(t *testing.T) {
		cr := newFakeCR(twoClusters(t, api.StatusCompleted))
		f := newBackupFixture(t, cr, nil, newBackupCR("b1"), otherOperationOn(t, "cold"))
		f.reconcile("b1")
		cr.set(installation(t, api.StatusCompleted, backupCluster("hot", 2, true)))
		f.reconcile("b1")
		got := f.backup("b1")
		require.Equal(t, api.OperationPhaseFailed, got.Status.Clusters[1].Phase)
		require.Equal(t, api.BackupReasonClusterNotFound, got.Status.Clusters[1].Reason)
		require.Empty(t, got.Status.Clusters[1].JobName)
		require.Equal(t, api.OperationPhaseRunning, got.Status.Phase, "the other cluster goes on")
	})
}

// Every cluster verifies the backup it made in a Job of its own; the backup is verified once every
// cluster verified it.
func TestBackupVerifiesEveryCluster(t *testing.T) {
	chb := newBackupCR("b1")
	chb.Spec.Verify = types.NewStringBool(true)
	f := newBackupFixture(t, newFakeCR(twoClusters(t, api.StatusCompleted)), nil, chb)
	f.reconcile("b1")
	finishJob(t, f.client, "b1-hot-backup", batch.JobComplete, "")
	finishJob(t, f.client, "b1-cold-backup", batch.JobComplete, "")

	f.reconcile("b1")
	got := f.backup("b1")
	require.Equal(t, api.OperationPhaseRunning, got.Status.Phase)
	require.Equal(t, "b1-hot-verify", got.Status.Clusters[0].JobName)
	require.Equal(t, "b1-cold-verify", got.Status.Clusters[1].JobName)
	require.Equal(t, api.BackupReasonVerifying, condition(t, got.Status.Conditions, api.BackupConditionVerified).Reason)
	require.Equal(t, "chi-demo-cold-0-0", jobEnv(t, f.client, "b1-cold-verify", "VERIFY_HOSTS"))

	finishJob(t, f.client, "b1-hot-verify", batch.JobComplete, "")
	f.reconcile("b1")
	require.Equal(t, api.OperationPhaseRunning, f.backup("b1").Status.Phase, "a backup is verified once every cluster verified it")

	finishJob(t, f.client, "b1-cold-verify", batch.JobFailed, batch.JobReasonBackoffLimitExceeded)
	f.reconcile("b1")
	got = f.backup("b1")
	require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase)
	verified := condition(t, got.Status.Conditions, api.BackupConditionVerified)
	require.Equal(t, meta.ConditionFalse, verified.Status)
	require.Equal(t, api.BackupReasonVerificationFailed, verified.Reason)
	require.Contains(t, verified.Message, "cluster cold: Job b1-cold-verify failed")
	require.Equal(t, api.BackupReasonVerified, got.Status.Clusters[0].Reason)

	t.Run("a cluster that left", func(t *testing.T) {
		cr := newFakeCR(twoClusters(t, api.StatusCompleted))
		f := newBackupFixture(t, cr, nil, chb)
		f.reconcile("b1")
		finishJob(t, f.client, "b1-hot-backup", batch.JobComplete, "")
		finishJob(t, f.client, "b1-cold-backup", batch.JobComplete, "")
		cr.set(installation(t, api.StatusCompleted, backupCluster("hot", 2, true)))
		f.reconcile("b1")
		finishJob(t, f.client, "b1-hot-verify", batch.JobComplete, "")

		f.reconcile("b1")
		got := f.backup("b1")
		require.Equal(t, api.OperationPhaseCompleted, got.Status.Phase, "a cluster that can never be verified concludes the verification")
		require.Equal(t, api.BackupReasonClusterNotFound, condition(t, got.Status.Conditions, api.BackupConditionVerified).Reason)
	})
}
