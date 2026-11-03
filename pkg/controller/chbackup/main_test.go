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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	apiErrors "k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiMachinery "k8s.io/apimachinery/pkg/runtime"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	clientGoScheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
)

const (
	testNamespace = "ns"
	testCHI       = "demo"
)

// IsNamespaceWatched, the normalizer and the event emitter resolve configuration through the
// global chop singleton, which nil-panics before chop.New. A watch namespace exported in the
// shell would change what the namespace guard tests assert, so it is cleared first.
func TestMain(m *testing.M) {
	os.Unsetenv("WATCH_NAMESPACE")
	os.Unsetenv("WATCH_NAMESPACES")
	chop.New(nil, nil, "")
	os.Exit(m.Run())
}

// errEmptyName is what the clientset answers a get of no name with, which is no NotFound
var errEmptyName = errors.New("resource name may not be empty")

// fakeCR serves installations together with their normalized completed state, as the operator's
// storage-backed CR reader does.
type fakeCR struct {
	chis map[string]*api.ClickHouseInstallation
}

func newFakeCR(chis ...*api.ClickHouseInstallation) *fakeCR {
	f := &fakeCR{chis: map[string]*api.ClickHouseInstallation{}}
	for _, chi := range chis {
		f.set(chi)
	}
	return f
}

func (f *fakeCR) set(chi *api.ClickHouseInstallation) {
	f.chis[kubeTypes.NamespacedName{Namespace: chi.Namespace, Name: chi.Name}.String()] = chi
}

func (f *fakeCR) Get(_ context.Context, namespace, name string) (api.ICustomResource, error) {
	// As client-go refuses to build the request
	if name == "" {
		return nil, errEmptyName
	}
	chi, ok := f.chis[kubeTypes.NamespacedName{Namespace: namespace, Name: name}.String()]
	if !ok {
		return nil, apiErrors.NewNotFound(api.Resource("clickhouseinstallations"), name)
	}
	return chi.DeepCopy(), nil
}

func (f *fakeCR) StatusUpdate(context.Context, api.ICustomResource, types.UpdateStatusOptions) error {
	return nil
}

const (
	// testStorageSecret is the storage Secret of the backup-enabled clusters of the tests, without
	// which their backup stays off
	testStorageSecret = "s3-credentials"
	// testStorage is the backup storage of the test installation, whose Secret is testStorageSecret
	testStorage = "main"
)

func backupCluster(name string, shards int, enabled bool) *api.Cluster {
	return &api.Cluster{
		Name:   name,
		Layout: &api.ChiClusterLayout{ShardsCount: shards, ReplicasCount: 2},
		Backup: &api.ClusterBackup{
			Enabled:  types.NewStringBool(enabled),
			Storages: []api.BackupStorage{{Name: testStorage, SecretRef: &core.LocalObjectReference{Name: testStorageSecret}}},
		},
	}
}

// lackingCluster is a cluster that turns backup on without a storage, which leaves its backup off.
func lackingCluster(name string, shards int) *api.Cluster {
	cluster := backupCluster(name, shards, true)
	cluster.Backup.Storages = nil
	return cluster
}

// installation returns a CHI whose last reconcile completed with the given clusters, with the
// normalized state the controllers resolve hosts from, in the serialized form it is stored in.
func installation(t *testing.T, status string, clusters ...*api.Cluster) *api.ClickHouseInstallation {
	t.Helper()
	return installationFrom(t, installationSpec(clusters...), status)
}

// installationSpec returns the test CHI with the given clusters, as the user created it.
func installationSpec(clusters ...*api.Cluster) *api.ClickHouseInstallation {
	chi := &api.ClickHouseInstallation{}
	chi.Name = testCHI
	chi.Namespace = testNamespace
	chi.UID = "chi-uid"
	chi.Spec.Configuration = &api.Configuration{Clusters: clusters}
	return chi
}

// installationFrom returns chi with the given status and the normalized state of its last completed
// reconcile, in the serialized form it is stored in.
func installationFrom(t *testing.T, chi *api.ClickHouseInstallation, status string) *api.ClickHouseInstallation {
	t.Helper()
	chi = chi.DeepCopy()
	normalized, err := normalizer.New(nil).CreateTemplated(chi.DeepCopy(), commonNormalizer.NewOptions[api.ClickHouseInstallation]())
	require.NoError(t, err)
	data, err := json.Marshal(normalized)
	require.NoError(t, err)
	stored := &api.ClickHouseInstallation{}
	require.NoError(t, json.Unmarshal(data, stored))

	chi.EnsureStatus().Status = status
	chi.SetAncestor(stored)
	return chi
}

// twoClusters returns an installation whose last reconcile completed with two backup-enabled
// clusters, hot with two shards and cold with one, of two replicas each, and plain without backup.
func twoClusters(t *testing.T, status string) *api.ClickHouseInstallation {
	return installation(t, status, backupCluster("hot", 2, true), backupCluster("cold", 1, true), backupCluster("plain", 1, false))
}

// jobEnv returns the plain value of a variable of the container of a Job.
func jobEnv(t *testing.T, c client.Client, name, variable string) string {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(name), job))
	for _, e := range job.Spec.Template.Spec.Containers[0].Env {
		if e.Name == variable {
			return e.Value
		}
	}
	return ""
}

func readyInstallation(t *testing.T) *api.ClickHouseInstallation {
	return installation(t, api.StatusCompleted, backupCluster("main", 2, true))
}

// stoppedInstallations returns a ready installation stopped by its spec, and one whose spec no
// longer stops it while the state of its last completed reconcile still does.
func stoppedInstallations(t *testing.T) map[string]*api.ClickHouseInstallation {
	stopped := readyInstallation(t)
	stopped.Spec.Stop = types.NewStringBool(true)
	starting := readyInstallation(t)
	starting.GetAncestorT().Spec.Stop = types.NewStringBool(true)
	return map[string]*api.ClickHouseInstallation{"stopped": stopped, "still stopped": starting}
}

func newScheme(t *testing.T) *apiMachinery.Scheme {
	t.Helper()
	scheme := apiMachinery.NewScheme()
	require.NoError(t, clientGoScheme.AddToScheme(scheme))
	require.NoError(t, api.AddToScheme(scheme))
	return scheme
}

// newClient returns a fake client that, like the API server, gives every created object a UID:
// ownership checks compare UIDs, and the fake leaves them empty.
func newClient(t *testing.T, funcs *interceptor.Funcs, objs ...client.Object) client.WithWatch {
	t.Helper()
	if funcs == nil {
		funcs = &interceptor.Funcs{}
	}
	if funcs.Create == nil {
		created := 0
		funcs.Create = func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				created++
				obj.SetUID(kubeTypes.UID(fmt.Sprintf("created-%d", created)))
			}
			return c.Create(ctx, obj, opts...)
		}
	}
	return fake.NewClientBuilder().
		WithScheme(newScheme(t)).
		WithStatusSubresource(&api.ClickHouseBackup{}, &api.ClickHouseBackupSchedule{}, &api.ClickHouseRestore{}).
		WithObjects(objs...).
		WithInterceptorFuncs(*funcs).
		Build()
}

func request(name string) ctrl.Request {
	return ctrl.Request{NamespacedName: kubeTypes.NamespacedName{Namespace: testNamespace, Name: name}}
}

func key(name string) kubeTypes.NamespacedName {
	return kubeTypes.NamespacedName{Namespace: testNamespace, Name: name}
}

func newBackupCR(name string) *api.ClickHouseBackup {
	chb := &api.ClickHouseBackup{}
	chb.Name = name
	chb.Namespace = testNamespace
	chb.UID = kubeTypes.UID(name + "-uid")
	chb.Spec.ClickHouseInstallation = testCHI
	return chb
}

func newRestoreCR(name string) *api.ClickHouseRestore {
	chr := &api.ClickHouseRestore{}
	chr.Name = name
	chr.Namespace = testNamespace
	chr.UID = kubeTypes.UID(name + "-uid")
	chr.Spec.ClickHouseInstallation = testCHI
	chr.Spec.BackupName = "nightly-20260101-000000"
	return chr
}

func newScheduleCR(name string) *api.ClickHouseBackupSchedule {
	chbs := &api.ClickHouseBackupSchedule{}
	chbs.Name = name
	chbs.Namespace = testNamespace
	chbs.UID = kubeTypes.UID(name + "-uid")
	chbs.Spec.ClickHouseInstallation = testCHI
	chbs.Spec.Schedule = "0 3 * * *"
	return chbs
}

// runningOn returns the status of an operation running on the main cluster of the test installation in
// the given Job.
func runningOn(jobName string) []api.OperationClusterStatus {
	return []api.OperationClusterStatus{{Name: "main", Phase: api.OperationPhaseRunning, JobName: jobName}}
}

// ageSuspensions makes the suspension of every CronJob the operator suspended old enough for a restore
// to start, as if the time a restore waits for the CronJob controller to see it had passed.
func ageSuspensions(t *testing.T, c client.Client) {
	t.Helper()
	cronJobs := &batch.CronJobList{}
	require.NoError(t, c.List(context.Background(), cronJobs, client.InNamespace(testNamespace)))
	for i := range cronJobs.Items {
		cronJob := &cronJobs.Items[i]
		if _, suspended := cronJob.Annotations[model.AnnotationCronJobSuspendedAt]; !suspended {
			continue
		}
		cronJob.Annotations[model.AnnotationCronJobSuspendedAt] = time.Now().Add(-2 * pendingRequeueInterval).UTC().Format(time.RFC3339)
		require.NoError(t, c.Update(context.Background(), cronJob))
	}
}

// jobOwnedBy returns a running Job controlled by owner, as the controllers create them.
func jobOwnedBy(t *testing.T, name string, owner client.Object, labels map[string]string) *batch.Job {
	t.Helper()
	job := &batch.Job{}
	job.Name = name
	job.Namespace = testNamespace
	job.Labels = labels
	require.NoError(t, ctrl.SetControllerReference(owner, job, newScheme(t)))
	return job
}

// finishJob marks a Job finished now with the given condition.
func finishJob(t *testing.T, c client.Client, name string, conditionType batch.JobConditionType, reason string) {
	t.Helper()
	finishJobAt(t, c, name, conditionType, reason, time.Now())
}

// finishJobAt marks a Job finished at the given time with the given condition, as the Job
// controller does.
func finishJobAt(t *testing.T, c client.Client, name string, conditionType batch.JobConditionType, reason string, at time.Time) {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(name), job))
	finished := meta.NewTime(at)
	job.Status.Conditions = append(job.Status.Conditions, batch.JobCondition{
		Type:               conditionType,
		Status:             core.ConditionTrue,
		Reason:             reason,
		LastTransitionTime: finished,
	})
	if conditionType == batch.JobComplete {
		job.Status.CompletionTime = &finished
	}
	require.NoError(t, c.Status().Update(context.Background(), job))
}

// failPodOfJob gives a Job a failed pod whose Job container terminated with the given message, as
// kubelet reports it from the container's termination message path.
func failPodOfJob(t *testing.T, c client.Client, jobName, message string) {
	t.Helper()
	failPodOfJobWithExitCode(t, c, jobName, jobName+"-pod", message, 1)
}

// failPodOfJobWithExitCode is failPodOfJob for a pod of the given name whose Job container exited
// with the given code.
func failPodOfJobWithExitCode(t *testing.T, c client.Client, jobName, podName, message string, exitCode int32) {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(jobName), job))
	podOfJob(t, c, jobName, podName, core.PodStatus{
		Phase: core.PodFailed,
		ContainerStatuses: []core.ContainerStatus{{
			Name:  job.Spec.Template.Spec.Containers[0].Name,
			State: core.ContainerState{Terminated: &core.ContainerStateTerminated{ExitCode: exitCode, Message: message}},
		}},
	})
}

// succeedPodOfJob gives a Job a succeeded pod whose Job container terminated with the given message,
// as kubelet reports it from the container's termination message path.
func succeedPodOfJob(t *testing.T, c client.Client, jobName, message string) {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(jobName), job))
	podOfJob(t, c, jobName, jobName+"-pod", core.PodStatus{
		Phase: core.PodSucceeded,
		ContainerStatuses: []core.ContainerStatus{{
			Name:  job.Spec.Template.Spec.Containers[0].Name,
			State: core.ContainerState{Terminated: &core.ContainerStateTerminated{Message: message}},
		}},
	})
}

// eventMessages returns the messages of the events with the given reason.
func eventMessages(t *testing.T, c client.Client, reason string) []string {
	t.Helper()
	return eventMessagesOfType(t, c, reason, "")
}

// eventMessagesOfType returns the messages of the events with the given reason and type, of any
// type when eventType is empty.
func eventMessagesOfType(t *testing.T, c client.Client, reason, eventType string) []string {
	t.Helper()
	events := &core.EventList{}
	require.NoError(t, c.List(context.Background(), events, client.InNamespace(testNamespace)))
	var messages []string
	for _, event := range events.Items {
		if (event.Reason == reason) && ((eventType == "") || (event.Type == eventType)) {
			messages = append(messages, event.Message)
		}
	}
	return messages
}

// outcome is a value a last-outcome gauge of a cluster of an installation is set to.
type outcome struct {
	namespace string
	chi       string
	cluster   string
	at        int64
}

// outcomes collects what the controllers set the last-outcome gauges to.
type outcomes struct {
	successes []outcome
	failures  []outcome
}

// captureOutcomes collects what the controllers set the last-outcome gauges to for the duration of
// the test.
func captureOutcomes(t *testing.T) *outcomes {
	t.Helper()
	captured := &outcomes{}
	savedSuccess, savedFailure := reportSuccess, reportFailure
	reportSuccess = func(_ context.Context, namespace, chi, cluster string, at time.Time) {
		captured.successes = append(captured.successes, outcome{namespace: namespace, chi: chi, cluster: cluster, at: at.Unix()})
	}
	reportFailure = func(_ context.Context, namespace, chi, cluster string, at time.Time) {
		captured.failures = append(captured.failures, outcome{namespace: namespace, chi: chi, cluster: cluster, at: at.Unix()})
	}
	t.Cleanup(func() {
		reportSuccess, reportFailure = savedSuccess, savedFailure
	})
	return captured
}

// stallJob makes a Job older than jobStartTimeout, with the pod counts status sets.
func stallJob(t *testing.T, c client.Client, name string, status func(*batch.JobStatus)) {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(name), job))
	job.CreationTimestamp = meta.NewTime(time.Now().Add(-jobStartTimeout - time.Minute))
	require.NoError(t, c.Update(context.Background(), job))
	status(&job.Status)
	require.NoError(t, c.Status().Update(context.Background(), job))
}

// neverStarted is the status of a Job whose only pod is still pending, on a cluster that reports
// ready pods. A pod that ran and stopped being ready, on a node gone NotReady, shows the same.
func neverStarted(status *batch.JobStatus) {
	status.Active = 1
	status.Ready = ptr.To(int32(0))
}

// runningReady is the status of a Job whose only pod runs and is ready.
func runningReady(status *batch.JobStatus) {
	status.Active = 1
	status.Ready = ptr.To(int32(1))
}

// setJobStatus sets the pod counts of a Job, as the Job controller does.
func setJobStatus(t *testing.T, c client.Client, name string, status func(*batch.JobStatus)) {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(name), job))
	status(&job.Status)
	require.NoError(t, c.Status().Update(context.Background(), job))
}

// podOfJob creates a pod of the named Job with the given status, which the Job selects by the labels
// selectPodsOfJob gives it.
func podOfJob(t *testing.T, c client.Client, jobName, podName string, status core.PodStatus) {
	t.Helper()
	labels := selectPodsOfJob(t, c, jobName)

	pod := &core.Pod{}
	pod.Name = podName
	pod.Namespace = testNamespace
	pod.Labels = labels
	pod.Status = status
	require.NoError(t, c.Create(context.Background(), pod))
}

// selectPodsOfJob gives a Job the selector the API server generates for it, which the fake client
// leaves out, and returns the labels it selects pods by.
func selectPodsOfJob(t *testing.T, c client.Client, jobName string) map[string]string {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(jobName), job))
	labels := map[string]string{batch.ControllerUidLabel: string(job.GetUID())}
	job.Spec.Selector = &meta.LabelSelector{MatchLabels: labels}
	require.NoError(t, c.Update(context.Background(), job))
	return labels
}

// requireRunning asserts that a Job is left running, with its start recorded.
func requireRunning(t *testing.T, c client.Client, name string) {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(name), job))
	require.NotContains(t, job.Annotations, model.AnnotationJobNotStarted, "a Job whose pod started is never stopped")
	require.Contains(t, job.Annotations, model.AnnotationJobStarted, "a seen start is recorded")
}

// requireStopped asserts that the operator stopped a Job whose pod never started.
func requireStopped(t *testing.T, c client.Client, name string) {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(name), job))
	require.Contains(t, job.Annotations, model.AnnotationJobNotStarted)
	require.Equal(t, jobNotStartedDeadlineSeconds, ptr.Deref(job.Spec.ActiveDeadlineSeconds, 0),
		"the Job controller fails a Job past its deadline and deletes its pods")
}

// eventCount counts the events with the given reason.
func eventCount(t *testing.T, c client.Client, reason string) int {
	t.Helper()
	events := &core.EventList{}
	require.NoError(t, c.List(context.Background(), events, client.InNamespace(testNamespace)))
	count := 0
	for _, event := range events.Items {
		if event.Reason == reason {
			count++
		}
	}
	return count
}

// envValues returns the plain values of the environment of a Job's container.
func envValues(spec core.PodSpec) []string {
	var values []string
	for _, e := range spec.Containers[0].Env {
		values = append(values, e.Value)
	}
	return values
}

func jobCount(t *testing.T, c client.Client) int {
	t.Helper()
	jobs := &batch.JobList{}
	require.NoError(t, c.List(context.Background(), jobs, client.InNamespace(testNamespace)))
	return len(jobs.Items)
}

func condition(t *testing.T, conditions []meta.Condition, conditionType string) meta.Condition {
	t.Helper()
	for _, c := range conditions {
		if c.Type == conditionType {
			return c
		}
	}
	require.Failf(t, "missing condition", "%s", conditionType)
	return meta.Condition{}
}

// watchOnly restricts the operator to the given namespace for the duration of the test.
func watchOnly(t *testing.T, namespace string) {
	t.Helper()
	saved := chop.Config().Watch.Namespaces.Include
	chop.Config().Watch.Namespaces.Include = types.NewStrings([]string{namespace})
	t.Cleanup(func() { chop.Config().Watch.Namespaces.Include = saved })
}

// conflictingStatusUpdates fails the status writes of custom resources with a conflict, as a
// concurrent writer would, while *conflicts is positive, counting each failure down. Job status
// writes, which the tests make themselves, always pass.
func conflictingStatusUpdates(conflicts *int) *interceptor.Funcs {
	return &interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, isJob := obj.(*batch.Job); !isJob && (*conflicts > 0) {
				*conflicts--
				return apiErrors.NewConflict(api.Resource("clickhousebackups"), obj.GetName(), nil)
			}
			return c.SubResource(subResource).Update(ctx, obj, opts...)
		},
	}
}
