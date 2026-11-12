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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	batch "k8s.io/api/batch/v1"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeTypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	model "github.com/altinity/clickhouse-operator/pkg/model/chbackup"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
)

// hostPod is the running pod of host shard/replica of cluster main of the test installation, on a node.
func hostPod(t *testing.T, chi *api.ClickHouseInstallation, shard, replica int, node string, ready bool) *core.Pod {
	t.Helper()
	shards, err := model.BackupShards(chi.GetAncestorT())
	require.NoError(t, err)
	host := shards[shard].Hosts[replica]
	status := core.ConditionFalse
	if ready {
		status = core.ConditionTrue
	}
	name := model.ReplicaPodName(host)
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
			UID:       kubeTypes.UID(name + "-uid"),
			Labels:    map[string]string{model.LabelApp: model.InstallationPodAppValue, installationLabel: testCHI},
		},
		Spec: core.PodSpec{
			NodeName: node,
			Containers: []core.Container{{
				Name:         config.ClickHouseContainerName,
				VolumeMounts: []core.VolumeMount{{Name: "data", MountPath: config.DirPathDataStorage}},
			}},
			Volumes: []core.Volume{{Name: "data", VolumeSource: core.VolumeSource{
				PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{ClaimName: "data-" + name},
			}}},
		},
		Status: core.PodStatus{
			Phase: core.PodRunning,
			Conditions: []core.PodCondition{{
				Type: core.PodReady, Status: status, LastTransitionTime: meta.NewTime(time.Now().Add(-time.Hour)),
			}},
		},
	}
}

func shardPods(t *testing.T, c client.Client) map[string]*core.Pod {
	t.Helper()
	list := &core.PodList{}
	require.NoError(t, c.List(context.Background(), list, client.MatchingLabels{model.LabelBackupComponent: model.BackupComponentShardPod}))
	pods := map[string]*core.Pod{}
	for i := range list.Items {
		pods[list.Items[i].Name] = &list.Items[i]
	}
	return pods
}

// newShardPodController builds the controller over a client that holds the installation, the objects, and a backup
// Job active on cluster main
func newShardPodController(t *testing.T, chi *api.ClickHouseInstallation, objs ...client.Object) (*ShardPodController, client.Client) {
	t.Helper()
	c := newClient(t, nil, append([]client.Object{chi, activeJob("backup-main", "main")}, objs...)...)
	return NewShardPodController(c, c, newScheme(t), newFakeCR(chi)), c
}

// podModeCluster is a backup-enabled cluster of 2 replicas per shard backed up by backup Pods.
func podModeCluster(name string, shards int) *api.Cluster {
	cluster := backupCluster(name, shards, true)
	cluster.Backup.Mode = types.NewString(api.BackupModePod)
	return cluster
}

// podModeInstallation is the test installation with cluster main backed up by backup Pods.
func podModeInstallation(t *testing.T, shards int) *api.ClickHouseInstallation {
	t.Helper()
	return installation(t, api.StatusCompleted, podModeCluster("main", shards))
}

// activeJob is a backup Job of the test installation on the cluster that has neither completed nor failed.
func activeJob(name, cluster string) *batch.Job {
	return &batch.Job{ObjectMeta: meta.ObjectMeta{
		Name:      name,
		Namespace: testNamespace,
		Labels:    model.Labels(api.ClickHouseBackupCRDResourceKind, "chb", testCHI, cluster),
	}}
}

// endJob marks the Job complete, or failed.
func endJob(t *testing.T, c client.Client, name string, conditionType batch.JobConditionType) {
	t.Helper()
	job := &batch.Job{}
	require.NoError(t, c.Get(context.Background(), key(name), job))
	job.Status.Conditions = append(job.Status.Conditions, batch.JobCondition{Type: conditionType, Status: core.ConditionTrue})
	require.NoError(t, c.Status().Update(context.Background(), job))
}

func shardServices(t *testing.T, c client.Client) map[string]*core.Service {
	t.Helper()
	list := &core.ServiceList{}
	require.NoError(t, c.List(context.Background(), list, client.MatchingLabels{model.LabelBackupComponent: model.BackupComponentShardService}))
	services := map[string]*core.Service{}
	for i := range list.Items {
		services[list.Items[i].Name] = &list.Items[i]
	}
	return services
}

func TestShardPodPerShardOnTheNodeOfThePreferredReplica(t *testing.T) {
	chi := podModeInstallation(t, 2) // cluster main: 2 shards x 2 replicas
	r, c := newShardPodController(t, chi,
		hostPod(t, chi, 0, 0, "node-a", true), hostPod(t, chi, 0, 1, "node-b", true),
		hostPod(t, chi, 1, 0, "node-c", true), hostPod(t, chi, 1, 1, "node-d", true))

	_, err := r.Reconcile(context.Background(), request(testCHI))
	require.NoError(t, err)

	pods := shardPods(t, c)
	require.Len(t, pods, 2)
	require.Equal(t, "node-a", pods[model.ShardPodName(testCHI, "main", "0")].Spec.NodeSelector["kubernetes.io/hostname"])
	require.Equal(t, "node-c", pods[model.ShardPodName(testCHI, "main", "1")].Spec.NodeSelector["kubernetes.io/hostname"])
	owner := pods[model.ShardPodName(testCHI, "main", "0")].OwnerReferences[0]
	require.Equal(t, testCHI, owner.Name)
	require.True(t, *owner.Controller)
}

func TestShardPodFollowsThePreferredReplicaOfTheSpec(t *testing.T) {
	cluster := podModeCluster("main", 1)
	cluster.Backup.PreferredReplica = types.NewInt32(1)
	chi := installation(t, api.StatusCompleted, cluster)
	r, c := newShardPodController(t, chi, hostPod(t, chi, 0, 0, "node-a", true), hostPod(t, chi, 0, 1, "node-b", true))

	_, err := r.Reconcile(context.Background(), request(testCHI))
	require.NoError(t, err)
	pod := shardPods(t, c)[model.ShardPodName(testCHI, "main", "0")]
	require.Equal(t, "node-b", pod.Spec.NodeSelector["kubernetes.io/hostname"])
}

func TestShardPodFallsBackOnceThePreferredReplicaStaysNotReady(t *testing.T) {
	chi := podModeInstallation(t, 1)
	notReady := hostPod(t, chi, 0, 0, "node-a", false)
	r, c := newShardPodController(t, chi, notReady, hostPod(t, chi, 0, 1, "node-b", true))

	_, err := r.Reconcile(context.Background(), request(testCHI))
	require.NoError(t, err)
	pod := shardPods(t, c)[model.ShardPodName(testCHI, "main", "0")]
	require.Equal(t, "node-b", pod.Spec.NodeSelector["kubernetes.io/hostname"], "not ready for an hour")
}

func TestShardPodWaitsOutABriefNotReady(t *testing.T) {
	chi := podModeInstallation(t, 1)
	blip := hostPod(t, chi, 0, 0, "node-a", false)
	blip.Status.Conditions[0].LastTransitionTime = meta.NewTime(time.Now().Add(-10 * time.Second))
	r, c := newShardPodController(t, chi, blip, hostPod(t, chi, 0, 1, "node-b", true))

	result, err := r.Reconcile(context.Background(), request(testCHI))
	require.NoError(t, err)
	require.Greater(t, result.RequeueAfter, time.Duration(0))
	pod := shardPods(t, c)[model.ShardPodName(testCHI, "main", "0")]
	require.Equal(t, "node-a", pod.Spec.NodeSelector["kubernetes.io/hostname"], "the preferred replica keeps it")
}

func TestShardPodIsReplacedWhenItsReplicaMoves(t *testing.T) {
	chi := podModeInstallation(t, 1)
	replica := hostPod(t, chi, 0, 0, "node-a", true)
	r, c := newShardPodController(t, chi, replica, hostPod(t, chi, 0, 1, "node-b", true))
	ctx := context.Background()
	name := model.ShardPodName(testCHI, "main", "0")

	_, err := r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.Contains(t, shardPods(t, c), name)

	// The StatefulSet creates the replica pod anew on another node
	moved := &core.Pod{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(replica), moved))
	require.NoError(t, c.Delete(ctx, moved))
	moved = hostPod(t, chi, 0, 0, "node-c", true)
	moved.UID = "moved-uid"
	require.NoError(t, c.Create(ctx, moved))

	_, err = r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.NotContains(t, shardPods(t, c), name, "deleted at once, to free the volume on the former node")

	_, err = r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	pod := shardPods(t, c)[name]
	require.Equal(t, "node-c", pod.Spec.NodeSelector["kubernetes.io/hostname"])
	require.Equal(t, "moved-uid", pod.Annotations[model.AnnotationShardPodReplicaUID])
}

func TestShardPodsGoWithBackup(t *testing.T) {
	chi := podModeInstallation(t, 1)
	r, c := newShardPodController(t, chi, hostPod(t, chi, 0, 0, "node-a", true), hostPod(t, chi, 0, 1, "node-b", true))
	ctx := context.Background()
	_, err := r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.Len(t, shardPods(t, c), 1)

	off := installation(t, api.StatusCompleted, backupCluster("main", 1, false))
	r.cr = newFakeCR(off)
	_, err = r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.Empty(t, shardPods(t, c))
}

func TestShardPodRefusedForDataInAnEmptyDir(t *testing.T) {
	chi := podModeInstallation(t, 1)
	replica := hostPod(t, chi, 0, 0, "node-a", true)
	replica.Spec.Volumes[0].VolumeSource = core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}
	other := hostPod(t, chi, 0, 1, "node-b", true)
	other.Spec.Volumes[0].VolumeSource = core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}
	r, c := newShardPodController(t, chi, replica, other)

	_, err := r.Reconcile(context.Background(), request(testCHI))
	require.NoError(t, err)
	require.Empty(t, shardPods(t, c))
}

func TestInstallationsForPod(t *testing.T) {
	chi := podModeInstallation(t, 2)
	r, _ := newShardPodController(t, chi)
	ctx := context.Background()

	requests := r.InstallationsForPod(ctx, hostPod(t, chi, 0, 0, "node-a", true))
	require.Len(t, requests, 1)
	require.Equal(t, testCHI, requests[0].Name)

	shardPod := &core.Pod{ObjectMeta: meta.ObjectMeta{
		Namespace:       testNamespace,
		Labels:          model.ShardPodLabels(testCHI, "main", "0"),
		OwnerReferences: []meta.OwnerReference{model.ShardPodOwner(chi)},
	}}
	requests = r.InstallationsForPod(ctx, shardPod)
	require.Len(t, requests, 1)
	require.Equal(t, testCHI, requests[0].Name)

	require.Empty(t, r.InstallationsForPod(ctx, &core.Pod{ObjectMeta: meta.ObjectMeta{Namespace: testNamespace, Name: "other"}}))
}

// The backup Pods exist only while a backup, verify or restore Job is active on their cluster: none before, and
// deleted once it completes or fails, so that a second pod holds the volumes of a replica no longer than that
func TestShardPodsLiveAsLongAsAJob(t *testing.T) {
	chi := podModeInstallation(t, 1)
	ctx := context.Background()
	c := newClient(t, nil, chi, hostPod(t, chi, 0, 0, "node-a", true), hostPod(t, chi, 0, 1, "node-b", true))
	r := NewShardPodController(c, c, newScheme(t), newFakeCR(chi))

	_, err := r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.Empty(t, shardPods(t, c), "no Job, no backup Pod")
	require.Contains(t, shardServices(t, c), model.ShardPodServiceName(testCHI, "main"), "the Service is in place before")

	require.NoError(t, c.Create(ctx, activeJob("other-cluster", "other")))
	_, err = r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.Empty(t, shardPods(t, c), "a Job on another cluster")

	for _, end := range []batch.JobConditionType{batch.JobComplete, batch.JobFailed} {
		name := "job-" + string(end)
		require.NoError(t, c.Create(ctx, activeJob(name, "main")))
		_, err = r.Reconcile(ctx, request(testCHI))
		require.NoError(t, err)
		require.Len(t, shardPods(t, c), 1, "a Job is active on the cluster")

		endJob(t, c, name, end)
		_, err = r.Reconcile(ctx, request(testCHI))
		require.NoError(t, err)
		require.Empty(t, shardPods(t, c), "the Job %s", end)
	}
}

// A cluster backed up by sidecars gets no backup Pod and no Service, whatever Job runs on it
func TestShardPodsNotInSidecarMode(t *testing.T) {
	pods := podModeInstallation(t, 1)
	chi := installation(t, api.StatusCompleted, backupCluster("main", 1, true))
	r, c := newShardPodController(t, chi, hostPod(t, pods, 0, 0, "node-a", true), hostPod(t, pods, 0, 1, "node-b", true))
	_, err := r.Reconcile(context.Background(), request(testCHI))
	require.NoError(t, err)
	require.Empty(t, shardPods(t, c))
	require.Empty(t, shardServices(t, c))
}

// Every backup Pod is reached by its DNS name under the headless Service of its cluster, on the port of the REST API.
// The Service publishes not-ready Pods too: ClickHouse lets a backup Pod in by its reverse DNS name before it is ready
func TestShardPodDNSName(t *testing.T) {
	chi := podModeInstallation(t, 2)
	r, c := newShardPodController(t, chi,
		hostPod(t, chi, 0, 0, "node-a", true), hostPod(t, chi, 0, 1, "node-b", true),
		hostPod(t, chi, 1, 0, "node-c", true), hostPod(t, chi, 1, 1, "node-d", true))
	ctx := context.Background()
	_, err := r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)

	serviceName := model.ShardPodServiceName(testCHI, "main")
	require.Equal(t, testCHI+"-main-backup", serviceName)
	service := shardServices(t, c)[serviceName]
	require.NotNil(t, service)
	require.Equal(t, core.ClusterIPNone, service.Spec.ClusterIP)
	require.True(t, service.Spec.PublishNotReadyAddresses)
	require.Equal(t, model.ShardPodSelector(testCHI, "main"), service.Spec.Selector)
	require.Len(t, service.Spec.Ports, 1)
	require.Equal(t, int32(config.ClickHouseBackupAPIPortNumber), service.Spec.Ports[0].Port)
	require.Equal(t, testCHI, service.OwnerReferences[0].Name)

	pods := shardPods(t, c)
	require.Len(t, pods, 2)
	var hosts []string
	for _, pod := range pods {
		require.Equal(t, pod.Name, pod.Spec.Hostname)
		require.Equal(t, serviceName, pod.Spec.Subdomain)
		for key, value := range service.Spec.Selector {
			require.Equal(t, value, pod.Labels[key], "the Service selects the Pod")
		}
		hosts = append(hosts, pod.Name+"."+serviceName+"."+testNamespace+".svc")
	}
	require.ElementsMatch(t, hosts, model.ShardPodHosts(chi.GetAncestorT(), "main"))

	// The Service goes with the backup of the cluster
	r.cr = newFakeCR(installation(t, api.StatusCompleted, backupCluster("main", 2, false)))
	_, err = r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.Empty(t, shardServices(t, c))
	require.Empty(t, shardPods(t, c))
}

// A backup Pod whose settings changed while its replica stays where it is serves the running operation to its end:
// it is replaced only once it is not ready, or its replica moved
func TestShardPodOutdatedKeptWhileReady(t *testing.T) {
	chi := podModeInstallation(t, 1)
	r, c := newShardPodController(t, chi, hostPod(t, chi, 0, 0, "node-a", true), hostPod(t, chi, 0, 1, "node-b", true))
	ctx := context.Background()
	name := model.ShardPodName(testCHI, "main", "0")
	_, err := r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)

	pod := shardPods(t, c)[name]
	pod.Annotations[model.AnnotationShardPodSpecHash] = "outdated"
	require.NoError(t, c.Update(ctx, pod))
	pod.Status = core.PodStatus{Phase: core.PodRunning, Conditions: []core.PodCondition{{Type: core.PodReady, Status: core.ConditionTrue}}}
	require.NoError(t, c.Status().Update(ctx, pod))
	_, err = r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.Equal(t, "outdated", shardPods(t, c)[name].Annotations[model.AnnotationShardPodSpecHash], "ready: kept")

	pod = shardPods(t, c)[name]
	pod.Status = core.PodStatus{}
	require.NoError(t, c.Status().Update(ctx, pod))
	_, err = r.Reconcile(ctx, request(testCHI))
	require.NoError(t, err)
	require.NotContains(t, shardPods(t, c), name, "not ready: replaced")
}

func TestInstallationsForJobAndService(t *testing.T) {
	chi := podModeInstallation(t, 1)
	r, _ := newShardPodController(t, chi)
	ctx := context.Background()

	requests := r.InstallationsForJob(ctx, activeJob("j", "main"))
	require.Len(t, requests, 1)
	require.Equal(t, testCHI, requests[0].Name)
	require.Empty(t, r.InstallationsForJob(ctx, &batch.Job{ObjectMeta: meta.ObjectMeta{Namespace: testNamespace, Name: "other"}}))

	service := model.NewShardPodService(testNamespace, testCHI, "main", model.ShardPodOwner(chi))
	requests = r.InstallationsForService(ctx, service)
	require.Len(t, requests, 1)
	require.Equal(t, testCHI, requests[0].Name)
	require.Empty(t, r.InstallationsForService(ctx, &core.Service{ObjectMeta: meta.ObjectMeta{Namespace: testNamespace, Name: "x"}}))
}
