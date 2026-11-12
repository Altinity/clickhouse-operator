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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sLabels "k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/backupconfig"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	chiCreator "github.com/altinity/clickhouse-operator/pkg/model/chi/creator"
)

// podModeBackup is enabledBackup in backup.mode pod.
func podModeBackup() *api.ClusterBackup {
	backup := enabledBackup()
	backup.Mode = types.NewString(api.BackupModePod)
	return backup
}

// shardPodHost returns the first host of a one-shard, two-replica cluster backed up by backup Pods.
func shardPodHost(t *testing.T) *api.Host {
	t.Helper()
	inMemory, _ := normalize(t, newCHI("demo", layoutCluster("main", 1, 2, podModeBackup())))
	return inMemory.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0]
}

// replicaPod is a running pod of a host as the StatefulSet makes it: data on a claim, configuration from ConfigMaps,
// logs in an emptyDir.
func replicaPod() *core.Pod {
	return &core.Pod{
		ObjectMeta: meta.ObjectMeta{Name: "chi-demo-main-0-0-0", Namespace: testNamespace, UID: "replica-uid"},
		Spec: core.PodSpec{
			NodeName:           "node-a",
			ServiceAccountName: "ch",
			SecurityContext:    &core.PodSecurityContext{RunAsUser: ptr.To(int64(101)), FSGroup: ptr.To(int64(101))},
			Tolerations:        []core.Toleration{{Key: "dedicated", Value: "clickhouse", Effect: core.TaintEffectNoSchedule}},
			ImagePullSecrets:   []core.LocalObjectReference{{Name: "registry"}},
			// As the StatefulSet makes it: the host name of the replica on loopback, then those of the pod template.
			HostAliases: []core.HostAlias{
				{IP: "127.0.0.1", Hostnames: []string{"chi-demo-main-0-0"}},
				{IP: "10.0.0.5", Hostnames: []string{"s3.internal"}},
			},
			Containers: []core.Container{
				{
					Name: config.ClickHouseContainerName,
					VolumeMounts: []core.VolumeMount{
						{Name: "data", MountPath: config.DirPathDataStorage},
						{Name: "common-configd", MountPath: "/etc/clickhouse-server/config.d"},
						{Name: "logs", MountPath: "/var/log/clickhouse-server"},
						{Name: "disk2", MountPath: "/mnt/disk2", SubPath: "ch"},
					},
				},
				{Name: config.ClickHouseBackupContainerName},
			},
			Volumes: []core.Volume{
				{Name: "data", VolumeSource: core.VolumeSource{PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{ClaimName: "data-chi-demo-main-0-0-0"}}},
				{Name: "disk2", VolumeSource: core.VolumeSource{PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{ClaimName: "disk2-chi-demo-main-0-0-0"}}},
				{Name: "common-configd", VolumeSource: core.VolumeSource{ConfigMap: &core.ConfigMapVolumeSource{LocalObjectReference: core.LocalObjectReference{Name: "chi-demo-common-configd"}}}},
				{Name: "logs", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}},
			},
		},
	}
}

func newTestShardPod(t *testing.T, replica *core.Pod, claims map[string]*core.PersistentVolumeClaim) (*core.Pod, error) {
	t.Helper()
	return NewShardPod(ShardPodSource{
		Host:       shardPodHost(t),
		ReplicaPod: replica,
		Claims:     claims,
		Owner:      meta.OwnerReference{APIVersion: "clickhouse.altinity.com/v1", Kind: "ClickHouseInstallation", Name: "demo", UID: "chi-uid"},
	})
}

func mountAt(c core.Container, mountPath string) (core.VolumeMount, bool) {
	for _, m := range c.VolumeMounts {
		if m.MountPath == mountPath {
			return m, true
		}
	}
	return core.VolumeMount{}, false
}

func TestShardPodLandsOnTheNodeOfItsReplica(t *testing.T) {
	pod, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)

	require.Equal(t, map[string]string{"kubernetes.io/hostname": "node-a"}, pod.Spec.NodeSelector)
	require.Nil(t, pod.Spec.Affinity, "the node is chosen by the nodeSelector alone")
	require.Equal(t, "chi-demo-main-0-bk", pod.Name)
	require.Equal(t, testNamespace, pod.Namespace)
	require.Equal(t, "chi-demo-main-0-0-0", pod.Annotations[AnnotationShardPodReplica])
	require.Equal(t, "replica-uid", pod.Annotations[AnnotationShardPodReplicaUID])
	require.Equal(t, "chi-uid", string(pod.OwnerReferences[0].UID))
}

func TestShardPodTakesTheIdentityOfItsReplica(t *testing.T) {
	pod, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)

	require.Equal(t, int64(101), *pod.Spec.SecurityContext.RunAsUser)
	require.Equal(t, int64(101), *pod.Spec.SecurityContext.FSGroup)
	require.Equal(t, "ch", pod.Spec.ServiceAccountName)
	require.Len(t, pod.Spec.Tolerations, 1)
	require.Equal(t, "registry", pod.Spec.ImagePullSecrets[0].Name)
	require.False(t, *pod.Spec.EnableServiceLinks)
	require.False(t, *pod.Spec.AutomountServiceAccountToken)
}

// The replica pod reaches its own host name on loopback; the backup Pod reaches ClickHouse by that name, the host
// Service of the replica, so it must not take that alias, only the others.
func TestShardPodDropsTheLoopbackAliasOfItsReplica(t *testing.T) {
	pod, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)

	require.Equal(t, []core.HostAlias{{IP: "10.0.0.5", Hostnames: []string{"s3.internal"}}}, pod.Spec.HostAliases)
	host := ""
	for _, env := range pod.Spec.Containers[0].Env {
		if env.Name == config.ClickHouseBackupEnvClickHouseHost {
			host = env.Value
		}
	}
	require.Equal(t, "chi-demo-main-0-0", host, "the backup Pod reaches ClickHouse by the name the dropped alias mapped")
}

func TestShardPodMountsWhatClickHouseMounts(t *testing.T) {
	pod, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)
	require.Len(t, pod.Spec.Containers, 1)
	c := pod.Spec.Containers[0]

	data, ok := mountAt(c, config.DirPathDataStorage)
	require.True(t, ok)
	require.Equal(t, "data", data.Name)
	require.False(t, data.ReadOnly)

	disk2, ok := mountAt(c, "/mnt/disk2")
	require.True(t, ok)
	require.Equal(t, "ch", disk2.SubPath, "a mount is taken with its subPath")

	configd, ok := mountAt(c, "/etc/clickhouse-server/config.d")
	require.True(t, ok)
	require.True(t, configd.ReadOnly, "configuration is mounted read-only")

	_, ok = mountAt(c, "/var/log/clickhouse-server")
	require.False(t, ok, "an emptyDir of the replica pod is not shared")

	configMount, ok := mountAt(c, config.DirPathClickHouseBackupConfig)
	require.True(t, ok)
	require.True(t, configMount.ReadOnly)

	var names []string
	var configVolume *core.Volume
	for i, v := range pod.Spec.Volumes {
		names = append(names, v.Name)
		if v.Name == config.ClickHouseBackupConfigVolumeName {
			configVolume = &pod.Spec.Volumes[i]
		}
	}
	require.ElementsMatch(t, []string{"data", "disk2", "common-configd", config.ClickHouseBackupConfigVolumeName}, names)
	require.Equal(t, "demo-main-backup-config", configVolume.Secret.SecretName, "the configuration file Secret of the sidecar")
	require.Equal(t, []core.KeyToPath{
		{Key: backupconfig.FileName, Path: backupconfig.FileName},
		{Key: backupconfig.APIFingerprintFileName, Path: backupconfig.APIFingerprintFileName},
	}, configVolume.Secret.Items)
}

// The backup Pod runs the server of clickhouse-backup as the sidecar does - its REST API on the port of the sidecar,
// behind the credentials of the configuration file - and is ready once the API accepts connections. It reaches
// ClickHouse through the host Service of its replica, and pulls the stable image Always.
func TestShardPodServesTheRESTAPI(t *testing.T) {
	pod, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)
	c := pod.Spec.Containers[0]

	require.True(t, chiCreator.IsBackupSidecarCommand(&c), "the operator's command of the sidecar, which runs the server")
	require.Equal(t, []core.ContainerPort{{Name: config.ClickHouseBackupAPIPortName, ContainerPort: config.ClickHouseBackupAPIPortNumber}}, c.Ports)
	require.NotNil(t, c.ReadinessProbe)
	require.Equal(t, int32(config.ClickHouseBackupAPIPortNumber), c.ReadinessProbe.TCPSocket.Port.IntVal)
	vars := env(c)
	require.Equal(t, "0.0.0.0:7171", vars[config.ClickHouseBackupEnvAPIListen])
	require.Equal(t, "chi-demo-main-0-0", vars[config.ClickHouseBackupEnvClickHouseHost], "ClickHouse is reached through the host Service")
	require.Equal(t, "", vars[config.ClickHouseBackupEnvRestoreSchemaOnCluster])
	require.NotContains(t, vars, config.ClickHouseBackupSecretKeyAPIUsername, "the credentials are in the configuration file")
	require.Equal(t, "0", vars[config.ClickHouseBackupEnvBackupsToKeepRemote], "retention is the orchestrator's alone")
	require.Equal(t, "true", vars[config.ClickHouseBackupEnvRebaseDuringDelete])
	require.Equal(t, "true", vars[config.ClickHouseBackupEnvRebaseBeforeRemoveOldRemote])
	require.Equal(t, "altinity/clickhouse-backup:stable", c.Image)
	require.Equal(t, core.PullAlways, c.ImagePullPolicy)
	require.Equal(t, "false", vars[config.ClickHouseBackupEnvAPICreateIntegrationTables],
		"no integration tables: they would point at an API the pod of ClickHouse does not run")
}

// The integration tables stay off in the backup Pod also when the clickhouse-backup container of the backup pod template
// turns them on: the env var overrides the configuration file, and the operator's value overrides the template's.
func TestShardPodCreatesNoIntegrationTables(t *testing.T) {
	backup := podModeBackup()
	backup.PodTemplate = types.NewString("backup")
	chi := newCHI("demo", layoutCluster("main", 1, 2, backup))
	chi.Spec.Templates = &api.Templates{PodTemplates: []api.PodTemplate{{Name: "backup", Spec: core.PodSpec{Containers: []core.Container{{
		Name: config.ClickHouseBackupContainerName,
		Env:  []core.EnvVar{{Name: config.ClickHouseBackupEnvAPICreateIntegrationTables, Value: "true"}},
	}}}}}}
	inMemory, _ := normalize(t, chi)
	pod, err := NewShardPod(ShardPodSource{Host: inMemory.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0], ReplicaPod: replicaPod()})
	require.NoError(t, err)
	var values []string
	for _, e := range pod.Spec.Containers[0].Env {
		if e.Name == config.ClickHouseBackupEnvAPICreateIntegrationTables {
			values = append(values, e.Value)
		}
	}
	require.Equal(t, []string{"false"}, values)
}

// The image and pull policy of the clickhouse-backup container of the backup pod template win
func TestShardPodTakesTheImageOfTheBackupPodTemplate(t *testing.T) {
	backup := podModeBackup()
	backup.PodTemplate = types.NewString("backup")
	chi := newCHI("demo", layoutCluster("main", 1, 2, backup))
	chi.Spec.Templates = &api.Templates{PodTemplates: []api.PodTemplate{{Name: "backup", Spec: core.PodSpec{Containers: []core.Container{{
		Name: config.ClickHouseBackupContainerName, Image: "registry/backup:1", ImagePullPolicy: core.PullIfNotPresent,
	}}}}}}
	inMemory, _ := normalize(t, chi)
	pod, err := NewShardPod(ShardPodSource{Host: inMemory.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0], ReplicaPod: replicaPod()})
	require.NoError(t, err)
	require.Equal(t, "registry/backup:1", pod.Spec.Containers[0].Image)
	require.Equal(t, core.PullIfNotPresent, pod.Spec.Containers[0].ImagePullPolicy)
}

// The Pod is reached as <pod>.<service>.<namespace>.svc: its host name is its name, its subdomain the headless
// Service of the backup Pods of its cluster, which selects it
func TestShardPodDNSName(t *testing.T) {
	pod, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)
	require.Equal(t, pod.Name, pod.Spec.Hostname)
	require.Equal(t, "demo-main-backup", pod.Spec.Subdomain)
	require.Equal(t, "chi-demo-main-0-bk.demo-main-backup."+testNamespace+".svc", ShardPodHost(testNamespace, "demo", "main", "0"))

	service := NewShardPodService(testNamespace, "demo", "main", meta.OwnerReference{Name: "demo"})
	require.Equal(t, pod.Spec.Subdomain, service.Name)
	require.Equal(t, core.ClusterIPNone, service.Spec.ClusterIP)
	// ClickHouse lets the backup user of the Pod in by its reverse DNS name, before the Pod can be ready
	require.True(t, service.Spec.PublishNotReadyAddresses, "the name of a backup Pod resolves before it is ready")
	for key, value := range service.Spec.Selector {
		require.Equal(t, value, pod.Labels[key])
	}
	require.Equal(t, config.ClickHouseBackupAPIPortNumber, int(service.Spec.Ports[0].Port))
	require.True(t, ShardServiceSelector().Matches(k8sLabels.Set(service.Labels)))

	inMemory, _ := normalize(t, newCHI("demo", layoutCluster("main", 3, 2, podModeBackup())))
	require.Equal(t, []string{
		ShardPodHost(testNamespace, "demo", "main", "0"),
		ShardPodHost(testNamespace, "demo", "main", "1"),
		ShardPodHost(testNamespace, "demo", "main", "2"),
	}, ShardPodHosts(inMemory, "main"), "one per shard, in shard order")
	require.Empty(t, ShardPodHosts(inMemory, "other"))
}

func TestShardPodServiceNameFitsADNSLabel(t *testing.T) {
	name := ShardPodServiceName(strings.Repeat("c", 60), strings.Repeat("l", 30))
	require.LessOrEqual(t, len(name), shardPodNameMaxLength)
	require.True(t, strings.HasSuffix(name, "-backup"))
}

// Only the clusters backed up by backup Pods have shards for them
func TestBackupShardsOfPodModeClustersOnly(t *testing.T) {
	inMemory, _ := normalize(t, newCHI("demo", layoutCluster("pods", 2, 2, podModeBackup()), layoutCluster("sidecars", 2, 2, enabledBackup())))
	shards, err := BackupShards(inMemory)
	require.NoError(t, err)
	require.Len(t, shards, 2)
	for _, shard := range shards {
		require.Equal(t, "pods", shard.Cluster)
	}
}

func TestShardPodRefusesDataThatLivesInTheReplicaPod(t *testing.T) {
	replica := replicaPod()
	replica.Spec.Volumes[0] = core.Volume{Name: "data", VolumeSource: core.VolumeSource{EmptyDir: &core.EmptyDirVolumeSource{}}}
	_, err := newTestShardPod(t, replica, nil)
	require.ErrorIs(t, err, ErrShardPodNoPersistentData)
	require.True(t, strings.Contains(err.Error(), "volumeClaimTemplate"), err.Error())

	replica = replicaPod()
	replica.Spec.Containers[0].VolumeMounts = replica.Spec.Containers[0].VolumeMounts[1:]
	_, err = newTestShardPod(t, replica, nil)
	require.ErrorIs(t, err, ErrShardPodNoPersistentData, "data in the filesystem of the container")
}

func TestShardPodRefusesAClaimOfOnePod(t *testing.T) {
	claims := map[string]*core.PersistentVolumeClaim{
		"data-chi-demo-main-0-0-0": {
			ObjectMeta: meta.ObjectMeta{Name: "data-chi-demo-main-0-0-0"},
			Spec:       core.PersistentVolumeClaimSpec{AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOncePod}},
		},
	}
	_, err := newTestShardPod(t, replicaPod(), claims)
	require.ErrorIs(t, err, ErrShardPodVolumeUnshareable)

	claims["data-chi-demo-main-0-0-0"].Spec.AccessModes = []core.PersistentVolumeAccessMode{core.ReadWriteOnce}
	_, err = newTestShardPod(t, replicaPod(), claims)
	require.NoError(t, err)
}

func TestShardPodWaitsForAScheduledReplica(t *testing.T) {
	replica := replicaPod()
	replica.Spec.NodeName = ""
	_, err := newTestShardPod(t, replica, nil)
	require.ErrorIs(t, err, ErrShardPodNotScheduled)
}

func TestShardPodIsBuiltAnewForAnotherReplicaPod(t *testing.T) {
	first, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)
	same, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)
	require.False(t, IsShardPodOutdated(first, same))

	moved := replicaPod()
	moved.Spec.NodeName = "node-b"
	other, err := newTestShardPod(t, moved, nil)
	require.NoError(t, err)
	require.True(t, IsShardPodOutdated(first, other), "the replica moved to another node")

	recreated := replicaPod()
	recreated.UID = "replica-uid-2"
	other, err = newTestShardPod(t, recreated, nil)
	require.NoError(t, err)
	require.True(t, IsShardPodOutdated(first, other), "the replica pod was created anew")
}

func TestShardPodNameFitsAHostName(t *testing.T) {
	name := ShardPodName(strings.Repeat("c", 60), strings.Repeat("l", 30), "0")
	require.LessOrEqual(t, len(name), shardPodNameMaxLength)
	require.True(t, strings.HasSuffix(name, "-bk"))
	require.Equal(t, name, ShardPodName(strings.Repeat("c", 60), strings.Repeat("l", 30), "0"))
}

func TestShardPodCarriesNoLabelOfTheInstallationPods(t *testing.T) {
	pod, err := newTestShardPod(t, replicaPod(), nil)
	require.NoError(t, err)
	for key, value := range pod.Labels {
		if key == LabelApp {
			require.Equal(t, ShardPodAppValue, value, "never the app label of the installation's pods")
			continue
		}
		require.True(t, strings.HasPrefix(key, "clickhouse.altinity.com/backup-"), key)
	}
	_, ok := pod.Labels["clickhouse.altinity.com/chi"]
	require.False(t, ok, "the Services of the installation select their pods by it")
}
