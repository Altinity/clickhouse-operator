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
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	core "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
	"github.com/altinity/clickhouse-operator/pkg/chop"
	"github.com/altinity/clickhouse-operator/pkg/interfaces"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/config"
	"github.com/altinity/clickhouse-operator/pkg/model/chi/normalizer"
	commonNormalizer "github.com/altinity/clickhouse-operator/pkg/model/common/normalizer"
	"github.com/altinity/clickhouse-operator/pkg/model/managers"
)

const testNamespace = "ns"

const (
	// testStorageSecret is the storage Secret enabledBackup names, without which backup stays off
	testStorageSecret = "s3-credentials"
	// testStorage is the backup storage of the test installation, whose Secret is testStorageSecret
	testStorage = "main"
)

// The normalizer and the namer resolve configuration through the global chop singleton.
func TestMain(m *testing.M) {
	os.Unsetenv("WATCH_NAMESPACE")
	os.Unsetenv("WATCH_NAMESPACES")
	chop.New(nil, nil, "")
	os.Exit(m.Run())
}

func enabledBackup() *api.ClusterBackup {
	return &api.ClusterBackup{
		Enabled:  types.NewStringBool(true),
		Storages: []api.BackupStorage{{Name: testStorage, SecretRef: &core.LocalObjectReference{Name: testStorageSecret}}},
	}
}

// withSidecarImage gives the cluster a pod template of its own whose clickhouse-backup container runs the image.
func withSidecarImage(chi *api.ClickHouseInstallation, cluster *api.Cluster, image string) {
	if chi.Spec.Templates == nil {
		chi.Spec.Templates = &api.Templates{}
	}
	sidecarTemplate(chi, cluster.Name+"-sidecar", core.Container{Image: image})
	useTemplate(chi, cluster, cluster.Name+"-sidecar")
}

func newCHI(name string, clusters ...*api.Cluster) *api.ClickHouseInstallation {
	chi := &api.ClickHouseInstallation{}
	chi.Name = name
	chi.Namespace = testNamespace
	chi.Spec.Configuration = &api.Configuration{Clusters: clusters}
	return chi
}

func layoutCluster(name string, shards, replicas int, backup *api.ClusterBackup) *api.Cluster {
	return &api.Cluster{
		Name:   name,
		Layout: &api.ChiClusterLayout{ShardsCount: shards, ReplicasCount: replicas},
		Backup: backup,
	}
}

// normalize returns chi normalized in memory, as the creator names host Services from it, and as
// read back from storage, which is what topology resolves from.
func normalize(t *testing.T, chi *api.ClickHouseInstallation) (inMemory, stored *api.ClickHouseInstallation) {
	t.Helper()
	inMemory, err := normalizer.New(nil).CreateTemplated(chi, commonNormalizer.NewOptions[api.ClickHouseInstallation]())
	require.NoError(t, err)

	data, err := json.Marshal(inMemory)
	require.NoError(t, err)
	stored = &api.ClickHouseInstallation{}
	require.NoError(t, json.Unmarshal(data, stored))
	return inMemory, stored
}

// createdServices returns the host Service names the creator makes for a cluster, by shard.
func createdServices(chi *api.ClickHouseInstallation, cluster string) [][]string {
	namer := managers.NewNameManager(managers.NameManagerTypeClickHouse)
	var shards [][]string
	for _, c := range chi.Spec.Configuration.Clusters {
		if c.Name != cluster {
			continue
		}
		for _, shard := range c.Layout.Shards {
			var hosts []string
			for _, host := range shard.Hosts {
				hosts = append(hosts, namer.Name(interfaces.NameStatefulSetService, host))
			}
			shards = append(shards, hosts)
		}
	}
	return shards
}

func topologyShards(topology *Topology, cluster string) [][]string {
	var shards [][]string
	for _, c := range topology.clusters {
		if c.name != cluster {
			continue
		}
		for _, shard := range c.shards {
			shards = append(shards, shard.hosts)
		}
	}
	return shards
}

func TestTopologyFromLayoutCounts(t *testing.T) {
	_, stored := normalize(t, newCHI("demo", layoutCluster("main", 2, 2, enabledBackup())))

	topology, err := NewTopology(stored, "")
	require.NoError(t, err)

	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-1-0"}, topology.firstPerShard())
	require.Equal(t, []string{"chi-demo-main-0-0", "chi-demo-main-0-1", "chi-demo-main-1-0", "chi-demo-main-1-1"},
		topology.allReplicas())
	require.Equal(t, topology.firstPerShard(), topology.hosts(api.ReplicaSelectionFirstPerShard))
	require.Equal(t, topology.allReplicas(), topology.hosts(api.ReplicaSelectionAllReplicas))
}

// Explicit shard and replica lists name their hosts after the listed entries and may give each
// shard its own replica count; a count-based guess gets both wrong.
func TestTopologyFromExplicitShardsAndReplicas(t *testing.T) {
	cluster := &api.Cluster{
		Name: "main",
		Layout: &api.ChiClusterLayout{
			Shards: []*api.ChiShard{
				{Name: "east", ReplicasCount: 2},
				{Name: "west", ReplicasCount: 1},
			},
		},
		Backup: enabledBackup(),
	}
	inMemory, stored := normalize(t, newCHI("demo", cluster))

	topology, err := NewTopology(stored, "")
	require.NoError(t, err)

	expected := createdServices(inMemory, "main")
	require.Len(t, expected, 2)
	require.Len(t, expected[0], 2)
	require.Len(t, expected[1], 1)
	require.Equal(t, expected, topologyShards(topology, "main"))
	require.Contains(t, expected[0][0], "east", "explicit shard names must reach the host names")
	require.Equal(t, []string{expected[0][0], expected[1][0]}, topology.firstPerShard())
}

// A cluster may come from a template alone; the installation's own spec then has no cluster to
// count hosts from.
func TestTopologyFromTemplateProvidedCluster(t *testing.T) {
	chit := &api.ClickHouseInstallation{}
	chit.Name = "chit-backup-cluster"
	chit.Namespace = testNamespace
	chit.Spec.Configuration = &api.Configuration{
		Clusters: []*api.Cluster{layoutCluster("fromtpl", 1, 2, enabledBackup())},
	}
	chop.Config().AddCHITemplate(chit)
	t.Cleanup(func() { chop.Config().DeleteCHITemplate(chit) })

	chi := newCHI("demo")
	chi.Spec.Configuration = nil
	chi.Spec.UseTemplates = []*api.TemplateRef{{Name: chit.Name}}
	inMemory, stored := normalize(t, chi)

	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	require.Equal(t, createdServices(inMemory, "fromtpl"), topologyShards(topology, "fromtpl"))
	require.Equal(t, []string{"chi-demo-fromtpl-0-0", "chi-demo-fromtpl-0-1"}, topology.allReplicas())
}

// Host Service names follow a service template's generateName, and long installation names are
// shortened by the namer. Neither survives storage on its own: the template index and the host
// addresses are not serialized, so topology has to rebuild them.
func TestTopologyFollowsGeneratedAndShortenedNames(t *testing.T) {
	longName := "a-very-long-installation-name-" + strings.Repeat("x", 40)

	generated := newCHI("demo", layoutCluster("main", 2, 1, enabledBackup()))
	generated.Spec.Templates = &api.Templates{
		ServiceTemplates: []api.ServiceTemplate{{Name: "replica-svc", GenerateName: "svc-{chi}-{shard}-{replica}"}},
	}
	generated.Spec.Defaults = &api.Defaults{Templates: &api.TemplatesList{ReplicaServiceTemplate: "replica-svc"}}

	for name, chi := range map[string]*api.ClickHouseInstallation{
		"generateName": generated,
		"long name":    newCHI(longName, layoutCluster("main", 2, 1, enabledBackup())),
	} {
		t.Run(name, func(t *testing.T) {
			inMemory, stored := normalize(t, chi)

			topology, err := NewTopology(stored, "")
			require.NoError(t, err)
			require.Equal(t, createdServices(inMemory, "main"), topologyShards(topology, "main"))
		})
	}

	_, stored := normalize(t, generated)
	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	require.Equal(t, []string{"svc-demo-0-0", "svc-demo-1-0"}, topology.firstPerShard())

	_, stored = normalize(t, newCHI(longName, layoutCluster("main", 1, 1, enabledBackup())))
	topology, err = NewTopology(stored, "")
	require.NoError(t, err)
	require.NotContains(t, topology.firstPerShard()[0], longName, "the namer shortens long installation names")
}

func TestTopologyTargetsBackupEnabledClustersOnly(t *testing.T) {
	_, stored := normalize(t, newCHI("demo",
		layoutCluster("plain", 1, 1, nil),
		layoutCluster("backed", 2, 1, enabledBackup()),
		layoutCluster("off", 1, 1, &api.ClusterBackup{Enabled: types.NewStringBool(false)}),
	))

	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	require.Len(t, topology.clusters, 1)
	require.Equal(t, "backed", topology.clusters[0].name)
	require.Equal(t, []string{"chi-demo-backed-0-0", "chi-demo-backed-1-0"}, topology.allReplicas())

	topology, err = NewTopology(stored, "backed")
	require.NoError(t, err)
	require.Len(t, topology.clusters, 1)

	_, err = NewTopology(stored, "plain")
	require.ErrorIs(t, err, ErrBackupDisabled, "a named cluster without backup must not fall back to another one")

	_, err = NewTopology(stored, "missing")
	require.ErrorIs(t, err, ErrClusterNotFound)
}

// An operation resolves its clusters once and runs one Job per cluster, each from the topology of
// its own cluster; a cluster that left the installation, or switched backup off, meanwhile says so.
func TestTopologyForCluster(t *testing.T) {
	hotCluster := layoutCluster("hot", 2, 1, enabledBackup())
	coldCluster := layoutCluster("cold", 1, 2, enabledBackup())
	chi := newCHI("demo", hotCluster, layoutCluster("plain", 1, 1, nil), coldCluster)
	withSidecarImage(chi, hotCluster, "registry/backup:hot")
	withSidecarImage(chi, coldCluster, "registry/backup:cold")
	_, stored := normalize(t, chi)
	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	require.Equal(t, []string{"hot", "cold"}, topology.ClusterNames())

	cold, err := topology.ForCluster("cold")
	require.NoError(t, err)
	require.Equal(t, []string{"cold"}, cold.ClusterNames())
	require.Equal(t, "cold", cold.cluster())
	require.Equal(t, []string{"chi-demo-cold-0-0", "chi-demo-cold-0-1"}, cold.allReplicas())
	require.Equal(t, "registry/backup:cold", cold.pod().image, "the Job of a cluster runs that cluster's image")
	require.Equal(t, topology.enabled, cold.enabled, "every backup-enabled cluster stays known, for the path check")

	_, err = topology.ForCluster("plain")
	require.ErrorIs(t, err, ErrBackupDisabled)
	_, err = topology.ForCluster("gone")
	require.ErrorIs(t, err, ErrClusterNotFound)

	hot, err := NewTopology(stored, "hot")
	require.NoError(t, err)
	_, err = hot.ForCluster("cold")
	require.ErrorIs(t, err, ErrClusterNotFound, "a topology restricted to one cluster targets no other")
}

// The {shard} macro of every host is the name of its shard, which the path check expands storage
// paths with; a sidecar defaults an unset storage path only when it runs the operator's own command.
func TestTopologyKnowsWhatThePathCheckNeeds(t *testing.T) {
	cluster := &api.Cluster{
		Name: "main",
		Layout: &api.ChiClusterLayout{
			Shards: []*api.ChiShard{{Name: "east", ReplicasCount: 1}, {Name: "west", ReplicasCount: 1}},
		},
		Backup: enabledBackup(),
	}
	own := layoutCluster("own", 1, 1, enabledBackup())
	args := layoutCluster("args", 1, 1, enabledBackup())
	image := layoutCluster("image", 1, 1, enabledBackup())
	chi := newCHI("demo", cluster, own, args, image)
	sidecar := func(c core.Container) core.PodSpec {
		c.Name = config.ClickHouseBackupContainerName
		return core.PodSpec{Containers: []core.Container{{Name: config.ClickHouseContainerName, Image: "clickhouse/clickhouse-server"}, c}}
	}
	chi.Spec.Templates = &api.Templates{PodTemplates: []api.PodTemplate{
		{Name: "own", Spec: sidecar(core.Container{Image: "backup", Command: []string{"clickhouse-backup"}})},
		{Name: "args", Spec: sidecar(core.Container{Image: "backup", Args: []string{"server"}})},
		{Name: "image", Spec: sidecar(core.Container{Image: "backup"})},
	}}
	useTemplate(chi, own, "own")
	useTemplate(chi, args, "args")
	useTemplate(chi, image, "image")
	_, stored := normalize(t, chi)
	topology, err := NewTopology(stored, "")
	require.NoError(t, err)

	defaults := map[string]bool{}
	for _, c := range topology.enabled {
		defaults[c.name] = c.sidecar.defaultsPaths
	}
	require.Equal(t, map[string]bool{"main": true, "own": false, "args": false, "image": true}, defaults)
	require.Equal(t, "east", topology.enabled[0].shards[0].name)
	require.Equal(t, "west", topology.enabled[0].shards[1].name)
	require.Equal(t, "demo", topology.installation)
}

func TestTopologyRefusesWhatItCannotTarget(t *testing.T) {
	_, stored := normalize(t, newCHI("demo", layoutCluster("plain", 1, 1, nil)))
	_, err := NewTopology(stored, "")
	require.ErrorIs(t, err, ErrBackupDisabled)

	empty := newCHI("demo", &api.Cluster{Name: "main", Layout: &api.ChiClusterLayout{}, Backup: enabledBackup()})
	_, err = NewTopology(empty, "")
	require.ErrorIs(t, err, ErrNoTargetHosts, "an empty host list must never let an operation report success")

	_, err = NewTopology(nil, "")
	require.ErrorIs(t, err, ErrNoTargetHosts)
}

// A cluster that turns backup on without a storage has no sidecar to drive: it is refused as such, telling
// why its backup is off, and an operation of every cluster leaves it out, telling which.
func TestTopologyRefusesAClusterWithoutStorage(t *testing.T) {
	lacking := func(name string) *api.Cluster {
		return layoutCluster(name, 1, 1, &api.ClusterBackup{Enabled: types.NewStringBool(true)})
	}
	_, stored := normalize(t, newCHI("demo",
		layoutCluster("backed", 1, 1, enabledBackup()),
		lacking("lacking"),
		layoutCluster("off", 1, 1, &api.ClusterBackup{Enabled: types.NewStringBool(false)}),
	))

	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	require.Equal(t, []string{"backed"}, topology.ClusterNames())
	require.Equal(t, []string{"backed"}, topology.EnabledClusterNames())
	require.Equal(t, []string{"lacking"}, topology.ClustersWithoutStorage())
	require.Equal(t, []string{"backed", "lacking"}, BackupRequestedClusters(stored))

	_, err = NewTopology(stored, "lacking")
	require.ErrorIs(t, err, ErrNoBackupStorage)
	require.NotErrorIs(t, err, ErrBackupDisabled)
	require.EqualError(t, err, ErrNoBackupStorage.Error()+": lacking")
	_, err = topology.ForCluster("lacking")
	require.ErrorIs(t, err, ErrNoBackupStorage)
	_, err = NewTopology(stored, "off")
	require.ErrorIs(t, err, ErrBackupDisabled, "a cluster turned off is not told to lack a storage")

	_, stored = normalize(t, newCHI("demo", lacking("a"), lacking("b"), layoutCluster("plain", 1, 1, nil)))
	_, err = NewTopology(stored, "")
	require.EqualError(t, err, ErrNoBackupStorage.Error()+": a, b",
		"an operation of every cluster, none of them on, tells the ones that lack a storage")
}

func TestTopologyImage(t *testing.T) {
	first := layoutCluster("first", 1, 1, enabledBackup())
	second := layoutCluster("second", 1, 1, enabledBackup())
	chi := newCHI("demo", first, second, layoutCluster("default", 1, 1, enabledBackup()))
	withSidecarImage(chi, first, "registry/backup:first")
	withSidecarImage(chi, second, "registry/backup:second")
	_, stored := normalize(t, chi)

	topology, err := NewTopology(stored, "")
	require.NoError(t, err)
	require.Equal(t, "registry/backup:first", topology.pod().image, "the first target cluster in spec order names the image")

	topology, err = NewTopology(stored, "second")
	require.NoError(t, err)
	require.Equal(t, "registry/backup:second", topology.pod().image)

	topology, err = NewTopology(stored, "default")
	require.NoError(t, err)
	require.Equal(t, config.DefaultClickHouseBackupDockerImage, topology.pod().image)
}

// Jobs inherit the pod settings of the first target host's pod template. The template index is
// not serialized, so the stored installation has to have it rebuilt for the host to find it.
func TestTopologyPodSettings(t *testing.T) {
	chi := newCHI("demo",
		layoutCluster("plain", 1, 1, enabledBackup()),
		layoutCluster("custom", 1, 2, enabledBackup()),
	)
	chi.Spec.Templates = &api.Templates{PodTemplates: []api.PodTemplate{{
		Name: "pod",
		Spec: core.PodSpec{
			SecurityContext:    &core.PodSecurityContext{FSGroup: ptr.To(int64(101))},
			ImagePullSecrets:   []core.LocalObjectReference{{Name: "registry"}},
			ServiceAccountName: "clickhouse",
			Containers: []core.Container{
				{Name: "sidecar", Image: "sidecar", SecurityContext: &core.SecurityContext{RunAsUser: ptr.To(int64(1337))}},
				{Name: config.ClickHouseContainerName, Image: "clickhouse/clickhouse-server", SecurityContext: &core.SecurityContext{RunAsUser: ptr.To(int64(101))}},
			},
		},
	}}}
	chi.Spec.Configuration.Clusters[1].Templates = &api.TemplatesList{PodTemplate: "pod"}
	_, stored := normalize(t, chi)

	topology, err := NewTopology(stored, "custom")
	require.NoError(t, err)
	require.Equal(t, podSettings{
		image:                    config.DefaultClickHouseBackupDockerImage,
		securityContext:          &core.PodSecurityContext{FSGroup: ptr.To(int64(101))},
		containerSecurityContext: &core.SecurityContext{RunAsUser: ptr.To(int64(101))},
		imagePullSecrets:         []core.LocalObjectReference{{Name: "registry"}},
		serviceAccountName:       "clickhouse",
	}, topology.pod(), "the ClickHouse container is found by its name")

	topology, err = NewTopology(stored, "")
	require.NoError(t, err)
	require.Equal(t, podSettings{image: config.DefaultClickHouseBackupDockerImage}, topology.pod(), "the first target cluster uses no pod template")
	require.Equal(t, podSettings{}, (*Topology)(nil).pod())

	// The sidecar's own security context, which the Jobs share an image with, names their identity.
	backupSecurityContext := &core.SecurityContext{RunAsUser: ptr.To(int64(2000)), RunAsNonRoot: ptr.To(true)}
	chi.Spec.Templates.PodTemplates[0].Spec.Containers = append(chi.Spec.Templates.PodTemplates[0].Spec.Containers, core.Container{
		Name: config.ClickHouseBackupContainerName, Image: "registry/backup:own", ImagePullPolicy: core.PullAlways, SecurityContext: backupSecurityContext,
	})
	useTemplate(chi, chi.Spec.Configuration.Clusters[1], "pod")
	_, stored = normalize(t, chi)
	topology, err = NewTopology(stored, "custom")
	require.NoError(t, err)
	require.Equal(t, backupSecurityContext, topology.pod().containerSecurityContext)
	require.Equal(t, "registry/backup:own", topology.pod().image)
	require.Equal(t, core.PullAlways, topology.pod().imagePullPolicy)
}

// An installation normalized in memory links its hosts back to itself; topology must resolve it
// like the stored form rather than recurse through those links.
func TestTopologyFromInMemoryNormalizedInstallation(t *testing.T) {
	inMemory, stored := normalize(t, newCHI("demo", layoutCluster("main", 2, 2, enabledBackup())))

	fromMemory, err := NewTopology(inMemory, "")
	require.NoError(t, err)
	fromStorage, err := NewTopology(stored, "")
	require.NoError(t, err)
	require.Equal(t, fromStorage, fromMemory)
}

func TestTopologyLeavesItsInputUntouched(t *testing.T) {
	_, stored := normalize(t, newCHI("demo", layoutCluster("main", 1, 1, enabledBackup())))
	before, err := json.Marshal(stored)
	require.NoError(t, err)

	_, err = NewTopology(stored, "")
	require.NoError(t, err)

	after, err := json.Marshal(stored)
	require.NoError(t, err)
	require.JSONEq(t, string(before), string(after))
	require.Empty(t, stored.Spec.Configuration.Clusters[0].Layout.Shards[0].Hosts[0].Runtime.Address.CHIName)
}
