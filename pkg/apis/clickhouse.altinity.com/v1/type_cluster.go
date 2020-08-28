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

package v1

// ChiCluster defines item of a clusters section of .configuration
type ChiCluster struct {
	Name      string             `json:"name"`
	Zookeeper ChiZookeeperConfig `json:"zookeeper,omitempty"`
	Settings  Settings           `json:"settings,omitempty"`
	Files     Settings           `json:"files,omitempty"`
	Templates ChiTemplateNames   `json:"templates,omitempty"`
	Layout    ChiClusterLayout   `json:"layout"`

	// Internal data
	Address ChiClusterAddress       `json:"address,omitempty"`
	CHI     *ClickHouseInstallation `json:"-" testdiff:"ignore"`
}

// ChiClusterAddress defines address of a cluster within ClickHouseInstallation
type ChiClusterAddress struct {
	Namespace    string `json:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty"`
}

// ChiClusterLayout defines layout section of .spec.configuration.clusters
type ChiClusterLayout struct {
	// DEPRECATED - to be removed soon
	Type          string `json:"type,omitempty"`
	ShardsCount   int    `json:"shardsCount,omitempty"`
	ReplicasCount int    `json:"replicasCount,omitempty"`
	// TODO refactor into map[string]ChiShard
	Shards            []ChiShard   `json:"shards,omitempty"`
	Replicas          []ChiReplica `json:"replicas,omitempty"`
	ShardsSpecified   bool         `json:"-" testdiff:"ignore"`
	ReplicasSpecified bool         `json:"-" testdiff:"ignore"`

	HostsField *HostsField `json:"-" testdiff:"ignore"`
}

func (cluster *ChiCluster) FillShardReplicaSpecified() {
	if len(cluster.Layout.Shards) > 0 {
		cluster.Layout.ShardsSpecified = true
	}
	if len(cluster.Layout.Replicas) > 0 {
		cluster.Layout.ReplicasSpecified = true
	}
}

func (cluster *ChiCluster) isShardSpecified() bool {
	return cluster.Layout.ShardsSpecified == true
}

func (cluster *ChiCluster) isReplicaSpecified() bool {
	return (cluster.Layout.ShardsSpecified == false) && (cluster.Layout.ReplicasSpecified == true)
}

func (cluster *ChiCluster) IsShardSpecified() bool {
	if !cluster.isShardSpecified() && !cluster.isReplicaSpecified() {
		return true
	}

	return cluster.isShardSpecified()
}

func (cluster *ChiCluster) InheritZookeeperFrom(chi *ClickHouseInstallation) {
	if cluster.Zookeeper.IsEmpty() {
		(&cluster.Zookeeper).MergeFrom(&chi.Spec.Configuration.Zookeeper, MergeTypeFillEmptyValues)
	}
}

func (cluster *ChiCluster) InheritSettingsFrom(chi *ClickHouseInstallation) {
	(&cluster.Settings).MergeFrom(chi.Spec.Configuration.Settings)
}

func (cluster *ChiCluster) InheritFilesFrom(chi *ClickHouseInstallation) {
	(&cluster.Files).MergeFromCB(chi.Spec.Configuration.Files, func(path string, _ *Setting) bool {
		if section, err := getSectionFromPath(path); err == nil {
			if section == SectionHost {
				return true
			}
		}

		return false
	})
}

func (cluster *ChiCluster) InheritTemplatesFrom(chi *ClickHouseInstallation) {
	(&cluster.Templates).MergeFrom(&chi.Spec.Defaults.Templates, MergeTypeFillEmptyValues)
	(&cluster.Templates).HandleDeprecatedFields()
}

func (cluster *ChiCluster) GetServiceTemplate() (*ChiServiceTemplate, bool) {
	name := cluster.Templates.ClusterServiceTemplate
	template, ok := cluster.CHI.GetServiceTemplate(name)
	return template, ok
}

func (cluster *ChiCluster) GetCHI() *ClickHouseInstallation {
	return cluster.CHI
}

func (cluster *ChiCluster) GetShard(shard int) *ChiShard {
	return &cluster.Layout.Shards[shard]
}

func (cluster *ChiCluster) GetReplica(replica int) *ChiReplica {
	return &cluster.Layout.Replicas[replica]
}

func (cluster *ChiCluster) WalkShards(
	f func(index int, shard *ChiShard) error,
) []error {
	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := &cluster.Layout.Shards[shardIndex]
		res = append(res, f(shardIndex, shard))
	}

	return res
}

func (cluster *ChiCluster) WalkReplicas(
	f func(index int, replica *ChiReplica) error,
) []error {
	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := &cluster.Layout.Replicas[replicaIndex]
		res = append(res, f(replicaIndex, replica))
	}

	return res
}

func (cluster *ChiCluster) WalkHosts(
	f func(host *ChiHost) error,
) []error {

	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := &cluster.Layout.Shards[shardIndex]
		for replicaIndex := range shard.Hosts {
			host := shard.Hosts[replicaIndex]
			res = append(res, f(host))
		}
	}

	return res
}

func (cluster *ChiCluster) WalkHostsByShards(
	f func(shard, replica int, host *ChiHost) error,
) []error {

	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := &cluster.Layout.Shards[shardIndex]
		for replicaIndex := range shard.Hosts {
			host := shard.Hosts[replicaIndex]
			res = append(res, f(shardIndex, replicaIndex, host))
		}
	}

	return res
}

func (cluster *ChiCluster) WalkHostsByReplicas(
	f func(shard, replica int, host *ChiHost) error,
) []error {

	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := &cluster.Layout.Replicas[replicaIndex]
		for shardIndex := range replica.Hosts {
			host := replica.Hosts[shardIndex]
			res = append(res, f(shardIndex, replicaIndex, host))
		}
	}

	return res
}

func (cluster *ChiCluster) HostsCount() int {
	count := 0
	cluster.WalkHosts(func(host *ChiHost) error {
		count++
		return nil
	})
	return count
}
