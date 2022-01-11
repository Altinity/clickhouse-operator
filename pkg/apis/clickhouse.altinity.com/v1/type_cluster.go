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
	Name      string              `json:"name,omitempty"      yaml:"name,omitempty"`
	Zookeeper *ChiZookeeperConfig `json:"zookeeper,omitempty" yaml:"zookeeper,omitempty"`
	Settings  *Settings           `json:"settings,omitempty"  yaml:"settings,omitempty"`
	Files     *Settings           `json:"files,omitempty"     yaml:"files,omitempty"`
	Templates *ChiTemplateNames   `json:"templates,omitempty" yaml:"templates,omitempty"`
	Layout    *ChiClusterLayout   `json:"layout,omitempty"    yaml:"layout,omitempty"`

	// Internal data
	Address ChiClusterAddress       `json:"-" yaml:"-"`
	CHI     *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

// ChiClusterAddress defines address of a cluster within ClickHouseInstallation
type ChiClusterAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
}

// ChiClusterLayout defines layout section of .spec.configuration.clusters
type ChiClusterLayout struct {
	// DEPRECATED - to be removed soon
	Type          string `json:"type,omitempty"          yaml:"type,omitempty"`
	ShardsCount   int    `json:"shardsCount,omitempty"   yaml:"shardsCount,omitempty"`
	ReplicasCount int    `json:"replicasCount,omitempty" yaml:"replicasCount,omitempty"`
	// TODO refactor into map[string]ChiShard
	Shards   []ChiShard   `json:"shards,omitempty"   yaml:"shards,omitempty"`
	Replicas []ChiReplica `json:"replicas,omitempty" yaml:"replicas,omitempty"`

	// Internal data
	// Whether shards or replicas are explicitly specified as Shards []ChiShard or Replicas []ChiReplica
	ShardsSpecified   bool        `json:"-" yaml:"-" testdiff:"ignore"`
	ReplicasSpecified bool        `json:"-" yaml:"-" testdiff:"ignore"`
	HostsField        *HostsField `json:"-" yaml:"-" testdiff:"ignore"`
}

// NewChiClusterLayout creates new cluster layout
func NewChiClusterLayout() *ChiClusterLayout {
	return new(ChiClusterLayout)
}

// FillShardReplicaSpecified fills whether shard or replicas are explicitly specified
func (cluster *ChiCluster) FillShardReplicaSpecified() {
	if len(cluster.Layout.Shards) > 0 {
		cluster.Layout.ShardsSpecified = true
	}
	if len(cluster.Layout.Replicas) > 0 {
		cluster.Layout.ReplicasSpecified = true
	}
}

// isShardSpecified checks whether shard is explicitly specified
func (cluster *ChiCluster) isShardSpecified() bool {
	return cluster.Layout.ShardsSpecified == true
}

// isReplicaSpecified checks whether replica is explicitly specified
func (cluster *ChiCluster) isReplicaSpecified() bool {
	return (cluster.Layout.ShardsSpecified == false) && (cluster.Layout.ReplicasSpecified == true)
}

// IsShardSpecified checks whether shard is explicitly specified
func (cluster *ChiCluster) IsShardSpecified() bool {
	if !cluster.isShardSpecified() && !cluster.isReplicaSpecified() {
		return true
	}

	return cluster.isShardSpecified()
}

// InheritZookeeperFrom inherits zookeeper config from CHI
func (cluster *ChiCluster) InheritZookeeperFrom(chi *ClickHouseInstallation) {
	if !cluster.Zookeeper.IsEmpty() {
		// Has zk config explicitly specified alread
		return
	}
	if chi.Spec.Configuration == nil {
		return
	}
	if chi.Spec.Configuration.Zookeeper == nil {
		return
	}

	cluster.Zookeeper = cluster.Zookeeper.MergeFrom(chi.Spec.Configuration.Zookeeper, MergeTypeFillEmptyValues)
}

// InheritFilesFrom inherits files from CHI
func (cluster *ChiCluster) InheritFilesFrom(chi *ClickHouseInstallation) {
	if chi.Spec.Configuration == nil {
		return
	}
	if chi.Spec.Configuration.Files == nil {
		return
	}

	cluster.Files = cluster.Files.MergeFromCB(chi.Spec.Configuration.Files, func(path string, _ *Setting) bool {
		if section, err := getSectionFromPath(path); err == nil {
			if section == SectionHost {
				return true
			}
		}

		return false
	})
}

// InheritTemplatesFrom inherits templates from CHI
func (cluster *ChiCluster) InheritTemplatesFrom(chi *ClickHouseInstallation) {
	if chi.Spec.Defaults == nil {
		return
	}
	if chi.Spec.Defaults.Templates == nil {
		return
	}
	cluster.Templates = cluster.Templates.MergeFrom(chi.Spec.Defaults.Templates, MergeTypeFillEmptyValues)
	cluster.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate returns service template, if exists
func (cluster *ChiCluster) GetServiceTemplate() (*ChiServiceTemplate, bool) {
	if !cluster.Templates.HasClusterServiceTemplate() {
		return nil, false
	}
	name := cluster.Templates.GetClusterServiceTemplate()
	return cluster.CHI.GetServiceTemplate(name)
}

// GetCHI gets parent CHI
func (cluster *ChiCluster) GetCHI() *ClickHouseInstallation {
	return cluster.CHI
}

// GetShard gets shard with specified index
func (cluster *ChiCluster) GetShard(shard int) *ChiShard {
	return &cluster.Layout.Shards[shard]
}

// GetOrCreateHost gets or creates host on specified coordinates
func (cluster *ChiCluster) GetOrCreateHost(shard, replica int) *ChiHost {
	return cluster.Layout.HostsField.GetOrCreate(shard, replica)
}

// GetReplica gets replica with specified index
func (cluster *ChiCluster) GetReplica(replica int) *ChiReplica {
	return &cluster.Layout.Replicas[replica]
}

// FindShard finds shard by either name or index
func (cluster *ChiCluster) FindShard(needle interface{}) *ChiShard {
	var resultShard *ChiShard
	cluster.WalkShards(func(index int, shard *ChiShard) error {
		switch v := needle.(type) {
		case string:
			if shard.Name == v {
				resultShard = shard
			}
		case int:
			if index == v {
				resultShard = shard
			}
		}
		return nil
	})
	return resultShard
}

// FindHost finds host in the cluster
func (cluster *ChiCluster) FindHost(needle interface{}) *ChiHost {
	var result *ChiHost
	cluster.WalkHosts(func(host *ChiHost) error {
		switch typed := needle.(type) {
		case *ChiHost:
			if typed == host {
				result = host
			}
		}
		return nil
	})
	return result
}

// FirstHost finds first host in the cluster
func (cluster *ChiCluster) FirstHost() *ChiHost {
	var result *ChiHost
	cluster.WalkHosts(func(host *ChiHost) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

// WalkShards walks shards
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

// WalkReplicas walks replicas
func (cluster *ChiCluster) WalkReplicas(f func(index int, replica *ChiReplica) error) []error {
	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := &cluster.Layout.Replicas[replicaIndex]
		res = append(res, f(replicaIndex, replica))
	}

	return res
}

// WalkHosts walks hosts
func (cluster *ChiCluster) WalkHosts(f func(host *ChiHost) error) []error {

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

// WalkHostsByShards walks hosts by shards
func (cluster *ChiCluster) WalkHostsByShards(f func(shard, replica int, host *ChiHost) error) []error {

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

// WalkHostsByReplicas walks hosts by replicas
func (cluster *ChiCluster) WalkHostsByReplicas(f func(shard, replica int, host *ChiHost) error) []error {

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

// HostsCount counts hosts
func (cluster *ChiCluster) HostsCount() int {
	count := 0
	cluster.WalkHosts(func(host *ChiHost) error {
		count++
		return nil
	})
	return count
}
