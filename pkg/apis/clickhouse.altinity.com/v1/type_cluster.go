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

// Cluster defines item of a clusters section of .configuration
type Cluster struct {
	Name         string              `json:"name,omitempty"         yaml:"name,omitempty"`
	Zookeeper    *ChiZookeeperConfig `json:"zookeeper,omitempty"    yaml:"zookeeper,omitempty"`
	Settings     *Settings           `json:"settings,omitempty"     yaml:"settings,omitempty"`
	Files        *Settings           `json:"files,omitempty"        yaml:"files,omitempty"`
	Templates    *TemplatesList      `json:"templates,omitempty"    yaml:"templates,omitempty"`
	SchemaPolicy *SchemaPolicy       `json:"schemaPolicy,omitempty" yaml:"schemaPolicy,omitempty"`
	Insecure     *StringBool         `json:"insecure,omitempty"     yaml:"insecure,omitempty"`
	Secure       *StringBool         `json:"secure,omitempty"       yaml:"secure,omitempty"`
	Secret       *ClusterSecret      `json:"secret,omitempty"       yaml:"secret,omitempty"`
	Layout       *ChiClusterLayout   `json:"layout,omitempty"       yaml:"layout,omitempty"`

	Runtime ClusterRuntime `json:"-" yaml:"-"`
}

type ClusterRuntime struct {
	Address ChiClusterAddress       `json:"-" yaml:"-"`
	CHI     *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

// SchemaPolicy defines schema management policy - replica or shard-based
type SchemaPolicy struct {
	Replica string `json:"replica" yaml:"replica"`
	Shard   string `json:"shard"   yaml:"shard"`
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

// NewClusterSchemaPolicy creates new cluster layout
func NewClusterSchemaPolicy() *SchemaPolicy {
	return new(SchemaPolicy)
}

// NewChiClusterLayout creates new cluster layout
func NewChiClusterLayout() *ChiClusterLayout {
	return new(ChiClusterLayout)
}

// FillShardReplicaSpecified fills whether shard or replicas are explicitly specified
func (cluster *Cluster) FillShardReplicaSpecified() {
	if len(cluster.Layout.Shards) > 0 {
		cluster.Layout.ShardsSpecified = true
	}
	if len(cluster.Layout.Replicas) > 0 {
		cluster.Layout.ReplicasSpecified = true
	}
}

// isShardSpecified checks whether shard is explicitly specified
func (cluster *Cluster) isShardSpecified() bool {
	return cluster.Layout.ShardsSpecified == true
}

// isReplicaSpecified checks whether replica is explicitly specified
func (cluster *Cluster) isReplicaSpecified() bool {
	return (cluster.Layout.ShardsSpecified == false) && (cluster.Layout.ReplicasSpecified == true)
}

// IsShardSpecified checks whether shard is explicitly specified
func (cluster *Cluster) IsShardSpecified() bool {
	if !cluster.isShardSpecified() && !cluster.isReplicaSpecified() {
		return true
	}

	return cluster.isShardSpecified()
}

// InheritZookeeperFrom inherits zookeeper config from CHI
func (cluster *Cluster) InheritZookeeperFrom(chi *ClickHouseInstallation) {
	if !cluster.Zookeeper.IsEmpty() {
		// Has zk config explicitly specified alread
		return
	}
	if chi.GetSpec().Configuration == nil {
		return
	}
	if chi.GetSpec().Configuration.Zookeeper == nil {
		return
	}

	cluster.Zookeeper = cluster.Zookeeper.MergeFrom(chi.GetSpec().Configuration.Zookeeper, MergeTypeFillEmptyValues)
}

// InheritFilesFrom inherits files from CHI
func (cluster *Cluster) InheritFilesFrom(chi *ClickHouseInstallation) {
	if chi.GetSpec().Configuration == nil {
		return
	}
	if chi.GetSpec().Configuration.Files == nil {
		return
	}

	// Propagate host section only
	cluster.Files = cluster.Files.MergeFromCB(chi.GetSpec().Configuration.Files, func(path string, _ *Setting) bool {
		if section, err := getSectionFromPath(path); err == nil {
			if section.Equal(SectionHost) {
				return true
			}
		}

		return false
	})
}

// InheritTemplatesFrom inherits templates from CHI
func (cluster *Cluster) InheritTemplatesFrom(chi *ClickHouseInstallation) {
	if chi.GetSpec().Defaults == nil {
		return
	}
	if chi.GetSpec().Defaults.Templates == nil {
		return
	}
	cluster.Templates = cluster.Templates.MergeFrom(chi.GetSpec().Defaults.Templates, MergeTypeFillEmptyValues)
	cluster.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate returns service template, if exists
func (cluster *Cluster) GetServiceTemplate() (*ServiceTemplate, bool) {
	if !cluster.Templates.HasClusterServiceTemplate() {
		return nil, false
	}
	name := cluster.Templates.GetClusterServiceTemplate()
	return cluster.Runtime.CHI.GetServiceTemplate(name)
}

// GetCHI gets parent CHI
func (cluster *Cluster) GetCHI() *ClickHouseInstallation {
	return cluster.Runtime.CHI
}

// GetShard gets shard with specified index
func (cluster *Cluster) GetShard(shard int) *ChiShard {
	return &cluster.Layout.Shards[shard]
}

// GetOrCreateHost gets or creates host on specified coordinates
func (cluster *Cluster) GetOrCreateHost(shard, replica int) *ChiHost {
	return cluster.Layout.HostsField.GetOrCreate(shard, replica)
}

// GetReplica gets replica with specified index
func (cluster *Cluster) GetReplica(replica int) *ChiReplica {
	return &cluster.Layout.Replicas[replica]
}

// FindShard finds shard by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cluster *Cluster) FindShard(needle interface{}) *ChiShard {
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

// FindHost finds host by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cluster *Cluster) FindHost(needleShard interface{}, needleHost interface{}) *ChiHost {
	return cluster.FindShard(needleShard).FindHost(needleHost)
}

// FirstHost finds first host in the cluster
func (cluster *Cluster) FirstHost() *ChiHost {
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
func (cluster *Cluster) WalkShards(
	f func(index int, shard *ChiShard) error,
) []error {
	if cluster == nil {
		return nil
	}
	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := &cluster.Layout.Shards[shardIndex]
		res = append(res, f(shardIndex, shard))
	}

	return res
}

// WalkReplicas walks replicas
func (cluster *Cluster) WalkReplicas(f func(index int, replica *ChiReplica) error) []error {
	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := &cluster.Layout.Replicas[replicaIndex]
		res = append(res, f(replicaIndex, replica))
	}

	return res
}

// WalkHosts walks hosts
func (cluster *Cluster) WalkHosts(f func(host *ChiHost) error) []error {

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
func (cluster *Cluster) WalkHostsByShards(f func(shard, replica int, host *ChiHost) error) []error {

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
func (cluster *Cluster) WalkHostsByReplicas(f func(shard, replica int, host *ChiHost) error) []error {

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
func (cluster *Cluster) HostsCount() int {
	count := 0
	cluster.WalkHosts(func(host *ChiHost) error {
		count++
		return nil
	})
	return count
}

// GetInsecure is a getter
func (cluster *Cluster) GetInsecure() *StringBool {
	if cluster == nil {
		return nil
	}
	return cluster.Insecure
}

// GetSecure is a getter
func (cluster *Cluster) GetSecure() *StringBool {
	if cluster == nil {
		return nil
	}
	return cluster.Secure
}
