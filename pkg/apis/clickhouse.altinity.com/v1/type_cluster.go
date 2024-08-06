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

import (
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// ChiCluster defines item of a clusters section of .configuration
type ChiCluster struct {
	Name              string            `json:"name,omitempty"              yaml:"name,omitempty"`
	Zookeeper         *ZookeeperConfig  `json:"zookeeper,omitempty"         yaml:"zookeeper,omitempty"`
	Settings          *Settings         `json:"settings,omitempty"          yaml:"settings,omitempty"`
	Files             *Settings         `json:"files,omitempty"             yaml:"files,omitempty"`
	Templates         *TemplatesList    `json:"templates,omitempty"         yaml:"templates,omitempty"`
	SchemaPolicy      *SchemaPolicy     `json:"schemaPolicy,omitempty"      yaml:"schemaPolicy,omitempty"`
	Insecure          *types.StringBool `json:"insecure,omitempty"          yaml:"insecure,omitempty"`
	Secure            *types.StringBool `json:"secure,omitempty"            yaml:"secure,omitempty"`
	Secret            *ClusterSecret    `json:"secret,omitempty"            yaml:"secret,omitempty"`
	PDBMaxUnavailable *types.Int32      `json:"pdbMaxUnavailable,omitempty" yaml:"pdbMaxUnavailable,omitempty"`
	Layout            *ChiClusterLayout `json:"layout,omitempty"            yaml:"layout,omitempty"`

	Runtime ChiClusterRuntime `json:"-" yaml:"-"`
}

type ChiClusterRuntime struct {
	Address ChiClusterAddress       `json:"-" yaml:"-"`
	CHI     *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r *ChiClusterRuntime) GetAddress() IClusterAddress {
	return &r.Address
}

func (r ChiClusterRuntime) GetCR() ICustomResource {
	return r.CHI
}

func (r *ChiClusterRuntime) SetCR(cr ICustomResource) {
	r.CHI = cr.(*ClickHouseInstallation)
}

// ChiClusterAddress defines address of a cluster within ClickHouseInstallation
type ChiClusterAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
}

func (a *ChiClusterAddress) GetNamespace() string {
	return a.Namespace
}

func (a *ChiClusterAddress) SetNamespace(namespace string) {
	a.Namespace = namespace
}

func (a *ChiClusterAddress) GetCRName() string {
	return a.CHIName
}

func (a *ChiClusterAddress) SetCRName(name string) {
	a.CHIName = name
}

func (a *ChiClusterAddress) GetClusterName() string {
	return a.ClusterName
}

func (a *ChiClusterAddress) SetClusterName(name string) {
	a.ClusterName = name
}

func (a *ChiClusterAddress) GetClusterIndex() int {
	return a.ClusterIndex
}

func (a *ChiClusterAddress) SetClusterIndex(index int) {
	a.ClusterIndex = index
}

func (cluster *ChiCluster) GetName() string {
	return cluster.Name
}

func (c *ChiCluster) GetZookeeper() *ZookeeperConfig {
	return c.Zookeeper
}

func (c *ChiCluster) GetSchemaPolicy() *SchemaPolicy {
	return c.SchemaPolicy
}

// GetInsecure is a getter
func (cluster *ChiCluster) GetInsecure() *types.StringBool {
	if cluster == nil {
		return nil
	}
	return cluster.Insecure
}

// GetSecure is a getter
func (cluster *ChiCluster) GetSecure() *types.StringBool {
	if cluster == nil {
		return nil
	}
	return cluster.Secure
}

func (c *ChiCluster) GetSecret() *ClusterSecret {
	return c.Secret
}

func (cluster *ChiCluster) GetRuntime() IClusterRuntime {
	return &cluster.Runtime
}

func (cluster *ChiCluster) GetPDBMaxUnavailable() *types.Int32 {
	return cluster.PDBMaxUnavailable
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
	if chi.GetSpecT().Configuration == nil {
		return
	}
	if chi.GetSpecT().Configuration.Zookeeper == nil {
		return
	}

	cluster.Zookeeper = cluster.Zookeeper.MergeFrom(chi.GetSpecT().Configuration.Zookeeper, MergeTypeFillEmptyValues)
}

// InheritFilesFrom inherits files from CHI
func (cluster *ChiCluster) InheritFilesFrom(chi *ClickHouseInstallation) {
	if chi.GetSpecT().Configuration == nil {
		return
	}
	if chi.GetSpecT().Configuration.Files == nil {
		return
	}

	// Propagate host section only
	cluster.Files = cluster.Files.MergeFromCB(chi.GetSpecT().Configuration.Files, func(path string, _ *Setting) bool {
		if section, err := GetSectionFromPath(path); err == nil {
			if section.Equal(SectionHost) {
				return true
			}
		}

		return false
	})
}

// InheritTemplatesFrom inherits templates from CHI
func (cluster *ChiCluster) InheritTemplatesFrom(chi *ClickHouseInstallation) {
	if chi.GetSpec().GetDefaults() == nil {
		return
	}
	if chi.GetSpec().GetDefaults().Templates == nil {
		return
	}
	cluster.Templates = cluster.Templates.MergeFrom(chi.GetSpec().GetDefaults().Templates, MergeTypeFillEmptyValues)
	cluster.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate returns service template, if exists
func (cluster *ChiCluster) GetServiceTemplate() (*ServiceTemplate, bool) {
	if !cluster.Templates.HasClusterServiceTemplate() {
		return nil, false
	}
	name := cluster.Templates.GetClusterServiceTemplate()
	return cluster.Runtime.CHI.GetServiceTemplate(name)
}

// GetCHI gets parent CHI
func (cluster *ChiCluster) GetCHI() *ClickHouseInstallation {
	return cluster.Runtime.CHI
}

// GetShard gets shard with specified index
func (cluster *ChiCluster) GetShard(shard int) *ChiShard {
	return cluster.Layout.Shards[shard]
}

// GetOrCreateHost gets or creates host on specified coordinates
func (cluster *ChiCluster) GetOrCreateHost(shard, replica int) *Host {
	return cluster.Layout.HostsField.GetOrCreate(shard, replica)
}

// GetReplica gets replica with specified index
func (cluster *ChiCluster) GetReplica(replica int) *ChiReplica {
	return cluster.Layout.Replicas[replica]
}

// FindShard finds shard by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cluster *ChiCluster) FindShard(needle interface{}) IShard {
	var resultShard *ChiShard
	cluster.WalkShards(func(index int, shard IShard) error {
		switch v := needle.(type) {
		case string:
			if shard.GetName() == v {
				resultShard = shard.(*ChiShard)
			}
		case int:
			if index == v {
				resultShard = shard.(*ChiShard)
			}
		}
		return nil
	})
	return resultShard
}

// FindHost finds host by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cluster *ChiCluster) FindHost(needleShard interface{}, needleHost interface{}) *Host {
	return cluster.FindShard(needleShard).FindHost(needleHost)
}

// FirstHost finds first host in the cluster
func (cluster *ChiCluster) FirstHost() *Host {
	var result *Host
	cluster.WalkHosts(func(host *Host) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

// WalkShards walks shards
func (cluster *ChiCluster) WalkShards(f func(index int, shard IShard) error) []error {
	if cluster == nil {
		return nil
	}
	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := cluster.Layout.Shards[shardIndex]
		res = append(res, f(shardIndex, shard))
	}

	return res
}

// WalkReplicas walks replicas
func (cluster *ChiCluster) WalkReplicas(f func(index int, replica *ChiReplica) error) []error {
	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := cluster.Layout.Replicas[replicaIndex]
		res = append(res, f(replicaIndex, replica))
	}

	return res
}

// WalkHosts walks hosts
func (cluster *ChiCluster) WalkHosts(f func(host *Host) error) []error {
	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := cluster.Layout.Shards[shardIndex]
		for replicaIndex := range shard.Hosts {
			host := shard.Hosts[replicaIndex]
			res = append(res, f(host))
		}
	}

	return res
}

// WalkHostsByShards walks hosts by shards
func (cluster *ChiCluster) WalkHostsByShards(f func(shard, replica int, host *Host) error) []error {

	res := make([]error, 0)

	for shardIndex := range cluster.Layout.Shards {
		shard := cluster.Layout.Shards[shardIndex]
		for replicaIndex := range shard.Hosts {
			host := shard.Hosts[replicaIndex]
			res = append(res, f(shardIndex, replicaIndex, host))
		}
	}

	return res
}

func (cluster *ChiCluster) GetLayout() *ChiClusterLayout {
	return cluster.Layout
}

// WalkHostsByReplicas walks hosts by replicas
func (cluster *ChiCluster) WalkHostsByReplicas(f func(shard, replica int, host *Host) error) []error {

	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := cluster.Layout.Replicas[replicaIndex]
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
	cluster.WalkHosts(func(host *Host) error {
		count++
		return nil
	})
	return count
}

// ChiClusterLayout defines layout section of .spec.configuration.clusters
type ChiClusterLayout struct {
	ShardsCount   int `json:"shardsCount,omitempty"   yaml:"shardsCount,omitempty"`
	ReplicasCount int `json:"replicasCount,omitempty" yaml:"replicasCount,omitempty"`

	// TODO refactor into map[string]ChiShard
	Shards   []*ChiShard   `json:"shards,omitempty"   yaml:"shards,omitempty"`
	Replicas []*ChiReplica `json:"replicas,omitempty" yaml:"replicas,omitempty"`

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

func (l *ChiClusterLayout) GetReplicasCount() int {
	return l.ReplicasCount
}

// SchemaPolicy defines schema management policy - replica or shard-based
type SchemaPolicy struct {
	Replica string `json:"replica" yaml:"replica"`
	Shard   string `json:"shard"   yaml:"shard"`
}

// NewClusterSchemaPolicy creates new cluster layout
func NewClusterSchemaPolicy() *SchemaPolicy {
	return new(SchemaPolicy)
}
