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
	apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// Cluster defines item of a clusters section of .configuration
type Cluster struct {
	Name string `json:"name,omitempty"         yaml:"name,omitempty"`

	Settings          *apiChi.Settings        `json:"settings,omitempty"          yaml:"settings,omitempty"`
	Files             *apiChi.Settings        `json:"files,omitempty"             yaml:"files,omitempty"`
	Templates         *apiChi.TemplatesList   `json:"templates,omitempty"         yaml:"templates,omitempty"`
	Layout            *ChkClusterLayout       `json:"layout,omitempty"            yaml:"layout,omitempty"`
	PDBManaged        *types.StringBool       `json:"pdbManaged,omitempty"        yaml:"pdbManaged,omitempty"`
	PDBMaxUnavailable *types.Int32            `json:"pdbMaxUnavailable,omitempty" yaml:"pdbMaxUnavailable,omitempty"`
	Reconcile         apiChi.ClusterReconcile `json:"reconcile"                   yaml:"reconcile"`

	Runtime ChkClusterRuntime `json:"-" yaml:"-"`
}

type ChkClusterRuntime struct {
	Address ChkClusterAddress             `json:"-" yaml:"-"`
	CHK     *ClickHouseKeeperInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r *ChkClusterRuntime) GetAddress() apiChi.IClusterAddress {
	return &r.Address
}

func (r ChkClusterRuntime) GetCR() apiChi.ICustomResource {
	return r.CHK
}

func (r *ChkClusterRuntime) SetCR(cr apiChi.ICustomResource) {
	r.CHK = cr.(*ClickHouseKeeperInstallation)
}

// ChkClusterAddress defines address of a cluster within ClickHouseInstallation
type ChkClusterAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
}

func (a *ChkClusterAddress) GetNamespace() string {
	return a.Namespace
}

func (a *ChkClusterAddress) SetNamespace(namespace string) {
	a.Namespace = namespace
}

func (a *ChkClusterAddress) GetCRName() string {
	return a.CHIName
}

func (a *ChkClusterAddress) SetCRName(name string) {
	a.CHIName = name
}

func (a *ChkClusterAddress) GetClusterName() string {
	return a.ClusterName
}

func (a *ChkClusterAddress) SetClusterName(name string) {
	a.ClusterName = name
}

func (a *ChkClusterAddress) GetClusterIndex() int {
	return a.ClusterIndex
}

func (a *ChkClusterAddress) SetClusterIndex(index int) {
	a.ClusterIndex = index
}

func (cluster *Cluster) GetName() string {
	return cluster.Name
}

// HasName checks whether cluster has a name
func (cluster *Cluster) HasName() bool {
	if cluster == nil {
		return false
	}
	return len(cluster.GetName()) > 0
}

func (c *Cluster) GetZookeeper() *apiChi.ZookeeperConfig {
	return nil
}

func (c *Cluster) GetSchemaPolicy() *apiChi.SchemaPolicy {
	return nil
}

// GetInsecure is a getter
func (cluster *Cluster) GetInsecure() *types.StringBool {
	return nil
}

// GetSecure is a getter
func (cluster *Cluster) GetSecure() *types.StringBool {
	return nil
}

// GetSecret is a getter
func (c *Cluster) GetSecret() *apiChi.ClusterSecret {
	return nil
}

// GetPDBManaged is a getter
func (cluster *Cluster) GetPDBManaged() *types.StringBool {
	return cluster.PDBManaged
}

// GetPDBMaxUnavailable is a getter
func (cluster *Cluster) GetPDBMaxUnavailable() *types.Int32 {
	return cluster.PDBMaxUnavailable
}

// GetReconcile is a getter
func (cluster *Cluster) GetReconcile() apiChi.ClusterReconcile {
	return cluster.Reconcile
}

// GetRuntime is a getter
func (cluster *Cluster) GetRuntime() apiChi.IClusterRuntime {
	return &cluster.Runtime
}

// FillShardsReplicasExplicitlySpecified fills whether shard or replicas are explicitly specified
func (cluster *Cluster) FillShardsReplicasExplicitlySpecified() {
	if len(cluster.Layout.Shards) > 0 {
		cluster.Layout.ShardsExplicitlySpecified = true
	}
	if len(cluster.Layout.Replicas) > 0 {
		cluster.Layout.ReplicasExplicitlySpecified = true
	}
}

// isShardExplicitlySpecified checks whether shard is explicitly specified
func (cluster *Cluster) isShardExplicitlySpecified() bool {
	return cluster.Layout.ShardsExplicitlySpecified
}

// isReplicaExplicitlySpecified checks whether replica is explicitly specified
func (cluster *Cluster) isReplicaExplicitlySpecified() bool {
	return cluster.Layout.ReplicasExplicitlySpecified
}

// IsShardSpecified checks whether shard is explicitly specified
func (cluster *Cluster) isShardToBeUsedToInheritSettingsFrom() bool {
	if !cluster.isShardExplicitlySpecified() && !cluster.isReplicaExplicitlySpecified() {
		return true
	}

	// When both shards and replicas are explicitly specified, prefer replicas
	// since replica-level configuration is more specific than shard-level
	if cluster.isShardExplicitlySpecified() && cluster.isReplicaExplicitlySpecified() {
		return false
	}

	return cluster.isShardExplicitlySpecified()
}

func (cluster *Cluster) SelectSettingsSourceFrom(shard apiChi.IShard, replica apiChi.IReplica) any {
	if cluster.isShardToBeUsedToInheritSettingsFrom() {
		return shard
	}
	return replica
}

// InheritFilesFrom inherits files from CHI
func (cluster *Cluster) InheritFilesFrom(chk *ClickHouseKeeperInstallation) {
	if chk.GetSpecT().Configuration == nil {
		return
	}
	if chk.GetSpecT().Configuration.Files == nil {
		return
	}

	// Propagate host section only
	cluster.Files = cluster.Files.MergeFromCB(chk.GetSpecT().Configuration.Files, func(path string, _ *apiChi.Setting) bool {
		if section, err := apiChi.GetSectionFromPath(path); err == nil {
			if section.Equal(apiChi.SectionHost) {
				return true
			}
		}

		return false
	})
}

// InheritTemplatesFrom inherits templates from CHI
func (cluster *Cluster) InheritTemplatesFrom(chk *ClickHouseKeeperInstallation) {
	if chk.GetSpec().GetDefaults() == nil {
		return
	}
	if chk.GetSpec().GetDefaults().Templates == nil {
		return
	}
	cluster.Templates = cluster.Templates.MergeFrom(chk.GetSpec().GetDefaults().Templates, apiChi.MergeTypeFillEmptyValues)
	cluster.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate returns service template, if exists
func (cluster *Cluster) GetServiceTemplate() (*apiChi.ServiceTemplate, bool) {
	return nil, false
}

func (cluster *Cluster) GetAncestor() apiChi.ICluster {
	return (*Cluster)(nil)
}

// GetShard gets shard with specified index
func (cluster *Cluster) GetShard(shard int) *ChkShard {
	return cluster.Layout.Shards[shard]
}

// GetOrCreateHost gets or creates host on specified coordinates
func (cluster *Cluster) GetOrCreateHost(shard, replica int) *apiChi.Host {
	return cluster.Layout.HostsField.GetOrCreate(shard, replica)
}

// GetReplica gets replica with specified index
func (cluster *Cluster) GetReplica(replica int) *ChkReplica {
	return cluster.Layout.Replicas[replica]
}

// FindShard finds shard by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cluster *Cluster) FindShard(needle interface{}) apiChi.IShard {
	var resultShard *ChkShard
	cluster.WalkShards(func(index int, shard apiChi.IShard) error {
		switch v := needle.(type) {
		case string:
			if shard.GetName() == v {
				resultShard = shard.(*ChkShard)
			}
		case int:
			if index == v {
				resultShard = shard.(*ChkShard)
			}
		}
		return nil
	})
	return resultShard
}

// FindHost finds host by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (cluster *Cluster) FindHost(needleShard interface{}, needleHost interface{}) *apiChi.Host {
	return cluster.FindShard(needleShard).FindHost(needleHost)
}

// FirstHost finds first host in the cluster
func (cluster *Cluster) FirstHost() *apiChi.Host {
	var result *apiChi.Host
	cluster.WalkHosts(func(host *apiChi.Host) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

// WalkShards walks shards
func (cluster *Cluster) WalkShards(f func(index int, shard apiChi.IShard) error) []error {
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
func (cluster *Cluster) WalkReplicas(f func(index int, replica *ChkReplica) error) []error {
	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := cluster.Layout.Replicas[replicaIndex]
		res = append(res, f(replicaIndex, replica))
	}

	return res
}

// WalkHosts walks hosts
func (cluster *Cluster) WalkHosts(f func(host *apiChi.Host) error) []error {
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
func (cluster *Cluster) WalkHostsByShards(f func(shard, replica int, host *apiChi.Host) error) []error {

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

func (cluster *Cluster) GetLayout() *ChkClusterLayout {
	return cluster.Layout
}

// WalkHostsByReplicas walks hosts by replicas
func (cluster *Cluster) WalkHostsByReplicas(f func(shard, replica int, host *apiChi.Host) error) []error {

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
func (cluster *Cluster) HostsCount() int {
	count := 0
	cluster.WalkHosts(func(host *apiChi.Host) error {
		count++
		return nil
	})
	return count
}

func (cluster *Cluster) IsZero() bool {
	return cluster == nil
}

func (cluster *Cluster) IsNonZero() bool {
	return cluster != nil
}

func (cluster *Cluster) Ensure(create func() *Cluster) *Cluster {
	if cluster == nil {
		cluster = create()
	}
	return cluster
}

// ChkClusterLayout defines layout section of .spec.configuration.clusters
type ChkClusterLayout struct {
	ShardsCount   int `json:"shardsCount,omitempty"   yaml:"shardsCount,omitempty"`
	ReplicasCount int `json:"replicasCount,omitempty" yaml:"replicasCount,omitempty"`

	// TODO refactor into map[string]ChiShard
	Shards   []*ChkShard   `json:"shards,omitempty"   yaml:"shards,omitempty"`
	Replicas []*ChkReplica `json:"replicas,omitempty" yaml:"replicas,omitempty"`

	// Internal data
	// Whether shards or replicas are explicitly specified as Shards []ChiShard or Replicas []ChiReplica
	ShardsExplicitlySpecified   bool               `json:"-" yaml:"-" testdiff:"ignore"`
	ReplicasExplicitlySpecified bool               `json:"-" yaml:"-" testdiff:"ignore"`
	HostsField                  *apiChi.HostsField `json:"-" yaml:"-" testdiff:"ignore"`
}

// NewChiClusterLayout creates new cluster layout
func NewChkClusterLayout() *ChkClusterLayout {
	return new(ChkClusterLayout)
}

func (l *ChkClusterLayout) GetReplicasCount() int {
	return l.ReplicasCount
}

func (l *ChkClusterLayout) Ensure() *ChkClusterLayout {
	if l == nil {
		l = NewChkClusterLayout()
	}
	return l
}
