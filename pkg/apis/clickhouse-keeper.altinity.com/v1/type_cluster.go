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

// ChkCluster defines item of a clusters section of .configuration
type ChkCluster struct {
	Name      string                `json:"name,omitempty"         yaml:"name,omitempty"`
	Settings  *apiChi.Settings      `json:"settings,omitempty"          yaml:"settings,omitempty"`
	Files     *apiChi.Settings      `json:"files,omitempty"             yaml:"files,omitempty"`
	Templates *apiChi.TemplatesList `json:"templates,omitempty"         yaml:"templates,omitempty"`
	Layout    *ChkClusterLayout     `json:"layout,omitempty"       yaml:"layout,omitempty"`

	Runtime ChkClusterRuntime `json:"-" yaml:"-"`
}

type ChkClusterRuntime struct {
	Address ChkClusterAddress             `json:"-" yaml:"-"`
	CHK     *ClickHouseKeeperInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r *ChkClusterRuntime) GetAddress() apiChi.IClusterAddress {
	return &r.Address
}

func (r ChkClusterRuntime) GetRoot() apiChi.IRoot {
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

// FillShardReplicaSpecified fills whether shard or replicas are explicitly specified
func (cluster *ChkCluster) FillShardReplicaSpecified() {
	if len(cluster.Layout.Shards) > 0 {
		cluster.Layout.ShardsSpecified = true
	}
	if len(cluster.Layout.Replicas) > 0 {
		cluster.Layout.ReplicasSpecified = true
	}
}

// isShardSpecified checks whether shard is explicitly specified
func (cluster *ChkCluster) isShardSpecified() bool {
	return cluster.Layout.ShardsSpecified == true
}

// isReplicaSpecified checks whether replica is explicitly specified
func (cluster *ChkCluster) isReplicaSpecified() bool {
	return (cluster.Layout.ShardsSpecified == false) && (cluster.Layout.ReplicasSpecified == true)
}

// IsShardSpecified checks whether shard is explicitly specified
func (cluster *ChkCluster) IsShardSpecified() bool {
	if !cluster.isShardSpecified() && !cluster.isReplicaSpecified() {
		return true
	}

	return cluster.isShardSpecified()
}

// InheritFilesFrom inherits files from CHI
func (cluster *ChkCluster) InheritFilesFrom(chk *ClickHouseKeeperInstallation) {
	if chk.GetSpec().Configuration == nil {
		return
	}
	if chk.GetSpec().Configuration.Files == nil {
		return
	}

	// Propagate host section only
	cluster.Files = cluster.Files.MergeFromCB(chk.GetSpec().Configuration.Files, func(path string, _ *apiChi.Setting) bool {
		if section, err := apiChi.GetSectionFromPath(path); err == nil {
			if section.Equal(apiChi.SectionHost) {
				return true
			}
		}

		return false
	})
}

// InheritTemplatesFrom inherits templates from CHI
func (cluster *ChkCluster) InheritTemplatesFrom(chk *ClickHouseKeeperInstallation) {
	if chk.GetSpec().Defaults == nil {
		return
	}
	if chk.GetSpec().Defaults.Templates == nil {
		return
	}
	cluster.Templates = cluster.Templates.MergeFrom(chk.GetSpec().Defaults.Templates, apiChi.MergeTypeFillEmptyValues)
	cluster.Templates.HandleDeprecatedFields()
}

func (cluster *ChkCluster) GetName() string {
	return cluster.Name
}
func (cluster *ChkCluster) GetRuntime() apiChi.IClusterRuntime {
	return nil
}
func (cluster *ChkCluster) GetServiceTemplate() (*apiChi.ServiceTemplate, bool) {
	return nil, false
}
func (c *ChkCluster) GetSecret() *apiChi.ClusterSecret {
	return nil
}
func (cluster *ChkCluster) GetPDBMaxUnavailable() *types.Int32 {
	return types.NewInt32(1)
}

func (cluster *ChkCluster) WalkShards(f func(index int, shard apiChi.IShard) error) []error {
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
func (cluster *ChkCluster) WalkReplicas(f func(index int, replica *ChkReplica) error) []error {
	res := make([]error, 0)

	for replicaIndex := range cluster.Layout.Replicas {
		replica := cluster.Layout.Replicas[replicaIndex]
		res = append(res, f(replicaIndex, replica))
	}

	return res
}

func (cluster *ChkCluster) WalkHosts(func(host *apiChi.Host) error) []error {
	return nil
}

// GetShard gets shard with specified index
func (cluster *ChkCluster) GetShard(shard int) *ChkShard {
	return cluster.Layout.Shards[shard]
}

// GetOrCreateHost gets or creates host on specified coordinates
func (cluster *ChkCluster) GetOrCreateHost(shard, replica int) *apiChi.Host {
	return cluster.Layout.HostsField.GetOrCreate(shard, replica)
}

// GetReplica gets replica with specified index
func (cluster *ChkCluster) GetReplica(replica int) *ChkReplica {
	return cluster.Layout.Replicas[replica]
}

// WalkHostsByShards walks hosts by shards
func (cluster *ChkCluster) WalkHostsByShards(f func(shard, replica int, host *apiChi.Host) error) []error {

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

func (cluster *ChkCluster) GetLayout() *ChkClusterLayout {
	return cluster.Layout
}

// WalkHostsByReplicas walks hosts by replicas
func (cluster *ChkCluster) WalkHostsByReplicas(f func(shard, replica int, host *apiChi.Host) error) []error {

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
func (cluster *ChkCluster) HostsCount() int {
	count := 0
	cluster.WalkHosts(func(host *apiChi.Host) error {
		count++
		return nil
	})
	return count
}

type ChkClusterLayout struct {
	ShardsCount   int `json:"shardsCount,omitempty"   yaml:"shardsCount,omitempty"`
	ReplicasCount int `json:"replicasCount,omitempty" yaml:"replicasCount,omitempty"`

	// TODO refactor into map[string]ChiShard
	Shards   []*ChkShard   `json:"shards,omitempty"   yaml:"shards,omitempty"`
	Replicas []*ChkReplica `json:"replicas,omitempty" yaml:"replicas,omitempty"`

	// Internal data
	// Whether shards or replicas are explicitly specified as Shards []ChiShard or Replicas []ChiReplica
	ShardsSpecified   bool               `json:"-" yaml:"-" testdiff:"ignore"`
	ReplicasSpecified bool               `json:"-" yaml:"-" testdiff:"ignore"`
	HostsField        *apiChi.HostsField `json:"-" yaml:"-" testdiff:"ignore"`
}

// NewChiClusterLayout creates new cluster layout
func NewChkClusterLayout() *ChkClusterLayout {
	return new(ChkClusterLayout)
}

func (l *ChkClusterLayout) GetReplicasCount() int {
	return l.ReplicasCount
}

// ChiShard defines item of a shard section of .spec.configuration.clusters[n].shards
// TODO unify with ChiReplica based on HostsSet
type ChkShard struct {
	Name                string                `json:"name,omitempty"                yaml:"name,omitempty"`
	Weight              *int                  `json:"weight,omitempty"              yaml:"weight,omitempty"`
	InternalReplication *types.StringBool     `json:"internalReplication,omitempty" yaml:"internalReplication,omitempty"`
	Settings            *apiChi.Settings      `json:"settings,omitempty"            yaml:"settings,omitempty"`
	Files               *apiChi.Settings      `json:"files,omitempty"               yaml:"files,omitempty"`
	Templates           *apiChi.TemplatesList `json:"templates,omitempty"           yaml:"templates,omitempty"`
	ReplicasCount       int                   `json:"replicasCount,omitempty"       yaml:"replicasCount,omitempty"`
	// TODO refactor into map[string]Host
	Hosts []*apiChi.Host `json:"replicas,omitempty" yaml:"replicas,omitempty"`

	Runtime ChkShardRuntime `json:"-" yaml:"-"`

	// DefinitionType is DEPRECATED - to be removed soon
	DefinitionType string `json:"definitionType,omitempty" yaml:"definitionType,omitempty"`
}

type ChkShardRuntime struct {
	Address ChkShardAddress               `json:"-" yaml:"-"`
	CHK     *ClickHouseKeeperInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r ChkShardRuntime) GetAddress() apiChi.IShardAddress {
	return &r.Address
}

func (r *ChkShardRuntime) SetCR(cr apiChi.ICustomResource) {
	r.CHK = cr.(*ClickHouseKeeperInstallation)
}

// ChiReplica defines item of a replica section of .spec.configuration.clusters[n].replicas
// TODO unify with ChiShard based on HostsSet
type ChkReplica struct {
	Name        string                `json:"name,omitempty"        yaml:"name,omitempty"`
	Settings    *apiChi.Settings      `json:"settings,omitempty"    yaml:"settings,omitempty"`
	Files       *apiChi.Settings      `json:"files,omitempty"       yaml:"files,omitempty"`
	Templates   *apiChi.TemplatesList `json:"templates,omitempty"   yaml:"templates,omitempty"`
	ShardsCount int                   `json:"shardsCount,omitempty" yaml:"shardsCount,omitempty"`
	// TODO refactor into map[string]Host
	Hosts []*apiChi.Host `json:"shards,omitempty" yaml:"shards,omitempty"`

	Runtime ChkReplicaRuntime `json:"-" yaml:"-"`
}

type ChkReplicaRuntime struct {
	Address ChkReplicaAddress             `json:"-" yaml:"-"`
	CHK     *ClickHouseKeeperInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r ChkReplicaRuntime) GetAddress() apiChi.IReplicaAddress {
	return &r.Address
}

func (r *ChkReplicaRuntime) SetCR(cr apiChi.ICustomResource) {
	r.CHK = cr.(*ClickHouseKeeperInstallation)
}

// ChiShardAddress defines address of a shard within ClickHouseInstallation
type ChkShardAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
	ShardName    string `json:"shardName,omitempty"    yaml:"shardName,omitempty"`
	ShardIndex   int    `json:"shardIndex,omitempty"   yaml:"shardIndex,omitempty"`
}

func (a *ChkShardAddress) GetNamespace() string {
	return a.Namespace
}

func (a *ChkShardAddress) SetNamespace(namespace string) {
	a.Namespace = namespace
}

func (a *ChkShardAddress) GetCRName() string {
	return a.CHIName
}

func (a *ChkShardAddress) SetCRName(name string) {
	a.CHIName = name
}

func (a *ChkShardAddress) GetClusterName() string {
	return a.ClusterName
}

func (a *ChkShardAddress) SetClusterName(name string) {
	a.ClusterName = name
}

func (a *ChkShardAddress) GetClusterIndex() int {
	return a.ClusterIndex
}

func (a *ChkShardAddress) SetClusterIndex(index int) {
	a.ClusterIndex = index
}

func (a *ChkShardAddress) GetShardName() string {
	return a.ShardName
}

func (a *ChkShardAddress) SetShardName(name string) {
	a.ShardName = name
}

func (a *ChkShardAddress) GetShardIndex() int {
	return a.ShardIndex
}

func (a *ChkShardAddress) SetShardIndex(index int) {
	a.ShardIndex = index
}

// ChiReplicaAddress defines address of a replica within ClickHouseInstallation
type ChkReplicaAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
	ReplicaName  string `json:"replicaName,omitempty"  yaml:"replicaName,omitempty"`
	ReplicaIndex int    `json:"replicaIndex,omitempty" yaml:"replicaIndex,omitempty"`
}

func (a *ChkReplicaAddress) GetNamespace() string {
	return a.Namespace
}

func (a *ChkReplicaAddress) SetNamespace(namespace string) {
	a.Namespace = namespace
}

func (a *ChkReplicaAddress) GetCRName() string {
	return a.CHIName
}

func (a *ChkReplicaAddress) SetCRName(name string) {
	a.CHIName = name
}

func (a *ChkReplicaAddress) GetClusterName() string {
	return a.ClusterName
}

func (a *ChkReplicaAddress) SetClusterName(name string) {
	a.ClusterName = name
}

func (a *ChkReplicaAddress) GetClusterIndex() int {
	return a.ClusterIndex
}

func (a *ChkReplicaAddress) SetClusterIndex(index int) {
	a.ClusterIndex = index
}

func (a *ChkReplicaAddress) GetReplicaName() string {
	return a.ReplicaName
}

func (a *ChkReplicaAddress) SetReplicaName(name string) {
	a.ReplicaName = name
}

func (a *ChkReplicaAddress) GetReplicaIndex() int {
	return a.ReplicaIndex
}

func (a *ChkReplicaAddress) SetReplicaIndex(index int) {
	a.ReplicaIndex = index
}
