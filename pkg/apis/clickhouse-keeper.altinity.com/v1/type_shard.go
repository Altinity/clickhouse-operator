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

func (shard *ChkShard) GetName() string {
	return shard.Name
}

func (shard *ChkShard) GetInternalReplication() *types.StringBool {
	return shard.InternalReplication
}

// InheritSettingsFrom inherits settings from specified cluster
func (shard *ChkShard) InheritSettingsFrom(cluster *ChkCluster) {
	//shard.Settings = shard.Settings.MergeFrom(cluster.Settings)
}

// InheritFilesFrom inherits files from specified cluster
func (shard *ChkShard) InheritFilesFrom(cluster *ChkCluster) {
	//shard.Files = shard.Files.MergeFrom(cluster.Files)
}

// InheritTemplatesFrom inherits templates from specified cluster
func (shard *ChkShard) InheritTemplatesFrom(cluster *ChkCluster) {
	//shard.Templates = shard.Templates.MergeFrom(cluster.Templates, MergeTypeFillEmptyValues)
	//shard.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate gets service template
func (shard *ChkShard) GetServiceTemplate() (*apiChi.ServiceTemplate, bool) {
	if !shard.Templates.HasShardServiceTemplate() {
		return nil, false
	}
	name := shard.Templates.GetShardServiceTemplate()
	return shard.Runtime.CHK.GetServiceTemplate(name)
}

// HasReplicasCount checks whether shard has replicas count specified
func (shard *ChkShard) HasReplicasCount() bool {
	if shard == nil {
		return false
	}

	return shard.ReplicasCount > 0
}

// WalkHosts runs specified function on each host
func (shard *ChkShard) WalkHosts(f func(host *apiChi.Host) error) []error {
	if shard == nil {
		return nil
	}

	res := make([]error, 0)

	for replicaIndex := range shard.Hosts {
		host := shard.Hosts[replicaIndex]
		res = append(res, f(host))
	}

	return res
}

// FindHost finds host by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (shard *ChkShard) FindHost(needle interface{}) (res *apiChi.Host) {
	shard.WalkHosts(func(host *apiChi.Host) error {
		switch v := needle.(type) {
		case string:
			if host.Runtime.Address.HostName == v {
				res = host
			}
		case int:
			if host.Runtime.Address.ShardScopeIndex == v {
				res = host
			}
		}
		return nil
	})
	return
}

// FirstHost finds first host in the shard
func (shard *ChkShard) FirstHost() *apiChi.Host {
	var result *apiChi.Host
	shard.WalkHosts(func(host *apiChi.Host) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

// HostsCount returns count of hosts in the shard
func (shard *ChkShard) HostsCount() int {
	count := 0
	shard.WalkHosts(func(host *apiChi.Host) error {
		count++
		return nil
	})
	return count
}

// GetCHI gets CHI of the shard
func (shard *ChkShard) GetCHK() *ClickHouseKeeperInstallation {
	return shard.Runtime.CHK
}

// GetCluster gets cluster of the shard
func (shard *ChkShard) GetCluster() *ChkCluster {
	return shard.Runtime.CHK.GetSpecT().Configuration.Clusters[shard.Runtime.Address.ClusterIndex]
}

// HasWeight checks whether shard has applicable weight value specified
func (shard *ChkShard) HasWeight() bool {
	if shard == nil {
		return false
	}
	if shard.Weight == nil {
		return false
	}
	return *shard.Weight >= 0
}

// GetWeight gets weight
func (shard *ChkShard) GetWeight() int {
	if shard.HasWeight() {
		return *shard.Weight
	}
	return 0
}

func (shard *ChkShard) GetRuntime() apiChi.IShardRuntime {
	if shard == nil {
		return (*ChkShardRuntime)(nil)
	}
	return &shard.Runtime
}

func (shard *ChkShard) HasSettings() bool {
	return shard.GetSettings() != nil
}

func (shard *ChkShard) GetSettings() *apiChi.Settings {
	if shard == nil {
		return nil
	}
	return shard.Settings
}

func (shard *ChkShard) HasFiles() bool {
	return shard.GetFiles() != nil
}

func (shard *ChkShard) GetFiles() *apiChi.Settings {
	if shard == nil {
		return nil
	}
	return shard.Files
}

func (shard *ChkShard) HasTemplates() bool {
	return shard.GetTemplates() != nil
}

func (shard *ChkShard) GetTemplates() *apiChi.TemplatesList {
	if shard == nil {
		return nil
	}
	return shard.Templates
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
