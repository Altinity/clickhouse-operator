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

// ChiShard defines item of a shard section of .spec.configuration.clusters[n].shards
// TODO unify with ChiReplica based on HostsSet
type ChiShard struct {
	Name                string            `json:"name,omitempty"                yaml:"name,omitempty"`
	Weight              *int              `json:"weight,omitempty"              yaml:"weight,omitempty"`
	InternalReplication *types.StringBool `json:"internalReplication,omitempty" yaml:"internalReplication,omitempty"`
	Settings            *Settings         `json:"settings,omitempty"            yaml:"settings,omitempty"`
	Files               *Settings         `json:"files,omitempty"               yaml:"files,omitempty"`
	Templates           *TemplatesList    `json:"templates,omitempty"           yaml:"templates,omitempty"`
	ReplicasCount       int               `json:"replicasCount,omitempty"       yaml:"replicasCount,omitempty"`
	// TODO refactor into map[string]Host
	Hosts []*Host `json:"replicas,omitempty" yaml:"replicas,omitempty"`

	Runtime ChiShardRuntime `json:"-" yaml:"-"`

	// DefinitionType is DEPRECATED - to be removed soon
	DefinitionType string `json:"definitionType,omitempty" yaml:"definitionType,omitempty"`
}

type ChiShardRuntime struct {
	Address ChiShardAddress         `json:"-" yaml:"-"`
	CHI     *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r *ChiShardRuntime) GetAddress() IShardAddress {
	return &r.Address
}

func (r *ChiShardRuntime) GetCR() ICustomResource {
	return r.CHI
}

func (r *ChiShardRuntime) SetCR(cr ICustomResource) {
	r.CHI = cr.(*ClickHouseInstallation)
}

func (shard *ChiShard) GetName() string {
	return shard.Name
}

func (shard *ChiShard) GetInternalReplication() *types.StringBool {
	return shard.InternalReplication
}

// InheritSettingsFrom inherits settings from specified cluster
func (shard *ChiShard) InheritSettingsFrom(cluster *ChiCluster) {
	shard.Settings = shard.Settings.MergeFrom(cluster.Settings)
}

// InheritFilesFrom inherits files from specified cluster
func (shard *ChiShard) InheritFilesFrom(cluster *ChiCluster) {
	shard.Files = shard.Files.MergeFrom(cluster.Files)
}

// InheritTemplatesFrom inherits templates from specified cluster
func (shard *ChiShard) InheritTemplatesFrom(cluster *ChiCluster) {
	shard.Templates = shard.Templates.MergeFrom(cluster.Templates, MergeTypeFillEmptyValues)
	shard.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate gets service template
func (shard *ChiShard) GetServiceTemplate() (*ServiceTemplate, bool) {
	if !shard.Templates.HasShardServiceTemplate() {
		return nil, false
	}
	name := shard.Templates.GetShardServiceTemplate()
	return shard.Runtime.CHI.GetServiceTemplate(name)
}

// HasReplicasCount checks whether shard has replicas count specified
func (shard *ChiShard) HasReplicasCount() bool {
	if shard == nil {
		return false
	}

	return shard.ReplicasCount > 0
}

// WalkHosts runs specified function on each host
func (shard *ChiShard) WalkHosts(f func(host *Host) error) []error {
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

// WalkHosts runs specified function on each host
func (shard *ChiShard) WalkHostsAbortOnError(f func(host *Host) error) error {
	if shard == nil {
		return nil
	}

	for replicaIndex := range shard.Hosts {
		host := shard.Hosts[replicaIndex]
		if err := f(host); err != nil {
			return err
		}
	}

	return nil
}

// FindHost finds host by name or index.
// Expectations: name is expected to be a string, index is expected to be an int.
func (shard *ChiShard) FindHost(needle interface{}) (res *Host) {
	shard.WalkHosts(func(host *Host) error {
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
func (shard *ChiShard) FirstHost() *Host {
	var result *Host
	shard.WalkHosts(func(host *Host) error {
		if result == nil {
			result = host
		}
		return nil
	})
	return result
}

// HostsCount returns count of hosts in the shard
func (shard *ChiShard) HostsCount() int {
	count := 0
	shard.WalkHosts(func(host *Host) error {
		count++
		return nil
	})
	return count
}

// GetCHI gets Custom Resource of the shard
func (shard *ChiShard) GetCHI() *ClickHouseInstallation {
	return shard.Runtime.CHI
}

// GetCluster gets cluster of the shard
func (shard *ChiShard) GetCluster() *ChiCluster {
	return shard.Runtime.CHI.GetSpecT().Configuration.Clusters[shard.Runtime.Address.ClusterIndex]
}

// HasWeight checks whether shard has applicable weight value specified
func (shard *ChiShard) HasWeight() bool {
	if shard == nil {
		return false
	}
	if shard.Weight == nil {
		return false
	}
	return *shard.Weight >= 0
}

// GetWeight gets weight
func (shard *ChiShard) GetWeight() int {
	if shard.HasWeight() {
		return *shard.Weight
	}
	return 0
}

func (shard *ChiShard) GetRuntime() IShardRuntime {
	if shard == nil {
		return (*ChiShardRuntime)(nil)
	}
	return &shard.Runtime
}

func (shard *ChiShard) HasSettings() bool {
	return shard.GetSettings() != nil
}

func (shard *ChiShard) GetSettings() *Settings {
	if shard == nil {
		return nil
	}
	return shard.Settings
}

func (shard *ChiShard) HasFiles() bool {
	return shard.GetFiles() != nil
}

func (shard *ChiShard) GetFiles() *Settings {
	if shard == nil {
		return nil
	}
	return shard.Files
}

func (shard *ChiShard) HasTemplates() bool {
	return shard.GetTemplates() != nil
}

func (shard *ChiShard) GetTemplates() *TemplatesList {
	if shard == nil {
		return nil
	}
	return shard.Templates
}

// ChiShardAddress defines address of a shard within ClickHouseInstallation
type ChiShardAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
	ShardName    string `json:"shardName,omitempty"    yaml:"shardName,omitempty"`
	ShardIndex   int    `json:"shardIndex,omitempty"   yaml:"shardIndex,omitempty"`
}

func (a *ChiShardAddress) GetNamespace() string {
	return a.Namespace
}

func (a *ChiShardAddress) SetNamespace(namespace string) {
	a.Namespace = namespace
}

func (a *ChiShardAddress) GetCRName() string {
	return a.CHIName
}

func (a *ChiShardAddress) SetCRName(name string) {
	a.CHIName = name
}

func (a *ChiShardAddress) GetClusterName() string {
	return a.ClusterName
}

func (a *ChiShardAddress) SetClusterName(name string) {
	a.ClusterName = name
}

func (a *ChiShardAddress) GetClusterIndex() int {
	return a.ClusterIndex
}

func (a *ChiShardAddress) SetClusterIndex(index int) {
	a.ClusterIndex = index
}

func (a *ChiShardAddress) GetShardName() string {
	return a.ShardName
}

func (a *ChiShardAddress) SetShardName(name string) {
	a.ShardName = name
}

func (a *ChiShardAddress) GetShardIndex() int {
	return a.ShardIndex
}

func (a *ChiShardAddress) SetShardIndex(index int) {
	a.ShardIndex = index
}
