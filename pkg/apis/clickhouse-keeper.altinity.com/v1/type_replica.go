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

import apiChi "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"

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

func (replica *ChkReplica) GetName() string {
	return replica.Name
}

// InheritSettingsFrom inherits settings from specified cluster
func (replica *ChkReplica) InheritSettingsFrom(cluster *Cluster) {
	replica.Settings = replica.Settings.MergeFrom(cluster.Settings)
}

// InheritFilesFrom inherits files from specified cluster
func (replica *ChkReplica) InheritFilesFrom(cluster *Cluster) {
	replica.Files = replica.Files.MergeFrom(cluster.Files)
}

// InheritTemplatesFrom inherits templates from specified cluster
func (replica *ChkReplica) InheritTemplatesFrom(cluster *Cluster) {
	replica.Templates = replica.Templates.MergeFrom(cluster.Templates, apiChi.MergeTypeFillEmptyValues)
	replica.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate gets service template
func (replica *ChkReplica) GetServiceTemplate() (*apiChi.ServiceTemplate, bool) {
	if !replica.Templates.HasReplicaServiceTemplate() {
		return nil, false
	}
	name := replica.Templates.GetReplicaServiceTemplate()
	return replica.Runtime.CHK.GetServiceTemplate(name)
}

// HasShardsCount checks whether replica has shards count specified
func (replica *ChkReplica) HasShardsCount() bool {
	if replica == nil {
		return false
	}

	return replica.ShardsCount > 0
}

// WalkHosts walks over hosts
func (replica *ChkReplica) WalkHosts(f func(host *apiChi.Host) error) []error {
	res := make([]error, 0)

	for shardIndex := range replica.Hosts {
		host := replica.Hosts[shardIndex]
		res = append(res, f(host))
	}

	return res
}

// HostsCount returns number of hosts
func (replica *ChkReplica) HostsCount() int {
	count := 0
	replica.WalkHosts(func(host *apiChi.Host) error {
		count++
		return nil
	})
	return count
}

func (replica *ChkReplica) HasSettings() bool {
	return replica.GetSettings() != nil
}

func (replica *ChkReplica) GetSettings() *apiChi.Settings {
	if replica == nil {
		return nil
	}
	return replica.Settings
}

func (replica *ChkReplica) HasFiles() bool {
	return replica.GetFiles() != nil
}

func (replica *ChkReplica) GetFiles() *apiChi.Settings {
	if replica == nil {
		return nil
	}
	return replica.Files
}

func (replica *ChkReplica) HasTemplates() bool {
	return replica.GetTemplates() != nil
}

func (replica *ChkReplica) GetTemplates() *apiChi.TemplatesList {
	if replica == nil {
		return nil
	}
	return replica.Templates
}

func (replica *ChkReplica) GetRuntime() apiChi.IReplicaRuntime {
	if replica == nil {
		return (*ChkReplicaRuntime)(nil)
	}
	return &replica.Runtime
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
