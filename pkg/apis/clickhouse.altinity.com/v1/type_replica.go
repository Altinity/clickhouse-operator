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

// ChiReplica defines item of a replica section of .spec.configuration.clusters[n].replicas
// TODO unify with ChiShard based on HostsSet
type ChiReplica struct {
	Name        string         `json:"name,omitempty"        yaml:"name,omitempty"`
	Settings    *Settings      `json:"settings,omitempty"    yaml:"settings,omitempty"`
	Files       *Settings      `json:"files,omitempty"       yaml:"files,omitempty"`
	Templates   *TemplatesList `json:"templates,omitempty"   yaml:"templates,omitempty"`
	ShardsCount int            `json:"shardsCount,omitempty" yaml:"shardsCount,omitempty"`
	// TODO refactor into map[string]Host
	Hosts []*Host `json:"shards,omitempty" yaml:"shards,omitempty"`

	Runtime ChiReplicaRuntime `json:"-" yaml:"-"`
}

type ChiReplicaRuntime struct {
	Address ChiReplicaAddress       `json:"-" yaml:"-"`
	CHI     *ClickHouseInstallation `json:"-" yaml:"-" testdiff:"ignore"`
}

func (r *ChiReplicaRuntime) GetAddress() IReplicaAddress {
	return &r.Address
}

func (r *ChiReplicaRuntime) SetCR(cr ICustomResource) {
	r.CHI = cr.(*ClickHouseInstallation)
}

func (replica *ChiReplica) GetName() string {
	return replica.Name
}

// InheritSettingsFrom inherits settings from specified cluster
func (replica *ChiReplica) InheritSettingsFrom(cluster *Cluster) {
	replica.Settings = replica.Settings.MergeFrom(cluster.Settings)
}

// InheritFilesFrom inherits files from specified cluster
func (replica *ChiReplica) InheritFilesFrom(cluster *Cluster) {
	replica.Files = replica.Files.MergeFrom(cluster.Files)
}

// InheritTemplatesFrom inherits templates from specified cluster
func (replica *ChiReplica) InheritTemplatesFrom(cluster *Cluster) {
	replica.Templates = replica.Templates.MergeFrom(cluster.Templates, MergeTypeFillEmptyValues)
	replica.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate gets service template
func (replica *ChiReplica) GetServiceTemplate() (*ServiceTemplate, bool) {
	if !replica.Templates.HasReplicaServiceTemplate() {
		return nil, false
	}
	name := replica.Templates.GetReplicaServiceTemplate()
	return replica.Runtime.CHI.GetServiceTemplate(name)
}

// HasShardsCount checks whether replica has shards count specified
func (replica *ChiReplica) HasShardsCount() bool {
	if replica == nil {
		return false
	}

	return replica.ShardsCount > 0
}

// WalkHosts walks over hosts
func (replica *ChiReplica) WalkHosts(f func(host *Host) error) []error {
	res := make([]error, 0)

	for shardIndex := range replica.Hosts {
		host := replica.Hosts[shardIndex]
		res = append(res, f(host))
	}

	return res
}

// HostsCount returns number of hosts
func (replica *ChiReplica) HostsCount() int {
	count := 0
	replica.WalkHosts(func(host *Host) error {
		count++
		return nil
	})
	return count
}

func (replica *ChiReplica) HasSettings() bool {
	return replica.GetSettings() != nil
}

func (replica *ChiReplica) GetSettings() *Settings {
	if replica == nil {
		return nil
	}
	return replica.Settings
}

func (replica *ChiReplica) HasFiles() bool {
	return replica.GetFiles() != nil
}

func (replica *ChiReplica) GetFiles() *Settings {
	if replica == nil {
		return nil
	}
	return replica.Files
}

func (replica *ChiReplica) HasTemplates() bool {
	return replica.GetTemplates() != nil
}

func (replica *ChiReplica) GetTemplates() *TemplatesList {
	if replica == nil {
		return nil
	}
	return replica.Templates
}

func (replica *ChiReplica) GetRuntime() IReplicaRuntime {
	if replica == nil {
		return (*ChiReplicaRuntime)(nil)
	}
	return &replica.Runtime
}

// ChiReplicaAddress defines address of a replica within ClickHouseInstallation
type ChiReplicaAddress struct {
	Namespace    string `json:"namespace,omitempty"    yaml:"namespace,omitempty"`
	CHIName      string `json:"chiName,omitempty"      yaml:"chiName,omitempty"`
	ClusterName  string `json:"clusterName,omitempty"  yaml:"clusterName,omitempty"`
	ClusterIndex int    `json:"clusterIndex,omitempty" yaml:"clusterIndex,omitempty"`
	ReplicaName  string `json:"replicaName,omitempty"  yaml:"replicaName,omitempty"`
	ReplicaIndex int    `json:"replicaIndex,omitempty" yaml:"replicaIndex,omitempty"`
}

func (a *ChiReplicaAddress) GetNamespace() string {
	return a.Namespace
}

func (a *ChiReplicaAddress) SetNamespace(namespace string) {
	a.Namespace = namespace
}

func (a *ChiReplicaAddress) GetCRName() string {
	return a.CHIName
}

func (a *ChiReplicaAddress) SetCRName(name string) {
	a.CHIName = name
}

func (a *ChiReplicaAddress) GetClusterName() string {
	return a.ClusterName
}

func (a *ChiReplicaAddress) SetClusterName(name string) {
	a.ClusterName = name
}

func (a *ChiReplicaAddress) GetClusterIndex() int {
	return a.ClusterIndex
}

func (a *ChiReplicaAddress) SetClusterIndex(index int) {
	a.ClusterIndex = index
}

func (a *ChiReplicaAddress) GetReplicaName() string {
	return a.ReplicaName
}

func (a *ChiReplicaAddress) SetReplicaName(name string) {
	a.ReplicaName = name
}

func (a *ChiReplicaAddress) GetReplicaIndex() int {
	return a.ReplicaIndex
}

func (a *ChiReplicaAddress) SetReplicaIndex(index int) {
	a.ReplicaIndex = index
}
