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

func (replica *ChkReplica) GetName() string {
	return replica.Name
}

// InheritSettingsFrom inherits settings from specified cluster
func (replica *ChkReplica) InheritSettingsFrom(cluster *ChkCluster) {
	// replica.Settings = replica.Settings.MergeFrom(cluster.Settings)
}

// InheritFilesFrom inherits files from specified cluster
func (replica *ChkReplica) InheritFilesFrom(cluster *ChkCluster) {
	// replica.Files = replica.Files.MergeFrom(cluster.Files)
}

// InheritTemplatesFrom inherits templates from specified cluster
func (replica *ChkReplica) InheritTemplatesFrom(cluster *ChkCluster) {
	// replica.Templates = replica.Templates.MergeFrom(cluster.Templates, MergeTypeFillEmptyValues)
	// replica.Templates.HandleDeprecatedFields()
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
		return nil
	}
	return &replica.Runtime
}
