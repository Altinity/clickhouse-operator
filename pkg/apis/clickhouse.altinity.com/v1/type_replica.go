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

// InheritSettingsFrom inherits settings from specified cluster
func (replica *ChiReplica) InheritSettingsFrom(cluster *ChiCluster) {
	replica.Settings = replica.Settings.MergeFrom(cluster.Settings)
}

// InheritFilesFrom inherits files from specified cluster
func (replica *ChiReplica) InheritFilesFrom(cluster *ChiCluster) {
	replica.Files = replica.Files.MergeFrom(cluster.Files)
}

// InheritTemplatesFrom inherits templates from specified cluster
func (replica *ChiReplica) InheritTemplatesFrom(cluster *ChiCluster) {
	replica.Templates = replica.Templates.MergeFrom(cluster.Templates, MergeTypeFillEmptyValues)
	replica.Templates.HandleDeprecatedFields()
}

// GetServiceTemplate gets service template
func (replica *ChiReplica) GetServiceTemplate() (*ChiServiceTemplate, bool) {
	if !replica.Templates.HasReplicaServiceTemplate() {
		return nil, false
	}
	name := replica.Templates.GetReplicaServiceTemplate()
	return replica.CHI.GetServiceTemplate(name)
}

// WalkHosts walks over hosts
func (replica *ChiReplica) WalkHosts(f func(host *ChiHost) error) []error {
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
	replica.WalkHosts(func(host *ChiHost) error {
		count++
		return nil
	})
	return count
}
