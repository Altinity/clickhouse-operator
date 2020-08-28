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

func (replica *ChiReplica) InheritSettingsFrom(cluster *ChiCluster) {
	(&replica.Settings).MergeFrom(cluster.Settings)
}

func (replica *ChiReplica) InheritFilesFrom(cluster *ChiCluster) {
	(&replica.Files).MergeFrom(cluster.Files)
}

func (replica *ChiReplica) InheritTemplatesFrom(cluster *ChiCluster) {
	(&replica.Templates).MergeFrom(&cluster.Templates, MergeTypeFillEmptyValues)
	(&replica.Templates).HandleDeprecatedFields()
}

func (replica *ChiReplica) GetServiceTemplate() (*ChiServiceTemplate, bool) {
	name := replica.Templates.ReplicaServiceTemplate
	template, ok := replica.CHI.GetServiceTemplate(name)
	return template, ok
}

func (replica *ChiReplica) WalkHosts(
	f func(host *ChiHost) error,
) []error {
	res := make([]error, 0)

	for shardIndex := range replica.Hosts {
		host := replica.Hosts[shardIndex]
		res = append(res, f(host))
	}

	return res
}

func (replica *ChiReplica) HostsCount() int {
	count := 0
	replica.WalkHosts(func(host *ChiHost) error {
		count++
		return nil
	})
	return count
}
