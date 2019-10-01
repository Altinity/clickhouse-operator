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

import "github.com/altinity/clickhouse-operator/pkg/util"

func (host *ChiHost) InheritTemplates(shard *ChiShard) {
	(&host.Templates).MergeFrom(&shard.Templates)
	(&host.Templates).HandleDeprecatedFields()
}

func (host *ChiHost) GetPodTemplate() (*ChiPodTemplate, bool) {
	name := host.Templates.PodTemplate
	template, ok := host.Chi.GetPodTemplate(name)
	return template, ok
}

func (host *ChiHost) GetServiceTemplate() (*ChiServiceTemplate, bool) {
	name := host.Templates.ReplicaServiceTemplate
	template, ok := host.Chi.GetServiceTemplate(name)
	return template, ok
}

func (host *ChiHost) GetReplicasNum() int32 {
	if util.IsStringBoolTrue(host.Chi.Spec.Stop) {
		return 0
	} else {
		return 1
	}
}
