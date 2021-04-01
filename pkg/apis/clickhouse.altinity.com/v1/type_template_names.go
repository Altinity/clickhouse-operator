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

func NewChiTemplateNames() *ChiTemplateNames {
	return new(ChiTemplateNames)
}

func (templateNames *ChiTemplateNames) HasHostTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.HostTemplate) > 0
}

func (templateNames *ChiTemplateNames) GetHostTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.HostTemplate
}

func (templateNames *ChiTemplateNames) HasPodTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.PodTemplate) > 0
}

func (templateNames *ChiTemplateNames) GetPodTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.PodTemplate
}

func (templateNames *ChiTemplateNames) HasDataVolumeClaimTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.DataVolumeClaimTemplate) > 0
}

func (templateNames *ChiTemplateNames) GetDataVolumeClaimTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.DataVolumeClaimTemplate
}

func (templateNames *ChiTemplateNames) HasLogVolumeClaimTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.LogVolumeClaimTemplate) > 0
}

func (templateNames *ChiTemplateNames) GetLogVolumeClaimTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.LogVolumeClaimTemplate
}

func (templateNames *ChiTemplateNames) HasServiceTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.ServiceTemplate) > 0
}

func (templateNames *ChiTemplateNames) GetServiceTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.ServiceTemplate
}

func (templateNames *ChiTemplateNames) HasClusterServiceTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.ClusterServiceTemplate) > 0
}

func (templateNames *ChiTemplateNames) GetClusterServiceTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.ClusterServiceTemplate
}

func (templateNames *ChiTemplateNames) HasShardServiceTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.ShardServiceTemplate) > 0
}

func (templateNames *ChiTemplateNames) GetShardServiceTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.ShardServiceTemplate
}

func (templateNames *ChiTemplateNames) HasReplicaServiceTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.ReplicaServiceTemplate) > 0
}

func (templateNames *ChiTemplateNames) GetReplicaServiceTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.ReplicaServiceTemplate
}

func (templateNames *ChiTemplateNames) HandleDeprecatedFields() {
	if templateNames == nil {
		return
	}
	if templateNames.DataVolumeClaimTemplate == "" {
		templateNames.DataVolumeClaimTemplate = templateNames.VolumeClaimTemplate
	}
}

func (templateNames *ChiTemplateNames) MergeFrom(from *ChiTemplateNames, _type MergeType) *ChiTemplateNames {
	if from == nil {
		return templateNames
	}
	if templateNames == nil {
		templateNames = NewChiTemplateNames()
	}
	switch _type {
	case MergeTypeFillEmptyValues:
		if templateNames.HostTemplate == "" {
			templateNames.HostTemplate = from.HostTemplate
		}
		if templateNames.PodTemplate == "" {
			templateNames.PodTemplate = from.PodTemplate
		}
		if templateNames.DataVolumeClaimTemplate == "" {
			templateNames.DataVolumeClaimTemplate = from.DataVolumeClaimTemplate
		}
		if templateNames.LogVolumeClaimTemplate == "" {
			templateNames.LogVolumeClaimTemplate = from.LogVolumeClaimTemplate
		}
		if templateNames.VolumeClaimTemplate == "" {
			templateNames.VolumeClaimTemplate = from.VolumeClaimTemplate
		}
		if templateNames.ServiceTemplate == "" {
			templateNames.ServiceTemplate = from.ServiceTemplate
		}
		if templateNames.ClusterServiceTemplate == "" {
			templateNames.ClusterServiceTemplate = from.ClusterServiceTemplate
		}
		if templateNames.ShardServiceTemplate == "" {
			templateNames.ShardServiceTemplate = from.ShardServiceTemplate
		}
		if templateNames.ReplicaServiceTemplate == "" {
			templateNames.ReplicaServiceTemplate = from.ReplicaServiceTemplate
		}
	case MergeTypeOverrideByNonEmptyValues:
		// Override by non-empty values only
		if from.HostTemplate != "" {
			templateNames.HostTemplate = from.HostTemplate
		}
		if from.PodTemplate != "" {
			templateNames.PodTemplate = from.PodTemplate
		}
		if from.DataVolumeClaimTemplate != "" {
			templateNames.DataVolumeClaimTemplate = from.DataVolumeClaimTemplate
		}
		if from.LogVolumeClaimTemplate != "" {
			templateNames.LogVolumeClaimTemplate = from.LogVolumeClaimTemplate
		}
		if from.VolumeClaimTemplate != "" {
			templateNames.VolumeClaimTemplate = from.VolumeClaimTemplate
		}
		if from.ServiceTemplate != "" {
			templateNames.ServiceTemplate = from.ServiceTemplate
		}
		if from.ClusterServiceTemplate != "" {
			templateNames.ClusterServiceTemplate = from.ClusterServiceTemplate
		}
		if from.ShardServiceTemplate != "" {
			templateNames.ShardServiceTemplate = from.ShardServiceTemplate
		}
		if from.ReplicaServiceTemplate != "" {
			templateNames.ReplicaServiceTemplate = from.ReplicaServiceTemplate
		}
	}

	return templateNames
}
