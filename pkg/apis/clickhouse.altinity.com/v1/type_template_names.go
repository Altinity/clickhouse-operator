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

// NewChiTemplateNames creates new TemplatesList object
func NewChiTemplateNames() *TemplatesList {
	return new(TemplatesList)
}

// HasHostTemplate checks whether host template is specified
func (templateNames *TemplatesList) HasHostTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.HostTemplate) > 0
}

// GetHostTemplate gets host template
func (templateNames *TemplatesList) GetHostTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.HostTemplate
}

// HasPodTemplate checks whether pod template is specified
func (templateNames *TemplatesList) HasPodTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.PodTemplate) > 0
}

// GetPodTemplate gets pod template
func (templateNames *TemplatesList) GetPodTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.PodTemplate
}

// HasDataVolumeClaimTemplate checks whether data volume claim template is specified
func (templateNames *TemplatesList) HasDataVolumeClaimTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.DataVolumeClaimTemplate) > 0
}

// GetDataVolumeClaimTemplate gets data volume claim template
func (templateNames *TemplatesList) GetDataVolumeClaimTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.DataVolumeClaimTemplate
}

// HasLogVolumeClaimTemplate checks whether log volume claim template is specified
func (templateNames *TemplatesList) HasLogVolumeClaimTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.LogVolumeClaimTemplate) > 0
}

// GetLogVolumeClaimTemplate gets log volume claim template
func (templateNames *TemplatesList) GetLogVolumeClaimTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.LogVolumeClaimTemplate
}

// HasServiceTemplate checks whether service template is specified
func (templateNames *TemplatesList) HasServiceTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.ServiceTemplate) > 0
}

// GetServiceTemplate gets service template
func (templateNames *TemplatesList) GetServiceTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.ServiceTemplate
}

// HasClusterServiceTemplate checks whether cluster service template is specified
func (templateNames *TemplatesList) HasClusterServiceTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.ClusterServiceTemplate) > 0
}

// GetClusterServiceTemplate gets cluster service template
func (templateNames *TemplatesList) GetClusterServiceTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.ClusterServiceTemplate
}

// HasShardServiceTemplate checks whether shard service template is specified
func (templateNames *TemplatesList) HasShardServiceTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.ShardServiceTemplate) > 0
}

// GetShardServiceTemplate gets shard service template
func (templateNames *TemplatesList) GetShardServiceTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.ShardServiceTemplate
}

// HasReplicaServiceTemplate checks whether replica service template is specified
func (templateNames *TemplatesList) HasReplicaServiceTemplate() bool {
	if templateNames == nil {
		return false
	}
	return len(templateNames.ReplicaServiceTemplate) > 0
}

// GetReplicaServiceTemplate gets replica service template
func (templateNames *TemplatesList) GetReplicaServiceTemplate() string {
	if templateNames == nil {
		return ""
	}
	return templateNames.ReplicaServiceTemplate
}

// HandleDeprecatedFields helps to deal with deprecated fields
func (templateNames *TemplatesList) HandleDeprecatedFields() {
	if templateNames == nil {
		return
	}
	if templateNames.DataVolumeClaimTemplate == "" {
		templateNames.DataVolumeClaimTemplate = templateNames.VolumeClaimTemplate
	}
}

// MergeFrom merges from specified object
func (templateNames *TemplatesList) MergeFrom(from *TemplatesList, _type MergeType) *TemplatesList {
	if from == nil {
		return templateNames
	}

	if templateNames == nil {
		templateNames = NewChiTemplateNames()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		return templateNames.mergeFromFillEmptyValues(from)
	case MergeTypeOverrideByNonEmptyValues:
		return templateNames.mergeFromOverwriteByNonEmptyValues(from)
	}

	return templateNames
}

// mergeFromFillEmptyValues fills empty values
func (templateNames *TemplatesList) mergeFromFillEmptyValues(from *TemplatesList) *TemplatesList {
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
	return templateNames
}

// mergeFromOverwriteByNonEmptyValues overwrites by non-empty values
func (templateNames *TemplatesList) mergeFromOverwriteByNonEmptyValues(from *TemplatesList) *TemplatesList {
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
	return templateNames
}
