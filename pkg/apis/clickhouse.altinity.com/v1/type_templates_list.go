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

// TemplatesList defines references to .spec.templates to be used
type TemplatesList struct {
	HostTemplate            string `json:"hostTemplate,omitempty"            yaml:"hostTemplate,omitempty"`
	PodTemplate             string `json:"podTemplate,omitempty"             yaml:"podTemplate,omitempty"`
	DataVolumeClaimTemplate string `json:"dataVolumeClaimTemplate,omitempty" yaml:"dataVolumeClaimTemplate,omitempty"`
	LogVolumeClaimTemplate  string `json:"logVolumeClaimTemplate,omitempty"  yaml:"logVolumeClaimTemplate,omitempty"`
	ServiceTemplate         string `json:"serviceTemplate,omitempty"         yaml:"serviceTemplate,omitempty"`
	ClusterServiceTemplate  string `json:"clusterServiceTemplate,omitempty"  yaml:"clusterServiceTemplate,omitempty"`
	ShardServiceTemplate    string `json:"shardServiceTemplate,omitempty"    yaml:"shardServiceTemplate,omitempty"`
	ReplicaServiceTemplate  string `json:"replicaServiceTemplate,omitempty"  yaml:"replicaServiceTemplate,omitempty"`

	// VolumeClaimTemplate is deprecated in favor of DataVolumeClaimTemplate and LogVolumeClaimTemplate
	// !!! DEPRECATED !!!
	VolumeClaimTemplate string `json:"volumeClaimTemplate,omitempty"     yaml:"volumeClaimTemplate,omitempty"`
}

// NewTemplatesList creates new TemplatesList object
func NewTemplatesList() *TemplatesList {
	return new(TemplatesList)
}

// HasHostTemplate checks whether host template is specified
func (tl *TemplatesList) HasHostTemplate() bool {
	if tl == nil {
		return false
	}
	return len(tl.HostTemplate) > 0
}

// GetHostTemplate gets host template
func (tl *TemplatesList) GetHostTemplate() string {
	if tl == nil {
		return ""
	}
	return tl.HostTemplate
}

// HasPodTemplate checks whether pod template is specified
func (tl *TemplatesList) HasPodTemplate() bool {
	if tl == nil {
		return false
	}
	return len(tl.PodTemplate) > 0
}

// GetPodTemplate gets pod template
func (tl *TemplatesList) GetPodTemplate() string {
	if tl == nil {
		return ""
	}
	return tl.PodTemplate
}

// HasDataVolumeClaimTemplate checks whether data volume claim template is specified
func (tl *TemplatesList) HasDataVolumeClaimTemplate() bool {
	if tl == nil {
		return false
	}
	return len(tl.DataVolumeClaimTemplate) > 0
}

// GetDataVolumeClaimTemplate gets data volume claim template
func (tl *TemplatesList) GetDataVolumeClaimTemplate() string {
	if tl == nil {
		return ""
	}
	return tl.DataVolumeClaimTemplate
}

// HasLogVolumeClaimTemplate checks whether log volume claim template is specified
func (tl *TemplatesList) HasLogVolumeClaimTemplate() bool {
	if tl == nil {
		return false
	}
	return len(tl.LogVolumeClaimTemplate) > 0
}

// GetLogVolumeClaimTemplate gets log volume claim template
func (tl *TemplatesList) GetLogVolumeClaimTemplate() string {
	if tl == nil {
		return ""
	}
	return tl.LogVolumeClaimTemplate
}

// HasServiceTemplate checks whether service template is specified
func (tl *TemplatesList) HasServiceTemplate() bool {
	if tl == nil {
		return false
	}
	return len(tl.ServiceTemplate) > 0
}

// GetServiceTemplate gets service template
func (tl *TemplatesList) GetServiceTemplate() string {
	if tl == nil {
		return ""
	}
	return tl.ServiceTemplate
}

// HasClusterServiceTemplate checks whether cluster service template is specified
func (tl *TemplatesList) HasClusterServiceTemplate() bool {
	if tl == nil {
		return false
	}
	return len(tl.ClusterServiceTemplate) > 0
}

// GetClusterServiceTemplate gets cluster service template
func (tl *TemplatesList) GetClusterServiceTemplate() string {
	if tl == nil {
		return ""
	}
	return tl.ClusterServiceTemplate
}

// HasShardServiceTemplate checks whether shard service template is specified
func (tl *TemplatesList) HasShardServiceTemplate() bool {
	if tl == nil {
		return false
	}
	return len(tl.ShardServiceTemplate) > 0
}

// GetShardServiceTemplate gets shard service template
func (tl *TemplatesList) GetShardServiceTemplate() string {
	if tl == nil {
		return ""
	}
	return tl.ShardServiceTemplate
}

// HasReplicaServiceTemplate checks whether replica service template is specified
func (tl *TemplatesList) HasReplicaServiceTemplate() bool {
	if tl == nil {
		return false
	}
	return len(tl.ReplicaServiceTemplate) > 0
}

// GetReplicaServiceTemplate gets replica service template
func (tl *TemplatesList) GetReplicaServiceTemplate() string {
	if tl == nil {
		return ""
	}
	return tl.ReplicaServiceTemplate
}

// HandleDeprecatedFields helps to deal with deprecated fields
func (tl *TemplatesList) HandleDeprecatedFields() {
	if tl == nil {
		return
	}
	if tl.DataVolumeClaimTemplate == "" {
		tl.DataVolumeClaimTemplate = tl.VolumeClaimTemplate
	}
}

// MergeFrom merges from specified object
func (tl *TemplatesList) MergeFrom(from *TemplatesList, _type MergeType) *TemplatesList {
	if from == nil {
		return tl
	}

	if tl == nil {
		tl = NewTemplatesList()
	}

	switch _type {
	case MergeTypeFillEmptyValues:
		return tl.mergeFromFillEmptyValues(from)
	case MergeTypeOverrideByNonEmptyValues:
		return tl.mergeFromOverwriteByNonEmptyValues(from)
	}

	return tl
}

// mergeFromFillEmptyValues fills empty values
func (tl *TemplatesList) mergeFromFillEmptyValues(from *TemplatesList) *TemplatesList {
	if tl.HostTemplate == "" {
		tl.HostTemplate = from.HostTemplate
	}
	if tl.PodTemplate == "" {
		tl.PodTemplate = from.PodTemplate
	}
	if tl.DataVolumeClaimTemplate == "" {
		tl.DataVolumeClaimTemplate = from.DataVolumeClaimTemplate
	}
	if tl.LogVolumeClaimTemplate == "" {
		tl.LogVolumeClaimTemplate = from.LogVolumeClaimTemplate
	}
	if tl.VolumeClaimTemplate == "" {
		tl.VolumeClaimTemplate = from.VolumeClaimTemplate
	}
	if tl.ServiceTemplate == "" {
		tl.ServiceTemplate = from.ServiceTemplate
	}
	if tl.ClusterServiceTemplate == "" {
		tl.ClusterServiceTemplate = from.ClusterServiceTemplate
	}
	if tl.ShardServiceTemplate == "" {
		tl.ShardServiceTemplate = from.ShardServiceTemplate
	}
	if tl.ReplicaServiceTemplate == "" {
		tl.ReplicaServiceTemplate = from.ReplicaServiceTemplate
	}
	return tl
}

// mergeFromOverwriteByNonEmptyValues overwrites by non-empty values
func (tl *TemplatesList) mergeFromOverwriteByNonEmptyValues(from *TemplatesList) *TemplatesList {
	if from.HostTemplate != "" {
		tl.HostTemplate = from.HostTemplate
	}
	if from.PodTemplate != "" {
		tl.PodTemplate = from.PodTemplate
	}
	if from.DataVolumeClaimTemplate != "" {
		tl.DataVolumeClaimTemplate = from.DataVolumeClaimTemplate
	}
	if from.LogVolumeClaimTemplate != "" {
		tl.LogVolumeClaimTemplate = from.LogVolumeClaimTemplate
	}
	if from.VolumeClaimTemplate != "" {
		tl.VolumeClaimTemplate = from.VolumeClaimTemplate
	}
	if from.ServiceTemplate != "" {
		tl.ServiceTemplate = from.ServiceTemplate
	}
	if from.ClusterServiceTemplate != "" {
		tl.ClusterServiceTemplate = from.ClusterServiceTemplate
	}
	if from.ShardServiceTemplate != "" {
		tl.ShardServiceTemplate = from.ShardServiceTemplate
	}
	if from.ReplicaServiceTemplate != "" {
		tl.ReplicaServiceTemplate = from.ReplicaServiceTemplate
	}
	return tl
}
