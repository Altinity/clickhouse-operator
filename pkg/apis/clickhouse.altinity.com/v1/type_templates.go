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
	log "github.com/golang/glog"

	core "k8s.io/api/core/v1"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Templates defines templates section of .spec
type Templates struct {
	// Templates
	HostTemplates        []HostTemplate        `json:"hostTemplates,omitempty"        yaml:"hostTemplates,omitempty"`
	PodTemplates         []PodTemplate         `json:"podTemplates,omitempty"         yaml:"podTemplates,omitempty"`
	VolumeClaimTemplates []VolumeClaimTemplate `json:"volumeClaimTemplates,omitempty" yaml:"volumeClaimTemplates,omitempty"`
	ServiceTemplates     []ServiceTemplate     `json:"serviceTemplates,omitempty"     yaml:"serviceTemplates,omitempty"`

	// Index maps template name to template itself
	HostTemplatesIndex        *HostTemplatesIndex        `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	PodTemplatesIndex         *PodTemplatesIndex         `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	VolumeClaimTemplatesIndex *VolumeClaimTemplatesIndex `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
	ServiceTemplatesIndex     *ServiceTemplatesIndex     `json:",omitempty" yaml:",omitempty" testdiff:"ignore"`
}

// HostTemplate defines full Host Template
type HostTemplate struct {
	Name             string             `json:"name,omitempty"             yaml:"name,omitempty"`
	PortDistribution []PortDistribution `json:"portDistribution,omitempty" yaml:"portDistribution,omitempty"`
	Spec             Host               `json:"spec,omitempty"             yaml:"spec,omitempty"`
}

// PortDistribution defines port distribution
type PortDistribution struct {
	Type string `json:"type,omitempty"   yaml:"type,omitempty"`
}

// PodTemplate defines full Pod Template, directly used by StatefulSet
type PodTemplate struct {
	Name            string            `json:"name"                      yaml:"name"`
	GenerateName    string            `json:"generateName,omitempty"    yaml:"generateName,omitempty"`
	Zone            PodTemplateZone   `json:"zone,omitempty"            yaml:"zone,omitempty"`
	PodDistribution []PodDistribution `json:"podDistribution,omitempty" yaml:"podDistribution,omitempty"`
	ObjectMeta      meta.ObjectMeta   `json:"metadata,omitempty"        yaml:"metadata,omitempty"`
	Spec            core.PodSpec      `json:"spec,omitempty"            yaml:"spec,omitempty"`
}

func (s *PodTemplate) HasGenerateName() bool {
	if s == nil {
		return false
	}
	return s.GenerateName != ""
}

func (s *PodTemplate) GetGenerateName() string {
	if s == nil {
		return ""
	}
	return s.GenerateName
}

// GetName returns template name
func (s *PodTemplate) GetName() string {
	if s == nil {
		return ""
	}
	return s.Name
}

// MergeFrom merges pod template by k8s strategic-merge semantics, direction decided by merge type
func (s *PodTemplate) MergeFrom(from *PodTemplate, _type MergeType) *PodTemplate {
	if from == nil {
		return s
	}
	if s == nil {
		s = new(PodTemplate)
	}
	mergeTemplate(s, from, _type)
	return s
}

// PodTemplateZone defines pod template zone
type PodTemplateZone struct {
	Key    string   `json:"key,omitempty"    yaml:"key,omitempty"`
	Values []string `json:"values,omitempty" yaml:"values,omitempty"`
}

// PodDistribution defines pod distribution
type PodDistribution struct {
	Type        string `json:"type,omitempty"        yaml:"type,omitempty"`
	Scope       string `json:"scope,omitempty"       yaml:"scope,omitempty"`
	Number      int    `json:"number,omitempty"      yaml:"number,omitempty"`
	TopologyKey string `json:"topologyKey,omitempty" yaml:"topologyKey,omitempty"`
}

// ServiceTemplate defines CHI service template
type ServiceTemplate struct {
	Name         string           `json:"name"                   yaml:"name"`
	GenerateName string           `json:"generateName,omitempty" yaml:"generateName,omitempty"`
	ObjectMeta   meta.ObjectMeta  `json:"metadata,omitempty"     yaml:"metadata,omitempty"`
	Spec         core.ServiceSpec `json:"spec,omitempty"         yaml:"spec,omitempty"`
}

func (s *ServiceTemplate) HasGenerateName() bool {
	if s == nil {
		return false
	}
	return s.GenerateName != ""
}

func (s *ServiceTemplate) GetGenerateName() string {
	if s == nil {
		return ""
	}
	return s.GenerateName
}

// GetName returns template name
func (s *HostTemplate) GetName() string {
	if s == nil {
		return ""
	}
	return s.Name
}

// MergeFrom merges host template by k8s strategic-merge semantics, direction decided by merge type.
// Host runtime is excluded from JSON, which the strategic merge travels through, so it is carried
// across by hand. Settings and files are merged on their own: they marshal to maps under
// user-defined keys, for which the strategic merge has no schema, and a vector under the same key
// on both sides makes it fail outright.
func (s *HostTemplate) MergeFrom(from *HostTemplate, _type MergeType) *HostTemplate {
	if from == nil {
		return s
	}
	if s == nil {
		s = new(HostTemplate)
	}
	runtime := s.Spec.Runtime
	settings := s.Spec.Settings.MergeFrom(from.Spec.Settings, _type)
	files := s.Spec.Files.MergeFrom(from.Spec.Files, _type)

	s.Spec.Settings, s.Spec.Files = nil, nil
	stripped := from.DeepCopy()
	stripped.Spec.Settings, stripped.Spec.Files = nil, nil
	mergeTemplate(s, stripped, _type)

	s.Spec.Runtime = runtime
	s.Spec.Settings, s.Spec.Files = settings, files
	return s
}

// GetName returns template name
func (s *ServiceTemplate) GetName() string {
	if s == nil {
		return ""
	}
	return s.Name
}

// MergeFrom merges service template by k8s strategic-merge semantics, direction decided by merge type
func (s *ServiceTemplate) MergeFrom(from *ServiceTemplate, _type MergeType) *ServiceTemplate {
	if from == nil {
		return s
	}
	if s == nil {
		s = new(ServiceTemplate)
	}
	mergeTemplate(s, from, _type)
	return s
}

// mergeTemplate merges one template. Should the merge itself fail, the winning side is taken as a whole,
// so that the documented precedence holds even then.
func mergeTemplate[T any, PT interface {
	*T
	GetName() string
	DeepCopy() *T
}](to, from *T, _type MergeType) {
	err := mergeStrategic(to, from, _type)
	if err == nil {
		return
	}
	log.Warningf("unable to merge template '%s': %v; taking the winning side as a whole, what only the other side set is dropped", PT(to).GetName(), err)
	if _type == MergeTypeOverrideByNonEmptyValues {
		*to = *PT(from).DeepCopy()
	}
}

// mergeNamedTemplates pairs templates by name: same-named templates are merged, the rest are appended
func mergeNamedTemplates[T any, PT interface {
	*T
	GetName() string
	DeepCopy() *T
	MergeFrom(*T, MergeType) *T
}](to, from []T, _type MergeType) []T {
	for i := range from {
		name := PT(&from[i]).GetName()
		merged := false
		for j := range to {
			if PT(&to[j]).GetName() == name {
				PT(&to[j]).MergeFrom(&from[i], _type)
				merged = true
				break
			}
		}
		if !merged {
			to = append(to, *PT(&from[i]).DeepCopy())
		}
	}
	return to
}

// NewTemplates creates new Templates object
func NewTemplates() *Templates {
	return new(Templates)
}

func (templates *Templates) GetHostTemplates() []HostTemplate {
	if templates == nil {
		return nil
	}
	return templates.HostTemplates
}

func (templates *Templates) GetPodTemplates() []PodTemplate {
	if templates == nil {
		return nil
	}
	return templates.PodTemplates
}

func (templates *Templates) GetVolumeClaimTemplates() []VolumeClaimTemplate {
	if templates == nil {
		return nil
	}
	return templates.VolumeClaimTemplates
}

func (templates *Templates) GetServiceTemplates() []ServiceTemplate {
	if templates == nil {
		return nil
	}
	return templates.ServiceTemplates
}

// Len returns accumulated len of all templates
func (templates *Templates) Len() int {
	if templates == nil {
		return 0
	}

	return 0 +
		len(templates.HostTemplates) +
		len(templates.PodTemplates) +
		len(templates.VolumeClaimTemplates) +
		len(templates.ServiceTemplates)
}

// MergeFrom merges from specified object
func (templates *Templates) MergeFrom(_from any, _type MergeType) *Templates {
	// Typed from
	var from *Templates

	// Ensure type
	switch typed := _from.(type) {
	case *Templates:
		from = typed
	default:
		return templates
	}

	// Sanity check

	if from.Len() == 0 {
		return templates
	}

	if templates == nil {
		templates = NewTemplates()
	}

	// Merge sections

	templates.HostTemplates = mergeNamedTemplates(templates.HostTemplates, from.HostTemplates, _type)
	templates.PodTemplates = mergeNamedTemplates(templates.PodTemplates, from.PodTemplates, _type)
	templates.VolumeClaimTemplates = mergeNamedTemplates(templates.VolumeClaimTemplates, from.VolumeClaimTemplates, _type)
	templates.ServiceTemplates = mergeNamedTemplates(templates.ServiceTemplates, from.ServiceTemplates, _type)

	return templates
}

// GetHostTemplatesIndex returns index of host templates
func (templates *Templates) GetHostTemplatesIndex() *HostTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.HostTemplatesIndex
}

// EnsureHostTemplatesIndex ensures index exists
func (templates *Templates) EnsureHostTemplatesIndex() *HostTemplatesIndex {
	if templates == nil {
		return nil
	}
	if templates.HostTemplatesIndex != nil {
		return templates.HostTemplatesIndex
	}
	templates.HostTemplatesIndex = NewHostTemplatesIndex()
	return templates.HostTemplatesIndex
}

// GetPodTemplatesIndex returns index of pod templates
func (templates *Templates) GetPodTemplatesIndex() *PodTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.PodTemplatesIndex
}

// EnsurePodTemplatesIndex ensures index exists
func (templates *Templates) EnsurePodTemplatesIndex() *PodTemplatesIndex {
	if templates == nil {
		return nil
	}
	if templates.PodTemplatesIndex != nil {
		return templates.PodTemplatesIndex
	}
	templates.PodTemplatesIndex = NewPodTemplatesIndex()
	return templates.PodTemplatesIndex
}

// GetVolumeClaimTemplatesIndex returns index of VolumeClaim templates
func (templates *Templates) GetVolumeClaimTemplatesIndex() *VolumeClaimTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.VolumeClaimTemplatesIndex
}

// EnsureVolumeClaimTemplatesIndex ensures index exists
func (templates *Templates) EnsureVolumeClaimTemplatesIndex() *VolumeClaimTemplatesIndex {
	if templates == nil {
		return nil
	}
	if templates.VolumeClaimTemplatesIndex != nil {
		return templates.VolumeClaimTemplatesIndex
	}
	templates.VolumeClaimTemplatesIndex = NewVolumeClaimTemplatesIndex()
	return templates.VolumeClaimTemplatesIndex
}

// GetServiceTemplatesIndex returns index of Service templates
func (templates *Templates) GetServiceTemplatesIndex() *ServiceTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.ServiceTemplatesIndex
}

// EnsureServiceTemplatesIndex ensures index exists
func (templates *Templates) EnsureServiceTemplatesIndex() *ServiceTemplatesIndex {
	if templates == nil {
		return nil
	}
	if templates.ServiceTemplatesIndex != nil {
		return templates.ServiceTemplatesIndex
	}
	templates.ServiceTemplatesIndex = NewServiceTemplatesIndex()
	return templates.ServiceTemplatesIndex
}
