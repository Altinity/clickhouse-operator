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
	"github.com/imdario/mergo"
)

// NewChiTemplates creates new ChiTemplates object
func NewChiTemplates() *ChiTemplates {
	return new(ChiTemplates)
}

func (templates *ChiTemplates) GetHostTemplates() []ChiHostTemplate {
	if templates == nil {
		return nil
	}
	return templates.HostTemplates
}

func (templates *ChiTemplates) GetPodTemplates() []ChiPodTemplate {
	if templates == nil {
		return nil
	}
	return templates.PodTemplates
}

func (templates *ChiTemplates) GetVolumeClaimTemplates() []VolumeClaimTemplate {
	if templates == nil {
		return nil
	}
	return templates.VolumeClaimTemplates
}

func (templates *ChiTemplates) GetServiceTemplates() []ServiceTemplate {
	if templates == nil {
		return nil
	}
	return templates.ServiceTemplates
}

// Len returns accumulated len of all templates
func (templates *ChiTemplates) Len() int {
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
func (templates *ChiTemplates) MergeFrom(_from any, _type MergeType) *ChiTemplates {
	// Typed from
	var from *ChiTemplates

	// Ensure type
	switch typed := _from.(type) {
	case *ChiTemplates:
		from = typed
	default:
		return templates
	}

	// Sanity check

	if from.Len() == 0 {
		return templates
	}

	if templates == nil {
		templates = NewChiTemplates()
	}

	// Merge sections

	templates.mergeHostTemplates(from)
	templates.mergePodTemplates(from)
	templates.mergeVolumeClaimTemplates(from)
	templates.mergeServiceTemplates(from)

	return templates
}

// mergeHostTemplates merges host templates section
func (templates *ChiTemplates) mergeHostTemplates(from *ChiTemplates) {
	if len(from.HostTemplates) == 0 {
		return
	}

	// We have templates to merge from
	// Loop over all 'from' templates and either copy it in case no such template in receiver or merge it
	for fromIndex := range from.HostTemplates {
		fromTemplate := &from.HostTemplates[fromIndex]

		// Try to find entry with the same name among local templates in receiver
		sameNameFound := false
		for toIndex := range templates.HostTemplates {
			toTemplate := &templates.HostTemplates[toIndex]
			if toTemplate.Name == fromTemplate.Name {
				// Receiver already have such a template
				sameNameFound = true
				// Merge `to` template with `from` template
				_ = mergo.Merge(toTemplate, *fromTemplate, mergo.WithSliceDeepMerge)
				// Receiver `to` template is processed
				break
			}
		}

		if !sameNameFound {
			// Receiver does not have template with such a name
			// Append template from `from`
			templates.HostTemplates = append(templates.HostTemplates, *fromTemplate.DeepCopy())
		}
	}
}

// mergePodTemplates merges pod templates section
func (templates *ChiTemplates) mergePodTemplates(from *ChiTemplates) {
	if len(from.PodTemplates) == 0 {
		return
	}

	// We have templates to merge from
	// Loop over all 'from' templates and either copy it in case no such template in receiver or merge it
	for fromIndex := range from.PodTemplates {
		fromTemplate := &from.PodTemplates[fromIndex]

		// Try to find entry with the same name among local templates in receiver
		sameNameFound := false
		for toIndex := range templates.PodTemplates {
			toTemplate := &templates.PodTemplates[toIndex]
			if toTemplate.Name == fromTemplate.Name {
				// Receiver already have such a template
				sameNameFound = true

				//toSpec := &toTemplate.Spec
				//fromSpec := &fromTemplate.Spec
				//_ = mergo.Merge(toSpec, *fromSpec, mergo.WithGrowSlice, mergo.WithOverride, mergo.WithOverrideEmptySlice)

				// Merge `to` template with `from` template
				_ = mergo.Merge(toTemplate, *fromTemplate, mergo.WithSliceDeepMerge)
				// Receiver `to` template is processed
				break
			}
		}

		if !sameNameFound {
			// Receiver does not have template with such a name
			// Append template from `from`
			templates.PodTemplates = append(templates.PodTemplates, *fromTemplate.DeepCopy())
		}
	}
}

// mergeVolumeClaimTemplates merges volume claim templates section
func (templates *ChiTemplates) mergeVolumeClaimTemplates(from *ChiTemplates) {
	if len(from.VolumeClaimTemplates) == 0 {
		return
	}

	// We have templates to merge from
	// Loop over all 'from' templates and either copy it in case no such template in receiver or merge it
	for fromIndex := range from.VolumeClaimTemplates {
		fromTemplate := &from.VolumeClaimTemplates[fromIndex]

		// Try to find entry with the same name among local templates in receiver
		sameNameFound := false
		for toIndex := range templates.VolumeClaimTemplates {
			toTemplate := &templates.VolumeClaimTemplates[toIndex]
			if toTemplate.Name == fromTemplate.Name {
				// Receiver already have such a template
				sameNameFound = true
				// Merge `to` template with `from` template
				_ = mergo.Merge(toTemplate, *fromTemplate, mergo.WithSliceDeepMerge)
				// Receiver `to` template is processed
				break
			}
		}

		if !sameNameFound {
			// Receiver does not have template with such a name
			// Append template from `from`
			templates.VolumeClaimTemplates = append(templates.VolumeClaimTemplates, *fromTemplate.DeepCopy())
		}
	}
}

// mergeServiceTemplates merges service templates section
func (templates *ChiTemplates) mergeServiceTemplates(from *ChiTemplates) {
	if len(from.ServiceTemplates) == 0 {
		return
	}

	// We have templates to merge from
	// Loop over all 'from' templates and either copy it in case no such template in receiver or merge it
	for fromIndex := range from.ServiceTemplates {
		fromTemplate := &from.ServiceTemplates[fromIndex]

		// Try to find entry with the same name among local templates in receiver
		sameNameFound := false
		for toIndex := range templates.ServiceTemplates {
			toTemplate := &templates.ServiceTemplates[toIndex]
			if toTemplate.Name == fromTemplate.Name {
				// Receiver already have such a template
				sameNameFound = true
				// Merge `to` template with `from` template
				_ = mergo.Merge(toTemplate, *fromTemplate, mergo.WithSliceDeepCopy)
				// Receiver `to` template is processed
				break
			}
		}

		if !sameNameFound {
			// Receiver does not have template with such a name
			// Append template from `from`
			templates.ServiceTemplates = append(templates.ServiceTemplates, *fromTemplate.DeepCopy())
		}
	}
}

// GetHostTemplatesIndex returns index of host templates
func (templates *ChiTemplates) GetHostTemplatesIndex() *HostTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.HostTemplatesIndex
}

// EnsureHostTemplatesIndex ensures index exists
func (templates *ChiTemplates) EnsureHostTemplatesIndex() *HostTemplatesIndex {
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
func (templates *ChiTemplates) GetPodTemplatesIndex() *PodTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.PodTemplatesIndex
}

// EnsurePodTemplatesIndex ensures index exists
func (templates *ChiTemplates) EnsurePodTemplatesIndex() *PodTemplatesIndex {
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
func (templates *ChiTemplates) GetVolumeClaimTemplatesIndex() *VolumeClaimTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.VolumeClaimTemplatesIndex
}

// EnsureVolumeClaimTemplatesIndex ensures index exists
func (templates *ChiTemplates) EnsureVolumeClaimTemplatesIndex() *VolumeClaimTemplatesIndex {
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
func (templates *ChiTemplates) GetServiceTemplatesIndex() *ServiceTemplatesIndex {
	if templates == nil {
		return nil
	}
	return templates.ServiceTemplatesIndex
}

// EnsureServiceTemplatesIndex ensures index exists
func (templates *ChiTemplates) EnsureServiceTemplatesIndex() *ServiceTemplatesIndex {
	if templates == nil {
		return nil
	}
	if templates.ServiceTemplatesIndex != nil {
		return templates.ServiceTemplatesIndex
	}
	templates.ServiceTemplatesIndex = NewServiceTemplatesIndex()
	return templates.ServiceTemplatesIndex
}
