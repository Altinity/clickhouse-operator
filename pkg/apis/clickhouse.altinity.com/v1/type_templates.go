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
	// log "k8s.io/klog"

	"github.com/imdario/mergo"
)

func (templates *ChiTemplates) MergeFrom(from *ChiTemplates, _type MergeType) {
	if from == nil {
		return
	}

	if len(from.HostTemplates) > 0 {
		// We have templates to copy from
		// Append HostTemplates from `from` to receiver
		if templates.HostTemplates == nil {
			templates.HostTemplates = make([]ChiHostTemplate, 0)
		}
		// Loop over all 'from' templates and copy it in case no such template in receiver
		for fromIndex := range from.HostTemplates {
			fromTemplate := &from.HostTemplates[fromIndex]

			// Try to find entry with the same name among local templates in receiver
			sameNameFound := false
			for toIndex := range templates.HostTemplates {
				toTemplate := &templates.HostTemplates[toIndex]
				if toTemplate.Name == fromTemplate.Name {
					// Receiver already have such a template
					sameNameFound = true
					// Override `to` template with `from` template
					//templates.PodTemplates[toIndex] = *fromTemplate.DeepCopy()
					if err := mergo.Merge(toTemplate, *fromTemplate, mergo.WithOverride); err != nil {
						log.V(1).Infof("ERROR merge template(%s): %v", toTemplate.Name, err)
					}
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

	if len(from.PodTemplates) > 0 {
		// We have templates to copy from
		// Append PodTemplates from `from` to receiver
		if templates.PodTemplates == nil {
			templates.PodTemplates = make([]ChiPodTemplate, 0)
		}
		// Loop over all 'from' templates and copy it in case no such template in receiver
		for fromIndex := range from.PodTemplates {
			fromTemplate := &from.PodTemplates[fromIndex]

			// Try to find entry with the same name among local templates in receiver
			sameNameFound := false
			for toIndex := range templates.PodTemplates {
				toTemplate := &templates.PodTemplates[toIndex]
				if toTemplate.Name == fromTemplate.Name {
					// Receiver already have such a template
					sameNameFound = true
					// Override `to` template with `from` template
					//templates.PodTemplates[toIndex] = *fromTemplate.DeepCopy()
					if err := mergo.Merge(toTemplate, *fromTemplate, mergo.WithOverride); err != nil {
						log.V(1).Infof("ERROR merge template(%s): %v", toTemplate.Name, err)
					}
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

	if len(from.VolumeClaimTemplates) > 0 {
		// We have templates to copy from
		// Append VolumeClaimTemplates from `from` to receiver
		if templates.VolumeClaimTemplates == nil {
			templates.VolumeClaimTemplates = make([]ChiVolumeClaimTemplate, 0)
		}
		// Loop over all 'from' templates and copy it in case no such template in receiver
		for fromIndex := range from.VolumeClaimTemplates {
			fromTemplate := &from.VolumeClaimTemplates[fromIndex]

			// Try to find entry with the same name among local templates in receiver
			sameNameFound := false
			for toIndex := range templates.VolumeClaimTemplates {
				toTemplate := &templates.VolumeClaimTemplates[toIndex]
				if toTemplate.Name == fromTemplate.Name {
					// Receiver already have such a template
					sameNameFound = true
					// Override `to` template with `from` template
					//templates.VolumeClaimTemplates[toIndex] = *fromTemplate.DeepCopy()
					if err := mergo.Merge(toTemplate, *fromTemplate, mergo.WithOverride); err != nil {
						log.V(1).Infof("ERROR merge template(%s): %v", toTemplate.Name, err)
					}
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

	if len(from.ServiceTemplates) > 0 {
		// We have templates to copy from
		// Append ServiceTemplates from `from` to receiver
		if templates.ServiceTemplates == nil {
			templates.ServiceTemplates = make([]ChiServiceTemplate, 0)
		}
		// Loop over all 'from' templates and copy it in case no such template in receiver
		for fromIndex := range from.ServiceTemplates {
			fromTemplate := &from.ServiceTemplates[fromIndex]

			// Try to find entry with the same name among local templates in receiver
			sameNameFound := false
			for toIndex := range templates.ServiceTemplates {
				toTemplate := &templates.ServiceTemplates[toIndex]
				if toTemplate.Name == fromTemplate.Name {
					// Receiver already have such a template
					sameNameFound = true
					// Override `to` template with `from` template
					//templates.ServiceTemplates[toIndex] = *fromTemplate.DeepCopy()
					if err := mergo.Merge(toTemplate, *fromTemplate, mergo.WithOverride); err != nil {
						log.V(1).Infof("ERROR merge template(%s): %v", toTemplate.Name, err)
					}
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
}
