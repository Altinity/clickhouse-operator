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

import "k8s.io/api/core/v1"

func (templates *ChiTemplates) MergeFrom(from *ChiTemplates) {
	if from == nil {
		return
	}

	if len(from.PodTemplates) > 0 {
		// We have templates to copy from
		// Append PodTemplates from `from` to receiver
		if templates.PodTemplates == nil {
			templates.PodTemplates = make([]v1.PodTemplateSpec, 0)
		}
		// Loop over all 'from' templates and copy it in case no such template in receiver
		for fromIndex := range from.PodTemplates {
			fromPodTemplate := &from.PodTemplates[fromIndex]

			// Try to find equal entry among local templates in receiver
			equalFound := false
			for toIndex := range templates.PodTemplates {
				toPodTemplate := &templates.PodTemplates[toIndex]
				if toPodTemplate.Name == fromPodTemplate.Name {
					// Receiver already have such a template
					equalFound = true
					break
				}
			}

			if !equalFound {
				// Receiver has not such template
				// Append template from `from`
				templates.PodTemplates = append(templates.PodTemplates, *fromPodTemplate.DeepCopy())
			}
		}
	}

	if len(from.VolumeClaimTemplates) > 0 {
		// We have templates to copy from
		// Append VolumeClaimTemplates from `from` to receiver
		if templates.VolumeClaimTemplates == nil {
			templates.VolumeClaimTemplates = make([]v1.PersistentVolumeClaim, 0)
		}
		// Loop over all 'from' templates and copy it in case no such template in receiver
		for fromIndex := range from.VolumeClaimTemplates {
			fromVolumeClaimTemplate := &from.VolumeClaimTemplates[fromIndex]

			// Try to find equal entry among local templates in receiver
			equalFound := false
			for toIndex := range templates.VolumeClaimTemplates {
				toVolumeClaimTemplate := &templates.VolumeClaimTemplates[toIndex]
				if toVolumeClaimTemplate.Name == fromVolumeClaimTemplate.Name {
					// Received already have such a node
					equalFound = true
					break
				}
			}

			if !equalFound {
				// Receiver has not such template
				// Append Node from `from`
				templates.VolumeClaimTemplates = append(templates.VolumeClaimTemplates, *fromVolumeClaimTemplate.DeepCopy())
			}
		}
	}
}
