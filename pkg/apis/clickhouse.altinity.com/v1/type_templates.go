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

func (templates *ChiTemplates) MergeFrom(from *ChiTemplates) {
	if from == nil {
		return
	}

	if len(from.PodTemplates) > 0 {
		// Append PodTemplates from `from`
		//if len(templates.PodTemplates) == 0 {
		//	templates.PodTemplates = make([]ChiPodTemplate, 0)
		//}
		for fromIndex := range from.PodTemplates {
			fromPodTemplate := &from.PodTemplates[fromIndex]

			// Try to find equal entry
			equalFound := false
			for toIndex := range templates.PodTemplates {
				toPodTemplate := &templates.PodTemplates[toIndex]
				if toPodTemplate.Name == fromPodTemplate.Name {
					// Received already have such a node
					equalFound = true
					break
				}
			}

			if !equalFound {
				// Append Node from `from`
				templates.PodTemplates = append(templates.PodTemplates, *fromPodTemplate.DeepCopy())
			}
		}
	}

	if len(from.VolumeClaimTemplates) > 0 {
		// Append VolumeClaimTemplates from `from`
		//if len(templates.VolumeClaimTemplates) == 0 {
		//	templates.VolumeClaimTemplates = make([]ChiVolumeClaimTemplate, 0)
		//}
		for fromIndex := range from.VolumeClaimTemplates {
			fromVolumeClaimTemplate := &from.VolumeClaimTemplates[fromIndex]

			// Try to find equal entry
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
				// Append Node from `from`
				templates.VolumeClaimTemplates = append(templates.VolumeClaimTemplates, *fromVolumeClaimTemplate.DeepCopy())
			}
		}
	}
}
