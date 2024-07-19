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

import core "k8s.io/api/core/v1"

// ComparableAttributes specifies CHI attributes that are comparable
type ComparableAttributes struct {
	AdditionalEnvVars      []core.EnvVar      `json:"-" yaml:"-"`
	AdditionalVolumes      []core.Volume      `json:"-" yaml:"-"`
	AdditionalVolumeMounts []core.VolumeMount `json:"-" yaml:"-"`
	SkipOwnerRef           bool               `json:"-" yaml:"-"`
}

func (a *ComparableAttributes) GetAdditionalEnvVars() []core.EnvVar {
	if a == nil {
		return nil
	}
	return a.AdditionalEnvVars
}

func (a *ComparableAttributes) AppendAdditionalEnvVars(envVars ...core.EnvVar) {
	if a == nil {
		return
	}
	a.AdditionalEnvVars = append(a.AdditionalEnvVars, envVars...)
}

func (a *ComparableAttributes) GetAdditionalVolumes() []core.Volume {
	if a == nil {
		return nil
	}
	return a.AdditionalVolumes
}

func (a *ComparableAttributes) AppendAdditionalVolume(volume core.Volume) {
	if a == nil {
		return
	}
	a.AdditionalVolumes = append(a.AdditionalVolumes, volume)
}

func (a *ComparableAttributes) AppendAdditionalVolumeIfNotExists(volume core.Volume) {
	if a == nil {
		return
	}

	// Sanity check
	if volume.Name == "" {
		// This volume is incorrect
		return
	}

	for _, existingVolume := range a.GetAdditionalVolumes() {
		if existingVolume.Name == volume.Name {
			// Such a volume already exists
			return
		}
	}

	// Volume looks good
	a.AppendAdditionalVolume(volume)
}

func (a *ComparableAttributes) GetAdditionalVolumeMounts() []core.VolumeMount {
	if a == nil {
		return nil
	}
	return a.AdditionalVolumeMounts
}

func (a *ComparableAttributes) AppendAdditionalVolumeMounts(volumeMounts ...core.VolumeMount) {
	if a == nil {
		return
	}
	a.AdditionalVolumeMounts = append(a.AdditionalVolumeMounts, volumeMounts...)
}

func (a *ComparableAttributes) GetSkipOwnerRef() bool {
	if a == nil {
		return false
	}
	return a.SkipOwnerRef
}

func (a *ComparableAttributes) SetSkipOwnerRef(skip bool) {
	if a == nil {
		return
	}
	a.SkipOwnerRef = skip
}
