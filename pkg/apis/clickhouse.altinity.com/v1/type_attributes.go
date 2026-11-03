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
	additionalEnvVars      []core.EnvVar      `json:"-" yaml:"-"`
	additionalVolumes      []core.Volume      `json:"-" yaml:"-"`
	additionalVolumeMounts []core.VolumeMount `json:"-" yaml:"-"`
	skipOwnerRef           bool               `json:"-" yaml:"-"`
}

func (a *ComparableAttributes) GetAdditionalEnvVars() []core.EnvVar {
	if a == nil {
		return nil
	}
	return a.additionalEnvVars
}

func (a *ComparableAttributes) AppendAdditionalEnvVar(envVar core.EnvVar) {
	if a == nil {
		return
	}
	a.additionalEnvVars = append(a.additionalEnvVars, envVar)
}

func (a *ComparableAttributes) AppendAdditionalEnvVarIfNotExists(envVar core.EnvVar) {
	if a == nil {
		return
	}

	// Sanity check
	if envVar.Name == "" {
		// This env var is incorrect
		return
	}

	for _, existingEnvVar := range a.GetAdditionalEnvVars() {
		if existingEnvVar.Name == envVar.Name {
			// Such a variable already exists
			return
		}
	}

	a.AppendAdditionalEnvVar(envVar)
}

func (a *ComparableAttributes) GetAdditionalVolumes() []core.Volume {
	if a == nil {
		return nil
	}
	return a.additionalVolumes
}

func (a *ComparableAttributes) AppendAdditionalVolume(volume core.Volume) {
	if a == nil {
		return
	}
	a.additionalVolumes = append(a.additionalVolumes, volume)
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
	return a.additionalVolumeMounts
}

func (a *ComparableAttributes) AppendAdditionalVolumeMount(volumeMount core.VolumeMount) {
	if a == nil {
		return
	}
	a.additionalVolumeMounts = append(a.additionalVolumeMounts, volumeMount)
}

func (a *ComparableAttributes) AppendAdditionalVolumeMountIfNotExists(volumeMount core.VolumeMount) {
	if a == nil {
		return
	}

	// Sanity check
	if volumeMount.Name == "" {
		return
	}

	for _, existingVolumeMount := range a.GetAdditionalVolumeMounts() {
		if existingVolumeMount.Name == volumeMount.Name {
			// Such a volume mount already exists
			return
		}
	}

	a.AppendAdditionalVolumeMount(volumeMount)
}

func (a *ComparableAttributes) GetSkipOwnerRef() bool {
	if a == nil {
		return false
	}
	return a.skipOwnerRef
}

func (a *ComparableAttributes) SetSkipOwnerRef(skip bool) {
	if a == nil {
		return
	}
	a.skipOwnerRef = skip
}
