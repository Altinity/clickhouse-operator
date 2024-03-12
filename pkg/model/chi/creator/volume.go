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

package creator

import (
	core "k8s.io/api/core/v1"

	api "github.com/altinity/clickhouse-operator/pkg/apis/clickhouse.altinity.com/v1"
)

// newVolumeForPVC returns core.Volume object with defined name
func newVolumeForPVC(name, claimName string) core.Volume {
	return core.Volume{
		Name: name,
		VolumeSource: core.VolumeSource{
			PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
				ClaimName: claimName,
				ReadOnly:  false,
			},
		},
	}
}

// newVolumeForConfigMap returns core.Volume object with defined name
func newVolumeForConfigMap(name string) core.Volume {
	var defaultMode int32 = 0644
	return core.Volume{
		Name: name,
		VolumeSource: core.VolumeSource{
			ConfigMap: &core.ConfigMapVolumeSource{
				LocalObjectReference: core.LocalObjectReference{
					Name: name,
				},
				DefaultMode: &defaultMode,
			},
		},
	}
}

// newVolumeMount returns core.VolumeMount object with name and mount path
func newVolumeMount(name, mountPath string) core.VolumeMount {
	return core.VolumeMount{
		Name:      name,
		MountPath: mountPath,
	}
}

func (c *Creator) getVolumeClaimTemplate(volumeMount *core.VolumeMount) (*api.ChiVolumeClaimTemplate, bool) {
	volumeClaimTemplateName := volumeMount.Name
	volumeClaimTemplate, ok := c.chi.GetVolumeClaimTemplate(volumeClaimTemplateName)
	// Sometimes it is impossible to find VolumeClaimTemplate related to specified volumeMount.
	// May be this volumeMount is not created from VolumeClaimTemplate, it may be a reference to a ConfigMap
	return volumeClaimTemplate, ok
}
