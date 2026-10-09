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

package k8s

import core "k8s.io/api/core/v1"

// CreateVolumeForPVC returns core.Volume object with specified name
func CreateVolumeForPVC(volumeName, pvcName string) core.Volume {
	return core.Volume{
		Name: volumeName,
		VolumeSource: core.VolumeSource{
			PersistentVolumeClaim: &core.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
				ReadOnly:  false,
			},
		},
	}
}

// CreateVolumeForConfigMap returns core.Volume object with defined name
func CreateVolumeForConfigMap(volumeName string) core.Volume {
	var defaultMode int32 = 0644
	return core.Volume{
		Name: volumeName,
		VolumeSource: core.VolumeSource{
			ConfigMap: &core.ConfigMapVolumeSource{
				LocalObjectReference: core.LocalObjectReference{
					Name: volumeName,
				},
				DefaultMode: &defaultMode,
			},
		},
	}
}

// CreateProjectedConfigAndSecretVolume mounts a ConfigMap and one Secret key into
// the same directory. The Secret key is not mounted via subPath, so kubelet
// refreshes the file when the Secret changes. The volume spec names the Secret
// and does not include its contents, so a value change does not change the Pod template.
func CreateProjectedConfigAndSecretVolume(volumeName, configMapName, secretName, secretFile string) core.Volume {
	var defaultMode int32 = 0644
	return core.Volume{
		Name: volumeName,
		VolumeSource: core.VolumeSource{
			Projected: &core.ProjectedVolumeSource{
				DefaultMode: &defaultMode,
				Sources: []core.VolumeProjection{
					{
						ConfigMap: &core.ConfigMapProjection{
							LocalObjectReference: core.LocalObjectReference{
								Name: configMapName,
							},
						},
					},
					{
						Secret: &core.SecretProjection{
							LocalObjectReference: core.LocalObjectReference{
								Name: secretName,
							},
							Items: []core.KeyToPath{
								{
									Key:  secretFile,
									Path: secretFile,
								},
							},
						},
					},
				},
			},
		},
	}
}

// CreateVolumeMount returns core.VolumeMount object with name and mount path
func CreateVolumeMount(name, mountPath string) core.VolumeMount {
	return core.VolumeMount{
		Name:      name,
		MountPath: mountPath,
	}
}
