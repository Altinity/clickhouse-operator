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

// SecretProjection is one Secret key projected next to ConfigMap files.
type SecretProjection struct {
	Secret string
	Key    string
	Path   string
}

// CreateConfigVolume returns the ConfigMap volume, or a projected volume that
// also contains Secret keys, when files is non-empty. Projected (not subPath)
// so the kubelet refreshes Secret contents without recreating the pod.
func CreateConfigVolume(volumeName string, files []SecretProjection) core.Volume {
	if len(files) == 0 {
		return CreateVolumeForConfigMap(volumeName)
	}
	var defaultMode int32 = 0644
	sources := []core.VolumeProjection{
		{
			ConfigMap: &core.ConfigMapProjection{
				LocalObjectReference: core.LocalObjectReference{Name: volumeName},
			},
		},
	}
	itemsBySecret := map[string][]core.KeyToPath{}
	var secrets []string
	for _, file := range files {
		if _, ok := itemsBySecret[file.Secret]; !ok {
			secrets = append(secrets, file.Secret)
		}
		itemsBySecret[file.Secret] = append(itemsBySecret[file.Secret], core.KeyToPath{
			Key:  file.Key,
			Path: file.Path,
		})
	}
	for _, secret := range secrets {
		sources = append(sources, core.VolumeProjection{
			Secret: &core.SecretProjection{
				LocalObjectReference: core.LocalObjectReference{Name: secret},
				Items:                itemsBySecret[secret],
			},
		})
	}
	return core.Volume{
		Name: volumeName,
		VolumeSource: core.VolumeSource{
			Projected: &core.ProjectedVolumeSource{
				Sources:     sources,
				DefaultMode: &defaultMode,
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
