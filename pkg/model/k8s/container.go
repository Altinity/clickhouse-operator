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

import (
	docker "github.com/novln/docker-parser"
	core "k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// PodSpecAddContainer adds container to PodSpec
func PodSpecAddContainer(podSpec *core.PodSpec, container core.Container) {
	podSpec.Containers = append(podSpec.Containers, container)
}

// ContainerAppendVolumeMounts appends multiple VolumeMount(s) to the specified container
func ContainerAppendVolumeMounts(container *core.Container, volumeMounts ...core.VolumeMount) {
	for _, volumeMount := range volumeMounts {
		ContainerAppendVolumeMount(container, volumeMount)
	}
}

// VolumeMountIsValid checks whether VolumeMount is a valid one
func VolumeMountIsValid(volumeMount core.VolumeMount) bool {
	if volumeMount.Name == "" {
		// VolumeMount must have a name
		return false
	}
	if volumeMount.MountPath == "" {
		// VolumeMount must have a mount path
		return false
	}
	return true
}

// ContainerAppendVolumeMount appends one VolumeMount to the specified container
func ContainerAppendVolumeMount(container *core.Container, volumeMount core.VolumeMount) {
	//
	// Sanity checks
	//

	if container == nil {
		return
	}

	// VolumeMount has to be valid
	if !VolumeMountIsValid(volumeMount) {
		return
	}

	// Check that:
	// 1. Mountable item (VolumeClaimTemplate or Volume) specified in this VolumeMount is NOT already mounted
	//    in this container by any other VolumeMount (to avoid double-mount of a mountable item)
	// 2. And specified `mountPath` (say '/var/lib/clickhouse') is NOT already mounted in this container
	//    by any VolumeMount (to avoid double-mount/rewrite into single `mountPath`)
	for i := range container.VolumeMounts {
		// Convenience wrapper
		existingVolumeMount := &container.VolumeMounts[i]

		// 1. Check whether this mountable item is already listed in VolumeMount of this container
		if volumeMount.Name == existingVolumeMount.Name {
			// This .templates.VolumeClaimTemplate is already used in VolumeMount
			return
		}

		// 2. Check whether `mountPath` (say '/var/lib/clickhouse') is already mounted
		if volumeMount.MountPath == existingVolumeMount.MountPath {
			// `mountPath` (say /var/lib/clickhouse) is already mounted
			return
		}
	}

	// Add VolumeMount to ClickHouse container to `mountPath` point
	container.VolumeMounts = append(container.VolumeMounts, volumeMount)
}

// ContainerEnsurePortByName
func ContainerEnsurePortByName(container *core.Container, name string, port int32) {
	if types.IsPortUnassigned(port) {
		return
	}

	// Find port with specified name
	for i := range container.Ports {
		// Convenience wrapper
		existingContainerPort := &container.Ports[i]
		if existingContainerPort.Name == name {
			// Port with specified name found in the container
			// Overwrite existing port spec with the following:
			//   1. No host port would be specified
			//   2. Specify new port value
			existingContainerPort.HostPort = 0
			existingContainerPort.ContainerPort = port
			return
		}
	}

	// Port with specified name is NOT found in the container.
	// Need to append it to the container.
	container.Ports = append(container.Ports, core.ContainerPort{
		Name:          name,
		ContainerPort: port,
	})
}

func ContainerGetImageTag(container *core.Container) (string, bool) {
	if container == nil {
		return "", false
	}
	parts, err := docker.Parse(container.Image)
	if err != nil {
		return "", false
	}
	return parts.Tag(), true
}
