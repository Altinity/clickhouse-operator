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
	"fmt"
	"path"
	"slices"
	"strings"

	docker "github.com/novln/docker-parser"
	core "k8s.io/api/core/v1"

	"github.com/altinity/clickhouse-operator/pkg/apis/common/types"
)

// PodSpecAddContainer adds container to PodSpec
func PodSpecAddContainer(podSpec *core.PodSpec, container core.Container) {
	podSpec.Containers = append(podSpec.Containers, container)
}

// PodSpecContainerGet gets container from the PodSpec either by name or by index
func PodSpecContainerGet(podSpec *core.PodSpec, namesOrIndexes ...any) (*core.Container, bool) {
	for _, nameOrIndex := range namesOrIndexes {
		switch typed := nameOrIndex.(type) {
		// Find by name
		case string:
			name := typed
			// Search for container name
			if len(name) > 0 {
				for i := range podSpec.Containers {
					// Convenience wrapper
					container := &podSpec.Containers[i]
					if container.Name == name {
						return container, true
					}
				}
			}
		// Find by index
		case int:
			index := typed
			if (0 <= index) && (index < len(podSpec.Containers)) {
				// Existing index, get specified container
				return &podSpec.Containers[index], true
			}
		}
	}

	return nil, false
}

// PodSpecContainerGetOrFirstOther gets the container of the PodSpec of the name, else the first container named none of
// the others
func PodSpecContainerGetOrFirstOther(podSpec *core.PodSpec, name string, others ...string) (*core.Container, bool) {
	if container, ok := PodSpecContainerGet(podSpec, name); ok {
		return container, true
	}
	for i := range podSpec.Containers {
		if container := &podSpec.Containers[i]; !slices.Contains(others, container.Name) {
			return container, true
		}
	}
	return nil, false
}

// NewEnvVarFromSecret builds an env var that takes its value from the key of a Secret
func NewEnvVarFromSecret(name, secretName, key string) core.EnvVar {
	return core.EnvVar{
		Name: name,
		ValueFrom: &core.EnvVarSource{
			SecretKeyRef: &core.SecretKeySelector{
				LocalObjectReference: core.LocalObjectReference{
					Name: secretName,
				},
				Key: key,
			},
		},
	}
}

// NewEnvVarFromLabel builds an env var that takes its value from a label of the pod, empty when the
// pod does not carry the label
func NewEnvVarFromLabel(name, label string) core.EnvVar {
	return core.EnvVar{
		Name: name,
		ValueFrom: &core.EnvVarSource{
			FieldRef: &core.ObjectFieldSelector{
				FieldPath: fmt.Sprintf("metadata.labels['%s']", label),
			},
		},
	}
}

func ContainerWalkVolumeMounts(container *core.Container, f func(volumeMount *core.VolumeMount)) {
	for i := range container.VolumeMounts {
		// Convenience wrapper
		volumeMount := &container.VolumeMounts[i]
		f(volumeMount)
	}
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
	// Sanity checks
	if container == nil {
		return
	}

	// VolumeMount has to be valid
	if !VolumeMountIsValid(volumeMount) {
		return
	}

	unableToAppend := false

	// Check that:
	// 1. Mountable item (VolumeClaimTemplate or Volume) specified in VolumeMount to add is NOT already mounted
	//    in this container by any other VolumeMount (to avoid double-mount of a mountable item)
	// 2. And specified `mountPath` (say '/var/lib/clickhouse') is NOT already mounted in this container
	//    by any VolumeMount (to avoid double-mount/rewrite into single `mountPath`)
	ContainerWalkVolumeMounts(container, func(existingVolumeMount *core.VolumeMount) {
		// 1. Check whether this mountable item is already listed in VolumeMount of this container
		if volumeMount.Name == existingVolumeMount.Name {
			// This .templates.VolumeClaimTemplate is already used in VolumeMount
			unableToAppend = true
		}

		// 2. Check whether `mountPath` (say '/var/lib/clickhouse') is already mounted
		if volumeMount.MountPath == existingVolumeMount.MountPath {
			// `mountPath` (say /var/lib/clickhouse) is already used as mount point
			unableToAppend = true
		}
	})

	if unableToAppend {
		// VolumeMount is not good to be added into container
		return
	}

	// Add VolumeMount into container to `mountPath` point
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

// ImageGetBaseName gets the image name without its registry, organization and tag or digest - the
// part that stays the same across mirrors and forks of one image.
func ImageGetBaseName(image string) (string, bool) {
	parts, err := docker.Parse(image)
	if err != nil {
		return "", false
	}
	return path.Base(parts.ShortName()), true
}

// EscapeVariableReferences returns s with every "$" doubled. Kubelet expands $(VAR_NAME) references in the command
// and args of a container, and in the command of an exec probe, and reduces "$$" to "$", so a script placed there
// must be escaped to reach the container as written.
func EscapeVariableReferences(s string) string {
	return strings.ReplaceAll(s, "$", "$$")
}

// EscapeVariableReferencesAll escapes every element of a container's command or args, see EscapeVariableReferences.
func EscapeVariableReferencesAll(args []string) []string {
	escaped := make([]string, 0, len(args))
	for _, arg := range args {
		escaped = append(escaped, EscapeVariableReferences(arg))
	}
	return escaped
}

// ExpandVariableReferences expands s as kubelet expands the command and args of a container: "$$" becomes "$", a
// $(VAR_NAME) reference becomes the value lookup finds for it and is left as it is when lookup finds none, and any
// other "$" stays. It tells what a container, or an exec probe, receives of a command EscapeVariableReferences built.
func ExpandVariableReferences(s string, lookup func(name string) (string, bool)) string {
	var expanded strings.Builder
	for i := 0; i < len(s); i++ {
		if (s[i] != '$') || (i+1 == len(s)) {
			expanded.WriteByte(s[i])
			continue
		}
		switch s[i+1] {
		case '$':
			expanded.WriteByte('$')
			i++
		case '(':
			end := strings.IndexByte(s[i+2:], ')')
			if end < 0 {
				expanded.WriteString("$(")
				i++
				continue
			}
			name := s[i+2 : i+2+end]
			if value, ok := lookup(name); ok {
				expanded.WriteString(value)
			} else {
				expanded.WriteString("$(" + name + ")")
			}
			i += 2 + end
		default:
			expanded.WriteByte('$')
		}
	}
	return expanded.String()
}
